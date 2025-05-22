// Package ocf implements encoding and decoding of Avro Object Container Files as defined by the Avro specification.
//
// See the Avro specification for an understanding of Avro: http://avro.apache.org/docs/current/
package ocf

import (
	"bytes"
	"compress/flate"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/internal/bytesx"
	"github.com/klauspost/compress/zstd"
)

const (
	schemaKey = "avro.schema"
	codecKey  = "avro.codec"
)

var (
	magicBytes = [4]byte{'O', 'b', 'j', 1}

	// HeaderSchema is the Avro schema of a container file header.
	HeaderSchema = avro.MustParse(`{
	"type": "record",
	"name": "org.apache.avro.file.Header",
	"fields": [
		{"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
		{"name": "meta", "type": {"type": "map", "values": "bytes"}},
		{"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
	]
}`)

	// DefaultSchemaMarshaler calls the schema's String() method, to produce
	// a "canonical" schema.
	DefaultSchemaMarshaler = defaultMarshalSchema
	// FullSchemaMarshaler calls the schema's MarshalJSON() method, to produce
	// a schema with all details preserved. The "canonical" schema returned by
	// the default marshaler does not preserve a type's extra properties.
	FullSchemaMarshaler = fullMarshalSchema
)

// Header represents an Avro container file header.
type Header struct {
	Magic [4]byte           `avro:"magic"`
	Meta  map[string][]byte `avro:"meta"`
	Sync  [16]byte          `avro:"sync"`
}

type decoderConfig struct {
	DecoderConfig avro.API
	SchemaCache   *avro.SchemaCache
	CodecOptions  codecOptions
}

// DecoderFunc represents a configuration function for Decoder.
type DecoderFunc func(cfg *decoderConfig)

// WithDecoderConfig sets the value decoder config on the OCF decoder.
func WithDecoderConfig(wCfg avro.API) DecoderFunc {
	return func(cfg *decoderConfig) {
		cfg.DecoderConfig = wCfg
	}
}

// WithDecoderSchemaCache sets the schema cache for the decoder.
// If not specified, defaults to avro.DefaultSchemaCache.
func WithDecoderSchemaCache(cache *avro.SchemaCache) DecoderFunc {
	return func(cfg *decoderConfig) {
		cfg.SchemaCache = cache
	}
}

// WithZStandardDecoderOptions sets the options for the ZStandard decoder.
func WithZStandardDecoderOptions(opts ...zstd.DOption) DecoderFunc {
	return func(cfg *decoderConfig) {
		cfg.CodecOptions.ZStandardOptions.DOptions = append(cfg.CodecOptions.ZStandardOptions.DOptions, opts...)
	}
}

// Decoder reads and decodes Avro values from a container file.
type Decoder struct {
	reader      *avro.Reader
	resetReader *bytesx.ResetReader
	decoder     *avro.Decoder
	meta        map[string][]byte
	sync        [16]byte
	schema      avro.Schema

	codec Codec

	count int64
}

// NewDecoder returns a new decoder that reads from reader r.
func NewDecoder(r io.Reader, opts ...DecoderFunc) (*Decoder, error) {
	cfg := decoderConfig{
		DecoderConfig: avro.DefaultConfig,
		SchemaCache:   avro.DefaultSchemaCache,
		CodecOptions: codecOptions{
			DeflateCompressionLevel: flate.DefaultCompression,
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	reader := avro.NewReader(r, 1024)

	h, err := readHeader(reader, cfg.SchemaCache, cfg.CodecOptions)
	if err != nil {
		return nil, fmt.Errorf("decoder: %w", err)
	}

	decReader := bytesx.NewResetReader([]byte{})

	return &Decoder{
		reader:      reader,
		resetReader: decReader,
		decoder:     cfg.DecoderConfig.NewDecoder(h.Schema, decReader),
		meta:        h.Meta,
		sync:        h.Sync,
		codec:       h.Codec,
		schema:      h.Schema,
	}, nil
}

// Metadata returns the header metadata.
func (d *Decoder) Metadata() map[string][]byte {
	return d.meta
}

// Schema returns the schema that was parsed from the file's metadata
// and that is used to interpret the file's contents.
func (d *Decoder) Schema() avro.Schema {
	return d.schema
}

// HasNext determines if there is another value to read.
func (d *Decoder) HasNext() bool {
	if d.count <= 0 {
		count := d.readBlock()
		d.count = count
	}

	if d.reader.Error != nil {
		return false
	}

	return d.count > 0
}

// Decode reads the next Avro encoded value from its input and stores it in the value pointed to by v.
func (d *Decoder) Decode(v any) error {
	if d.count <= 0 {
		return errors.New("decoder: no data found, call HasNext first")
	}

	d.count--

	return d.decoder.Decode(v)
}

// Error returns the last reader error.
func (d *Decoder) Error() error {
	if errors.Is(d.reader.Error, io.EOF) {
		return nil
	}

	return d.reader.Error
}

func (d *Decoder) readBlock() int64 {
	_ = d.reader.Peek()
	if errors.Is(d.reader.Error, io.EOF) {
		// There is no next block
		return 0
	}

	count := d.reader.ReadLong()
	size := d.reader.ReadLong()

	// Read the blocks data
	switch {
	case count > 0:
		data := make([]byte, size)
		d.reader.Read(data)

		data, err := d.codec.Decode(data)
		if err != nil {
			d.reader.Error = err
		}

		d.resetReader.Reset(data)

	case size > 0:
		// Skip the block data when count is 0
		data := make([]byte, size)
		d.reader.Read(data)
	}

	// Read the sync.
	var sync [16]byte
	d.reader.Read(sync[:])
	if d.sync != sync && !errors.Is(d.reader.Error, io.EOF) {
		d.reader.Error = errors.New("decoder: invalid block")
	}

	return count
}

type encoderConfig struct {
	BlockLength     int
	BlockSize       int
	CodecName       CodecName
	CodecOptions    codecOptions
	Metadata        map[string][]byte
	Sync            [16]byte
	EncodingConfig  avro.API
	SchemaCache     *avro.SchemaCache
	SchemaMarshaler func(avro.Schema) ([]byte, error)
}

// EncoderFunc represents a configuration function for Encoder.
type EncoderFunc func(cfg *encoderConfig)

// WithBlockLength sets the block length on the encoder.
func WithBlockLength(length int) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.BlockLength = length
	}
}

// WithBlockSize sets the maximum uncompressed size of a buffered block before
// it is written and flushed to the underlying io.Writer (after compression).
func WithBlockSize(size int) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.BlockSize = size
	}
}

// WithCodec sets the compression codec on the encoder.
func WithCodec(codec CodecName) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.CodecName = codec
	}
}

// WithCompressionLevel sets the compression codec to deflate and
// the compression level on the encoder.
func WithCompressionLevel(compLvl int) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.CodecName = Deflate
		cfg.CodecOptions.DeflateCompressionLevel = compLvl
	}
}

// WithZStandardEncoderOptions sets the options for the ZStandard encoder.
func WithZStandardEncoderOptions(opts ...zstd.EOption) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.CodecOptions.ZStandardOptions.EOptions = append(cfg.CodecOptions.ZStandardOptions.EOptions, opts...)
	}
}

// WithMetadata sets the metadata on the encoder header.
func WithMetadata(meta map[string][]byte) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.Metadata = meta
	}
}

// WithMetadataKeyVal sets a single key-value pair for the metadata on
// the encoder header.
func WithMetadataKeyVal(key string, val []byte) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.Metadata[key] = val
	}
}

// WithEncoderSchemaCache sets the schema cache for the encoder.
// If not specified, defaults to avro.DefaultSchemaCache.
func WithEncoderSchemaCache(cache *avro.SchemaCache) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.SchemaCache = cache
	}
}

// WithSchemaMarshaler sets the schema marshaler for the encoder.
// If not specified, defaults to DefaultSchemaMarshaler.
func WithSchemaMarshaler(m func(avro.Schema) ([]byte, error)) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.SchemaMarshaler = m
	}
}

// WithSyncBlock sets the sync block.
func WithSyncBlock(sync [16]byte) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.Sync = sync
	}
}

// WithEncodingConfig sets the value encoder config on the OCF encoder.
func WithEncodingConfig(wCfg avro.API) EncoderFunc {
	return func(cfg *encoderConfig) {
		cfg.EncodingConfig = wCfg
	}
}

// Encoder writes Avro container file to an output stream.
type Encoder struct {
	writer  *avro.Writer
	buf     *bytes.Buffer
	encoder *avro.Encoder
	sync    [16]byte

	codec Codec

	blockLength int
	count       int
	blockSize   int
}

// NewEncoder returns a new encoder that writes to w using schema s.
//
// If the writer is an existing ocf file, it will append data using the
// existing schema.
func NewEncoder(s string, w io.Writer, opts ...EncoderFunc) (*Encoder, error) {
	cfg := computeEncoderConfig(opts)
	schema, err := avro.ParseWithCache(s, "", cfg.SchemaCache)
	if err != nil {
		return nil, err
	}
	return newEncoder(schema, w, cfg)
}

// NewEncoderWithSchema returns a new encoder that writes to w using schema s.
//
// If the writer is an existing ocf file, it will append data using the
// existing schema.
func NewEncoderWithSchema(schema avro.Schema, w io.Writer, opts ...EncoderFunc) (*Encoder, error) {
	return newEncoder(schema, w, computeEncoderConfig(opts))
}

func newEncoder(schema avro.Schema, w io.Writer, cfg encoderConfig) (*Encoder, error) {
	switch file := w.(type) {
	case nil:
		return nil, errors.New("writer cannot be nil")
	case *os.File:
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}

		if info.Size() > 0 {
			reader := avro.NewReader(file, 1024)
			h, err := readHeader(reader, cfg.SchemaCache, cfg.CodecOptions)
			if err != nil {
				return nil, err
			}
			if err = skipToEnd(reader, h.Sync); err != nil {
				return nil, err
			}

			writer := avro.NewWriter(w, 512, avro.WithWriterConfig(cfg.EncodingConfig))
			buf := &bytes.Buffer{}
			e := &Encoder{
				writer:      writer,
				buf:         buf,
				encoder:     cfg.EncodingConfig.NewEncoder(h.Schema, buf),
				sync:        h.Sync,
				codec:       h.Codec,
				blockLength: cfg.BlockLength,
				blockSize:   cfg.BlockSize,
			}
			return e, nil
		}
	}

	schemaJSON, err := cfg.SchemaMarshaler(schema)
	if err != nil {
		return nil, err
	}

	cfg.Metadata[schemaKey] = schemaJSON
	cfg.Metadata[codecKey] = []byte(cfg.CodecName)
	header := Header{
		Magic: magicBytes,
		Meta:  cfg.Metadata,
	}
	header.Sync = cfg.Sync
	if header.Sync == [16]byte{} {
		_, _ = rand.Read(header.Sync[:])
	}

	codec, err := resolveCodec(cfg.CodecName, cfg.CodecOptions)
	if err != nil {
		return nil, err
	}

	writer := avro.NewWriter(w, 512, avro.WithWriterConfig(cfg.EncodingConfig))
	writer.WriteVal(HeaderSchema, header)
	if err = writer.Flush(); err != nil {
		return nil, err
	}

	buf := &bytes.Buffer{}
	e := &Encoder{
		writer:      writer,
		buf:         buf,
		encoder:     cfg.EncodingConfig.NewEncoder(schema, buf),
		sync:        header.Sync,
		codec:       codec,
		blockLength: cfg.BlockLength,
		blockSize:   cfg.BlockSize,
	}
	return e, nil
}

func computeEncoderConfig(opts []EncoderFunc) encoderConfig {
	cfg := encoderConfig{
		BlockLength: 100,
		CodecName:   Null,
		CodecOptions: codecOptions{
			DeflateCompressionLevel: flate.DefaultCompression,
		},
		Metadata:        map[string][]byte{},
		EncodingConfig:  avro.DefaultConfig,
		SchemaCache:     avro.DefaultSchemaCache,
		SchemaMarshaler: DefaultSchemaMarshaler,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// Write v to the internal buffer. This method skips the internal encoder and
// therefore the caller is responsible for encoding the bytes. No error will be
// thrown if the bytes does not conform to the schema given to NewEncoder, but
// the final ocf data will be corrupted.
func (e *Encoder) Write(p []byte) (n int, err error) {
	n, err = e.buf.Write(p)
	if err != nil {
		return n, err
	}

	e.count++
	if e.count >= e.blockLength || (e.blockSize != 0 && e.buf.Len() >= e.blockSize) {
		if err = e.writerBlock(); err != nil {
			return n, err
		}
	}

	return n, e.writer.Error
}

// Encode writes the Avro encoding of v to the stream.
func (e *Encoder) Encode(v any) error {
	if err := e.encoder.Encode(v); err != nil {
		return err
	}

	e.count++
	if e.count >= e.blockLength {
		if err := e.writerBlock(); err != nil {
			return err
		}
	}

	return e.writer.Error
}

// Flush flushes the underlying writer.
func (e *Encoder) Flush() error {
	if e.count == 0 {
		return nil
	}

	if err := e.writerBlock(); err != nil {
		return err
	}

	return e.writer.Error
}

// Close closes the encoder, flushing the writer.
func (e *Encoder) Close() error {
	return e.Flush()
}

func (e *Encoder) writerBlock() error {
	e.writer.WriteLong(int64(e.count))

	b := e.codec.Encode(e.buf.Bytes())

	e.writer.WriteLong(int64(len(b)))
	_, _ = e.writer.Write(b)

	_, _ = e.writer.Write(e.sync[:])

	e.count = 0
	e.buf.Reset()
	return e.writer.Flush()
}

type ocfHeader struct {
	Schema avro.Schema
	Codec  Codec
	Meta   map[string][]byte
	Sync   [16]byte
}

func readHeader(reader *avro.Reader, schemaCache *avro.SchemaCache, codecOpts codecOptions) (*ocfHeader, error) {
	var h Header
	reader.ReadVal(HeaderSchema, &h)
	if reader.Error != nil {
		return nil, fmt.Errorf("unexpected error: %w", reader.Error)
	}

	if h.Magic != magicBytes {
		return nil, errors.New("invalid avro file")
	}
	schema, err := avro.ParseBytesWithCache(h.Meta[schemaKey], "", schemaCache)
	if err != nil {
		return nil, err
	}

	codec, err := resolveCodec(CodecName(h.Meta[codecKey]), codecOpts)
	if err != nil {
		return nil, err
	}

	return &ocfHeader{
		Schema: schema,
		Codec:  codec,
		Meta:   h.Meta,
		Sync:   h.Sync,
	}, nil
}

func skipToEnd(reader *avro.Reader, sync [16]byte) error {
	for {
		_ = reader.ReadLong()
		if errors.Is(reader.Error, io.EOF) {
			return nil
		}
		size := reader.ReadLong()
		reader.SkipNBytes(int(size))
		if reader.Error != nil {
			return reader.Error
		}

		var synMark [16]byte
		reader.Read(synMark[:])
		if sync != synMark && !errors.Is(reader.Error, io.EOF) {
			reader.Error = errors.New("invalid block")
		}
	}
}

func defaultMarshalSchema(schema avro.Schema) ([]byte, error) {
	return []byte(schema.String()), nil
}

func fullMarshalSchema(schema avro.Schema) ([]byte, error) {
	return json.Marshal(schema)
}
