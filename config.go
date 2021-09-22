package avro

import (
	"errors"
	"io"
	"sync"

	"github.com/modern-go/reflect2"
)

// DefaultConfig is the default API.
var DefaultConfig = Config{}.Freeze()

// Config customises how the codec should behave.
type Config struct {
	// TagKey is the struct tag key used when en/decoding structs.
	// This defaults to "avro".
	TagKey string

	// BlockLength is the length of blocks for maps and arrays.
	// This defaults to 100.
	BlockLength int

	// UnionResolutionError determines if an error will be returned
	// when a type cannot be resolved while decoding a union.
	UnionResolutionError bool
}

// Freeze makes the configuration immutable.
func (c Config) Freeze() API {
	api := &frozenConfig{
		config:   c,
		resolver: NewTypeResolver(),
	}

	api.readerPool = &sync.Pool{
		New: func() interface{} {
			return &Reader{
				cfg:    api,
				reader: nil,
				buf:    nil,
				head:   0,
				tail:   0,
			}
		},
	}
	api.writerPool = &sync.Pool{
		New: func() interface{} {
			return &Writer{
				cfg:   api,
				out:   nil,
				buf:   make([]byte, 0, 512),
				Error: nil,
			}
		},
	}

	return api
}

// API represents a frozen Config.
type API interface {
	// Marshal returns the Avro encoding of v.
	Marshal(schema Schema, v interface{}) ([]byte, error)

	// Unmarshal parses the Avro encoded data and stores the result in the value pointed to by v.
	// If v is nil or not a pointer, Unmarshal returns an error.
	Unmarshal(schema Schema, data []byte, v interface{}) error

	// NewEncoder returns a new encoder that writes to w using schema.
	NewEncoder(schema Schema, w io.Writer) *Encoder

	// NewDecoder returns a new decoder that reads from reader r using schema.
	NewDecoder(schema Schema, r io.Reader) *Decoder

	// DecoderOf returns the value decoder for a given schema and type.
	DecoderOf(schema Schema, typ reflect2.Type) ValDecoder

	// EncoderOf returns the value encoder for a given schema and type.
	EncoderOf(schema Schema, tpy reflect2.Type) ValEncoder

	// Register registers names to their types for resolution. All primitive types are pre-registered.
	Register(name string, obj interface{})
}

type frozenConfig struct {
	config Config

	decoderCache sync.Map // map[cacheKey]ValDecoder
	encoderCache sync.Map // map[cacheKey]ValEncoder

	readerPool *sync.Pool
	writerPool *sync.Pool

	resolver *TypeResolver
}

func (c *frozenConfig) Marshal(schema Schema, v interface{}) ([]byte, error) {
	writer := c.borrowWriter()

	writer.WriteVal(schema, v)
	if err := writer.Error; err != nil {
		c.returnWriter(writer)
		return nil, err
	}

	result := writer.Buffer()
	copied := make([]byte, len(result))
	copy(copied, result)

	c.returnWriter(writer)
	return copied, nil
}

func (c *frozenConfig) borrowWriter() *Writer {
	writer := c.writerPool.Get().(*Writer)
	writer.Reset(nil)
	return writer
}

func (c *frozenConfig) returnWriter(writer *Writer) {
	writer.out = nil
	writer.Error = nil

	c.writerPool.Put(writer)
}

func (c *frozenConfig) Unmarshal(schema Schema, data []byte, v interface{}) error {
	reader := c.borrowReader(data)

	reader.ReadVal(schema, v)
	err := reader.Error
	c.returnReader(reader)

	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

func (c *frozenConfig) borrowReader(data []byte) *Reader {
	reader := c.readerPool.Get().(*Reader)
	reader.Reset(data)
	return reader
}

func (c *frozenConfig) returnReader(reader *Reader) {
	reader.Error = nil
	c.readerPool.Put(reader)
}

func (c *frozenConfig) NewEncoder(schema Schema, w io.Writer) *Encoder {
	writer := NewWriter(w, 512, WithWriterConfig(c))
	return &Encoder{
		s: schema,
		w: writer,
	}
}

func (c *frozenConfig) NewDecoder(schema Schema, r io.Reader) *Decoder {
	reader := NewReader(r, 512, WithReaderConfig(c))
	return &Decoder{
		s: schema,
		r: reader,
	}
}

func (c *frozenConfig) Register(name string, obj interface{}) {
	c.resolver.Register(name, obj)
}

type cacheKey struct {
	fingerprint [32]byte
	rtype       uintptr
}

func (c *frozenConfig) addDecoderToCache(fingerprint [32]byte, rtype uintptr, dec ValDecoder) {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	c.decoderCache.Store(key, dec)
}

func (c *frozenConfig) getDecoderFromCache(fingerprint [32]byte, rtype uintptr) ValDecoder {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	if dec, ok := c.decoderCache.Load(key); ok {
		return dec.(ValDecoder)
	}

	return nil
}

func (c *frozenConfig) addEncoderToCache(fingerprint [32]byte, rtype uintptr, enc ValEncoder) {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	c.encoderCache.Store(key, enc)
}

func (c *frozenConfig) getEncoderFromCache(fingerprint [32]byte, rtype uintptr) ValEncoder {
	key := cacheKey{fingerprint: fingerprint, rtype: rtype}
	if enc, ok := c.encoderCache.Load(key); ok {
		return enc.(ValEncoder)
	}

	return nil
}

func (c *frozenConfig) getTagKey() string {
	tagKey := c.config.TagKey
	if tagKey == "" {
		return "avro"
	}
	return tagKey
}

func (c *frozenConfig) getBlockLength() int {
	blockSize := c.config.BlockLength
	if blockSize <= 0 {
		return 100
	}
	return blockSize
}
