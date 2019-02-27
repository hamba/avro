package container

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"github.com/hamba/avro"
)

const (
	schemaKey = "avro.schema"
	codecKey  = "avro.codec"
)

var magicBytes = [4]byte{'O', 'b', 'j', 1}

// HeaderSchema is the Avro schema of a container file header.
var HeaderSchema = avro.MustParse(`{
	"type": "record", 
	"name": "org.apache.avro.file.Header",
	"fields": [
		{"name": "magic", "type": {"type": "fixed", "name": "Magic", "size": 4}},
		{"name": "meta", "type": {"type": "map", "values": "bytes"}},
		{"name": "sync", "type": {"type": "fixed", "name": "Sync", "size": 16}}
	]
}`)

// Header represents an Avro container file header.
type Header struct {
	Magic [4]byte           `avro:"magic"`
	Meta  map[string][]byte `avro:"meta"`
	Sync  [16]byte          `avro:"sync"`
}

// Decoder reads and decodes Avro values from a container file.
type Decoder struct {
	reader      *avro.Reader
	resetReader *resetReader
	decoder     *avro.Decoder
	sync        [16]byte

	count int64
}

// NewDecoder returns a new decoder that reads from reader r.
func NewDecoder(r io.Reader) (*Decoder, error) {
	reader := avro.NewReader(r, 1024)

	var h Header
	reader.ReadVal(HeaderSchema, &h)
	if reader.Error != nil {
		return nil, fmt.Errorf("file: unexpected error: %v", reader.Error)
	}

	if h.Magic != magicBytes {
		return nil, errors.New("file: invalid avro file")
	}
	schema, err := avro.Parse(string(h.Meta[schemaKey]))
	if err != nil {
		return nil, err
	}

	decReader := &resetReader{}

	// TODO: File Codecs
	// codec, ok := codecs[string(h.Meta[codecKey])]
	//if codec, ok := codecs[string(h.Meta[codecKey])]; !ok {
	//	return nil, fmt.Errorf("file: unknown codec %s", string(h.Meta[codecKey]))
	//}

	return &Decoder{
		reader:      reader,
		resetReader: decReader,
		decoder:     avro.NewDecoderForSchema(schema, decReader),
		sync:        h.Sync,
	}, nil
}

// Decode reads the next Avro encoded value from its input and stores it in the value pointed to by v.
func (d *Decoder) Decode(v interface{}) error {
	if d.count <= 0 {
		count, err := d.readBlock()
		if err != nil {
			return err
		}

		d.count = count
	}

	d.count--

	err := d.decoder.Decode(v)
	if err == io.EOF {
		return nil
	}
	return err
}

func (d *Decoder) readBlock() (int64, error) {
	count := d.reader.ReadLong()
	size := d.reader.ReadLong()

	data := make([]byte, size)
	d.reader.Read(data)
	d.resetReader.Reset(data)

	var sync [16]byte
	d.reader.Read(sync[:])
	if d.sync != sync {
		return count, errors.New("file: invalid block")
	}

	if d.reader.Error == io.EOF {
		return count, nil
	}
	return count, d.reader.Error
}

type Encoder struct {
	writer  *avro.Writer
	buf     *bytes.Buffer
	encoder *avro.Encoder
	sync    [16]byte

	blockLength int
	count       int
}

func NewEncoder(s string, w io.Writer) (*Encoder, error) {
	schema, err := avro.Parse(s)
	if err != nil {
		return nil, err
	}

	writer := avro.NewWriter(w, 512)

	header := Header{
		Magic: magicBytes,
		Meta: map[string][]byte{
			schemaKey: []byte(schema.String()),
		},
	}
	_, err = rand.Read(header.Sync[:])
	if err != nil {
		return nil, err
	}

	writer.WriteVal(HeaderSchema, header)
	if writer.Error != nil {
		return nil, writer.Error
	}

	buf := &bytes.Buffer{}

	return &Encoder{
		writer:      writer,
		buf:         buf,
		encoder:     avro.NewEncoderForSchema(schema, buf),
		sync:        header.Sync,
		blockLength: 100,
	}, nil
}

func (e *Encoder) Encode(v interface{}) error {
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

func (e *Encoder) Close() error {
	if e.count == 0 {
		return nil
	}

	if err := e.writerBlock(); err != nil {
		return err
	}

	return e.writer.Error
}

func (e *Encoder) writerBlock() error {
	e.writer.WriteLong(int64(e.count))
	e.writer.WriteLong(int64(e.buf.Len()))
	e.writer.Write(e.buf.Bytes())
	e.writer.Write(e.sync[:])
	e.count = 0
	e.buf.Reset()
	return e.writer.Flush()
}

type resetReader struct {
	buf  []byte
	head int
	tail int
}

func (r *resetReader) Read(p []byte) (int, error) {
	if r.head == r.tail {
		return 0, io.EOF
	}

	n := copy(p, r.buf)
	r.head += n

	return n, nil
}

func (r *resetReader) Reset(buf []byte) {
	r.buf = buf
	r.head = 0
	r.tail = len(buf)
}
