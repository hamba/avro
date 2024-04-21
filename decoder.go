package avro

import (
	"io"
)

// Decoder reads and decodes Avro values from an input stream.
type Decoder struct {
	s Schema
	r *Reader
}

// NewDecoder returns a new decoder that reads from reader r using schema s.
func NewDecoder(s string, r io.Reader) (*Decoder, error) {
	sch, err := Parse(s)
	if err != nil {
		return nil, err
	}

	return NewDecoderForSchema(sch, r), nil
}

// NewDecoderForSchema returns a new decoder that reads from r using schema.
func NewDecoderForSchema(schema Schema, reader io.Reader) *Decoder {
	return DefaultConfig.NewDecoder(schema, reader)
}

// Decode reads the next Avro encoded value from its input and stores it in the value pointed to by v.
func (d *Decoder) Decode(obj any) error {
	if d.r.head == d.r.tail && d.r.reader != nil {
		if !d.r.loadMore() {
			return io.EOF
		}
	}

	d.r.ReadVal(d.s, obj)

	//nolint:errorlint // Only direct EOF errors should be discarded.
	if d.r.Error == io.EOF {
		return nil
	}
	return d.r.Error
}

// Unmarshal parses the Avro encoded data and stores the result in the value pointed to by v.
// If v is nil or not a pointer, Unmarshal returns an error.
func Unmarshal(schema Schema, data []byte, v any) error {
	return DefaultConfig.Unmarshal(schema, data, v)
}
