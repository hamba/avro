package avro

import (
	"io"
)

// Encoder writes Avro values to an output stream.
type Encoder struct {
	s Schema
	w *Writer
}

// NewEncoder returns a new encoder that writes to w using schema s.
func NewEncoder(s string, w io.Writer) (*Encoder, error) {
	sch, err := Parse(s)
	if err != nil {
		return nil, err
	}
	return NewEncoderForSchema(sch, w), nil
}

// NewEncoderForSchema returns a new encoder that writes to w using schema.
func NewEncoderForSchema(schema Schema, w io.Writer) *Encoder {
	return DefaultConfig.NewEncoder(schema, w)
}

// Encode writes the Avro encoding of v to the stream.
func (e *Encoder) Encode(v any) error {
	e.w.WriteVal(e.s, v)
	_ = e.w.Flush()
	return e.w.Error
}

// Marshal returns the Avro encoding of v.
func Marshal(schema Schema, v any) ([]byte, error) {
	return DefaultConfig.Marshal(schema, v)
}
