package soe

import (
	"bytes"
	"fmt"

	"github.com/hamba/avro/v2"
)

// Codec marshals values to/from bytes, with the Avro binary payload wrapped in
// an SOE frame containing the writer schema fingerprint.
type Codec struct {
	api    avro.API
	schema avro.Schema
	header []byte
}

// NewCodec creates a new Codec for a Schema and the default config.
func NewCodec(schema avro.Schema) (*Codec, error) {
	return NewCodecWithAPI(schema, avro.DefaultConfig)
}

// NewCodecWithAPI creates a new Codec for a Schema and an API.
func NewCodecWithAPI(schema avro.Schema, api avro.API) (*Codec, error) {
	// Precompute SOE header
	header, err := BuildHeader(schema)
	if err != nil {
		return nil, err
	}
	return &Codec{
		schema: schema,
		api:    api,
		header: header,
	}, nil
}

// Encode marshals a value to SOE-encoded Avro binary.
func (c *Codec) Encode(v any) ([]byte, error) {
	data, err := c.api.Marshal(c.schema, v)
	if err != nil {
		return nil, err
	}
	return append(c.header, data...), nil
}

// Decode unmarshals a value from SOE-encoded Avro binary, and fails if
// the schema fingerprint doesn't match the held schema.
func (c *Codec) Decode(data []byte, v any) error {
	fingerprint, data, err := ParseHeader(data)
	if err != nil {
		return err
	}
	expected := c.getFingerprint()
	if !bytes.Equal(fingerprint, expected) {
		return fmt.Errorf("bad fingerprint %x, expected %x", fingerprint, expected)
	}
	return c.api.Unmarshal(c.schema, data, v)
}

// DecodeUnverified unmarshals a value from SOE-encoded Avro binary without
// validating the schema fingerprint.
func (c *Codec) DecodeUnverified(data []byte, v any) error {
	_, data, err := ParseHeader(data)
	if err != nil {
		return err
	}
	return c.api.Unmarshal(c.schema, data, v)
}

func (c *Codec) getFingerprint() []byte {
	// We know the header is well-formed here, so make assumptions.
	return c.header[2:]
}
