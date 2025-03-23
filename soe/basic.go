package soe

import (
	"bytes"
	"fmt"

	"github.com/hamba/avro/v2"
)

// Encoder marshals a value into bytes, and wraps the Avro binary payload in an
// SOE frame containing the writer schema fingerprint.
type Encoder struct {
	api    avro.API
	schema avro.Schema
	header []byte
}

func NewEncoder(api avro.API, schema avro.Schema) (*Encoder, error) {
	// Precompute an SOE header
	hdr, err := BuildHeaderForSchema(schema)
	if err != nil {
		return nil, err
	}

	return &Encoder{
		api:    api,
		schema: schema,
		header: hdr,
	}, nil
}

func (e *Encoder) Encode(v any) ([]byte, error) {
	data, err := e.api.Marshal(e.schema, v)
	if err != nil {
		return nil, err
	}
	return append(e.header, data...), nil
}

// Decoder unmarshals SOE-framed records. Default decoding ignores the SOE
// schema fingerprint, strict decoding verifies that it matches the given
// schema.
type Decoder struct {
	api         avro.API
	schema      avro.Schema
	fingerprint []byte
}

func NewDecoder(api avro.API, schema avro.Schema) (*Decoder, error) {
	fingerprint, err := ComputeFingerprint(schema)
	if err != nil {
		return nil, err
	}

	return &Decoder{
		api:         api,
		schema:      schema,
		fingerprint: fingerprint,
	}, nil
}

func (d *Decoder) Decode(data []byte, v any) error {
	_, data, err := ParseHeader(data)
	if err != nil {
		return err
	}
	return d.api.Unmarshal(d.schema, data, v)
}

func (d *Decoder) DecodeStrict(data []byte, v any) error {
	fingerprint, data, err := ParseHeader(data)
	if err != nil {
		return err
	}
	if !bytes.Equal(d.fingerprint, fingerprint) {
		return fmt.Errorf("bad fingerprint %x, expected %x", fingerprint, d.fingerprint)
	}
	return d.api.Unmarshal(d.schema, data, v)
}
