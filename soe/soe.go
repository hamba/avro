package soe

import (
	"bytes"
	"fmt"

	"github.com/hamba/avro/v2"
)

// https://avro.apache.org/docs/1.10.2/spec.html#single_object_encoding
var Magic = []byte{0xc3, 0x01}

// AvroGenerated is implemented by all avrogen-generated Avro types.
type AvroGenerated interface {
	Unmarshal(b []byte) error
	Marshal() ([]byte, error)
	Schema() avro.Schema
}

func GetSchema[T AvroGenerated]() avro.Schema {
	// The Schema method returns a global schema, so can be called on a nil
	// receiver.
	var val T
	return val.Schema()
}

// ComputeFingerprint returns an SOE-compatible (CRC64, little-endian) schema
// fingerprint.
func ComputeFingerprint(schema avro.Schema) ([]byte, error) {
	return schema.FingerprintUsing(avro.CRC64AvroLE)
}

// ParseHeader validates SOE magic and splits data into fingerprint,rest.
func ParseHeader(data []byte) ([]byte, []byte, error) {
	if len(data) < 10 {
		return nil, nil, fmt.Errorf("data too short: %x", data)
	}
	if !bytes.Equal(data[0:2], Magic) {
		return nil, nil, fmt.Errorf("invalid magic: %x", data[0:2])
	}
	return data[2:10], data[10:], nil
}

// BuildHeader builds an SOE header from a schema's fingerprint.
func BuildHeader(schema avro.Schema) ([]byte, error) {
	fingerprint, err := ComputeFingerprint(schema)
	if err != nil {
		return nil, err
	}
	return BuildHeaderForFingerprint(fingerprint)
}

// BuildHeader builds an SOE header from a fingerprint.
func BuildHeaderForFingerprint(fingerprint []byte) ([]byte, error) {
	if len(fingerprint) != 8 {
		return nil, fmt.Errorf("bad fingerprint length: %d", len(fingerprint))
	}
	return append(Magic, fingerprint...), nil
}
