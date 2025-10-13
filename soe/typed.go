package soe

import (
	"github.com/hamba/avro/v2"
)

// AvroGenerated is implemented by all avrogen-generated Avro types.
type AvroGenerated interface {
	Unmarshal(b []byte) error
	Marshal() ([]byte, error)
	Schema() avro.Schema
}

// GetSchema returns the avro.Schema associated with type T.
func GetSchema[T AvroGenerated]() avro.Schema {
	// The Schema method returns a global schema, so can be called on a nil
	// receiver.
	var val T
	return val.Schema()
}

// TypedCodec marshals/unmarshals AvroGenerated values to/from SOE-framed Avro
// binary payloads. Must be instantiated with a pointer type, e.g.
//
//	c, _ := NewTypedCodec[*MyType]()
//	var val MyType
//	c.Encode(&val)
//	c.Decode(&val)
//
//	var badVal any
//	c.Encode(badVal) // cannot use badVal (variable of type any)...
//
// It is a strongly typed version of soe.Codec.
type TypedCodec[T any] struct {
	codec *Codec
}

// NewTypedCodec creates a new TypedCodec for type T and the default config.
func NewTypedCodec[T AvroGenerated]() (*TypedCodec[T], error) {
	return NewTypedCodecWithAPI[T](avro.DefaultConfig)
}

// NewTypedCodecWithAPI creates a new TypedCodec for type T and an API.
func NewTypedCodecWithAPI[T AvroGenerated](api avro.API) (*TypedCodec[T], error) {
	schema := GetSchema[T]()
	coder, err := NewCodecWithAPI(schema, api)
	if err != nil {
		return nil, err
	}
	return &TypedCodec[T]{
		codec: coder,
	}, nil
}

// Encode marshals a typed value to SOE-encoded Avro binary.
func (c *TypedCodec[T]) Encode(v T) ([]byte, error) {
	return c.codec.Encode(v)
}

// Decode unmarshals a typed value from SOE-encoded Avro binary, and fails if
// the schema fingerprint doesn't match the held schema.
func (c *TypedCodec[T]) Decode(data []byte, v T) error {
	return c.codec.Decode(data, v)
}

// DecodeUnverified unmarshals a typed value from SOE-encoded Avro binary
// without validating the schema fingerprint.
func (c *TypedCodec[T]) DecodeUnverified(data []byte, v T) error {
	return c.codec.DecodeUnverified(data, v)
}
