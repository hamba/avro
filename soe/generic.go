package soe

import (
	"github.com/hamba/avro/v2"
)

// GenericCodec marshals/unmarshals AvroGenerated values to/from SOE-framed Avro
// binary payloads. Must be instantiated with a pointer type, e.g.
//
//    c, _ := NewGenericCodec[*MyType]()
//    val MyType
//    c.Encode(val)
//    c.Decode(&val)
//
// It is a strongly typed version of soe.Codec.
type GenericCodec[T any] struct {
	codec *Codec
}

// NewGenericCodec creates a new GenericCodec for type T and the default config.
func NewGenericCodec[T AvroGenerated]() (*GenericCodec[T], error) {
	return NewGenericCodecWithAPI[T](avro.DefaultConfig)
}

// NewGenericCodecWithAPI creates a new GenericCodec for type T and an API.
func NewGenericCodecWithAPI[T AvroGenerated](api avro.API) (*GenericCodec[T], error) {
	schema := GetSchema[T]()
	coder, err := NewCodecWithAPI(schema, api)
	if err != nil {
		return nil, err
	}
	return &GenericCodec[T]{
		codec: coder,
	}, nil
}

// Encode marshals a typed value to SOE-encoded Avro binary.
func (c *GenericCodec[T]) Encode(v T) ([]byte, error) {
	return c.codec.Encode(v)
}

// Decode unmarshals a typed value from SOE-encoded Avro binary.
func (c *GenericCodec[T]) Decode(data []byte, v T) error {
	return c.codec.Decode(data, v)
}

// DecodeStrict unmarshals a typed value from SOE-encoded Avro binary, and fails
// if the schema fingerprint doesn't match the held schema.
func (c *GenericCodec[T]) DecodeStrict(data []byte, v T) error {
	return c.codec.DecodeStrict(data, v)
}
