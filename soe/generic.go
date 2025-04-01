package soe

import (
	"github.com/hamba/avro/v2"
)

// GenericEncoder encodes AvroGenerated values into SOE-framed Avro binary
// payloads. Must be instantiated with a pointer type, e.g.
//    enc, _ := NewGenericEncoder[*MyType]()
//    val MyType
//    enc.Encode(val)
type GenericEncoder[T any] struct {
	codec *Codec
}

func NewGenericEncoder[T AvroGenerated](api avro.API) (*GenericEncoder[T], error) {
	schema := GetSchema[T]()
	coder, err := NewCodecWithAPI(schema, api)
	if err != nil {
		return nil, err
	}
	return &GenericEncoder[T]{
		codec: coder,
	}, nil
}

func (e *GenericEncoder[T]) Encode(v T) ([]byte, error) {
	return e.codec.Encode(v)
}

// GenericDecoder decodes SOE-framed records into values whose type implement
// AvroGenerated. Must be instantiated with a pointer type, e.g.
//    dec, _ := NewGenericDecoder[*MyType]()
//    val MyType
//    dec.Decode(data, &val)
type GenericDecoder[T any] struct {
	codec *Codec
}

func NewGenericDecoder[T AvroGenerated](api avro.API) (*GenericDecoder[T], error) {
	schema := GetSchema[T]()
	coder, err := NewCodecWithAPI(schema, api)
	if err != nil {
		return nil, err
	}
	return &GenericDecoder[T]{
		codec: coder,
	}, nil
}

func (d *GenericDecoder[T]) Decode(data []byte, v T) error {
	return d.codec.Decode(data, v)
}

func (d *GenericDecoder[T]) DecodeStrict(data []byte, v T) error {
	return d.codec.DecodeStrict(data, v)
}
