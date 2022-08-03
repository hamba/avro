package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
)

func TestNewDecoder_SchemaError(t *testing.T) {
	defer ConfigTeardown()

	schema := "{}"
	_, err := avro.NewDecoder(schema, nil)

	assert.Error(t, err)
}

func TestDecoder_DecodeUnsupportedTypeError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := avro.NewPrimitiveSchema(avro.Type("test"), nil, nil)
	dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))

	var b bool
	err := dec.Decode(&b)

	assert.Error(t, err)
}

func TestDecoder_DecodeEmptyReader(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{}
	schema := "boolean"
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var b bool
	err := dec.Decode(b)

	assert.Error(t, err)
}

func TestDecoder_DecodeNonPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := "boolean"
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var b bool
	err := dec.Decode(b)

	assert.Error(t, err)
}

func TestDecoder_DecodeNilPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := "boolean"
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	err := dec.Decode((*bool)(nil))

	assert.Error(t, err)
}

func TestDecoder_DecodeEOFDoesntReturnError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2}
	schema := "int"
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var i int
	err := dec.Decode(&i)

	assert.NoError(t, err)
}

func TestUnmarshal(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("int")

	var i int
	err := avro.Unmarshal(schema, []byte{0xE2}, &i)

	assert.NoError(t, err)
}

func TestUnmarshal_Ptr(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("boolean")

	var b bool
	err := avro.Unmarshal(schema, []byte{0x01}, b)

	assert.Error(t, err)
}

func TestUnmarshal_NilPtr(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("boolean")

	err := avro.Unmarshal(schema, []byte{0x01}, (*bool)(nil))

	assert.Error(t, err)
}
