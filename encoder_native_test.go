package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestEncoder_InvalidNative(t *testing.T) {
	defer ConfigTeardown()

	schema := "boolean"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode("test")

	assert.Error(t, err)
}

func TestEncoder_Bool(t *testing.T) {
	defer ConfigTeardown()

	schema := "boolean"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(true)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x01}, buf.Bytes())
}

func TestEncoder_BoolInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(true)

	assert.Error(t, err)
}

func TestEncoder_Int(t *testing.T) {
	defer ConfigTeardown()

	schema := "int"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(27)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_IntInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(27)

	assert.Error(t, err)
}

func TestEncoder_Int8(t *testing.T) {
	defer ConfigTeardown()

	schema := "int"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(int8(27))

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_Int8InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(int8(27))

	assert.Error(t, err)
}

func TestEncoder_Int16(t *testing.T) {
	defer ConfigTeardown()

	schema := "int"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(int16(27))

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_Int16InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(int16(27))

	assert.Error(t, err)
}

func TestEncoder_Int32(t *testing.T) {
	defer ConfigTeardown()

	schema := "int"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(int32(27))

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_Int32InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(int32(27))

	assert.Error(t, err)
}

func TestEncoder_Int64(t *testing.T) {
	defer ConfigTeardown()

	schema := "long"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(int64(27))

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_Int64InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(int64(27))

	assert.Error(t, err)
}

func TestEncoder_Float32(t *testing.T) {
	defer ConfigTeardown()

	schema := "float"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(float32(1.15))

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x33, 0x33, 0x93, 0x3F}, buf.Bytes())
}

func TestEncoder_Float32InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(float32(1.15))

	assert.Error(t, err)
}

func TestEncoder_Float64(t *testing.T) {
	defer ConfigTeardown()

	schema := "double"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(float64(1.15))

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F}, buf.Bytes())
}

func TestEncoder_Float64InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(float64(1.15))

	assert.Error(t, err)
}

func TestEncoder_String(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode("foo")

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_StringInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "int"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode("foo")

	assert.Error(t, err)
}

func TestEncoder_Bytes(t *testing.T) {
	defer ConfigTeardown()

	schema := "bytes"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode([]byte{0xEC, 0xAB, 0x44, 0x00})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x08, 0xEC, 0xAB, 0x44, 0x00}, buf.Bytes())
}

func TestEncoder_BytesInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode([]byte{0xEC, 0xAB, 0x44, 0x00})

	assert.Error(t, err)
}
