package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestDecoder_NativeInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := "boolean"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var want *string
	err = dec.Decode(&want)

	assert.Error(t, err)
}

func TestDecoder_Bool(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := "boolean"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var b bool
	err = dec.Decode(&b)

	assert.NoError(t, err)
	assert.True(t, b)
}

func TestDecoder_BoolInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var b bool
	err = dec.Decode(&b)

	assert.Error(t, err)
}

func TestDecoder_Int(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i int
	err = dec.Decode(&i)

	assert.NoError(t, err)
	assert.Equal(t, 27, i)
}

func TestDecoder_IntInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i int
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Int8(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i int8
	err = dec.Decode(&i)

	assert.NoError(t, err)
	assert.Equal(t, int8(27), i)
}

func TestDecoder_Int8InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i int8
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Int16(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i int16
	err = dec.Decode(&i)

	assert.NoError(t, err)
	assert.Equal(t, int16(27), i)
}

func TestDecoder_Int16InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i int16
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Int32(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i int32
	err = dec.Decode(&i)

	assert.NoError(t, err)
	assert.Equal(t, int32(27), i)
}

func TestDecoder_Int32InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i int32
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Int64(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "long"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i int64
	err = dec.Decode(&i)

	assert.NoError(t, err)
	assert.Equal(t, int64(27), i)
}

func TestDecoder_Int64InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i int64
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Float32(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x33, 0x33, 0x93, 0x3F}
	schema := "float"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i float32
	err = dec.Decode(&i)

	assert.NoError(t, err)
	assert.Equal(t, float32(1.15), i)
}

func TestDecoder_Float32InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x33, 0x33, 0x93, 0x3F}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i float32
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Float64(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F}
	schema := "double"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i float64
	err = dec.Decode(&i)

	assert.NoError(t, err)
	assert.Equal(t, float64(1.15), i)
}

func TestDecoder_Float64InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i float64
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_String(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x06, 0x66, 0x6F, 0x6F}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	assert.NoError(t, err)
	assert.Equal(t, "foo", str)
}

func TestDecoder_StringInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x06, 0x66, 0x6F, 0x6F}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	assert.Error(t, err)
}

func TestDecoder_Bytes(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x08, 0xEC, 0xAB, 0x44, 0x00}
	schema := "bytes"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var b []byte
	err = dec.Decode(&b)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0xEC, 0xAB, 0x44, 0x00}, b)
}

func TestDecoder_BytesInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x08, 0xEC, 0xAB, 0x44, 0x00}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var b []byte
	err = dec.Decode(&b)

	assert.Error(t, err)
}
