package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestDecoder_ArrayInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x38, 0x0}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	assert.Error(t, err)
}

func TestDecoder_ArraySlice(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x38, 0x0}
	schema := `{"type":"array", "items": "int"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got []int
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, []int{27, 28}, got)
}

func TestDecoder_ArraySliceOfStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}
	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got []TestRecord
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, []TestRecord{{A: 27, B: "foo"}, {A: 27, B: "foo"}}, got)
}

func TestDecoder_ArrayRecursiveStruct(t *testing.T) {
	defer ConfigTeardown()

	type record struct {
		A int      `avro:"a"`
		B []record `avro:"b"`
	}

	data := []byte{0x2, 0x3, 0x8, 0x4, 0x0, 0x6, 0x0, 0x0}
	schema := `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "int"}, {"name": "b", "type": {"type":"array", "items": "test"}}]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got record
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, record{A: 1, B: []record{{A: 2}, {A: 3}}}, got)
}

func TestDecoder_ArraySliceError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got []int
	err = dec.Decode(&got)

	assert.Error(t, err)
}
