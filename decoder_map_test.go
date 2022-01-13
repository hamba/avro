package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestDecoder_MapInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00}
	schema := `{"type":"map", "values": "string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	assert.Error(t, err)
}

func TestDecoder_MapMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00}
	schema := `{"type":"map", "values": "string"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]string
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"foo": "foo"}, got)
}

func TestDecoder_MapMapOfStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}
	schema := `{"type":"map", "values": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]TestRecord
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, map[string]TestRecord{"foo": {A: 27, B: "foo"}}, got)
}

func TestDecoder_MapOfRecursiveStruct(t *testing.T) {
	defer ConfigTeardown()

	type record struct {
		A int               `avro:"a"`
		B map[string]record `avro:"b"`
	}

	data := []byte{0x02, 0x01, 0x0c, 0x06, 0x66, 0x6f, 0x6f, 0x04, 0x0, 0x0}
	schema := `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "int"}, {"name": "b", "type": {"type":"map", "values": "test"}}]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got record
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, record{A: 1, B: map[string]record{"foo": {A: 2, B: map[string]record{}}}}, got)
}

func TestDecoder_MapMapError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	schema := `{"type":"map", "values": "string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got map[string]string
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_MapInvalidKeyType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00}
	schema := `{"type":"map", "values": "string"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[int]string
	err := dec.Decode(&got)

	assert.Error(t, err)
}
