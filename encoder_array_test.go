package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestEncoder_ArrayInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode("test")

	assert.Error(t, err)
}

func TestEncoder_Array(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode([]int{27, 28})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x04, 0x36, 0x38, 0x0}, buf.Bytes())
}

func TestEncoder_ArrayEmpty(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode([]int{})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x0}, buf.Bytes())
}

func TestEncoder_ArrayOfStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode([]TestRecord{{A: 27, B: "foo"}, {A: 27, B: "foo"}})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x14, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}, buf.Bytes())
}

func TestEncoder_ArrayRecursiveStruct(t *testing.T) {
	defer ConfigTeardown()

	type record struct {
		A int      `avro:"a"`
		B []record `avro:"b"`
	}

	schema := `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "int"}, {"name": "b", "type": {"type":"array", "items": "test"}}]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	rec := record{A: 1, B: []record{{A: 2}, {A: 3}}}
	err = enc.Encode(rec)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x2, 0x3, 0x8, 0x4, 0x0, 0x6, 0x0, 0x0}, buf.Bytes())
}

func TestEncoder_ArrayError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode([]string{"foo", "bar"})

	assert.Error(t, err)
}
