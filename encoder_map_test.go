package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestEncoder_MapInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode("test")

	assert.Error(t, err)
}

func TestEncoder_Map(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(map[string]string{})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_MapEmpty(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(map[string]string{"foo": "foo"})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x10, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00}, buf.Bytes())
}

func TestEncoder_MapOfStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(map[string]TestRecord{"foo": {A: 27, B: "foo"}})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x12, 0x06, 0x66, 0x6F, 0x6F, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}, buf.Bytes())
}

func TestEncoder_MapInvalidKeyType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(map[int]string{1: "foo"})

	assert.Error(t, err)
}

func TestEncoder_MapError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(map[string]int{"foo": 1})

	assert.Error(t, err)
}
