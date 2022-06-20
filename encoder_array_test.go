package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_ArrayInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode("test")

	assert.Error(t, err)
}

func TestEncoder_Array(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([]int{27, 28})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x04, 0x36, 0x38, 0x0}, buf.Bytes())
}

func TestEncoder_ArrayEmpty(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([]int{})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x0}, buf.Bytes())
}

func TestEncoder_ArrayOfStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([]TestRecord{{A: 27, B: "foo"}, {A: 27, B: "foo"}})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x14, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}, buf.Bytes())
}

func TestEncoder_ArrayError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([]string{"foo", "bar"})

	assert.Error(t, err)
}
