package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestEncoder_EnumInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(27)

	assert.Error(t, err)
}

func TestEncoder_Enum(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode("bar")

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02}, buf.Bytes())
}

func TestEncoder_EnumInvalidSymbol(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode("baz")

	assert.Error(t, err)
}
