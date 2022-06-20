package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_EnumInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var str int
	err = dec.Decode(&str)

	assert.Error(t, err)
}

func TestDecoder_Enum(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got string
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, "bar", got)
}

func TestDecoder_EnumInvalidSymbol(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got string
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_EnumError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got string
	err = dec.Decode(&got)

	assert.Error(t, err)
}
