package avro_test

import (
	"bytes"
	"errors"
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

	var got int
	err = dec.Decode(&got)

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

func TestDecoder_EnumTextUnmarshaler(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *testEnumTextUnmarshaler
	err := dec.Decode(&got)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, testEnumTextUnmarshaler(1), *got)
}

func TestDecoder_EnumTextUnmarshalerNonPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got testEnumTextUnmarshaler
	err := dec.Decode(&got)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, testEnumTextUnmarshaler(1), got)
}

func TestDecoder_EnumTextUnmarshalerObj(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type":"enum", "name": "test1", "symbols": ["foo", "bar"]}}
    ]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got testEnumUnmarshalerObj
	err = dec.Decode(&got)

	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, testEnumUnmarshalerObj{A: testEnumTextUnmarshaler(1)}, got)
}

func TestDecoder_EnumTextUnmarshalerInvalidSymbol(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *testEnumTextUnmarshaler
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_EnumTextUnmarshalerEnumError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got *testEnumTextUnmarshaler
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_EnumTextUnmarshalerError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar", "baz"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got *testEnumTextUnmarshaler
	err = dec.Decode(&got)

	assert.Error(t, err)
}

type testEnumUnmarshalerObj struct {
	A testEnumTextUnmarshaler `avro:"a"`
}

type testEnumTextUnmarshaler int

func (m *testEnumTextUnmarshaler) UnmarshalText(data []byte) error {
	switch string(data) {
	case "foo":
		*m = 0
		return nil
	case "bar":
		*m = 1
		return nil
	default:
		return errors.New("unknown symbol")
	}
}
