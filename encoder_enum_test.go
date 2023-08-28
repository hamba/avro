package avro_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_EnumInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(27)

	assert.Error(t, err)
}

func TestEncoder_Enum(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode("bar")

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02}, buf.Bytes())
}

func TestEncoder_EnumInvalidSymbol(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode("baz")

	assert.Error(t, err)
}

func TestEncoder_EnumTextMarshaler(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	m := testEnumTextMarshaler(1)
	err = enc.Encode(m)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02}, buf.Bytes())
}

func TestEncoder_EnumTextMarshalerPtr(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	m := testEnumTextMarshaler(1)
	err = enc.Encode(&m)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02}, buf.Bytes())
}

func TestEncoder_EnumTextMarshalerObj(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type":"enum", "name": "test1", "symbols": ["foo", "bar"]}}
    ]
}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	m := testEnumMarshlaerObj{A: testEnumTextMarshaler(1)}
	err = enc.Encode(&m)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02}, buf.Bytes())
}

func TestEncoder_EnumTextMarshalerInvalidSymbol(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	m := testEnumTextMarshaler(2)
	err = enc.Encode(m)

	assert.Error(t, err)
}

func TestEncoder_EnumTextMarshalerNil(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var m *testEnumTextMarshaler
	err = enc.Encode(m)

	assert.Error(t, err)
}

func TestEncoder_EnumTextMarshalerError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	m := testEnumTextMarshaler(3)
	err = enc.Encode(m)

	assert.Error(t, err)
}

type testEnumMarshlaerObj struct {
	A testEnumTextMarshaler `avro:"a"`
}

type testEnumTextMarshaler int

func (m *testEnumTextMarshaler) MarshalText() ([]byte, error) {
	switch *m {
	case 0:
		return []byte("foo"), nil
	case 1:
		return []byte("bar"), nil
	case 2:
		return []byte("baz"), nil
	default:
		return nil, errors.New("unknown symbol")
	}
}
