package avro_test

import (
	"bytes"
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
