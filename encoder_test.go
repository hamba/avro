package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEncoder_SchemaError(t *testing.T) {
	defer ConfigTeardown()

	schema := "{}"
	_, err := avro.NewEncoder(schema, nil)

	assert.Error(t, err)
}

func TestEncoder_EncodeUnsupportedType(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.NewPrimitiveSchema(avro.Type("test"), nil, nil)
	buf := bytes.NewBuffer([]byte{})
	enc := avro.NewEncoderForSchema(schema, buf)

	err := enc.Encode(true)

	assert.Error(t, err)
}

func TestMarshal(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("boolean")

	b, err := avro.Marshal(schema, true)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x01}, b)
}

func TestMarshal_Error(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("int")

	_, err := avro.Marshal(schema, true)

	assert.Error(t, err)
}
