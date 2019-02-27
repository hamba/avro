package avro_test

import (
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestNewEncoder_SchemaError(t *testing.T) {
	defer ConfigTeardown()

	schema := "{}"
	_, err := avro.NewEncoder(schema, nil)

	assert.Error(t, err)
}

func TestMarshal(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("boolean")

	b, err := avro.Marshal(schema, true)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x01}, b)
}

func TestMarshal_Error(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse("int")

	_, err := avro.Marshal(schema, true)

	assert.Error(t, err)
}
