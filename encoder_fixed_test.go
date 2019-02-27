package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestEncoder_FixedInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode([6]int{})

	assert.Error(t, err)
}

func TestEncoder_Fixed(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode([6]byte{'f', 'o', 'o', 'f', 'o', 'o'})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x6F, 0x6F, 0x66, 0x6F, 0x6F}, buf.Bytes())
}
