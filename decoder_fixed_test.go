package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestDecoder_FixedInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x66, 0x6F, 0x6F, 0x66, 0x6F, 0x6F}
	schema := `{"type":"fixed", "name": "test", "size": 6}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var i [6]int
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Fixed(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x66, 0x6F, 0x6F, 0x66, 0x6F, 0x6F}
	schema := `{"type":"fixed", "name": "test", "size": 6}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got [6]byte
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, [6]byte{'f', 'o', 'o', 'f', 'o', 'o'}, got)
}
