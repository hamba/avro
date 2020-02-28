package avro_test

import (
	"bytes"
	"math/big"
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

func TestEncoder_FixedRat_Positive(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":4,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoder_FixedRat_Negative(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6, "logicalType":"decimal","precision":4,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(big.NewRat(-1734, 5))

	assert.NoError(t, err)
	assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x78, 0x88}, buf.Bytes())
}

func TestEncoder_FixedRat_Zero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":4,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(big.NewRat(0, 1))

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00,}, buf.Bytes())
}

func TestEncoder_FixedRatInvalidLogicalSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	assert.Error(t, err)
}
