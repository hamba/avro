package avro_test

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_FixedInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x66, 0x6F, 0x6F, 0x66, 0x6F, 0x6F}
	schema := `{"type":"fixed", "name": "test", "size": 6}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

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

	require.NoError(t, err)
	assert.Equal(t, [6]byte{'f', 'o', 'o', 'f', 'o', 'o'}, got)
}

func TestDecoder_FixedRat_Positive(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}
	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":4,"scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(got)

	require.NoError(t, err)
	assert.Equal(t, big.NewRat(1734, 5), got)
}

func TestDecoder_FixedRat_Negative(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x78, 0x88}
	schema := `{"type":"fixed", "name": "test", "size": 6, "logicalType":"decimal","precision":4,"scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(got)

	require.NoError(t, err)
	assert.Equal(t, big.NewRat(-1734, 5), got)
}

func TestDecoder_FixedRat_Zero(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":4,"scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(got)

	require.NoError(t, err)
	assert.Equal(t, big.NewRat(0, 1), got)
}

func TestDecoder_FixedRatInvalidLogicalSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	schema := `{"type":"fixed", "name": "test", "size": 6}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(got)

	assert.Error(t, err)
}

func TestDecoder_FixedLogicalDuration(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xc, 0x0, 0x0, 0x0, 0x22, 0x0, 0x0, 0x0, 0x52, 0xaa, 0x8, 0x0}
	schema := `{"name":"foo","type":"fixed","logicalType":"duration","size":12}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := avro.LogicalDuration{}
	err = dec.Decode(&got)
	require.NoError(t, err)

	assert.Equal(t, uint32(12), got.Months)
	assert.Equal(t, uint32(34), got.Days)
	assert.Equal(t, uint32(567890), got.Milliseconds)
}

func TestDecoder_FixedLogicalDurationSizeNot12(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xc, 0x0, 0x0, 0x0, 0x22, 0x0, 0x0, 0x0, 0x52, 0xaa, 0x8}
	schema := `{"name":"foo","type":"fixed","logicalType":"duration","size":11}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := avro.LogicalDuration{}
	err = dec.Decode(&got)
	assert.Error(t, err)
	assert.Equal(t, fmt.Errorf("avro: avro.LogicalDuration is unsupported for Avro fixed, size=11"), err)
}

func TestDecoder_FixedUint64_Full(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	schema := `{"type":"fixed", "name": "test", "size": 8}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got uint64
	err = dec.Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64), got)
}
func TestDecoder_FixedUint64_Simple(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}
	schema := `{"type":"fixed", "name": "test", "size": 8}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got uint64
	err = dec.Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, uint64(256), got)
}
