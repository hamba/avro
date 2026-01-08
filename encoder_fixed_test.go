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

func TestEncoder_FixedInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([6]int{})

	assert.Error(t, err)
}

func TestEncoder_Fixed(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([6]byte{'f', 'o', 'o', 'f', 'o', 'o'})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x6F, 0x6F, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_FixedRat_Positive(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":5,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoder_FixedRat_Boundary(t *testing.T) {
	defer ConfigTeardown()
	schema := `{"type":"fixed","name":"test","size":6,"logicalType":"decimal","precision":5,"scale":2}`
	buf := &bytes.Buffer{}
	enc, _ := avro.NewEncoder(schema, buf)

	err := enc.Encode(big.NewRat(99999, 100))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x01, 0x86, 0x9f}, buf.Bytes())
}

func TestEncoder_FixedRat_Negative(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6, "logicalType":"decimal","precision":5,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(-1734, 5))

	require.NoError(t, err)
	assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x78, 0x88}, buf.Bytes())
}

func TestEncoder_FixedRat_Zero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":5,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(0, 1))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, buf.Bytes())
}

func TestEncoder_FixedRat_TooManyDigits(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":3,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	assert.ErrorContains(t, err, "avro: cannot encode 346.80 as Avro fixed.decimal with precision=3, has 5 significant digits")
	assert.Empty(t, buf.Bytes())
}

func TestEncoder_FixedRatInvalidLogicalSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 6}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	assert.Error(t, err)
}

func TestEncoder_FixedLogicalDuration(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"name":"foo","type":"fixed","logicalType":"duration","size":12}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	duration := avro.LogicalDuration{Months: 12, Days: 34, Milliseconds: 567890}
	err = enc.Encode(duration)

	require.NoError(t, err)
	assert.Equal(t, []byte{0xc, 0x0, 0x0, 0x0, 0x22, 0x0, 0x0, 0x0, 0x52, 0xaa, 0x8, 0x0}, buf.Bytes())
}

func TestEncoder_FixedLogicalDurationSizeNot12(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"name":"foo","type":"fixed","logicalType":"duration","size":11}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	duration := avro.LogicalDuration{}
	err = enc.Encode(duration)
	assert.Error(t, err)
	assert.Equal(t, fmt.Errorf("avro: avro.LogicalDuration is unsupported for Avro fixed, size=11"), err)
}

func TestEncoder_FixedUint64_Full(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 8}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(uint64(math.MaxUint64))

	require.NoError(t, err)
	assert.Equal(t, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, buf.Bytes())
}

func TestEncoder_FixedUint64_Small(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"fixed", "name": "test", "size": 8}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(uint64(256))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00}, buf.Bytes())
}

func TestEncoder_FixedUint64_Zero(t *testing.T) {
	defer ConfigTeardown()
	schema := `{"type":"fixed", "name":"test","size":8}`
	buf := &bytes.Buffer{}
	enc, _ := avro.NewEncoder(schema, buf)

	err := enc.Encode(uint64(0))

	require.NoError(t, err)
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0}, buf.Bytes())
}

func TestEncoder_FixedUint64_TooLarge(t *testing.T) {
	defer ConfigTeardown()
	schema := `{"type":"fixed", "name":"test","size":4}`
	buf := &bytes.Buffer{}
	enc, _ := avro.NewEncoder(schema, buf)

	err := enc.Encode(uint64(65535))

	assert.Error(t, err)
}

func TestEncoder_FixedWrongSizeInput(t *testing.T) {
	defer ConfigTeardown()
	schema := `{"type":"fixed", "name":"test", "size":6}`
	buf := &bytes.Buffer{}
	enc, _ := avro.NewEncoder(schema, buf)

	err := enc.Encode([3]byte{'f', 'o', 'o'})
	assert.Error(t, err)

	err = enc.Encode([9]byte{'f', 'o', 'o', 'b', 'a', 'r', 'x', 'y', 'z'})
	assert.Error(t, err)
}

func TestEncoder_FixedNilInput(t *testing.T) {
	defer ConfigTeardown()
	schema := `{"type":"fixed", "name":"test", "size":6}`
	buf := &bytes.Buffer{}
	enc, _ := avro.NewEncoder(schema, buf)

	var b []byte = nil
	err := enc.Encode(b)

	assert.Error(t, err)
}
