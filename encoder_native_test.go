package avro_test

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_InvalidNative(t *testing.T) {
	defer ConfigTeardown()

	schema := "boolean"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode("test")

	assert.Error(t, err)
}

func TestEncoder_Bool(t *testing.T) {
	defer ConfigTeardown()

	schema := "boolean"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(true)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x01}, buf.Bytes())
}

func TestEncoder_BoolInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(true)

	assert.Error(t, err)
}

func TestEncoder_Int(t *testing.T) {
	defer ConfigTeardown()

	schema := "int"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(27)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_IntInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(27)

	assert.Error(t, err)
}

func TestEncoder_Int8(t *testing.T) {
	defer ConfigTeardown()

	schema := "int"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(int8(27))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_Int8InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(int8(27))

	assert.Error(t, err)
}

func TestEncoder_Int16(t *testing.T) {
	defer ConfigTeardown()

	schema := "int"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(int16(27))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_Int16InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(int16(27))

	assert.Error(t, err)
}

func TestEncoder_Int32(t *testing.T) {
	defer ConfigTeardown()

	schema := "int"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(int32(27))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_Int32InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(int32(27))

	assert.Error(t, err)
}

func TestEncoder_Int64(t *testing.T) {
	defer ConfigTeardown()

	schema := "long"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(int64(27))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_Int64FromInt32(t *testing.T) {
	defer ConfigTeardown()

	schema := "long"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(int32(27))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36}, buf.Bytes())
}

func TestEncoder_Int64InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(int64(27))

	assert.Error(t, err)
}

func TestEncoder_Float32(t *testing.T) {
	defer ConfigTeardown()

	schema := "float"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(float32(1.15))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x33, 0x33, 0x93, 0x3F}, buf.Bytes())
}

func TestEncoder_Float32InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(float32(1.15))

	assert.Error(t, err)
}

func TestEncoder_Float64(t *testing.T) {
	defer ConfigTeardown()

	schema := "double"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(float64(1.15))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F}, buf.Bytes())
}

func TestEncoder_Float64FromFloat32(t *testing.T) {
	defer ConfigTeardown()

	schema := "double"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(float32(1.15))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x0, 0x0, 0x0, 0x60, 0x66, 0x66, 0xf2, 0x3f}, buf.Bytes())
}

func TestEncoder_Float64InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(float64(1.15))

	assert.Error(t, err)
}

func TestEncoder_String(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode("foo")

	require.NoError(t, err)
	assert.Equal(t, []byte{0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_StringInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "int"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode("foo")

	assert.Error(t, err)
}

func TestEncoder_Bytes(t *testing.T) {
	defer ConfigTeardown()

	schema := "bytes"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([]byte{0xEC, 0xAB, 0x44, 0x00})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x08, 0xEC, 0xAB, 0x44, 0x00}, buf.Bytes())
}

func TestEncoder_BytesInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := "string"
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([]byte{0xEC, 0xAB, 0x44, 0x00})

	assert.Error(t, err)
}

func TestEncoder_Time_Date(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"int","logicalType":"date"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(time.Date(2920, 1, 2, 0, 0, 0, 0, time.UTC))

	require.NoError(t, err)
	assert.Equal(t, []byte{0xCA, 0xAD, 0x2A}, buf.Bytes())
}

func TestEncoder_Time_TimestampMillis(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"long","logicalType":"timestamp-millis"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x90, 0xB2, 0xAE, 0xC3, 0xEC, 0x5B}, buf.Bytes())
}

func TestEncoder_Time_TimestampMillisZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"long","logicalType":"timestamp-millis"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(time.Time{})

	require.NoError(t, err)
	assert.Equal(t, []byte{0xff, 0xdf, 0xe6, 0xa2, 0xe2, 0xa0, 0x1c}, buf.Bytes())
}

func TestEncoder_Time_TimestampMillisOneMillis(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"long","logicalType":"timestamp-millis"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(time.Date(1970, 1, 1, 0, 0, 0, 1e6, time.UTC))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x2}, buf.Bytes())
}

func TestEncoder_Time_TimestampMicros(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"long","logicalType":"timestamp-micros"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x80, 0xCD, 0xB7, 0xA2, 0xEE, 0xC7, 0xCD, 0x05}, buf.Bytes())
}

func TestEncoder_Time_TimestampMicrosZero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"long","logicalType":"timestamp-micros"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(time.Time{})

	require.NoError(t, err)
	assert.Equal(t, []byte{0xff, 0xff, 0xdd, 0xf2, 0xdf, 0xff, 0xdf, 0xdc, 0x1}, buf.Bytes())
}

func TestEncoder_Time_TimestampMillisOneMicros(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"long","logicalType":"timestamp-micros"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(time.Date(1970, 1, 1, 0, 0, 0, 1e3, time.UTC))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x2}, buf.Bytes())
}

func TestEncoder_TimeInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"long"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC))

	assert.Error(t, err)
}

func TestEncoder_Duration_TimeMillis(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"int","logicalType":"time-millis"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(123456789 * time.Millisecond)

	require.NoError(t, err)
	assert.Equal(t, []byte{0xAA, 0xB4, 0xDE, 0x75}, buf.Bytes())
}

func TestEncoder_Duration_TimeMicros(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"long","logicalType":"time-micros"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(123456789123 * time.Microsecond)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x86, 0xEA, 0xC8, 0xE9, 0x97, 0x07}, buf.Bytes())
}

func TestEncoder_DurationInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(time.Millisecond)

	assert.Error(t, err)
}

func TestEncoder_BytesRat_Positive(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x6, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoder_BytesRat_Negative(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(-1734, 5))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x6, 0xFF, 0x78, 0x88}, buf.Bytes())
}

func TestEncoder_BytesRat_Zero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(0, 1))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x00}, buf.Bytes())
}

func TestEncoder_BytesRatNonPtr_Positive(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(*big.NewRat(1734, 5))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x6, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoder_BytesRatNonPtr_Negative(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(*big.NewRat(-1734, 5))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x6, 0xFF, 0x78, 0x88}, buf.Bytes())
}

func TestEncoder_BytesRatNonPtr_Zero(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(*big.NewRat(0, 1))

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x00}, buf.Bytes())
}

func TestEncoder_BytesRatInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	assert.Error(t, err)
}

func TestEncoder_BytesRatInvalidLogicalSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"int","logicalType":"date"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(big.NewRat(1734, 5))

	assert.Error(t, err)
}

func TestEncoder_String_JSON(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"string","sqlType":"JSON"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(`{"field":"value"}`)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x22, 0x7b, 0x22, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x22, 0x3a, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x7d}, buf.Bytes())
}
