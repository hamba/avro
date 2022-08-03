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

func TestDecoder_NativeInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := "boolean"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var want *string
	err = dec.Decode(&want)

	assert.Error(t, err)
}

func TestDecoder_Bool(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := "boolean"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var b bool
	err = dec.Decode(&b)

	require.NoError(t, err)
	assert.True(t, b)
}

func TestDecoder_BoolInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var b bool
	err = dec.Decode(&b)

	assert.Error(t, err)
}

func TestDecoder_Int(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int
	err = dec.Decode(&i)

	require.NoError(t, err)
	assert.Equal(t, 27, i)
}

func TestDecoder_IntInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Int8(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int8
	err = dec.Decode(&i)

	require.NoError(t, err)
	assert.Equal(t, int8(27), i)
}

func TestDecoder_Int8InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int8
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Int16(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int16
	err = dec.Decode(&i)

	require.NoError(t, err)
	assert.Equal(t, int16(27), i)
}

func TestDecoder_Int16InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int16
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Int32(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int32
	err = dec.Decode(&i)

	require.NoError(t, err)
	assert.Equal(t, int32(27), i)
}

func TestDecoder_Int32InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int32
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Int64(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "long"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int64
	err = dec.Decode(&i)

	require.NoError(t, err)
	assert.Equal(t, int64(27), i)
}

func TestDecoder_Int64InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i int64
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Float32(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x33, 0x33, 0x93, 0x3F}
	schema := "float"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i float32
	err = dec.Decode(&i)

	require.NoError(t, err)
	assert.Equal(t, float32(1.15), i)
}

func TestDecoder_Float32InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x33, 0x33, 0x93, 0x3F}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i float32
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_Float64(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F}
	schema := "double"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i float64
	err = dec.Decode(&i)

	require.NoError(t, err)
	assert.Equal(t, float64(1.15), i)
}

func TestDecoder_Float64InvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var i float64
	err = dec.Decode(&i)

	assert.Error(t, err)
}

func TestDecoder_String(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x06, 0x66, 0x6F, 0x6F}
	schema := "string"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	require.NoError(t, err)
	assert.Equal(t, "foo", str)
}

func TestDecoder_StringInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x06, 0x66, 0x6F, 0x6F}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	assert.Error(t, err)
}

func TestDecoder_Bytes(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x08, 0xEC, 0xAB, 0x44, 0x00}
	schema := "bytes"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var b []byte
	err = dec.Decode(&b)

	require.NoError(t, err)
	assert.Equal(t, []byte{0xEC, 0xAB, 0x44, 0x00}, b)
}

func TestDecoder_BytesInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x08, 0xEC, 0xAB, 0x44, 0x00}
	schema := "int"
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var b []byte
	err = dec.Decode(&b)

	assert.Error(t, err)
}

func TestDecoder_Time_Date(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xCA, 0xAD, 0x2A}
	schema := `{"type":"int","logicalType":"date"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Time
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, time.Date(2920, 1, 2, 0, 0, 0, 0, time.UTC), got)
}

func TestDecoder_Time_TimestampMillis(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x90, 0xB2, 0xAE, 0xC3, 0xEC, 0x5B}
	schema := `{"type":"long","logicalType":"timestamp-millis"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Time
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), got)
}

func TestDecoder_Time_TimestampMillisZero(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xff, 0xdf, 0xe6, 0xa2, 0xe2, 0xa0, 0x1c}
	schema := `{"type":"long","logicalType":"timestamp-millis"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Time
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, time.Time{}, got)
}

func TestDecoder_Time_TimestampMillisOneMillis(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `{"type":"long","logicalType":"timestamp-millis"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Time
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, time.Date(1970, 1, 1, 0, 0, 0, 1e6, time.UTC), got)
}

func TestDecoder_Time_TimestampMicros(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x80, 0xCD, 0xB7, 0xA2, 0xEE, 0xC7, 0xCD, 0x05}
	schema := `{"type":"long","logicalType":"timestamp-micros"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Time
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), got)
}

func TestDecoder_Time_TimestampMicrosZero(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xff, 0xff, 0xdd, 0xf2, 0xdf, 0xff, 0xdf, 0xdc, 0x1}
	schema := `{"type":"long","logicalType":"timestamp-micros"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Time
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, time.Time{}, got)
}

func TestDecoder_Time_TimestampMillisOneMicros(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `{"type":"long","logicalType":"timestamp-micros"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Time
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, time.Date(1970, 1, 1, 0, 0, 0, 1e3, time.UTC), got)
}

func TestDecoder_TimeInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x80, 0xCD, 0xB7, 0xA2, 0xEE, 0xC7, 0xCD, 0x05}
	schema := `{"type":"long"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Time
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_Duration_TimeMillis(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xAA, 0xB4, 0xDE, 0x75}
	schema := `{"type":"int","logicalType":"time-millis"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Duration
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, 123456789*time.Millisecond, got)
}

func TestDecoder_Duration_TimeMicros(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x86, 0xEA, 0xC8, 0xE9, 0x97, 0x07}
	schema := `{"type":"long","logicalType":"time-micros"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Duration
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, 123456789123*time.Microsecond, got)
}

func TestDecoder_DurationInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x86, 0xEA, 0xC8, 0xE9, 0x97, 0x07}
	schema := `{"type":"string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got time.Duration
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_BytesRat_Positive(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x00, 0x87, 0x78}
	schema := `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(got)

	require.NoError(t, err)
	assert.Equal(t, big.NewRat(1734, 5), got)
}

func TestDecoder_BytesRat_Negative(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0xFF, 0x78, 0x88}
	schema := `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(got)

	require.NoError(t, err)
	assert.Equal(t, big.NewRat(-1734, 5), got)
}

func TestDecoder_BytesRat_Zero(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x00}
	schema := `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(got)

	require.NoError(t, err)
	assert.Equal(t, big.NewRat(0, 1), got)
}

func TestDecoder_BytesRatInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x00}
	schema := `{"type":"string"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(got)

	assert.Error(t, err)
}

func TestDecoder_BytesRatInvalidLogicalSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x00}
	schema := `{"type":"string","logicalType":"uuid"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &big.Rat{}
	err = dec.Decode(got)

	assert.Error(t, err)
}

func TestDecoder_String_JSON(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x2a, 0x5b, 0x7b, 0x22, 0x66, 0x31, 0x22, 0x3a, 0x22, 0x31, 0x22, 0x7d, 0x2c, 0x7b, 0x22, 0x66, 0x32, 0x22, 0x3a, 0x32, 0x7d, 0x5d}
	schema := `{"type":"string", "sqlType": "JSON"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got string
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, `[{"f1":"1"},{"f2":2}]`, got)
}
