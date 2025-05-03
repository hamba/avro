package avro_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hamba/avro/v2"
)

func TestDecoderTypeConverter_Single(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := `{"type":"boolean"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(boolConverter)

	var got any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, "yes", got)
}

func TestDecoderTypeConverter_UnionResolved(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x01}
	schema := `{"type":["null","boolean"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(unionConverter)

	var got any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, "yes", got)
}

func TestDecoderTypeConverter_MapUnion(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x87, 0x78}
	schema := `{"type":["null",{"type":"fixed", "name":"fixed_decimal", "size":6, "logicalType":"decimal", "precision":4, "scale":2}]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(fixedDecimalConverter)

	var got map[string]any

	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"fixed_decimal": 346.8}, got)
}

func TestDecoderTypeConverter_UnionNullableSlice(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 'f', 'o', 'o'}
	schema := `["null", "bytes"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	avro.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(in any, schema avro.Schema) (any, error) {
			s := in.([]byte)
			for i, b := range s {
				s[i] = b - 0x20
			}
			return in, nil
		},
	})

	var got []byte
	err := dec.Decode(&got)

	want := []byte("FOO")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestDecoderTypeConverter_UnionNullablePtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36}
	schema := `["null", "int"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	avro.RegisterTypeConverters(avro.TypeConversionFuncs{
		AvroType: avro.Union,
		DecoderTypeConversion: func(in any, schema avro.Schema) (any, error) {
			i := in.(int)
			i = i * 2
			return i, nil
		},
	})

	var got *int
	err := dec.Decode(&got)

	want := int(54)
	require.NoError(t, err)
	assert.Equal(t, want, *got)
}

func TestDecoderTypeConverter_FixedRat(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}
	schema := `{"type":"fixed", "name":"fixed_decimal", "size":6, "logicalType":"decimal", "precision":4, "scale":2}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(fixedDecimalConverter)

	var got any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, float64(346.8), got)
}

func TestDecoderTypeConverter_NotSet(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := `{"type":"boolean"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(nonConverter(avro.Boolean))

	var got any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, true, got)
}

func TestDecoderTypeConverter_Error(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36}
	schema := `{"type":"int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	testError := errors.New("test error")
	avro.RegisterTypeConverters(errorConverter(avro.Int, testError))

	var got any
	err = dec.Decode(&got)

	assert.ErrorIs(t, err, testError)
	assert.Nil(t, got)
}

func TestDecoderTypeConverter_ErrorUnionResolved(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x01}
	schema := `{"type":["null","boolean"]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	testError := errors.New("test error")
	avro.RegisterTypeConverters(errorConverter(avro.Union, testError))

	var got any
	err = dec.Decode(&got)

	assert.ErrorIs(t, err, testError)
	assert.Nil(t, got)
}

func TestDecoderTypeConverter_ErrorUnionNullableSlice(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 'f', 'o', 'o'}
	schema := `["null", "bytes"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	testError := errors.New("test error")
	avro.RegisterTypeConverters(errorConverter(avro.Union, testError))

	var got []byte
	err := dec.Decode(&got)

	assert.ErrorIs(t, err, testError)
	assert.Nil(t, got)
}

func TestDecoderTypeConverter_ErrorUnionNullablePtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36}
	schema := `["null", "int"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	testError := errors.New("test error")
	avro.RegisterTypeConverters(errorConverter(avro.Union, testError))

	var got *int
	err := dec.Decode(&got)

	assert.ErrorIs(t, err, testError)
	assert.Nil(t, got)
}
