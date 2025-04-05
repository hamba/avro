package avro_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	boolConverter = avro.TypeConversionFuncs{
		AvroType: avro.Boolean,
		DecoderTypeConversion: func(in any) (any, error) {
			b := in.(bool)
			if b {
				return "yes", nil
			} else {
				return "no", nil
			}
		},
	}

	intConverter = avro.TypeConversionFuncs{
		AvroType: avro.Int,
		EncoderTypeConversion: func(in any) (any, error) {
			switch v := in.(type) {
			case float32:
				if float32(int(v)) != v {
					return 0, fmt.Errorf("%v is not an integer", in)
				}
				return int(v), nil
			case float64:
				if float64(int(v)) != v {
					return 0, fmt.Errorf("%v is not an integer", in)
				}
				return int(v), nil
			}
			return in, nil
		},
	}

	fixedDecimalConverter = avro.TypeConversionFuncs{
		AvroType:        avro.Fixed,
		AvroLogicalType: avro.Decimal,
		EncoderTypeConversion: func(in any) (any, error) {
			switch v := in.(type) {
			case string:
				val, _ := new(big.Rat).SetString(v)
				return val, nil
			}
			return in, nil
		},
		DecoderTypeConversion: func(in any) (any, error) {
			r := in.(*big.Rat)
			f, _ := r.Float64()
			return f, nil
		},
	}
)

func nonConverter(typ avro.Type) avro.TypeConversionFuncs {
	return avro.TypeConversionFuncs{
		AvroType: typ,
	}
}

func errorConverter(typ avro.Type, err error) avro.TypeConversionFuncs {
	return avro.TypeConversionFuncs{
		AvroType: typ,
		EncoderTypeConversion: func(in any) (any, error) {
			return nil, err
		},
		DecoderTypeConversion: func(in any) (any, error) {
			return nil, err
		},
	}
}

func TestEncoderTypeConverter_Array(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items":"int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(intConverter)

	val := []any{
		float32(27),
		float64(28),
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x04, 0x36, 0x38, 0x0}, buf.Bytes())
}

func TestEncoderTypeConverter_RecordMap(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":"int"},{"name":"b", "type": {"type":"fixed", "name":"fixed", "size":6, "logicalType":"decimal", "precision":4, "scale":2}}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(intConverter, fixedDecimalConverter)

	val := map[string]any{
		"a": float64(27),
		"b": "346.8",
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x00, 0x00, 0x00, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoderTypeConverter_RecordStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"record", "name":"test", "fields":[{"name":"a", "type":"int"},{"name":"b", "type": {"type":"fixed", "name":"fixed", "size":6, "logicalType":"decimal", "precision":4, "scale":2}}]}`
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(intConverter, fixedDecimalConverter)

	type TestRecord struct {
		A any `avro:"a"`
		B any `avro:"b"`
	}

	val := TestRecord{
		A: float64(27),
		B: "346.8",
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x00, 0x00, 0x00, 0x00, 0x87, 0x78}, buf.Bytes())
}

func TestEncoderTypeConverter_UnionInterfaceUnregisteredArray(t *testing.T) {
	defer ConfigTeardown()

	schema := `["int", {"type":"array", "items":"int"}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(intConverter)

	var val any = map[string]any{
		"array": []any{
			float32(27),
			float64(28),
		},
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x03, 0x04, 0x36, 0x38, 0x00}, buf.Bytes())
}

func TestEncoderTypeConverter_NotSet(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items":"int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	avro.RegisterTypeConverters(nonConverter(avro.Int))

	val := []any{
		27,
		28,
	}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x04, 0x36, 0x38, 0x0}, buf.Bytes())
}

func TestEncoderTypeConverter_Error(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items":"int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	testError := errors.New("test error")
	avro.RegisterTypeConverters(errorConverter(avro.Int, testError))

	val := []any{
		float32(27.1),
	}
	err = enc.Encode(val)

	assert.ErrorIs(t, err, testError)
	assert.Empty(t, buf.Bytes())
}

func TestDecoderTypeConverter_Single(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01}
	schema := `{"type":"boolean"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	avro.RegisterTypeConverters(boolConverter)

	var b any
	err = dec.Decode(&b)

	require.NoError(t, err)
	assert.Equal(t, "yes", b)
}

func TestDecoderTypeConverter_FixedRat(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78}
	schema := `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":4,"scale":2}`
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

	var b any
	err = dec.Decode(&b)

	require.NoError(t, err)
	assert.Equal(t, true, b)
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
