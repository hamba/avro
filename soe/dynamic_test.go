package soe_test

import (
	"context"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/soe"
	"github.com/hamba/avro/v2/soe/resolvers"
	"github.com/hamba/avro/v2/soe/internal/testdata"
	"github.com/stretchr/testify/require"
)

// Helper function to marshal v into an SOE-framed binary Avro encoding.
func encode(t *testing.T, schema avro.Schema, v any) []byte {
	t.Helper()

	// Use a basic codec to marshal value.
	codec, err := soe.NewCodec(schema)
	require.NoError(t, err)

	data, err := codec.Encode(&v)
	require.NoError(t, err)
	return data
}

// Helper function to create a new DynamicDecoder with a registry over zero or
// more schemas.
func newDynamicDecoder(t *testing.T, schemas ...avro.Schema) *soe.DynamicDecoder {
	t.Helper()

	// Set up a store with provided schemas.
	store := resolvers.NewMemorySchemaStore()
	for _, schema := range schemas {
		err := store.AddSchema(schema)
		require.NoError(t, err)
	}
	return soe.NewDynamicDecoder(store)
}

func TestDynamicDecoder_DecodeUnknownSchema(t *testing.T) {
	// Marshal a value
	data := encode(t, testdata.Dynamic1Schema, testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	})

	// Set up a decoder with an empty registry.
	decoder := newDynamicDecoder(t)

	var v1 testdata.Dynamic1
	err := decoder.Decode(context.Background(), data, &v1)

	// Decode should fail with unknown schema error.
	require.ErrorIs(t, err, soe.ErrUnknownSchema)
}

func TestDynamicDecoder_DecodeWithOneSchema(t *testing.T) {
	schema := testdata.Dynamic1Schema

	// Marshal a value
	v0 := testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	}
	data := encode(t, schema, v0)

	// Set up a decoder with a registry containing the same schema.
	decoder := newDynamicDecoder(t, schema)

	var v1 testdata.Dynamic1
	err := decoder.Decode(context.Background(), data, &v1)

	require.NoError(t, err)
	require.Equal(t, v0, v1)
}

func TestDynamicDecoder_DecodeWithTwoSchemas(t *testing.T) {
	s1 := testdata.Dynamic1Schema
	s2 := testdata.Dynamic2Schema

	// Marshal two values of the different schemas
	data1 := encode(t, s1, testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	})
	data2 := encode(t, s2, testdata.Dynamic2{
		Key:     "ABC",
		Enabled: true,
	})

	// Set up a decoder with a registry containing the same two schemas.
	decoder := newDynamicDecoder(t, s1, s2)

	// Decode the payloads
	var v1 testdata.Dynamic1
	err1 := decoder.Decode(context.Background(), data1, &v1)

	var v2 testdata.Dynamic2
	err2 := decoder.Decode(context.Background(), data2, &v2)

	// Both payloads should be decodable since the registry knows about both
	// schemas.
	require.NoError(t, err1)
	require.Equal(t, "Bob", v1.Name)
	require.Equal(t, 16, v1.Age)

	require.NoError(t, err2)
	require.Equal(t, "ABC", v2.Key)
	require.Equal(t, true, v2.Enabled)
}

func TestDynamicDecoder_ValueMismatch(t *testing.T) {
	schema := testdata.Dynamic1Schema

	// Marshal a Dynamic1 value
	data := encode(t, schema, testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	})

	// Set up a decoder with a registry containing the schema.
	decoder := newDynamicDecoder(t, schema)

	// Attempt to decode into Dynamic2 type. Avro Unmarshal does best-effort
	// matching of fields to annotations, so this won't fail...
	var v2 testdata.Dynamic2
	err := decoder.Decode(context.Background(), data, &v2)

	// ... but we'll get an empty value.
	require.NoError(t, err)
	require.Equal(t, "", v2.Key)
	require.Equal(t, false, v2.Enabled)
}

func TestDynamicDecoder_DecodeBasePayloadWithExtendedType(t *testing.T) {
	// Marshal a Dynamic1 value
	data := encode(t, testdata.Dynamic1Schema, testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	})

	// Set up a decoder with a registry containing the schema of base type
	// Dynamic1.
	decoder := newDynamicDecoder(t, testdata.Dynamic1Schema)

	// Attempt to decode into Dynamic3 type.
	var v3 testdata.Dynamic3
	err := decoder.Decode(context.Background(), data, &v3)

	// The fields compatible with Dynamic1 should be populated.
	require.NoError(t, err)
	require.Equal(t, "Bob", v3.Name)
	require.Equal(t, 16, v3.Age)
	require.Equal(t, "", v3.Hobby)
}

func TestDynamicDecoder_DecodeExtendedPayloadWithBaseType(t *testing.T) {
	// Marshal a Dynamic3 value
	data := encode(t, testdata.Dynamic3Schema, testdata.Dynamic3{
		Name:  "Bob",
		Age:   16,
		Hobby: "Dancing",
	})

	// Set up a decoder with a registry containing the schema of extended
	// type Dynamic3.
	decoder := newDynamicDecoder(t, testdata.Dynamic3Schema)

	// Attempt to decode into Dynamic1 type.
	var v1 testdata.Dynamic1
	err := decoder.Decode(context.Background(), data, &v1)

	// The fields compatible with Dynamic1 should be populated.
	require.NoError(t, err)
	require.Equal(t, "Bob", v1.Name)
	require.Equal(t, 16, v1.Age)
}

func TestDynamicDecoder_DecodeDynamicallyIntoMap(t *testing.T) {
	s1 := testdata.Dynamic1Schema
	s2 := testdata.Dynamic2Schema

	// Marshal two values of the different schemas
	data1 := encode(t, s1, testdata.Dynamic1{
		Name: "Bob",
		Age:  16,
	})
	data2 := encode(t, s2, testdata.Dynamic2{
		Key:     "ABC",
		Enabled: true,
	})

	// Set up a decoder with a registry containing the same two schemas.
	decoder := newDynamicDecoder(t, s1, s2)

	// Decode both payloads using different schemas into two values of the
	// same type map[string]any. This is an example of fully dynamic
	// decoding where distinct schemas/payloads are decoded into the same
	// Go type.
	var v1 map[string]any
	err1 := decoder.Decode(context.Background(), data1, &v1)

	var v2 map[string]any
	err2 := decoder.Decode(context.Background(), data2, &v2)

	// Check that the decoding succeeds and the expected values come out.
	require.NoError(t, err1)
	require.Equal(t, map[string]any{
		"name": "Bob",
		"age": 16,
	}, v1)

	require.NoError(t, err2)
	require.Equal(t, map[string]any{
		"key": "ABC",
		"enabled": true,
	}, v2)
}
