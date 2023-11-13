package avro_test

import (
	"bytes"
	"math"
	"math/big"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_DefaultBool(t *testing.T) {

	defer ConfigTeardown()

	// write schema
	// `{
	// // 	"type": "record",
	// // 	"name": "test",
	// // 	"fields" : [
	// // 		{"name": "a", "type": "string"}
	// // 	]
	// // }`

	// {"a": "foo"}
	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{"name": "b", "type": "boolean", "default": true}
		]
	}`)

	// hack: set field action to force decode default behavior
	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))

	type TestRecord struct {
		A string `avro:"a"`
		B bool   `avro:"b"`
	}

	var got TestRecord
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{B: true, A: "foo"}, got)
}

func TestDecoder_DefaultInt(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{"name": "b", "type": "int", "default": 1000}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))

	type TestRecord struct {
		A string `avro:"a"`
		B int32  `avro:"b"`
	}

	var got TestRecord
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{B: 1000, A: "foo"}, got)
}

func TestDecoder_DefaultLong(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{"name": "b", "type": "long", "default": 1000}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	type TestRecord struct {
		A string `avro:"a"`
		B int64  `avro:"b"`
	}

	var got TestRecord
	err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{B: 1000, A: "foo"}, got)
}

func TestDecoder_DefaultFloat(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{"name": "b", "type": "float", "default": 10.45}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	type TestRecord struct {
		A string  `avro:"a"`
		B float32 `avro:"b"`
	}

	var got TestRecord
	err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{B: 10.45, A: "foo"}, got)
}

func TestDecoder_DefaultDouble(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{"name": "b", "type": "double", "default": 10.45}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	type TestRecord struct {
		A string  `avro:"a"`
		B float64 `avro:"b"`
	}

	var got TestRecord
	err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{B: 10.45, A: "foo"}, got)
}

func TestDecoder_DefaultBytes(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{"name": "b", "type": "bytes", "default": "value"}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	type TestRecord struct {
		A string `avro:"a"`
		B []byte `avro:"b"`
	}

	var got TestRecord
	err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{B: []byte("value"), A: "foo"}, got)
}

func TestDecoder_DefaultString(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{"name": "b", "type": "string", "default": "value"}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	type TestRecord struct {
		A string `avro:"a"`
		B string `avro:"b"`
	}

	var got TestRecord
	err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{B: "value", A: "foo"}, got)
}

func TestDecoder_DefaultEnum(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{
				"name": "b",
				"type": {
					"type": "enum",
					"name": "test.enum",
					"symbols": ["foo", "bar"]
				},
			  	"default": "bar"
			}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	t.Run("simple", func(t *testing.T) {
		type TestRecord struct {
			A string `avro:"a"`
			B string `avro:"b"`
		}

		var got TestRecord
		err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, TestRecord{B: "bar", A: "foo"}, got)

	})

	t.Run("TextUnmarshaler", func(t *testing.T) {
		type TestRecord struct {
			A string                  `avro:"a"`
			B testEnumTextUnmarshaler `avro:"b"`
		}

		var got TestRecord
		err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, TestRecord{B: 1, A: "foo"}, got)
	})

	t.Run("TextUnmarshaler Ptr", func(t *testing.T) {
		type TestRecord struct {
			A string                   `avro:"a"`
			B *testEnumTextUnmarshaler `avro:"b"`
		}

		var got TestRecord
		err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

		require.NoError(t, err)
		var v testEnumTextUnmarshaler = 1
		assert.Equal(t, TestRecord{B: &v, A: "foo"}, got)
	})
}

func TestDecoder_DefaultUnion(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	type TestRecord struct {
		A string `avro:"a"`
		B any    `avro:"b"`
	}

	t.Run("null default", func(t *testing.T) {
		schema := avro.MustParse(`{
			"type": "record",
			"name": "test",
			"fields" : [
				{"name": "a", "type": "string"},
				{"name": "b", "type": ["null", "long"], "default": null}
			]
		}`)

		avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

		var got TestRecord
		err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, TestRecord{B: nil, A: "foo"}, got)
	})

	t.Run("not null default", func(t *testing.T) {
		schema := avro.MustParse(`{
			"type": "record",
			"name": "test",
			"fields" : [
				{"name": "a", "type": "string"},
				{"name": "b", "type": ["string", "long"], "default": "bar"}
			]
		}`)

		avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

		var got TestRecord
		err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, TestRecord{B: "bar", A: "foo"}, got)
	})
}

func TestDecoder_DefaultArray(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{
				"name": "b",
				"type": {
					"type": "array", "items": "int"
				},
				"default":[1, 2, 3, 4]
			}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))

	type TestRecord struct {
		A string  `avro:"a"`
		B []int16 `avro:"b"`
	}

	var got TestRecord
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{B: []int16{1, 2, 3, 4}, A: "foo"}, got)
}

func TestDecoder_DefaultMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{
				"name": "b",
				"type": {
					"type": "map", "values": "string"
				},
				"default": {"foo":"bar"}
			}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))

	type TestRecord struct {
		A string            `avro:"a"`
		B map[string]string `avro:"b"`
	}

	var got TestRecord
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{B: map[string]string{"foo": "bar"}, A: "foo"}, got)
}

func TestDecoder_DefaultRecord(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{
				"name": "b",
				"type": {
					"type": "record",
					"name": "test.record",
					"fields" : [
						{"name": "a", "type": "string"},
						{"name": "b", "type": "string"}
					]
				},
				"default": {"a":"foo", "b": "bar"}
			}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	t.Run("struct", func(t *testing.T) {
		dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))

		type subRecord struct {
			A string `avro:"a"`
			B string `avro:"b"`
		}
		type TestRecord struct {
			A string    `avro:"a"`
			B subRecord `avro:"b"`
		}

		var got TestRecord
		err := dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, TestRecord{B: subRecord{A: "foo", B: "bar"}, A: "foo"}, got)
	})

	t.Run("map", func(t *testing.T) {
		dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))

		var got map[string]any
		err := dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, map[string]any{"b": map[string]any{"a": "foo", "b": "bar"}, "a": "foo"}, got)
	})
}

func TestDecoder_DefaultRef(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	_ = avro.MustParse(`{
		"type": "record",
		"name": "test.embed",
		"fields" : [
			{"name": "a", "type": "string"}
		]
	}`)

	schema := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"},
			{"name": "b", "type": "test.embed", "default": {"a": "foo"}}
		]
	}`)

	avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

	dec := avro.NewDecoderForSchema(schema, bytes.NewReader(data))

	var got map[string]any
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"b": map[string]any{"a": "foo"}, "a": "foo"}, got)
}

func TestDecoder_DefaultFixed(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x6, 0x66, 0x6f, 0x6f}

	t.Run("array", func(t *testing.T) {
		schema := avro.MustParse(`{
			"type": "record",
			"name": "test",
			"fields" : [
				{"name": "a", "type": "string"},
				{
					"name": "b",
					"type": {
						"type": "fixed",
						"name": "test.fixed",
						"size": 3
					}, 
					"default": "foo"
				}
			]
		}`)

		avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

		type TestRecord struct {
			A string  `avro:"a"`
			B [3]byte `avro:"b"`
		}

		var got TestRecord
		err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, TestRecord{B: [3]byte{'f', 'o', 'o'}, A: "foo"}, got)
	})

	t.Run("uint64", func(t *testing.T) {
		schema := avro.MustParse(`{
			"type": "record",
			"name": "test",
			"fields" : [
				{"name": "a", "type": "string"},
				{
					"name": "b",
					"type": {
						"type": "fixed",
						"name": "test.fixed",
						"size": 8
					}, 
					"default": "\u00ff\u00ff\u00ff\u00ff\u00ff\u00ff\u00ff\u00ff"
				}
			]
		}`)

		avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

		type TestRecord struct {
			A string `avro:"a"`
			B uint64 `avro:"b"`
		}

		var got TestRecord
		err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, TestRecord{B: uint64(math.MaxUint64), A: "foo"}, got)
	})

	t.Run("duration", func(t *testing.T) {
		schema := avro.MustParse(`{
			"type": "record",
			"name": "test",
			"fields" : [
				{"name": "a", "type": "string"},
				{
					"name": "b",
					"type": {
						"type": "fixed",
						"name": "test.fixed",
						"logicalType":"duration",
						"size":12
					}, 
					"default": "\u000c\u0000\u0000\u0000\u0022\u0000\u0000\u0000\u0052\u00aa\u0008\u0000"
				}
			]
		}`)

		avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

		type TestRecord struct {
			A string               `avro:"a"`
			B avro.LogicalDuration `avro:"b"`
		}

		var got TestRecord
		err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

		require.NoError(t, err)

		assert.Equal(t, uint32(12), got.B.Months)
		assert.Equal(t, uint32(34), got.B.Days)
		assert.Equal(t, uint32(567890), got.B.Milliseconds)
		assert.Equal(t, "foo", got.A)
	})

	t.Run("rat", func(t *testing.T) {
		schema := avro.MustParse(`{
			"type": "record",
			"name": "test",
			"fields" : [
				{"name": "a", "type": "string"},
				{
					"name": "b",
					"type": {
						"type": "fixed",
						"name": "test.fixed",
						"size": 6,
						"logicalType":"decimal",
						"precision":4,
						"scale":2
					}, 
					"default": "\u0000\u0000\u0000\u0000\u0087\u0078"
				}
			]
		}`)
		avro.SetFieldAction(schema.(*avro.RecordSchema).Fields()[1], avro.FieldSetDefault)

		type TestRecord struct {
			A string  `avro:"a"`
			B big.Rat `avro:"b"`
		}

		var got TestRecord
		err := avro.NewDecoderForSchema(schema, bytes.NewReader(data)).Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, big.NewRat(1734, 5), &got.B)
		assert.Equal(t, "foo", got.A)
	})

}
