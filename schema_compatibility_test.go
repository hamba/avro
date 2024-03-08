package avro_test

import (
	"math/big"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
)

func TestNewSchemaCompatibility(t *testing.T) {
	sc := avro.NewSchemaCompatibility()

	assert.IsType(t, &avro.SchemaCompatibility{}, sc)
}

func TestSchemaCompatibility_Compatible(t *testing.T) {
	tests := []struct {
		name    string
		reader  string
		writer  string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "Primitive Matching",
			reader:  `"int"`,
			writer:  `"int"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Int Promote Long",
			reader:  `"long"`,
			writer:  `"int"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Int Promote Float",
			reader:  `"float"`,
			writer:  `"int"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Int Promote Double",
			reader:  `"double"`,
			writer:  `"int"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Long Promote Float",
			reader:  `"float"`,
			writer:  `"long"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Long Promote Double",
			reader:  `"double"`,
			writer:  `"long"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Float Promote Double",
			reader:  `"double"`,
			writer:  `"float"`,
			wantErr: assert.NoError,
		},
		{
			name:    "String Promote Bytes",
			reader:  `"bytes"`,
			writer:  `"string"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Bytes Promote String",
			reader:  `"string"`,
			writer:  `"bytes"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Match",
			reader:  `["int", "long", "string"]`,
			writer:  `["string", "int", "long"]`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Reader Missing Schema",
			reader:  `["int", "string"]`,
			writer:  `["string", "int", "long"]`,
			wantErr: assert.Error,
		},
		{
			name:    "Union Writer Missing Schema",
			reader:  `["int", "long", "string"]`,
			writer:  `["string", "int"]`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Writer Not Union",
			reader:  `["int", "long", "string"]`,
			writer:  `"int"`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Writer Not Union With Error",
			reader:  `["string"]`,
			writer:  `"int"`,
			wantErr: assert.Error,
		},
		{
			name:    "Union Reader Not Union",
			reader:  `"int"`,
			writer:  `["int"]`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Reader Not Union With Error",
			reader:  `"int"`,
			writer:  `["string", "int", "long"]`,
			wantErr: assert.Error,
		},
		{
			name:    "Array Match",
			reader:  `{"type":"array", "items": "int"}`,
			writer:  `{"type":"array", "items": "int"}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Array Items Mismatch",
			reader:  `{"type":"array", "items": "int"}`,
			writer:  `{"type":"array", "items": "string"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Map Match",
			reader:  `{"type":"map", "values": "int"}`,
			writer:  `{"type":"map", "values": "int"}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Map Items Mismatch",
			reader:  `{"type":"map", "values": "int"}`,
			writer:  `{"type":"map", "values": "string"}`,
			wantErr: assert.Error,
		},
		{
			name:    "Fixed Match",
			reader:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			writer:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Fixed Name Mismatch",
			reader:  `{"type":"fixed", "name":"test1", "namespace": "org.hamba.avro", "size": 12}`,
			writer:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: assert.Error,
		},
		{
			name:    "Fixed Size Mismatch",
			reader:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 13}`,
			writer:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: assert.Error,
		},
		{
			name:    "Enum Match",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Enum Name Mismatch",
			reader:  `{"type":"enum", "name":"test1", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Enum Reader Missing Symbol",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Enum Reader Missing Symbol With Default",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1"], "default": "TEST1"}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Enum Writer Missing Symbol",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1", "TEST2"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST1"]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Match",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Name Mismatch",
			reader:  `{"type":"record", "name":"test1", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int", "default": 1}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "string", "default": "b"}, {"name": "a", "type": "int"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record Schema Mismatch",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record Reader Field Missing",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Writer Field Missing With Default",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string", "default": "test"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Writer Field Missing Without Default",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Ref Dereference",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name":"test1", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "int"}]}}, {"name": "b", "type": "test1"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name":"test1", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "int"}]}}, {"name": "b", "type": "test"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Breaks Recursion",
			reader:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "test"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "test"}]}`,
			wantErr: assert.NoError,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			r := avro.MustParse(test.reader)
			w := avro.MustParse(test.writer)
			sc := avro.NewSchemaCompatibility()

			err := sc.Compatible(r, w)

			test.wantErr(t, err)
		})
	}
}

func TestSchemaCompatibility_CompatibleUsesCacheWithNoError(t *testing.T) {
	reader := `"int"`
	writer := `"int"`

	r := avro.MustParse(reader)
	w := avro.MustParse(writer)
	sc := avro.NewSchemaCompatibility()

	_ = sc.Compatible(r, w)

	err := sc.Compatible(r, w)

	assert.NoError(t, err)
}

func TestSchemaCompatibility_CompatibleUsesCacheWithError(t *testing.T) {
	reader := `"int"`
	writer := `"string"`

	r := avro.MustParse(reader)
	w := avro.MustParse(writer)
	sc := avro.NewSchemaCompatibility()

	_ = sc.Compatible(r, w)

	err := sc.Compatible(r, w)

	assert.Error(t, err)
}

func TestSchemaCompatibility_Resolve(t *testing.T) {
	tests := []struct {
		name   string
		reader string
		writer string
		value  any
		want   any
	}{
		{
			name:   "Int Promote Long",
			reader: `"long"`,
			writer: `"int"`,
			value:  10,
			want:   int64(10),
		},
		{
			name:   "Int Promote Long Time millis",
			reader: `{"type":"long","logicalType":"timestamp-millis"}`,
			writer: `"int"`,
			value:  5000,
			want:   time.UnixMilli(5000).UTC(),
		},
		{
			name:   "Int Promote Long Time micros",
			reader: `{"type":"long","logicalType":"timestamp-micros"}`,
			writer: `"int"`,
			value:  5000,
			want:   time.UnixMicro(5000).UTC(),
		},
		{
			name:   "Int Promote Long Time micros",
			reader: `{"type":"long","logicalType":"time-micros"}`,
			writer: `"int"`,
			value:  5000,
			want:   5000 * time.Microsecond,
		},
		{
			name:   "Int Promote Float",
			reader: `"float"`,
			writer: `"int"`,
			value:  10,
			want:   float32(10),
		},
		{
			name:   "Int Promote Double",
			reader: `"double"`,
			writer: `"int"`,
			value:  10,
			want:   float64(10),
		},
		{
			name:   "Long Promote Float",
			reader: `"float"`,
			writer: `"long"`,
			value:  int64(10),
			want:   float32(10),
		},
		{
			name:   "Long Promote Double",
			reader: `"double"`,
			writer: `"long"`,
			value:  int64(10),
			want:   float64(10),
		},
		{
			name:   "Float Promote Double",
			reader: `"double"`,
			writer: `"float"`,
			value:  float32(10.5),
			want:   float64(10.5),
		},
		{
			name:   "String Promote Bytes",
			reader: `"bytes"`,
			writer: `"string"`,
			value:  "foo",
			want:   []byte("foo"),
		},
		{
			// I'm not sure about this edge cases;
			// I took the reverse path and tried to find a Decimal that can be encoded to
			// a binary that is a valid UTF-8 sequence.
			name:   "String Promote Bytes With Logical Decimal",
			reader: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			writer: `"string"`,
			value:  "d",
			want:   big.NewRat(1, 1),
		},
		{
			name:   "Bytes Promote String",
			reader: `"string"`,
			writer: `"bytes"`,
			value:  []byte("foo"),
			want:   "foo",
		},
		{
			name:   "Array With Items Promotion",
			reader: `{"type":"array", "items": "long"}`,
			writer: `{"type":"array", "items": "int"}`,
			value:  []any{int32(10), int32(15)},
			want:   []any{int64(10), int64(15)},
		},
		{
			name:   "Map With Items Promotion",
			reader: `{"type":"map", "values": "bytes"}`,
			writer: `{"type":"map", "values": "string"}`,
			value:  map[string]any{"foo": "bar"},
			want:   map[string]any{"foo": []byte("bar")},
		},
		{
			name: "Enum Reader Missing Symbols With Default",
			reader: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo"],
				"default": "foo"
			}`,
			writer: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo", "bar"]
			}`,
			value: "bar",
			want:  "foo",
		},
		{
			name: "Enum Writer Missing Symbols",
			reader: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo", "bar"]
			}`,
			writer: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo"]
			}`,
			value: "foo",
			want:  "foo",
		},
		{
			name: "Enum Writer Missing Symbols and Unused Reader Default",
			reader: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo", "bar"],
				"default": "bar"
			}`,
			writer: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo"]
			}`,
			value: "foo",
			want:  "foo",
		},
		{
			name: "Enum With Alias",
			reader: `{
				"type": "enum",
				"name": "test.enum2",
				"aliases": ["test.enum"],
				"symbols": ["foo", "bar"]
			}`,
			writer: `{
				"type": "enum",
				"name": "test.enum",
				"symbols": ["foo", "bar"]
			}`,
			value: "foo",
			want:  "foo",
		},
		{
			name: "Fixed With Alias",
			reader: `{
				"type": "fixed",
				"name": "test.fixed2",
				"aliases": ["test.fixed"],
				"size": 3
			}`,
			writer: `{
				"type": "fixed",
				"name": "test.fixed",
				"size": 3
			}`,
			value: [3]byte{'f', 'o', 'o'},
			want:  [3]byte{'f', 'o', 'o'},
		},
		{
			name:   "Union Match",
			reader: `["int", "long", "string"]`,
			writer: `["string", "int", "long"]`,
			value:  "foo",
			want:   "foo",
		}, {
			name:   "Union Writer Missing Schema",
			reader: `["int", "long", "string"]`,
			writer: `["string", "int"]`,
			value:  "foo",
			want:   "foo",
		},
		{
			name:   "Union Writer Not Union",
			reader: `["int", "long", "string"]`,
			writer: `"int"`,
			value:  10,
			want:   10,
		},
		{
			name:   "Union Reader Not Union",
			reader: `"int"`,
			writer: `["int"]`,
			value:  10,
			want:   10,
		},
		{
			name:   "Record Reader With Alias",
			reader: `{"type":"record", "name":"test2", "aliases": ["test"], "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10},
		},
		{
			name:   "Record Reader Field Missing",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10, "b": "foo"},
			want:   map[string]any{"a": 10},
		},
		{
			name:   "Record Writer Field Missing With Default",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string", "default": "test"}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": "test"},
		},
		{
			name:   "Record Reader Field With Alias",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "aa", "type": "int", "aliases": ["a"]}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"aa": 10},
		},
		{
			name:   "Record Reader Field With Alias And Promotion",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "aa", "type": "double", "aliases": ["a"]}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"aa": float64(10)},
		},
		{
			name:   "Record Writer Field Missing With Bytes Default",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "bytes", "default":"\u0066\u006f\u006f"}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": []byte("foo")},
		},
		{
			name:   "Record Writer Field Missing With Bytes Default",
			reader: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "bytes", "default":"\u0066\u006f\u006f"}]}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": []byte("foo")},
		},
		{
			name: "Record Writer Field Missing With Record Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
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
								"default":{"a":"foo", "b": "bar"}
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": map[string]any{"a": "foo", "b": "bar"}},
		},
		{
			// assert that we are not mistakenly using the wrong cached decoder.
			// decoder cache must be aware of fields defaults.
			name: "Record Writer Field Missing With Record Default 2",
			reader: `{
						"type":"record", 
						"name":"test", 
						"namespace": "org.hamba.avro",
						"fields":[
							{"name": "a", "type": "int"},
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
								"default":{"a":"foo 2", "b": "bar 2"}
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": map[string]any{"a": "foo 2", "b": "bar 2"}},
		},
		{
			name: "Record Writer Field Missing With Map Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type": {
									"type": "map", "values": "string"
								},
								"default":{"foo":"bar"}
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": map[string]any{"foo": "bar"}},
		},
		{
			name: "Record Writer Field Missing With Array Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type": {
									"type": "array", "items": "int"
								},
								"default":[1, 2, 3, 4]
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": []any{1, 2, 3, 4}},
		},
		{
			name: "Record Writer Field Missing With Union Null Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type":["null", "long"],
								"default": null
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": nil},
		},
		{
			name: "Record Writer Field Missing With Union Non-null Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
							{
								"name": "b",
								"type":["string", "long"],
								"default": "bar"
							}
						]
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want:   map[string]any{"a": 10, "b": "bar"},
		},
		{
			name: "Record Writer Field Missing With Fixed Duration Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
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
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want: map[string]any{
				"a": 10,
				"b": avro.LogicalDuration{
					Months:       uint32(12),
					Days:         uint32(34),
					Milliseconds: uint32(567890),
				},
			},
		},
		{
			name: "Record Writer Field Missing With Fixed Logical Decimal Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
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
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want: map[string]any{
				"a": 10,
				"b": big.NewRat(1734, 5),
			},
		},
		{
			name: "Record Writer Field Missing With Enum Duration Default",
			reader: `{
						"type":"record", "name":"test", "namespace": "org.hamba.avro", 
						"fields":[
							{"name": "a", "type": "int"},
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
					}`,
			writer: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int"}]}`,
			value:  map[string]any{"a": 10},
			want: map[string]any{
				"a": 10,
				"b": "bar",
			},
		},
		{
			name: "Record Writer Field Missing With Ref Default",
			reader: `{
				"type": "record",
				"name": "parent",
				"namespace": "org.hamba.avro",
				"fields": [
					{
						"name": "a",
						"type": {
							"type": "record",
							"name": "embed",
							"namespace": "org.hamba.avro",
							"fields": [{
								"name": "a",
								"type": "long"
							}]
						}
					},
					{
						"name": "b",
						"type": "embed",
						"default": {"a": 20}
					}
				]
			}`,
			writer: `{
				"type": "record",
				"name": "parent",
				"namespace": "org.hamba.avro",
				"fields": [
					{
						"name": "a",
						"type": {
							"type": "record",
							"name": "embed",
							"namespace": "org.hamba.avro",
							"fields": [{
								"name": "a",
								"type": "long"
							}]
						}
					}
				]
			}`,
			value: map[string]any{"a": map[string]any{"a": int64(10)}},
			want: map[string]any{
				"a": map[string]any{"a": int64(10)},
				"b": map[string]any{"a": int64(20)},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			r := avro.MustParse(test.reader)
			w := avro.MustParse(test.writer)
			sc := avro.NewSchemaCompatibility()

			b, err := avro.Marshal(w, test.value)
			assert.NoError(t, err)

			sch, err := sc.Resolve(r, w)
			assert.NoError(t, err)

			var result any
			err = avro.Unmarshal(sch, b, &result)
			assert.NoError(t, err)

			assert.Equal(t, test.want, result)
		})
	}
}

func TestSchemaCompatibility_ResolveWithRefs(t *testing.T) {
	sch1 := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "string"}
		]
	}`)
	sch2 := avro.MustParse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "bytes"}
		]
	}`)

	r := avro.NewRefSchema(sch1.(*avro.RecordSchema))
	w := avro.NewRefSchema(sch2.(*avro.RecordSchema))

	sc := avro.NewSchemaCompatibility()

	value := map[string]any{"a": []byte("foo")}
	b, err := avro.Marshal(w, value)
	assert.NoError(t, err)

	sch, err := sc.Resolve(r, w)
	assert.NoError(t, err)

	var result any
	err = avro.Unmarshal(sch, b, &result)
	assert.NoError(t, err)

	want := map[string]any{"a": "foo"}
	assert.Equal(t, want, result)
}

func TestSchemaCompatibility_ResolveWithComplexUnion(t *testing.T) {
	r := avro.MustParse(`[
				{
					"type":"record",
					"name":"testA",
					"aliases": ["test1"], 
					"namespace": "org.hamba.avro", 
					"fields":[{"name": "a", "type": "long"}]
				},
				{
					"type":"record",
					"name":"testB",
					"aliases": ["test2"], 
					"namespace": "org.hamba.avro", 
					"fields":[{"name": "b", "type": "bytes"}]
				}
			]`)

	w := avro.MustParse(`{
				"type":"record",
				"name":"test2",
				"namespace": "org.hamba.avro", 
				"fields":[{"name": "b", "type": "string"}]
			}`)

	value := map[string]any{"b": "foo"}
	b, err := avro.Marshal(w, value)
	assert.NoError(t, err)

	sc := avro.NewSchemaCompatibility()
	sch, err := sc.Resolve(r, w)
	assert.NoError(t, err)

	var result any
	err = avro.Unmarshal(sch, b, &result)
	assert.NoError(t, err)

	want := map[string]any{"b": []byte("foo")}
	assert.Equal(t, want, result)
}
