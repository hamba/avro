package avro_test

import (
	"testing"

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
