package avro_test

import (
	"testing"

	"github.com/hamba/avro"
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
		wantErr bool
	}{
		{
			name:    "Primitive Matching",
			reader:  `"int"`,
			writer:  `"int"`,
			wantErr: false,
		},
		{
			name:    "Union Match",
			reader:  `["int", "long", "string"]`,
			writer:  `["string", "int", "long"]`,
			wantErr: false,
		},
		{
			name:    "Union Reader Missing Schema",
			reader:  `["int", "string"]`,
			writer:  `["string", "int", "long"]`,
			wantErr: true,
		},
		{
			name:    "Union Writer Missing Schema",
			reader:  `["int", "long", "string"]`,
			writer:  `["string", "int"]`,
			wantErr: false,
		},
		{
			name:    "Union Writer Not Union",
			reader:  `["int", "long", "string"]`,
			writer:  `"int"`,
			wantErr: false,
		},
		{
			name:    "Union Writer Not Union With Error",
			reader:  `["string"]`,
			writer:  `"int"`,
			wantErr: true,
		},
		{
			name:    "Union Reader Not Union",
			reader:  `"int"`,
			writer:  `["int"]`,
			wantErr: false,
		},
		{
			name:    "Union Reader Not Union With Error",
			reader:  `"int"`,
			writer:  `["string", "int", "long"]`,
			wantErr: true,
		},
		{
			name:    "Array Match",
			reader:  `{"type":"array", "items": "int"}`,
			writer:  `{"type":"array", "items": "int"}`,
			wantErr: false,
		},
		{
			name:    "Array Items Mismatch",
			reader:  `{"type":"array", "items": "int"}`,
			writer:  `{"type":"array", "items": "string"}`,
			wantErr: true,
		},
		{
			name:    "Map Match",
			reader:  `{"type":"map", "values": "int"}`,
			writer:  `{"type":"map", "values": "int"}`,
			wantErr: false,
		},
		{
			name:    "Map Items Mismatch",
			reader:  `{"type":"map", "values": "int"}`,
			writer:  `{"type":"map", "values": "string"}`,
			wantErr: true,
		},
		{
			name:    "Fixed Match",
			reader:  `{"type":"fixed", "name":"test", "namespace": "org.apache.avro", "size": 12}`,
			writer:  `{"type":"fixed", "name":"test", "namespace": "org.apache.avro", "size": 12}`,
			wantErr: false,
		},
		{
			name:    "Fixed Name Mismatch",
			reader:  `{"type":"fixed", "name":"test1", "namespace": "org.apache.avro", "size": 12}`,
			writer:  `{"type":"fixed", "name":"test", "namespace": "org.apache.avro", "size": 12}`,
			wantErr: true,
		},
		{
			name:    "Fixed Size Mismatch",
			reader:  `{"type":"fixed", "name":"test", "namespace": "org.apache.avro", "size": 13}`,
			writer:  `{"type":"fixed", "name":"test", "namespace": "org.apache.avro", "size": 12}`,
			wantErr: true,
		},
		{
			name:    "Enum Match",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":["TEST1", "TEST2"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: false,
		},
		{
			name:    "Enum Name Mismatch",
			reader:  `{"type":"enum", "name":"test1", "namespace": "org.apache.avro", "symbols":["TEST1", "TEST2"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: true,
		},
		{
			name:    "Enum Reader Missing Symbol",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":["TEST1"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":["TEST1", "TEST2"]}`,
			wantErr: true,
		},
		{
			name:    "Enum Writer Missing Symbol",
			reader:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":["TEST1", "TEST2"]}`,
			writer:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":["TEST1"]}`,
			wantErr: false,
		},
		{
			name:    "Record Match",
			reader:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			wantErr: false,
		},
		{
			name:    "Record Name Mismatch",
			reader:  `{"type":"record", "name":"test1", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": "int", "default": 1}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "b", "type": "string", "default": "b"}, {"name": "a", "type": "int"}]}`,
			wantErr: true,
		},
		{
			name:    "Record Schema Mismatch",
			reader:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			wantErr: true,
		},
		{
			name:    "Record Reader Field Missing",
			reader:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": "int"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "b", "type": "string"}, {"name": "a", "type": "int"}]}`,
			wantErr: false,
		},
		{
			name:    "Record Writer Field Missing With Default",
			reader:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string", "default": "test"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": "int"}]}`,
			wantErr: false,
		},
		{
			name:    "Record Writer Field Missing Without Default",
			reader:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": "int"}]}`,
			wantErr: true,
		},
		{
			name:    "Ref Dereference",
			reader:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": {"type":"record", "name":"test1", "namespace": "org.apache.avro", "fields":[{"name": "b", "type": "int"}]}}, {"name": "b", "type": "test1"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": {"type":"record", "name":"test1", "namespace": "org.apache.avro", "fields":[{"name": "b", "type": "int"}]}}, {"name": "b", "type": "test"}]}`,
			wantErr: true,
		},
		{
			name:    "Breaks Recursion",
			reader:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": "test"}]}`,
			writer:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "a", "type": "test"}]}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := avro.MustParse(tt.reader)
			w := avro.MustParse(tt.writer)
			sc := avro.NewSchemaCompatibility()

			err := sc.Compatible(r, w)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestSchemaCompatibility_CompatibleUsesCacheWithNoError(t *testing.T) {
	reader :=  `"int"`
	writer :=  `"int"`

	r := avro.MustParse(reader)
	w := avro.MustParse(writer)
	sc := avro.NewSchemaCompatibility()

	_ = sc.Compatible(r, w)

	err := sc.Compatible(r, w)

	assert.NoError(t, err)
}

func TestSchemaCompatibility_CompatibleUsesCacheWithError(t *testing.T) {
	reader :=  `"int"`
	writer :=  `"string"`

	r := avro.MustParse(reader)
	w := avro.MustParse(writer)
	sc := avro.NewSchemaCompatibility()

	_ = sc.Compatible(r, w)

	err := sc.Compatible(r, w)

	assert.Error(t, err)
}
