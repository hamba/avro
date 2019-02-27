package avro_test

import (
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestParse_InvalidType(t *testing.T) {
	schemas := []string{
		`123`,
		`{"type": 123}`,
	}

	for _, schm := range schemas {
		_, err := avro.Parse(schm)

		assert.Error(t, err)
	}
}

func TestMustParse(t *testing.T) {
	s := avro.MustParse("null")

	assert.Equal(t, avro.Null, s.Type())
}

func TestMustParse_PanicsOnError(t *testing.T) {
	assert.Panics(t, func() {
		avro.MustParse("123")
	})
}

func TestParseFiles(t *testing.T) {
	s, err := avro.ParseFiles("testdata/schema.avsc")

	assert.NoError(t, err)
	assert.Equal(t, avro.String, s.Type())
}

func TestParseFiles_FileDoesntExist(t *testing.T) {
	_, err := avro.ParseFiles("test.something")

	assert.Error(t, err)
}

func TestParseFiles_InvalidSchema(t *testing.T) {
	_, err := avro.ParseFiles("testdata/bad-schema.avsc")

	assert.Error(t, err)
}

func TestNullSchema(t *testing.T) {
	schemas := []string{
		`null`,
		`{"type":"null"}`,
	}

	for _, schm := range schemas {
		s, err := avro.Parse(schm)

		assert.NoError(t, err)
		assert.Equal(t, avro.Null, s.Type())
	}
}

func TestPrimitiveSchema(t *testing.T) {
	tests := []struct {
		schema string
		want   avro.Type
	}{
		{
			schema: "string",
			want:   avro.String,
		},
		{
			schema: `{"type":"string"}`,
			want:   avro.String,
		},
		{
			schema: "bytes",
			want:   avro.Bytes,
		},
		{
			schema: `{"type":"bytes"}`,
			want:   avro.Bytes,
		},
		{
			schema: "int",
			want:   avro.Int,
		},
		{
			schema: `{"type":"int"}`,
			want:   avro.Int,
		},
		{
			schema: "long",
			want:   avro.Long,
		},
		{
			schema: `{"type":"long"}`,
			want:   avro.Long,
		},
		{
			schema: "float",
			want:   avro.Float,
		},
		{
			schema: `{"type":"float"}`,
			want:   avro.Float,
		},
		{
			schema: "double",
			want:   avro.Double,
		},
		{
			schema: `{"type":"double"}`,
			want:   avro.Double,
		},
		{
			schema: "boolean",
			want:   avro.Boolean,
		},
		{
			schema: `{"type":"boolean"}`,
			want:   avro.Boolean,
		},
	}

	for _, tt := range tests {
		t.Run(tt.schema, func(t *testing.T) {
			s, err := avro.Parse(tt.schema)

			assert.NoError(t, err)
			assert.Equal(t, tt.want, s.Type())
		})
	}
}

func TestRecordSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{
			name:    "Valid",
			schema:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "doc": "docs", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: false,
		},
		{
			name:    "Full Name",
			schema:  `{"type":"record", "name":"org.apache.avro.test", "doc": "docs", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: false,
		},
		{
			name:    "Invalid Name",
			schema:  `{"type":"record", "name":"test+", "namespace": "org.apache.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: true,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"record", "name":"", "namespace": "org.apache.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: true,
		},
		{
			name:    "No Name",
			schema:  `{"type":"record", "namespace": "org.apache.avro", "fields":[{"name": "intField", "type": "int"}]}`,
			wantErr: true,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"record", "name":"test", "namespace": "org.apache.avro+", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: true,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"record", "name":"test", "namespace": "", "fields":[{"name": "intField", "type": "int"}]}`,
			wantErr: true,
		},
		{
			name:    "No Fields",
			schema:  `{"type":"record", "name":"test", "namespace": "org.apache.avro"}`,
			wantErr: true,
		},
		{
			name:    "Invalid Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":["test"]}`,
			wantErr: true,
		},
		{
			name:    "No Field Name",
			schema:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"type": "int"}]}`,
			wantErr: true,
		},
		{
			name:    "Invalid Field Name",
			schema:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "field+", "type": "int"}]}`,
			wantErr: true,
		},
		{
			name:    "No Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "field"}]}`,
			wantErr: true,
		},
		{
			name:    "Invalid Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "field", "type": "blah"}]}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := avro.Parse(tt.schema)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, avro.Record, s.Type())
		})
	}
}

func TestRecordSchema_Default(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   interface{}
	}{
		{
			name:   "Normal",
			schema: `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "field", "type": "string", "default": "test"}]}`,
			want:   "test",
		},
		{
			name:   "Int",
			schema: `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "field", "type": "int", "default": 1}]}`,
			want:   int32(1),
		},
		{
			name:   "Long",
			schema: `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "field", "type": "long", "default": 1}]}`,
			want:   int64(1),
		},
		{
			name:   "Float",
			schema: `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "field", "type": "float", "default": 1}]}`,
			want:   float32(1),
		},
		{
			name:   "Double",
			schema: `{"type":"record", "name":"test", "namespace": "org.apache.avro", "fields":[{"name": "field", "type": "double", "default": 1}]}`,
			want:   float64(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := avro.Parse(tt.schema)

			assert.NoError(t, err)
			assert.Equal(t, tt.want, s.(*avro.RecordSchema).Fields()[0].Default())
		})
	}
}

func TestRecordSchema_WithReference(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "valid_name",
   "namespace": "org.apache.avro",
   "fields": [
       {
           "name": "intField",
           "type": "int"
       },
       {
           "name": "Ref",
           "type": "valid_name"
       }
   ]
}
`

	s, err := avro.Parse(schm)

	assert.NoError(t, err)
	assert.Equal(t, avro.Record, s.Type())
	assert.Equal(t, avro.Ref, s.(*avro.RecordSchema).Fields()[1].Type().Type())
}

func TestEnumSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{
			name:    "Valid",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":["TEST"]}`,
			wantErr: false,
		},
		{
			name:    "Invalid Name",
			schema:  `{"type":"enum", "name":"test+", "namespace": "org.apache.avro", "symbols":["TEST"]}`,
			wantErr: true,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"enum", "name":"", "namespace": "org.apache.avro", "symbols":["TEST"]}`,
			wantErr: true,
		},
		{
			name:    "No Name",
			schema:  `{"type":"enum", "namespace": "org.apache.avro", "symbols":["TEST"]}`,
			wantErr: true,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro+", "symbols":["TEST"]}`,
			wantErr: true,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"enum", "name":"test", "namespace": "", "symbols":["TEST"]}`,
			wantErr: true,
		},
		{
			name:    "No Symbols",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro"}`,
			wantErr: true,
		},
		{
			name:    "Empty Symbols",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":[]}`,
			wantErr: true,
		},
		{
			name:    "Invalid Symbol",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":["TEST+"]}`,
			wantErr: true,
		},
		{
			name:    "Invalid Symbol Type",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.apache.avro", "symbols":[1]}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := avro.Parse(tt.schema)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, avro.Enum, s.Type())
		})
	}
}

func TestArraySchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{
			name:    "Valid",
			schema:  `{"type":"array", "items": "int"}`,
			wantErr: false,
		},
		{
			name:    "No Items",
			schema:  `{"type":"array"}`,
			wantErr: true,
		},
		{
			name:    "Invalid Items Type",
			schema:  `{"type":"array", "items": "blah"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := avro.Parse(tt.schema)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, avro.Array, s.Type())
		})
	}
}

func TestMapSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{
			name:    "Valid",
			schema:  `{"type":"map", "values": "int"}`,
			wantErr: false,
		},
		{
			name:    "No Values",
			schema:  `{"type":"map"}`,
			wantErr: true,
		},
		{
			name:    "Invalid Values Type",
			schema:  `{"type":"map", "values": "blah"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := avro.Parse(tt.schema)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, avro.Map, s.Type())
		})
	}
}

func TestUnionSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{
			name:    "Valid Simple",
			schema:  `["null", "int"]`,
			wantErr: false,
		},
		{
			name:    "Valid Complex",
			schema:  `{"type":["null", "int"]}`,
			wantErr: false,
		},
		{
			name:    "Invalid Type",
			schema:  `["null", "blah"]`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := avro.Parse(tt.schema)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, avro.Union, s.Type())
		})
	}
}

func TestFixedSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{
			name:    "Valid",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.apache.avro", "size": 12}`,
			wantErr: false,
		},
		{
			name:    "Invalid Name",
			schema:  `{"type":"fixed", "name":"test+", "namespace": "org.apache.avro", "size": 12}`,
			wantErr: true,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"fixed", "name":"", "namespace": "org.apache.avro", "size": 12}`,
			wantErr: true,
		},
		{
			name:    "No Name",
			schema:  `{"type":"fixed", "namespace": "org.apache.avro", "size": 12}`,
			wantErr: true,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.apache.avro+", "size": 12}`,
			wantErr: true,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"fixed", "name":"test", "namespace": "", "size": 12}`,
			wantErr: true,
		},
		{
			name:    "No Size",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.apache.avro"}`,
			wantErr: true,
		},
		{
			name:    "Invalid Size Type",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.apache.avro", "size": "test"}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := avro.Parse(tt.schema)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, avro.Fixed, s.Type())
		})
	}
}

func TestSchema_Interop(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "Interop",
   "namespace": "org.apache.avro",
   "fields": [
       {
           "name": "intField",
           "type": "int"
       },
       {
           "name": "longField",
           "type": "long"
       },
       {
           "name": "stringField",
           "type": "string"
       },
       {
           "name": "boolField",
           "type": "boolean"
       },
       {
           "name": "floatField",
           "type": "float"
       },
       {
           "name": "doubleField",
           "type": "double"
       },
       {
           "name": "bytesField",
           "type": "bytes"
       },
       {
           "name": "nullField",
           "type": "null"
       },
       {
           "name": "arrayField",
           "type": {
               "type": "array",
               "items": "double"
           }
       },
       {
           "name": "mapField",
           "type": {
               "type": "map",
               "values": {
                   "type": "record",
                   "name": "Foo",
                   "fields": [
                       {
                           "name": "label",
                           "type": "string"
                       }
                   ]
               }
           }
       },
       {
           "name": "unionField",
           "type": [
               "boolean",
               "double",
               {
                   "type": "array",
                   "items": "bytes"
               }
           ]
       },
       {
           "name": "enumField",
           "type": {
               "type": "enum",
               "name": "Kind",
               "symbols": [
                   "A",
                   "B",
                   "C"
               ]
           }
       },
       {
           "name": "fixedField",
           "type": {
               "type": "fixed",
               "name": "MD5",
               "size": 16
           }
       },
       {
           "name": "recordField",
           "type": {
               "type": "record",
               "name": "Node",
               "fields": [
                   {
                       "name": "label",
                       "type": "string"
                   },
                   {
                       "name": "child",
                       "type": {"type": "org.apache.avro.Node"}
                   },
                   {
                       "name": "children",
                       "type": {
                           "type": "array",
                           "items": "Node"
                       }
                   }
               ]
           }
       }
   ]
}`

	_, err := avro.Parse(schm)

	assert.NoError(t, err)
}
