package avro_test

import (
	"errors"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	require.NoError(t, err)
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
		schema, err := avro.Parse(schm)

		require.NoError(t, err)
		assert.Equal(t, avro.Null, schema.Type())
		want := [32]byte{0xf0, 0x72, 0xcb, 0xec, 0x3b, 0xf8, 0x84, 0x18, 0x71, 0xd4, 0x28, 0x42, 0x30, 0xc5, 0xe9, 0x83, 0xdc, 0x21, 0x1a, 0x56, 0x83, 0x7a, 0xed, 0x86, 0x24, 0x87, 0x14, 0x8f, 0x94, 0x7d, 0x1a, 0x1f}
		assert.Equal(t, want, schema.Fingerprint())
	}
}

func TestPrimitiveSchema(t *testing.T) {
	tests := []struct {
		schema          string
		want            avro.Type
		wantFingerprint [32]byte
	}{
		{
			schema:          "string",
			want:            avro.String,
			wantFingerprint: [32]byte{0xe9, 0xe5, 0xc1, 0xc9, 0xe4, 0xf6, 0x27, 0x73, 0x39, 0xd1, 0xbc, 0xde, 0x7, 0x33, 0xa5, 0x9b, 0xd4, 0x2f, 0x87, 0x31, 0xf4, 0x49, 0xda, 0x6d, 0xc1, 0x30, 0x10, 0xa9, 0x16, 0x93, 0xd, 0x48},
		},
		{
			schema:          `{"type":"string"}`,
			want:            avro.String,
			wantFingerprint: [32]byte{0xe9, 0xe5, 0xc1, 0xc9, 0xe4, 0xf6, 0x27, 0x73, 0x39, 0xd1, 0xbc, 0xde, 0x7, 0x33, 0xa5, 0x9b, 0xd4, 0x2f, 0x87, 0x31, 0xf4, 0x49, 0xda, 0x6d, 0xc1, 0x30, 0x10, 0xa9, 0x16, 0x93, 0xd, 0x48},
		},
		{
			schema:          "bytes",
			want:            avro.Bytes,
			wantFingerprint: [32]byte{0x9a, 0xe5, 0x7, 0xa9, 0xdd, 0x39, 0xee, 0x5b, 0x7c, 0x7e, 0x28, 0x5d, 0xa2, 0xc0, 0x84, 0x65, 0x21, 0xc8, 0xae, 0x8d, 0x80, 0xfe, 0xea, 0xe5, 0x50, 0x4e, 0xc, 0x98, 0x1d, 0x53, 0xf5, 0xfa},
		},
		{
			schema:          `{"type":"bytes"}`,
			want:            avro.Bytes,
			wantFingerprint: [32]byte{0x9a, 0xe5, 0x7, 0xa9, 0xdd, 0x39, 0xee, 0x5b, 0x7c, 0x7e, 0x28, 0x5d, 0xa2, 0xc0, 0x84, 0x65, 0x21, 0xc8, 0xae, 0x8d, 0x80, 0xfe, 0xea, 0xe5, 0x50, 0x4e, 0xc, 0x98, 0x1d, 0x53, 0xf5, 0xfa},
		},
		{
			schema:          "int",
			want:            avro.Int,
			wantFingerprint: [32]byte{0x3f, 0x2b, 0x87, 0xa9, 0xfe, 0x7c, 0xc9, 0xb1, 0x38, 0x35, 0x59, 0x8c, 0x39, 0x81, 0xcd, 0x45, 0xe3, 0xe3, 0x55, 0x30, 0x9e, 0x50, 0x90, 0xaa, 0x9, 0x33, 0xd7, 0xbe, 0xcb, 0x6f, 0xba, 0x45},
		},
		{
			schema:          `{"type":"int"}`,
			want:            avro.Int,
			wantFingerprint: [32]byte{0x3f, 0x2b, 0x87, 0xa9, 0xfe, 0x7c, 0xc9, 0xb1, 0x38, 0x35, 0x59, 0x8c, 0x39, 0x81, 0xcd, 0x45, 0xe3, 0xe3, 0x55, 0x30, 0x9e, 0x50, 0x90, 0xaa, 0x9, 0x33, 0xd7, 0xbe, 0xcb, 0x6f, 0xba, 0x45},
		},
		{
			schema:          "long",
			want:            avro.Long,
			wantFingerprint: [32]byte{0xc3, 0x2c, 0x49, 0x7d, 0xf6, 0x73, 0xc, 0x97, 0xfa, 0x7, 0x36, 0x2a, 0xa5, 0x2, 0x3f, 0x37, 0xd4, 0x9a, 0x2, 0x7e, 0xc4, 0x52, 0x36, 0x7, 0x78, 0x11, 0x4c, 0xf4, 0x27, 0x96, 0x5a, 0xdd},
		},
		{
			schema:          `{"type":"long"}`,
			want:            avro.Long,
			wantFingerprint: [32]byte{0xc3, 0x2c, 0x49, 0x7d, 0xf6, 0x73, 0xc, 0x97, 0xfa, 0x7, 0x36, 0x2a, 0xa5, 0x2, 0x3f, 0x37, 0xd4, 0x9a, 0x2, 0x7e, 0xc4, 0x52, 0x36, 0x7, 0x78, 0x11, 0x4c, 0xf4, 0x27, 0x96, 0x5a, 0xdd},
		},
		{
			schema:          "float",
			want:            avro.Float,
			wantFingerprint: [32]byte{0x1e, 0x71, 0xf9, 0xec, 0x5, 0x1d, 0x66, 0x3f, 0x56, 0xb0, 0xd8, 0xe1, 0xfc, 0x84, 0xd7, 0x1a, 0xa5, 0x6c, 0xcf, 0xe9, 0xfa, 0x93, 0xaa, 0x20, 0xd1, 0x5, 0x47, 0xa7, 0xab, 0xeb, 0x5c, 0xc0},
		},
		{
			schema:          `{"type":"float"}`,
			want:            avro.Float,
			wantFingerprint: [32]byte{0x1e, 0x71, 0xf9, 0xec, 0x5, 0x1d, 0x66, 0x3f, 0x56, 0xb0, 0xd8, 0xe1, 0xfc, 0x84, 0xd7, 0x1a, 0xa5, 0x6c, 0xcf, 0xe9, 0xfa, 0x93, 0xaa, 0x20, 0xd1, 0x5, 0x47, 0xa7, 0xab, 0xeb, 0x5c, 0xc0},
		},
		{
			schema:          "double",
			want:            avro.Double,
			wantFingerprint: [32]byte{0x73, 0xa, 0x9a, 0x8c, 0x61, 0x16, 0x81, 0xd7, 0xee, 0xf4, 0x42, 0xe0, 0x3c, 0x16, 0xc7, 0xd, 0x13, 0xbc, 0xa3, 0xeb, 0x8b, 0x97, 0x7b, 0xb4, 0x3, 0xea, 0xff, 0x52, 0x17, 0x6a, 0xf2, 0x54},
		},
		{
			schema:          `{"type":"double"}`,
			want:            avro.Double,
			wantFingerprint: [32]byte{0x73, 0xa, 0x9a, 0x8c, 0x61, 0x16, 0x81, 0xd7, 0xee, 0xf4, 0x42, 0xe0, 0x3c, 0x16, 0xc7, 0xd, 0x13, 0xbc, 0xa3, 0xeb, 0x8b, 0x97, 0x7b, 0xb4, 0x3, 0xea, 0xff, 0x52, 0x17, 0x6a, 0xf2, 0x54},
		},
		{
			schema:          "boolean",
			want:            avro.Boolean,
			wantFingerprint: [32]byte{0xa5, 0xb0, 0x31, 0xab, 0x62, 0xbc, 0x41, 0x6d, 0x72, 0xc, 0x4, 0x10, 0xd8, 0x2, 0xea, 0x46, 0xb9, 0x10, 0xc4, 0xfb, 0xe8, 0x5c, 0x50, 0xa9, 0x46, 0xcc, 0xc6, 0x58, 0xb7, 0x4e, 0x67, 0x7e},
		},
		{
			schema:          `{"type":"boolean"}`,
			want:            avro.Boolean,
			wantFingerprint: [32]byte{0xa5, 0xb0, 0x31, 0xab, 0x62, 0xbc, 0x41, 0x6d, 0x72, 0xc, 0x4, 0x10, 0xd8, 0x2, 0xea, 0x46, 0xb9, 0x10, 0xc4, 0xfb, 0xe8, 0x5c, 0x50, 0xa9, 0x46, 0xcc, 0xc6, 0x58, 0xb7, 0x4e, 0x67, 0x7e},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.schema, func(t *testing.T) {
			s, err := avro.Parse(test.schema)

			require.NoError(t, err)
			assert.Equal(t, test.want, s.Type())
			assert.Equal(t, test.wantFingerprint, s.Fingerprint())
		})
	}
}

func TestPrimitiveSchema_HandlesProps(t *testing.T) {
	schm := `
{
   "type": "string",
   "foo": "bar",
   "baz": 1
}
`

	s, err := avro.Parse(schm)

	assert.NoError(t, err)
	assert.Equal(t, avro.String, s.Type())
	assert.Equal(t, "bar", s.(*avro.PrimitiveSchema).Prop("foo"))
	assert.Equal(t, float64(1), s.(*avro.PrimitiveSchema).Prop("baz"))
}

func TestRecordSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "Valid",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "doc": "docs", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.NoError,
		},
		{
			name:    "Invalid Name First Char",
			schema:  `{"type":"record", "name":"0test", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Name Other Char",
			schema:  `{"type":"record", "name":"test+", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"record", "name":"", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Name",
			schema:  `{"type":"record", "namespace": "org.hamba.avro", "fields":[{"name": "intField", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro+", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"record", "name":"test", "namespace": "", "fields":[{"name": "intField", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Fields",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro"}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":["test"]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Field Name",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Field Name",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "field+", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Alias",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "field", "aliases": ["test+"], "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "field"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "blah"}]}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			s, err := avro.Parse(test.schema)

			test.wantErr(t, err)
			if s != nil {
				assert.Equal(t, avro.Record, s.Type())
			}
		})
	}
}

func TestErrorRecordSchema(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		wantSchema bool
		wantErr    require.ErrorAssertionFunc
	}{
		{
			name:       "Valid",
			schema:     `{"type":"error", "name":"test", "namespace": "org.hamba.avro", "doc": "docs", "fields":[{"name": "field", "type": "int"}]}`,
			wantSchema: true,
			wantErr:    require.NoError,
		},
		{
			name:    "Invalid Name First Char",
			schema:  `{"type":"error", "name":"0test", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Name Other Char",
			schema:  `{"type":"error", "name":"test+", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"error", "name":"", "namespace": "org.hamba.avro", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Name",
			schema:  `{"type":"error", "namespace": "org.hamba.avro", "fields":[{"name": "intField", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"error", "name":"test", "namespace": "org.hamba.avro+", "fields":[{"name": "field", "type": "int"}]}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"error", "name":"test", "namespace": "", "fields":[{"name": "intField", "type": "int"}]}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {

			s, err := avro.Parse(test.schema)

			test.wantErr(t, err)
			if test.wantSchema {
				assert.Equal(t, avro.Record, s.Type())
				recSchema := s.(*avro.RecordSchema)
				assert.True(t, recSchema.IsError())
			}
		})
	}
}

func TestRecordSchema_ValidatesDefault(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "String",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "string", "default": "test"}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Int",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "int", "default": 1}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Long",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "long", "default": 1}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Float",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "float", "default": 1}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Double",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": "double", "default": 1}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Array",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"array", "items": "int"}, "default": [1,2]}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Array Not Array",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"array", "items": "int"}, "default": "test"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Array Invalid Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"array", "items": "int"}, "default": ["test"]}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Map",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"map", "values": "int"}, "default": {"b": 1}}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Map Not Map",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"map", "values": "int"}, "default": "test"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Map Invalid Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"map", "values": "int"}, "default": {"b": "test"}}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Union",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": ["string", "null"]}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Default",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": ["null", "string"], "default": null}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Union Invalid Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": ["null", "string"], "default": "string"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name": "test2", "fields":[{"name": "b", "type": "int"},{"name": "c", "type": "int", "default": 1}]}, "default": {"b": 1}}]}`,
			wantErr: assert.NoError,
		},
		{
			name:    "Record Not Map",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name": "test2", "fields":[{"name": "b", "type": "int"},{"name": "c", "type": "int", "default": 1}]}, "default": "test"}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record Invalid Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name": "test2", "fields":[{"name": "b", "type": "int"},{"name": "c", "type": "int", "default": 1}]}, "default": {"b": "test"}}]}`,
			wantErr: assert.Error,
		},
		{
			name:    "Record Invalid Field Type",
			schema:  `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": {"type":"record", "name": "test2", "fields":[{"name": "b", "type": "int"},{"name": "c", "type": "int", "default": "test"}]}, "default": {"b": 1}}]}`,
			wantErr: assert.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := avro.Parse(test.schema)

			test.wantErr(t, err)
		})
	}
}

func TestRecordSchema_ValidatesOrder(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		want    avro.Order
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "empty",
			schema:  `{"type":"record", "name":"test", "fields":[{"name": "a", "type": "string"}]}`,
			want:    avro.Asc,
			wantErr: assert.NoError,
		},
		{
			name:    "asc",
			schema:  `{"type":"record", "name":"test", "fields":[{"name": "a", "type": "string", "order": "ascending"}]}`,
			want:    avro.Asc,
			wantErr: assert.NoError,
		},
		{
			name:    "desc",
			schema:  `{"type":"record", "name":"test", "fields":[{"name": "a", "type": "string", "order": "descending"}]}`,
			want:    avro.Desc,
			wantErr: assert.NoError,
		},
		{
			name:    "ignore",
			schema:  `{"type":"record", "name":"test", "fields":[{"name": "a", "type": "string", "order": "ignore"}]}`,
			want:    avro.Ignore,
			wantErr: assert.NoError,
		},
		{
			name:    "invalid",
			schema:  `{"type":"record", "name":"test", "fields":[{"name": "a", "type": "string", "order": "blah"}]}`,
			wantErr: assert.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			s, err := avro.Parse(test.schema)

			test.wantErr(t, err)
			if test.want != "" {
				rs := s.(*avro.RecordSchema)
				require.Len(t, rs.Fields(), 1)
				assert.Equal(t, test.want, rs.Fields()[0].Order())
			}
		})
	}
}

func TestRecordSchema_HandlesProps(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "valid_name",
   "namespace": "org.hamba.avro",
   "doc": "foo",
   "foo": "bar1",
   "fields": [
       {"name": "intField", "doc": "bar", "type": "int", "foo": "bar2"}
   ]
}
`

	s, err := avro.Parse(schm)
	require.NoError(t, err)

	rs := s.(*avro.RecordSchema)
	assert.Equal(t, avro.Record, s.Type())
	assert.Equal(t, "foo", rs.Doc())
	assert.Equal(t, "bar1", rs.Prop("foo"))
	require.Len(t, rs.Fields(), 1)
	assert.Equal(t, "bar", rs.Fields()[0].Doc())
	assert.Equal(t, "bar2", rs.Fields()[0].Prop("foo"))
}

func TestRecordSchema_WithReference(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "valid_name",
   "namespace": "org.hamba.avro",
   "fields": [
       {"name": "intField", "type": "int"},
       {"name": "Ref", "type": "valid_name"}
   ]
}
`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Record, s.Type())
	assert.Equal(t, avro.Ref, s.(*avro.RecordSchema).Fields()[1].Type().Type())
	assert.Equal(t, s.Fingerprint(), s.(*avro.RecordSchema).Fields()[1].Type().Fingerprint())
}

func TestRecordSchema_WithAliasReference(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "valid_name",
   "namespace": "org.hamba.avro",
   "aliases": ["valid_alias"],
   "fields": [
       {"name": "intField", "type": "int"},
       {"name": "ref", "type": "valid_alias"}
   ]
}
`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Record, s.Type())
	assert.Equal(t, avro.Ref, s.(*avro.RecordSchema).Fields()[1].Type().Type())
	assert.Equal(t, s.Fingerprint(), s.(*avro.RecordSchema).Fields()[1].Type().Fingerprint())
}

func TestEnumSchema(t *testing.T) {
	tests := []struct {
		name        string
		schema      string
		wantName    string
		wantDefault string
		wantErr     require.ErrorAssertionFunc
	}{
		{
			name:     "Valid",
			schema:   `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST"]}`,
			wantName: "org.hamba.avro.test",
			wantErr:  require.NoError,
		},
		{
			name:        "Valid With Default",
			schema:      `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST"], "default": "TEST"}`,
			wantName:    "org.hamba.avro.test",
			wantDefault: "TEST",
			wantErr:     require.NoError,
		},
		{
			name:    "Invalid Name",
			schema:  `{"type":"enum", "name":"test+", "namespace": "org.hamba.avro", "symbols":["TEST"]}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"enum", "name":"", "namespace": "org.hamba.avro", "symbols":["TEST"]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Name",
			schema:  `{"type":"enum", "namespace": "org.hamba.avro", "symbols":["TEST"]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro+", "symbols":["TEST"]}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"enum", "name":"test", "namespace": "", "symbols":["TEST"]}`,
			wantErr: require.Error,
		},
		{
			name:    "No Symbols",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro"}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Symbols",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":[]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Symbol",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST+"]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Symbol Type",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":[1]}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Default",
			schema:  `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST"], "default": "foo"}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			schema, err := avro.Parse(test.schema)

			test.wantErr(t, err)
			if test.wantName != "" {
				assert.Equal(t, avro.Enum, schema.Type())
				named := schema.(*avro.EnumSchema)
				assert.Equal(t, test.wantName, named.FullName())
				assert.Equal(t, test.wantDefault, named.Default())
			}
		})
	}
}

func TestEnumSchema_HandlesProps(t *testing.T) {
	schm := `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "doc": "hello", "symbols":["TEST"], "foo":"bar"}`

	s, err := avro.Parse(schm)
	require.NoError(t, err)

	es := s.(*avro.EnumSchema)
	assert.Equal(t, avro.Enum, s.Type())
	assert.Equal(t, "hello", es.Doc())
	assert.Equal(t, "bar", es.Prop("foo"))
}

func TestArraySchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		want    avro.Schema
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "Valid",
			schema:  `{"type":"array", "items": "int"}`,
			want:    avro.NewArraySchema(avro.NewPrimitiveSchema(avro.Int, nil, nil)),
			wantErr: require.NoError,
		},
		{
			name:    "No Items",
			schema:  `{"type":"array"}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Items Type",
			schema:  `{"type":"array", "items": "blah"}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := avro.Parse(test.schema)

			test.wantErr(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestArraySchema_HandlesProps(t *testing.T) {
	schm := `{"type":"array", "items": "int", "foo":"bar"}`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Array, s.Type())
	assert.Equal(t, "bar", s.(*avro.ArraySchema).Prop("foo"))
}

func TestMapSchema(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		want    avro.Schema
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "Valid",
			schema:  `{"type":"map", "values": "int"}`,
			want:    avro.NewMapSchema(avro.NewPrimitiveSchema(avro.Int, nil, nil)),
			wantErr: require.NoError,
		},
		{
			name:    "No Values",
			schema:  `{"type":"map"}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Values Type",
			schema:  `{"type":"map", "values": "blah"}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := avro.Parse(test.schema)

			test.wantErr(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestMapSchema_HandlesProps(t *testing.T) {
	schm := `{"type":"map", "values": "int", "foo":"bar"}`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Map, s.Type())
	assert.Equal(t, "bar", s.(*avro.MapSchema).Prop("foo"))
}

func TestUnionSchema(t *testing.T) {
	tests := []struct {
		name            string
		schema          string
		wantFingerprint [32]byte
		wantErr         require.ErrorAssertionFunc
	}{
		{
			name:            "Valid Simple",
			schema:          `["null", "int"]`,
			wantFingerprint: [32]byte{0xb4, 0x94, 0x95, 0xc5, 0xb1, 0xc2, 0x6f, 0x4, 0x89, 0x6a, 0x5f, 0x68, 0x65, 0xf, 0xe2, 0xb7, 0x64, 0x23, 0x62, 0xc3, 0x41, 0x98, 0xd6, 0xbc, 0x74, 0x65, 0xa1, 0xd9, 0xf7, 0xe1, 0xaf, 0xce},
			wantErr:         require.NoError,
		},
		{
			name:            "Valid Complex",
			schema:          `{"type":["null", "int"]}`,
			wantFingerprint: [32]byte{0xb4, 0x94, 0x95, 0xc5, 0xb1, 0xc2, 0x6f, 0x4, 0x89, 0x6a, 0x5f, 0x68, 0x65, 0xf, 0xe2, 0xb7, 0x64, 0x23, 0x62, 0xc3, 0x41, 0x98, 0xd6, 0xbc, 0x74, 0x65, 0xa1, 0xd9, 0xf7, 0xe1, 0xaf, 0xce},
			wantErr:         require.NoError,
		},
		{
			name:            "Dereferences Ref Schemas",
			schema:          `[{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}, {"type":"enum", "name":"test1", "namespace": "org.hamba.avro", "symbols":["TEST"]}, {"type":"record", "name":"test2", "namespace": "org.hamba.avro", "fields":[{"name": "a", "type": ["null","org.hamba.avro.test","org.hamba.avro.test1"]}]}]`,
			wantFingerprint: [32]byte{0xc1, 0x42, 0x87, 0xde, 0x24, 0x3d, 0xee, 0x1d, 0xa5, 0x47, 0xa0, 0x13, 0x9e, 0xb, 0xe0, 0x6, 0xfd, 0xa, 0x76, 0xd9, 0xe8, 0x92, 0x9a, 0xd3, 0x46, 0xf, 0xbd, 0x86, 0x21, 0x72, 0x81, 0x1b},
			wantErr:         require.NoError,
		},
		{
			name:    "No Nested Union Type",
			schema:  `["null", ["string"]]`,
			wantErr: require.Error,
		},
		{
			name:    "No Duplicate Types",
			schema:  `["string", "string"]`,
			wantErr: require.Error,
		},
		{
			name:    "No Duplicate Names",
			schema:  `[{"type":"enum", "name":"test", "symbols":["TEST"]}, {"type":"enum", "name":"test", "symbols":["TEST"]}]`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Type",
			schema:  `["null", "blah"]`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			s, err := avro.Parse(test.schema)

			test.wantErr(t, err)
			if test.wantFingerprint != [32]byte{} {
				assert.Equal(t, avro.Union, s.Type())
				assert.Equal(t, test.wantFingerprint, s.Fingerprint())
			}
		})
	}
}

func TestUnionSchema_Indices(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   [2]int
	}{
		{
			name:   "Null First",
			schema: `["null", "string"]`,
			want:   [2]int{0, 1},
		},
		{
			name:   "Null Second",
			schema: `["string", "null"]`,
			want:   [2]int{1, 0},
		},
		{
			name:   "Not Nullable",
			schema: `["null", "string", "int"]`,
			want:   [2]int{0, 0},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			s, err := avro.Parse(test.schema)

			require.NoError(t, err)
			null, typ := s.(*avro.UnionSchema).Indices()
			assert.Equal(t, test.want[0], null)
			assert.Equal(t, test.want[1], typ)
		})
	}
}

func TestFixedSchema(t *testing.T) {
	tests := []struct {
		name            string
		schema          string
		wantName        string
		wantFingerprint [32]byte
		wantErr         require.ErrorAssertionFunc
	}{
		{
			name:            "Valid",
			schema:          `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			wantName:        "org.hamba.avro.test",
			wantFingerprint: [32]uint8{0x8c, 0x9e, 0xcb, 0x4, 0x83, 0x2f, 0x3b, 0xa7, 0x58, 0x85, 0x9, 0x99, 0x41, 0xe, 0xbf, 0xd4, 0x7, 0xc7, 0x87, 0x4f, 0x8a, 0x12, 0xf4, 0xd0, 0x7f, 0x45, 0xdd, 0xaa, 0x10, 0x6b, 0x2f, 0xb3},
			wantErr:         require.NoError,
		},
		{
			name:    "Invalid Name",
			schema:  `{"type":"fixed", "name":"test+", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Name",
			schema:  `{"type":"fixed", "name":"", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: require.Error,
		},
		{
			name:    "No Name",
			schema:  `{"type":"fixed", "namespace": "org.hamba.avro", "size": 12}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro+", "size": 12}`,
			wantErr: require.Error,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"type":"fixed", "name":"test", "namespace": "", "size": 12}`,
			wantErr: require.Error,
		},
		{
			name:    "No Size",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro"}`,
			wantErr: require.Error,
		},
		{
			name:    "Invalid Size Type",
			schema:  `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": "test"}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			schema, err := avro.Parse(test.schema)

			test.wantErr(t, err)
			if test.wantFingerprint != [32]byte{} {
				assert.Equal(t, avro.Fixed, schema.Type())
				named := schema.(avro.NamedSchema)
				assert.Equal(t, test.wantName, named.FullName())
				assert.Equal(t, test.wantFingerprint, named.Fingerprint())
			}
		})
	}
}

func TestFixedSchema_HandlesProps(t *testing.T) {
	schm := `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12, "foo":"bar"}`

	s, err := avro.Parse(schm)

	require.NoError(t, err)
	assert.Equal(t, avro.Fixed, s.Type())
	assert.Equal(t, "bar", s.(*avro.FixedSchema).Prop("foo"))
}

func TestSchema_LogicalTypes(t *testing.T) {
	tests := []struct {
		name            string
		schema          string
		wantType        avro.Type
		wantLogical     bool
		wantLogicalType avro.LogicalType
		assertFn        func(t *testing.T, ls avro.LogicalSchema)
	}{
		{
			name:        "Invalid",
			schema:      `{"type": "int", "logicalType": "test"}`,
			wantType:    avro.Int,
			wantLogical: false,
		},
		{
			name:            "Date",
			schema:          `{"type": "int", "logicalType": "date"}`,
			wantType:        avro.Int,
			wantLogical:     true,
			wantLogicalType: avro.Date,
		},
		{
			name:            "Time Millis",
			schema:          `{"type": "int", "logicalType": "time-millis"}`,
			wantType:        avro.Int,
			wantLogical:     true,
			wantLogicalType: avro.TimeMillis,
		},
		{
			name:            "Time Micros",
			schema:          `{"type": "long", "logicalType": "time-micros"}`,
			wantType:        avro.Long,
			wantLogical:     true,
			wantLogicalType: avro.TimeMicros,
		},
		{
			name:            "Timestamp Millis",
			schema:          `{"type": "long", "logicalType": "timestamp-millis"}`,
			wantType:        avro.Long,
			wantLogical:     true,
			wantLogicalType: avro.TimestampMillis,
		},
		{
			name:            "Timestamp Micros",
			schema:          `{"type": "long", "logicalType": "timestamp-micros"}`,
			wantType:        avro.Long,
			wantLogical:     true,
			wantLogicalType: avro.TimestampMicros,
		},
		{
			name:            "UUID",
			schema:          `{"type": "string", "logicalType": "uuid"}`,
			wantType:        avro.String,
			wantLogical:     true,
			wantLogicalType: avro.UUID,
		},
		{
			name:            "Duration",
			schema:          `{"type": "fixed", "name":"test", "size": 12, "logicalType": "duration"}`,
			wantType:        avro.Fixed,
			wantLogical:     true,
			wantLogicalType: avro.Duration,
		},
		{
			name:        "Invalid Duration",
			schema:      `{"type": "fixed", "name":"test", "size": 11, "logicalType": "duration"}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
		{
			name:            "Bytes Decimal",
			schema:          `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}`,
			wantType:        avro.Bytes,
			wantLogical:     true,
			wantLogicalType: avro.Decimal,
			assertFn: func(t *testing.T, ls avro.LogicalSchema) {
				dec, ok := ls.(*avro.DecimalLogicalSchema)
				require.True(t, ok)
				assert.Equal(t, 4, dec.Precision())
				assert.Equal(t, 2, dec.Scale())
			},
		},
		{
			name:            "Bytes Decimal No Scale",
			schema:          `{"type": "bytes", "logicalType": "decimal", "precision": 4}`,
			wantType:        avro.Bytes,
			wantLogical:     true,
			wantLogicalType: avro.Decimal,
			assertFn: func(t *testing.T, ls avro.LogicalSchema) {
				dec, ok := ls.(*avro.DecimalLogicalSchema)
				require.True(t, ok)
				assert.Equal(t, 4, dec.Precision())
				assert.Equal(t, 0, dec.Scale())
			},
		},
		{
			name:        "Bytes Decimal Negative Precision",
			schema:      `{"type": "bytes", "logicalType": "decimal", "precision": 0}`,
			wantType:    avro.Bytes,
			wantLogical: false,
		},
		{
			name:        "Bytes Decimal Negative Scale",
			schema:      `{"type": "bytes", "logicalType": "decimal", "precision": 1, "scale": -1}`,
			wantType:    avro.Bytes,
			wantLogical: false,
		},
		{
			name:        "Bytes Decimal Scale Larger Than Precision",
			schema:      `{"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 6}`,
			wantType:    avro.Bytes,
			wantLogical: false,
		},
		{
			name:            "Fixed Decimal",
			schema:          `{"type": "fixed", "name":"test", "size": 12, "logicalType": "decimal", "precision": 4, "scale": 2}`,
			wantType:        avro.Fixed,
			wantLogical:     true,
			wantLogicalType: avro.Decimal,
			assertFn: func(t *testing.T, ls avro.LogicalSchema) {
				dec, ok := ls.(*avro.DecimalLogicalSchema)
				require.True(t, ok)
				assert.Equal(t, 4, dec.Precision())
				assert.Equal(t, 2, dec.Scale())
			},
		},
		{
			name:            "Fixed Decimal No Scale",
			schema:          `{"type": "fixed", "name":"test", "size": 12, "logicalType": "decimal", "precision": 4}`,
			wantType:        avro.Fixed,
			wantLogical:     true,
			wantLogicalType: avro.Decimal,
			assertFn: func(t *testing.T, ls avro.LogicalSchema) {
				dec, ok := ls.(*avro.DecimalLogicalSchema)
				require.True(t, ok)
				assert.Equal(t, 4, dec.Precision())
				assert.Equal(t, 0, dec.Scale())
			},
		},
		{
			name:        "Fixed Decimal Negative Precision",
			schema:      `{"type": "fixed", "name":"test", "size": 12, "logicalType": "decimal", "precision": 0}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
		{
			name:        "Fixed Decimal Precision Too Large",
			schema:      `{"type": "fixed", "name":"test", "size": 4, "logicalType": "decimal", "precision": 10}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
		{
			name:        "Fixed Decimal Scale Larger Than Precision",
			schema:      `{"type": "fixed", "name":"test", "size": 12, "logicalType": "decimal", "precision": 4, "scale": 6}`,
			wantType:    avro.Fixed,
			wantLogical: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			schema, err := avro.Parse(test.schema)

			require.NoError(t, err)
			assert.Equal(t, test.wantType, schema.Type())

			lts, ok := schema.(avro.LogicalTypeSchema)
			if !ok {
				assert.Fail(t, "logical type schema expected")
				return
			}

			ls := lts.Logical()
			require.Equal(t, test.wantLogical, ls != nil)
			if !test.wantLogical {
				return
			}

			assert.Equal(t, test.wantLogicalType, ls.Type())

			if test.assertFn != nil {
				test.assertFn(t, ls)
			}
		})
	}
}
func TestSchema_SqlType(t *testing.T) {
	tests := []struct {
		name          string
		schema        string
		wantType      avro.Type
		wantSql       bool
		wantSqlType   avro.SqlType
		expectedError error
	}{
		{
			name:     "Invalid",
			schema:   `{"type": "int", "sqlType": "test"}`,
			wantType: avro.Int,
			wantSql:  false,
		},
		{
			name:        "JSON",
			schema:      `{"type": "string", "sqlType": "JSON"}`,
			wantType:    avro.String,
			wantSql:     true,
			wantSqlType: avro.Json,
		},
		{
			name:     "Ivalid json",
			schema:   `{"type": "string", "sqlType": "json"}`,
			wantType: avro.String,
			wantSql:  false,
		},
		{
			name:          "logical and sql type",
			schema:        `{"type": "string", "logicalType": "uuid", "sqlType": "JSON"}`,
			wantType:      avro.String,
			wantSql:       false,
			expectedError: errors.New("avro: error decoding primitive: provided logical and sql type at the same time"),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			schema, err := avro.Parse(test.schema)

			if test.expectedError == nil {
				require.NoError(t, err)
			} else {
				assert.EqualError(t, err, test.expectedError.Error())
				return
			}
			assert.Equal(t, test.wantType, schema.Type())

			sts, ok := schema.(avro.SqlTypeSchema)
			if !ok {
				assert.Fail(t, "sql type schema expected")
				return
			}

			s := sts.Sql()
			require.Equal(t, test.wantSql, s != nil)
			if !test.wantSql {
				return
			}

			assert.Equal(t, test.wantSqlType, s.Type())
		})
	}
}

func TestSchema_FingerprintUsing(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		typ    avro.FingerprintType
		want   []byte
	}{

		{
			name:   "Null CRC64",
			schema: "null",
			typ:    avro.CRC64Avro,
			want:   []byte{0x63, 0xdd, 0x24, 0xe7, 0xcc, 0x25, 0x8f, 0x8a},
		},
		{
			name:   "Null MD5",
			schema: "null",
			typ:    avro.MD5,
			want:   []byte{0x9b, 0x41, 0xef, 0x67, 0x65, 0x1c, 0x18, 0x48, 0x8a, 0x8b, 0x8, 0xbb, 0x67, 0xc7, 0x56, 0x99},
		},
		{
			name:   "Null SHA256",
			schema: "null",
			typ:    avro.SHA256,
			want:   []byte{0xf0, 0x72, 0xcb, 0xec, 0x3b, 0xf8, 0x84, 0x18, 0x71, 0xd4, 0x28, 0x42, 0x30, 0xc5, 0xe9, 0x83, 0xdc, 0x21, 0x1a, 0x56, 0x83, 0x7a, 0xed, 0x86, 0x24, 0x87, 0x14, 0x8f, 0x94, 0x7d, 0x1a, 0x1f},
		},
		{
			name:   "Primitive CRC64",
			schema: "string",
			typ:    avro.CRC64Avro,
			want:   []byte{0x8f, 0x1, 0x48, 0x72, 0x63, 0x45, 0x3, 0xc7},
		},
		{
			name:   "Record CRC64",
			schema: `{"type":"record", "name":"test", "namespace": "org.hamba.avro", "doc": "docs", "fields":[{"name": "field", "type": "int"}]}`,
			typ:    avro.CRC64Avro,
			want:   []byte{0xaf, 0x30, 0x30, 0xf0, 0x1c, 0x99, 0x76, 0xda},
		},
		{
			name:   "Enum CRC64",
			schema: `{"type":"enum", "name":"test", "namespace": "org.hamba.avro", "symbols":["TEST"]}`,
			typ:    avro.CRC64Avro,
			want:   []byte{0xc, 0xb0, 0xa2, 0xa6, 0x5f, 0x96, 0x8, 0xd1},
		},
		{
			name:   "Array CRC64",
			schema: `{"type":"array", "items": "int"}`,
			typ:    avro.CRC64Avro,
			want:   []byte{0x52, 0x2b, 0x81, 0x4f, 0xc9, 0x63, 0xb4, 0xbe},
		},
		{
			name:   "Map CRC64",
			schema: `{"type":"map", "values": "int"}`,
			typ:    avro.CRC64Avro,
			want:   []byte{0xdb, 0x39, 0xe2, 0xc2, 0x53, 0x4c, 0x89, 0x73},
		},
		{
			name:   "Union CRC64",
			schema: `["null", "int"]`,
			typ:    avro.CRC64Avro,
			want:   []byte{0xd5, 0x1c, 0xc0, 0x92, 0x2b, 0x46, 0xb1, 0xd7},
		},
		{
			name:   "Fixed CRC64",
			schema: `{"type":"fixed", "name":"test", "namespace": "org.hamba.avro", "size": 12}`,
			typ:    avro.CRC64Avro,
			want:   []byte{0x1, 0x7c, 0x1f, 0x7f, 0xa7, 0x6d, 0xa0, 0xa1},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			schema := avro.MustParse(test.schema)
			got, err := schema.FingerprintUsing(test.typ)

			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestSchema_FingerprintUsingReference(t *testing.T) {
	schema := avro.MustParse(`
{
   "type": "record",
   "name": "valid_name",
   "namespace": "org.hamba.avro",
   "fields": [
       {"name": "intField", "type": "int"},
       {"name": "Ref", "type": "valid_name"}
   ]
}
`)

	got, err := schema.(*avro.RecordSchema).Fields()[1].Type().FingerprintUsing(avro.CRC64Avro)

	require.NoError(t, err)
	assert.Equal(t, []byte{0xe1, 0xd6, 0x1e, 0x7c, 0x2f, 0xe3, 0x3c, 0x2b}, got)
}

func TestSchema_FingerprintUsingInvalidType(t *testing.T) {
	schema := avro.MustParse("string")

	_, err := schema.FingerprintUsing("test")

	assert.Error(t, err)
}

func TestSchema_Interop(t *testing.T) {
	schm := `
{
   "type": "record",
   "name": "Interop",
   "namespace": "org.hamba.avro",
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
                       "type": {"type": "org.hamba.avro.Node"}
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
