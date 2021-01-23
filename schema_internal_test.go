package avro

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestName_NameAndNamespace(t *testing.T) {
	n, _ := newName("bar", "foo")

	assert.Equal(t, "bar", n.Name())
	assert.Equal(t, "foo", n.Namespace())
	assert.Equal(t, "foo.bar", n.FullName())
}

func TestName_QualifiedName(t *testing.T) {
	n, _ := newName("foo.bar", "test")

	assert.Equal(t, "bar", n.Name())
	assert.Equal(t, "foo", n.Namespace())
	assert.Equal(t, "foo.bar", n.FullName())
}

func TestName_InvalidNameFirstChar(t *testing.T) {
	_, err := newName("+bar", "foo")

	assert.Error(t, err)
}

func TestName_InvalidNameOtherChar(t *testing.T) {
	_, err := newName("bar+", "foo")

	assert.Error(t, err)
}

func TestName_InvalidNamespaceFirstChar(t *testing.T) {
	_, err := newName("bar", "+foo")

	assert.Error(t, err)
}

func TestName_InvalidNamespaceOtherChar(t *testing.T) {
	_, err := newName("bar", "foo+")

	assert.Error(t, err)
}

func TestProperties_AddPropDoesNotAddReservedProperties(t *testing.T) {
	p := properties{reserved: []string{"test"}}

	p.AddProp("test", "foo")

	assert.Nil(t, p.Prop("test"))
}

func TestProperties_AddPropDoesNotOverwriteProperties(t *testing.T) {
	p := properties{}

	p.AddProp("test", "foo")
	p.AddProp("test", "bar")

	assert.Equal(t, "foo", p.Prop("test"))
}

func TestProperties_PropGetsFromEmptySet(t *testing.T) {
	p := properties{}

	assert.Nil(t, p.Prop("test"))
}

func TestIsValidDefault(t *testing.T) {
	tests := []struct {
		name     string
		schemaFn func() Schema
		def      interface{}
		want     interface{}
		wantOk   bool
	}{

		{
			name: "Null",
			schemaFn: func() Schema {
				return &NullSchema{}
			},
			def:    nil,
			want:   nullDefault,
			wantOk: true,
		},
		{
			name: "Null Invalid Type",
			schemaFn: func() Schema {
				return &NullSchema{}
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "String",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(String, nil)
			},
			def:    "test",
			want:   "test",
			wantOk: true,
		},
		{
			name: "String Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(String, nil)
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Bytes",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Bytes, nil)
			},
			def:    "test",
			want:   "test",
			wantOk: true,
		},
		{
			name: "Bytes Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Bytes, nil)
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Enum",
			schemaFn: func() Schema {
				s, _ := NewEnumSchema("foo", "", []string{"BAR"})
				return s
			},
			def:    "test",
			want:   "test",
			wantOk: true,
		},
		{
			name: "Enum Invalid Type",
			schemaFn: func() Schema {
				s, _ := NewEnumSchema("foo", "", []string{"BAR"})
				return s
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Fixed",
			schemaFn: func() Schema {
				s, _ := NewFixedSchema("foo", "", 1, nil)
				return s
			},
			def:    "test",
			want:   "test",
			wantOk: true,
		},
		{
			name: "Fixed Invalid Type",
			schemaFn: func() Schema {
				s, _ := NewFixedSchema("foo", "", 1, nil)
				return s
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Boolean",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Boolean, nil)
			},
			def:    true,
			want:   true,
			wantOk: true,
		},
		{
			name: "Boolean Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Boolean, nil)
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Int",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    1,
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Int8",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    int8(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Int16",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    int16(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Int32",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    int32(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Float64",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    float64(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil)
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "Long",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Long, nil)
			},
			def:    int64(1),
			want:   int64(1),
			wantOk: true,
		},
		{
			name: "Long Float64",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Long, nil)
			},
			def:    float64(1),
			want:   int64(1),
			wantOk: true,
		},
		{
			name: "Long Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Long, nil)
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "Float",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Float, nil)
			},
			def:    float32(1),
			want:   float32(1),
			wantOk: true,
		},
		{
			name: "Float Float64",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Float, nil)
			},
			def:    float64(1),
			want:   float32(1),
			wantOk: true,
		},
		{
			name: "Float Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Float, nil)
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "Double",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Double, nil)
			},
			def:    float64(1),
			want:   float64(1),
			wantOk: true,
		},
		{
			name: "Double Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Double, nil)
			},
			def:    "test",
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := isValidDefault(tt.schemaFn(), tt.def)

			assert.Equal(t, tt.wantOk, ok)
			if ok {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestRecursionError_Error(t *testing.T) {
	err := recursionError{}

	assert.Equal(t, "", err.Error())
}

func TestSchema_FingerprintUsingCaches(t *testing.T) {
	schema := NewPrimitiveSchema(String, nil)

	want, _ := schema.FingerprintUsing(CRC64Avro)

	got, _ := schema.FingerprintUsing(CRC64Avro)

	value, ok := schema.cache.Load(CRC64Avro)
	assert.True(t, ok)
	assert.Equal(t, want, value)
	assert.Equal(t, want, got)
}
