package avro

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestName_NameAndNamespace(t *testing.T) {
	n, err := newName("bar", "foo", nil)
	require.NoError(t, err)

	assert.Equal(t, "bar", n.Name())
	assert.Equal(t, "foo", n.Namespace())
	assert.Equal(t, "foo.bar", n.FullName())
}

func TestName_QualifiedName(t *testing.T) {
	n, err := newName("foo.bar", "test", nil)
	require.NoError(t, err)

	assert.Equal(t, "bar", n.Name())
	assert.Equal(t, "foo", n.Namespace())
	assert.Equal(t, "foo.bar", n.FullName())
}

func TestName_NameAndNamespaceAndAlias(t *testing.T) {
	n, err := newName("bar", "foo", []string{"baz", "test.bat"})
	require.NoError(t, err)

	assert.Equal(t, "bar", n.Name())
	assert.Equal(t, "foo", n.Namespace())
	assert.Equal(t, "foo.bar", n.FullName())
	assert.Equal(t, []string{"foo.baz", "test.bat"}, n.Aliases())
}

func TestName_EmpryName(t *testing.T) {
	_, err := newName("", "foo", nil)

	assert.Error(t, err)
}

func TestName_InvalidNameFirstChar(t *testing.T) {
	_, err := newName("+bar", "foo", nil)

	assert.Error(t, err)
}

func TestName_InvalidNameOtherChar(t *testing.T) {
	_, err := newName("bar+", "foo", nil)

	assert.Error(t, err)
}

func TestName_InvalidNamespaceFirstChar(t *testing.T) {
	_, err := newName("bar", "+foo", nil)

	assert.Error(t, err)
}

func TestName_InvalidNamespaceOtherChar(t *testing.T) {
	_, err := newName("bar", "foo+", nil)

	assert.Error(t, err)
}

func TestName_InvalidAliasFirstChar(t *testing.T) {
	_, err := newName("bar", "foo", []string{"+bar"})

	assert.Error(t, err)
}

func TestName_InvalidAliasOtherChar(t *testing.T) {
	_, err := newName("bar", "foo", []string{"bar+"})

	assert.Error(t, err)
}

func TestName_InvalidAliasFQNFirstChar(t *testing.T) {
	_, err := newName("bar", "foo", []string{"test.+bar"})

	assert.Error(t, err)
}

func TestName_InvalidAliasFQNOtherChar(t *testing.T) {
	_, err := newName("bar", "foo", []string{"test.bar+"})

	assert.Error(t, err)
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
				return NewPrimitiveSchema(String, nil, nil)
			},
			def:    "test",
			want:   "test",
			wantOk: true,
		},
		{
			name: "String Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(String, nil, nil)
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Bytes",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Bytes, nil, nil)
			},
			def:    "test",
			want:   "test",
			wantOk: true,
		},
		{
			name: "Bytes Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Bytes, nil, nil)
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
				return NewPrimitiveSchema(Boolean, nil, nil)
			},
			def:    true,
			want:   true,
			wantOk: true,
		},
		{
			name: "Boolean Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Boolean, nil, nil)
			},
			def:    1,
			wantOk: false,
		},
		{
			name: "Int",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil, nil)
			},
			def:    1,
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Int8",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil, nil)
			},
			def:    int8(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Int16",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil, nil)
			},
			def:    int16(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Int32",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil, nil)
			},
			def:    int32(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Float64",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil, nil)
			},
			def:    float64(1),
			want:   1,
			wantOk: true,
		},
		{
			name: "Int Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Int, nil, nil)
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "Long",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Long, nil, nil)
			},
			def:    int64(1),
			want:   int64(1),
			wantOk: true,
		},
		{
			name: "Long Float64",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Long, nil, nil)
			},
			def:    float64(1),
			want:   int64(1),
			wantOk: true,
		},
		{
			name: "Long Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Long, nil, nil)
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "Float",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Float, nil, nil)
			},
			def:    float32(1),
			want:   float32(1),
			wantOk: true,
		},
		{
			name: "Float Float64",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Float, nil, nil)
			},
			def:    float64(1),
			want:   float32(1),
			wantOk: true,
		},
		{
			name: "Float Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Float, nil, nil)
			},
			def:    "test",
			wantOk: false,
		},
		{
			name: "Double",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Double, nil, nil)
			},
			def:    float64(1),
			want:   float64(1),
			wantOk: true,
		},
		{
			name: "Double Invalid Type",
			schemaFn: func() Schema {
				return NewPrimitiveSchema(Double, nil, nil)
			},
			def:    "test",
			wantOk: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, ok := isValidDefault(test.schemaFn(), test.def)

			assert.Equal(t, test.wantOk, ok)
			if ok {
				assert.Equal(t, test.want, got)
			}
		})
	}
}

func TestRecursionError_Error(t *testing.T) {
	err := recursionError{}

	assert.Equal(t, "", err.Error())
}

func TestSchema_FingerprintUsingCaches(t *testing.T) {
	schema := NewPrimitiveSchema(String, nil, nil)

	want, _ := schema.FingerprintUsing(CRC64Avro)

	got, _ := schema.FingerprintUsing(CRC64Avro)

	value, ok := schema.cache.Load(CRC64Avro)
	require.True(t, ok)
	assert.Equal(t, want, value)
	assert.Equal(t, want, got)
}
