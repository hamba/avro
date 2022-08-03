package avro_test

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReader_ReadNext(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		schema  string
		want    interface{}
		wantErr require.ErrorAssertionFunc
	}{

		{
			name:    "Bool",
			data:    []byte{0x01},
			schema:  "boolean",
			want:    true,
			wantErr: require.NoError,
		},
		{
			name:    "Int",
			data:    []byte{0x36},
			schema:  "int",
			want:    27,
			wantErr: require.NoError,
		},
		{
			name:    "Int Date",
			data:    []byte{0xAE, 0x9D, 0x02},
			schema:  `{"type":"int","logicalType":"date"}`,
			want:    time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC),
			wantErr: require.NoError,
		},
		{
			name:    "Int Time-Millis",
			data:    []byte{0xAA, 0xB4, 0xDE, 0x75},
			schema:  `{"type":"int","logicalType":"time-millis"}`,
			want:    123456789 * time.Millisecond,
			wantErr: require.NoError,
		},
		{
			name:    "Long",
			data:    []byte{0x36},
			schema:  "long",
			want:    int64(27),
			wantErr: require.NoError,
		},
		{
			name:    "Long Time-Micros",
			data:    []byte{0x86, 0xEA, 0xC8, 0xE9, 0x97, 0x07},
			schema:  `{"type":"long","logicalType":"time-micros"}`,
			want:    123456789123 * time.Microsecond,
			wantErr: require.NoError,
		},
		{
			name:    "Long Timestamp-Millis",
			data:    []byte{0x90, 0xB2, 0xAE, 0xC3, 0xEC, 0x5B},
			schema:  `{"type":"long","logicalType":"timestamp-millis"}`,
			want:    time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
			wantErr: require.NoError,
		},
		{
			name:    "Long Timestamp-Micros",
			data:    []byte{0x80, 0xCD, 0xB7, 0xA2, 0xEE, 0xC7, 0xCD, 0x05},
			schema:  `{"type":"long","logicalType":"timestamp-micros"}`,
			want:    time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
			wantErr: require.NoError,
		},
		{
			name:    "Float",
			data:    []byte{0x33, 0x33, 0x93, 0x3F},
			schema:  "float",
			want:    float32(1.15),
			wantErr: require.NoError,
		},
		{
			name:    "Double",
			data:    []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F},
			schema:  "double",
			want:    float64(1.15),
			wantErr: require.NoError,
		},
		{
			name:    "String",
			data:    []byte{0x06, 0x66, 0x6F, 0x6F},
			schema:  "string",
			want:    "foo",
			wantErr: require.NoError,
		},
		{
			name:    "String JSON",
			data:    []byte{0x22, 0x7b, 0x22, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x22, 0x3a, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x7d},
			schema:  `{"type":"string","sqlType":"JSON"}`,
			want:    `{"field":"value"}`,
			wantErr: require.NoError,
		},
		{
			name:    "String JSON complex",
			data:    []byte{0x32, 0x7b, 0x22, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x22, 0x3a, 0x20, 0x7b, 0x22, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x78, 0x22, 0x3a, 0x20, 0x31, 0x7d, 0x7d},
			schema:  `{"type":"string","sqlType":"JSON"}`,
			want:    `{"field": {"complex": 1}}`,
			wantErr: require.NoError,
		},
		{
			name:    "Bytes",
			data:    []byte{0x08, 0xEC, 0xAB, 0x44, 0x00},
			schema:  "bytes",
			want:    []byte{0xEC, 0xAB, 0x44, 0x00},
			wantErr: require.NoError,
		},
		{
			name:    "Bytes Decimal",
			data:    []byte{0x6, 0x00, 0x87, 0x78},
			schema:  `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			want:    big.NewRat(1734, 5),
			wantErr: require.NoError,
		},
		{
			name:    "Record",
			data:    []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema:  `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			want:    map[string]interface{}{"a": int64(27), "b": "foo"},
			wantErr: require.NoError,
		},
		{
			name:    "Ref",
			data:    []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema:  `{"type":"record","name":"parent","fields":[{"name":"a","type":{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}},{"name":"b","type":"test"}]}`,
			want:    map[string]interface{}{"a": map[string]interface{}{"a": int64(27), "b": "foo"}, "b": map[string]interface{}{"a": int64(27), "b": "foo"}},
			wantErr: require.NoError,
		},
		{
			name:    "Array",
			data:    []byte{0x04, 0x36, 0x38, 0x0},
			schema:  `{"type":"array", "items": "int"}`,
			want:    []interface{}{27, 28},
			wantErr: require.NoError,
		},
		{
			name:    "Map",
			data:    []byte{0x02, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00},
			schema:  `{"type":"map", "values": "string"}`,
			want:    map[string]interface{}{"foo": "foo"},
			wantErr: require.NoError,
		},
		{
			name:    "Enum",
			data:    []byte{0x02},
			schema:  `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`,
			want:    "bar",
			wantErr: require.NoError,
		},
		{
			name:    "Enum Invalid Symbol",
			data:    []byte{0x04},
			schema:  `{"type":"enum", "name": "test", "symbols": ["foo", "bar"]}`,
			want:    nil,
			wantErr: require.Error,
		},
		{
			name:    "Union",
			data:    []byte{0x02, 0x06, 0x66, 0x6F, 0x6F},
			schema:  `["null", "string"]`,
			want:    map[string]interface{}{"string": "foo"},
			wantErr: require.NoError,
		},
		{
			name:    "Union Nil",
			data:    []byte{0x00},
			schema:  `["null", "string"]`,
			want:    nil,
			wantErr: require.NoError,
		},
		{
			name:    "Union Named",
			data:    []byte{0x02, 0x02},
			schema:  `["null", {"type":"enum", "name": "test", "symbols": ["foo", "bar"]}]`,
			want:    map[string]interface{}{"test": "bar"},
			wantErr: require.NoError,
		},
		{
			name:    "Union Invalid Schema",
			data:    []byte{0x04},
			schema:  `["null", "string"]`,
			want:    nil,
			wantErr: require.Error,
		},
		{
			name:    "Fixed",
			data:    []byte{0x66, 0x6F, 0x6F, 0x66, 0x6F, 0x6F},
			schema:  `{"type":"fixed", "name": "test", "size": 6}`,
			want:    []byte{'f', 'o', 'o', 'f', 'o', 'o'},
			wantErr: require.NoError,
		},
		{
			name:    "Fixed Decimal",
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0x87, 0x78},
			schema:  `{"type":"fixed", "name": "test", "size": 6,"logicalType":"decimal","precision":4,"scale":2}`,
			want:    big.NewRat(1734, 5),
			wantErr: require.NoError,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			schema := avro.MustParse(test.schema)
			r := avro.NewReader(bytes.NewReader(test.data), 10)

			got := r.ReadNext(schema)

			test.wantErr(t, r.Error)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestReader_ReadNextUnsupportedType(t *testing.T) {
	schema := avro.NewPrimitiveSchema(avro.Type("test"), nil, nil)
	r := avro.NewReader(bytes.NewReader([]byte{0x01}), 10)

	_ = r.ReadNext(schema)

	assert.Error(t, r.Error)
}
