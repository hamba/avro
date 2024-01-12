package avro

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConverter(t *testing.T) {
	tests := []struct {
		data         []byte
		want         any
		typ, wantTyp Type
		wantErr      require.ErrorAssertionFunc
	}{
		{
			data:    []byte{0xE2, 0xA2, 0xF3, 0xAD, 0x07},
			want:    int64(987654321),
			typ:     Int,
			wantTyp: Long,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
			want:    int64(9223372036854775807),
			typ:     Long,
			wantTyp: Long,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0xE2, 0xA2, 0xF3, 0xAD, 0x07},
			want:    float32(987654321),
			typ:     Int,
			wantTyp: Float,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
			want:    float32(9223372036854775807),
			typ:     Long,
			wantTyp: Float,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0x62, 0x20, 0x71, 0x49},
			want:    float32(987654.124),
			typ:     Float,
			wantTyp: Float,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0xE2, 0xA2, 0xF3, 0xAD, 0x07},
			want:    float64(987654321),
			typ:     Int,
			wantTyp: Double,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
			want:    float64(9223372036854775807),
			typ:     Long,
			wantTyp: Double,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0x62, 0x20, 0x71, 0x49},
			want:    float64(float32(987654.124)),
			typ:     Float,
			wantTyp: Double,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0xB6, 0xF3, 0x7D, 0x54, 0x34, 0x6F, 0x9D, 0xC1},
			want:    float64(-123456789.123),
			typ:     Double,
			wantTyp: Double,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0x08, 0xEC, 0xAB, 0x44, 0x00},
			want:    string([]byte{0xEC, 0xAB, 0x44, 0x00}),
			typ:     Bytes,
			wantTyp: String,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0x28, 0x6F, 0x70, 0x70, 0x61, 0x6E, 0x20, 0x67, 0x61, 0x6E, 0x67, 0x6E, 0x61, 0x6D, 0x20, 0x73, 0x74, 0x79, 0x6C, 0x65, 0x21},
			want:    "oppan gangnam style!",
			typ:     String,
			wantTyp: String,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0x36, 0xD1, 0x87, 0xD0, 0xB5, 0x2D, 0xD1, 0x82, 0xD0, 0xBE, 0x20, 0xD0, 0xBF, 0xD0, 0xBE, 0x20, 0xD1, 0x80, 0xD1, 0x83, 0xD1, 0x81, 0xD1, 0x81, 0xD0, 0xBA, 0xD0, 0xB8},
			want:    []byte("че-то по русски"),
			typ:     String,
			wantTyp: Bytes,
			wantErr: require.NoError,
		},
		{
			data:    []byte{0x0C, 0xAC, 0xDC, 0x01, 0x00, 0x10, 0x0F},
			want:    []byte{0xAC, 0xDC, 0x01, 0x00, 0x10, 0x0F},
			typ:     Bytes,
			wantTyp: Bytes,
			wantErr: require.NoError,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r := NewReader(bytes.NewReader(test.data), 10)
			conv := resolveConverter(test.typ)

			var got any
			switch test.wantTyp {
			case Long:
				got = conv.toLong(r)
			case Float:
				got = conv.toFloat(r)
			case Double:
				got = conv.toDouble(r)
			case String:
				got = conv.toString(r)
			case Bytes:
				got = conv.toBytes(r)
			default:
			}

			test.wantErr(t, r.Error)
			assert.Equal(t, test.want, got)
		})
	}
}
