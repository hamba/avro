package avro

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLongConverter(t *testing.T) {
	tests := []struct {
		data []byte
		typ  Type
		want int64
	}{
		{
			data: []byte{0xE2, 0xA2, 0xF3, 0xAD, 0x07},
			typ:  Int,
			want: 987654321,
		},
		{
			data: []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
			typ:  Long,
			want: 9223372036854775807,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r := NewReader(bytes.NewReader(test.data), 10)

			got := createLongConverter(test.typ)(r)

			assert.Equal(t, test.want, got)
		})
	}
}

func TestFloatConverter(t *testing.T) {
	tests := []struct {
		data []byte
		typ  Type
		want float32
	}{
		{
			data: []byte{0xE2, 0xA2, 0xF3, 0xAD, 0x07},
			typ:  Int,
			want: 987654321,
		},
		{
			data: []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
			typ:  Long,
			want: 9223372036854775807,
		},
		{
			data: []byte{0x62, 0x20, 0x71, 0x49},
			typ:  Float,
			want: 987654.124,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r := NewReader(bytes.NewReader(test.data), 10)

			got := createFloatConverter(test.typ)(r)

			assert.Equal(t, test.want, got)
		})
	}
}

func TestDoubleConverter(t *testing.T) {
	tests := []struct {
		data []byte
		typ  Type
		want float64
	}{
		{
			data: []byte{0xE2, 0xA2, 0xF3, 0xAD, 0x07},
			typ:  Int,
			want: 987654321,
		},
		{
			data: []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
			typ:  Long,
			want: 9223372036854775807,
		},
		{
			data: []byte{0x62, 0x20, 0x71, 0x49},
			typ:  Float,
			want: float64(float32(987654.124)),
		},
		{
			data: []byte{0xB6, 0xF3, 0x7D, 0x54, 0x34, 0x6F, 0x9D, 0xC1},
			typ:  Double,
			want: -123456789.123,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r := NewReader(bytes.NewReader(test.data), 10)

			got := createDoubleConverter(test.typ)(r)

			assert.Equal(t, test.want, got)
		})
	}
}

func TestStringConverter(t *testing.T) {
	tests := []struct {
		data []byte
		typ  Type
		want string
	}{
		{
			data: []byte{0x08, 0xEC, 0xAB, 0x44, 0x00},
			typ:  Bytes,
			want: string([]byte{0xEC, 0xAB, 0x44, 0x00}),
		},
		{
			data: []byte{0x28, 0x6F, 0x70, 0x70, 0x61, 0x6E, 0x20, 0x67, 0x61, 0x6E, 0x67, 0x6E, 0x61, 0x6D, 0x20, 0x73, 0x74, 0x79, 0x6C, 0x65, 0x21},
			typ:  String,
			want: "oppan gangnam style!",
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r := NewReader(bytes.NewReader(test.data), 10)

			got := createStringConverter(test.typ)(r)

			assert.Equal(t, test.want, got)
		})
	}
}

func TestBytesConverter(t *testing.T) {
	tests := []struct {
		data []byte
		typ  Type
		want []byte
	}{
		{
			data: []byte{0x36, 0xd1, 0x87, 0xD0, 0xB5, 0x2D, 0xD1, 0x82, 0xD0, 0xBE, 0x20, 0xD0, 0xBF, 0xD0, 0xBE, 0x20, 0xD1, 0x80, 0xD1, 0x83, 0xD1, 0x81, 0xD1, 0x81, 0xD0, 0xBA, 0xD0, 0xB8},
			typ:  String,
			want: []byte("че-то по русски"),
		},
		{
			data: []byte{0x0c, 0xac, 0xdc, 0x01, 0x00, 0x10, 0x0f},
			typ:  Bytes,
			want: []byte{0xac, 0xdc, 0x01, 0x00, 0x10, 0x0f},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r := NewReader(bytes.NewReader(test.data), 10)

			got := createBytesConverter(test.typ)(r)

			assert.Equal(t, test.want, got)
		})
	}
}
