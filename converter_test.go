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
