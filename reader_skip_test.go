package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestReader_SkipNBytes(t *testing.T) {
	data := []byte{0x01, 0x01, 0x01, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 2)

	r.SkipNBytes(3)

	assert.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipNBytesEOF(t *testing.T) {
	data := []byte{0x01, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 2)

	r.SkipNBytes(3)

	assert.Error(t, r.Error)
}

func TestReader_SkipBool(t *testing.T) {
	data := []byte{0x01, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipBool()

	assert.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipInt(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Skipped",
			data: []byte{0x38, 0x36},
		},
		{

			name: "Overflow",
			data: []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0x36},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := avro.NewReader(bytes.NewReader(tt.data), 10)

			r.SkipInt()

			assert.NoError(t, r.Error)
			assert.Equal(t, int32(27), r.ReadInt())
		})
	}
}

func TestReader_SkipLong(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Skipped",
			data: []byte{0x38, 0x36},
		},
		{

			name: "Overflow",
			data: []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0x36},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := avro.NewReader(bytes.NewReader(tt.data), 10)

			r.SkipLong()

			assert.NoError(t, r.Error)
			assert.Equal(t, int32(27), r.ReadInt())
		})
	}
}

func TestReader_SkipFloat(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipFloat()

	assert.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipDouble(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipDouble()

	assert.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipString(t *testing.T) {
	data := []byte{0x06, 0x66, 0x6F, 0x6F, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipString()

	assert.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipStringEmpty(t *testing.T) {
	data := []byte{0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipString()

	assert.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipBytes(t *testing.T) {
	data := []byte{0x06, 0x66, 0x6F, 0x6F, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipBytes()

	assert.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipBytesEmpty(t *testing.T) {
	data := []byte{0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipBytes()

	assert.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}
