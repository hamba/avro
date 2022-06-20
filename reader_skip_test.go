package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReader_SkipNBytes(t *testing.T) {
	data := []byte{0x01, 0x01, 0x01, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 2)

	r.SkipNBytes(3)

	require.NoError(t, r.Error)
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

	require.NoError(t, r.Error)
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

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			r := avro.NewReader(bytes.NewReader(test.data), 10)

			r.SkipInt()

			require.NoError(t, r.Error)
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

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			r := avro.NewReader(bytes.NewReader(test.data), 10)

			r.SkipLong()

			require.NoError(t, r.Error)
			assert.Equal(t, int32(27), r.ReadInt())
		})
	}
}

func TestReader_SkipFloat(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipFloat()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipDouble(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipDouble()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipString(t *testing.T) {
	data := []byte{0x06, 0x66, 0x6F, 0x6F, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipString()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipStringEmpty(t *testing.T) {
	data := []byte{0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipString()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipBytes(t *testing.T) {
	data := []byte{0x06, 0x66, 0x6F, 0x6F, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipBytes()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}

func TestReader_SkipBytesEmpty(t *testing.T) {
	data := []byte{0x00, 0x36}
	r := avro.NewReader(bytes.NewReader(data), 10)

	r.SkipBytes()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), r.ReadInt())
}
