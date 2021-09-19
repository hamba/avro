package avro_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewReader(t *testing.T) {
	r := avro.NewReader(bytes.NewBuffer([]byte{}), 10)

	assert.IsType(t, &avro.Reader{}, r)
}

func TestReader_Reset(t *testing.T) {
	r := &avro.Reader{}

	r.Reset([]byte{0x01})

	assert.True(t, r.ReadBool())
}

func TestReader_ReportError(t *testing.T) {
	r := &avro.Reader{}

	r.ReportError("test", "bar")

	assert.EqualError(t, r.Error, "avro: test: bar")
}

func TestReader_ReportErrorExistingError(t *testing.T) {
	err := errors.New("test")

	r := &avro.Reader{}
	r.Error = err

	r.ReportError("test", "bar")

	assert.Equal(t, err, r.Error)
}

func TestReader_ReadPastBuffer(t *testing.T) {
	r := (&avro.Reader{}).Reset([]byte{0xE2})

	r.ReadInt()

	assert.Error(t, r.Error)
}

func TestReader_ReadDelayedReader(t *testing.T) {
	rdr := &delayedReader{b: []byte{0x36}}
	r := avro.NewReader(rdr, 10)

	i := r.ReadInt()

	require.NoError(t, r.Error)
	assert.Equal(t, int32(27), i)
}

func TestReader_Read(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    []byte
		wantErr bool
	}{
		{
			name:    "valid",
			data:    []byte{0xAC, 0xDC, 0x01, 0x00, 0x10, 0x0F},
			want:    make([]byte, 6),
			wantErr: false,
		},
		{
			name:    "eof",
			data:    []byte{0xAC}, // io.EOF
			want:    make([]byte, 6),
			wantErr: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			r := avro.NewReader(bytes.NewReader(test.data), 2)

			r.Read(test.want)

			if test.wantErr {
				assert.Error(t, r.Error)
				return
			}

			require.NoError(t, r.Error)
			assert.Equal(t, test.want, test.data)
		})
	}
}

func TestReader_ReadBool(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    bool
		wantErr bool
	}{
		{
			name:    "false",
			data:    []byte{0x00},
			want:    false,
			wantErr: false,
		},
		{
			name:    "true",
			data:    []byte{0x01},
			want:    true,
			wantErr: false,
		},
		{
			name:    "invalid bool",
			data:    []byte{0x02},
			want:    false,
			wantErr: true,
		},
		{
			name:    "eof",
			data:    []byte(nil), // io.EOF
			want:    false,
			wantErr: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {

			r := avro.NewReader(bytes.NewReader(test.data), 10)

			got := r.ReadBool()

			if test.wantErr {
				assert.Error(t, r.Error)
				return
			}

			require.NoError(t, r.Error)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestReader_ReadInt(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    int32
		wantErr bool
	}{
		{
			name:    "positive int",
			data:    []byte{0x36},
			want:    27,
			wantErr: false,
		},
		{
			name:    "negative int",
			data:    []byte{0x0F},
			want:    -8,
			wantErr: false,
		},
		{
			name:    "negative int",
			data:    []byte{0x01},
			want:    -1,
			wantErr: false,
		},
		{
			name:    "zero",
			data:    []byte{0x00},
			want:    0,
			wantErr: false,
		},
		{
			name:    "one",
			data:    []byte{0x02},
			want:    1,
			wantErr: false,
		},
		{
			name:    "negative 64",
			data:    []byte{0x7F},
			want:    -64,
			wantErr: false,
		},
		{
			name:    "multi byte int",
			data:    []byte{0x80, 0x01},
			want:    64,
			wantErr: false,
		},
		{
			name:    "large int",
			data:    []byte{0xAA, 0xB4, 0xDE, 0x75},
			want:    123456789,
			wantErr: false,
		},
		{
			name:    "larger int",
			data:    []byte{0xE2, 0xA2, 0xF3, 0xAD, 0x07},
			want:    987654321,
			wantErr: false,
		},
		{
			name:    "overflow",
			data:    []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD},
			want:    0,
			wantErr: true,
		},
		{
			name:    "eof",
			data:    []byte{0xE2},
			want:    0,
			wantErr: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {

			r := avro.NewReader(bytes.NewReader(test.data), 10)

			got := r.ReadInt()

			if test.wantErr {
				assert.Error(t, r.Error)
				return
			}

			require.NoError(t, r.Error)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestReader_ReadLong(t *testing.T) {
	tests := []struct {
		data    []byte
		want    int64
		wantErr bool
	}{
		{
			data:    []byte{0x36},
			want:    27,
			wantErr: false,
		},
		{
			data:    []byte{0x0F},
			want:    -8,
			wantErr: false,
		},
		{
			data:    []byte{0x01},
			want:    -1,
			wantErr: false,
		},
		{
			data:    []byte{0x00},
			want:    0,
			wantErr: false,
		},
		{
			data:    []byte{0x02},
			want:    1,
			wantErr: false,
		},
		{
			data:    []byte{0x7F},
			want:    -64,
			wantErr: false,
		},
		{
			data:    []byte{0x80, 0x01},
			want:    64,
			wantErr: false,
		},
		{
			data:    []byte{0xAA, 0xB4, 0xDE, 0x75},
			want:    123456789,
			wantErr: false,
		},
		{
			data:    []byte{0xE2, 0xA2, 0xF3, 0xAD, 0x07},
			want:    987654321,
			wantErr: false,
		},
		{
			data:    []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
			want:    9223372036854775807,
			wantErr: false,
		},
		{
			data:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
			want:    -9223372036854775808,
			wantErr: false,
		},
		{
			data:    []byte{0xBD, 0xB1, 0xAE, 0xD4, 0xD2, 0xCD, 0xBD, 0xE4, 0x97, 0x01},
			want:    -5468631321897454687,
			wantErr: false,
		},
		{
			data:    []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}, // Overflow
			want:    0,
			wantErr: true,
		},
		{
			data:    []byte{0xE2}, // io.EOF
			want:    0,
			wantErr: true,
		},
	}

	for _, test := range tests {
		r := avro.NewReader(bytes.NewReader(test.data), 10)

		got := r.ReadLong()

		if test.wantErr {
			assert.Error(t, r.Error)
			continue
		}

		require.NoError(t, r.Error)
		assert.Equal(t, test.want, got)
	}
}

func TestReader_ReadFloat(t *testing.T) {
	tests := []struct {
		data    []byte
		want    float32
		wantErr bool
	}{
		{
			data:    []byte{0x00, 0x00, 0x00, 0x00},
			want:    0.0,
			wantErr: false,
		},
		{
			data:    []byte{0x00, 0x00, 0x80, 0x3F},
			want:    1.0,
			wantErr: false,
		},
		{
			data:    []byte{0x33, 0x33, 0x93, 0x3F},
			want:    1.15,
			wantErr: false,
		},
		{
			data:    []byte{0x23, 0xDB, 0x57, 0xC2},
			want:    -53.964,
			wantErr: false,
		},
		{
			data:    []byte{0xA3, 0x79, 0xEB, 0xCC},
			want:    -123456789.123,
			wantErr: false,
		},
		{
			data:    []byte{0x62, 0x20, 0x71, 0x49},
			want:    987654.111115,
			wantErr: false,
		},
		{
			data:    []byte(nil), // io.EOF
			want:    0,
			wantErr: true,
		},
	}

	for _, test := range tests {
		r := avro.NewReader(bytes.NewReader(test.data), 2)

		got := r.ReadFloat()

		if test.wantErr {
			assert.Error(t, r.Error)
			continue
		}

		require.NoError(t, r.Error)
		assert.Equal(t, test.want, got)
	}
}

func TestReader_ReadDouble(t *testing.T) {
	tests := []struct {
		data    []byte
		want    float64
		wantErr bool
	}{
		{
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			want:    0.0,
			wantErr: false,
		},
		{
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F},
			want:    1.0,
			wantErr: false,
		},
		{
			data:    []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F},
			want:    1.15,
			wantErr: false,
		},
		{
			data:    []byte{0x08, 0xAC, 0x1C, 0x5A, 0x64, 0xFB, 0x4A, 0xC0},
			want:    -53.964,
			wantErr: false,
		},
		{
			data:    []byte{0xB6, 0xF3, 0x7D, 0x54, 0x34, 0x6F, 0x9D, 0xC1},
			want:    -123456789.123,
			wantErr: false,
		},
		{
			data:    []byte{0xB6, 0x10, 0xE4, 0x38, 0x0C, 0x24, 0x2E, 0x41},
			want:    987654.111115,
			wantErr: false,
		},
		{
			data:    []byte{0x75, 0x6B, 0x7E, 0x54, 0x34, 0x6F, 0x9D, 0x41},
			want:    123456789.123456789,
			wantErr: false,
		},
		{
			data:    []byte{0x00, 0x00, 0x00, 0x00, 0xD0, 0x12, 0x63, 0x41},
			want:    9999999.99999999999999999999999,
			wantErr: false,
		},
		{
			data:    []byte{0x18, 0xFC, 0x1A, 0xDD, 0x1F, 0x0E, 0x0A, 0x43},
			want:    916734926348163.01973408746523,
			wantErr: false,
		},
		{
			data:    []byte{0x0A, 0x8F, 0xA6, 0x40, 0xAC, 0xAD, 0x8D, 0xC3},
			want:    -267319348967891263.1928357138913857,
			wantErr: false,
		},
		{
			data:    []byte(nil), // io.EOF
			want:    0,
			wantErr: true,
		},
	}

	for _, test := range tests {
		r := avro.NewReader(bytes.NewReader(test.data), 4)

		got := r.ReadDouble()

		if test.wantErr {
			assert.Error(t, r.Error)
			continue
		}

		require.NoError(t, r.Error)
		assert.Equal(t, test.want, got)
	}
}

func TestReader_ReadBytes(t *testing.T) {
	tests := []struct {
		data    []byte
		want    []byte
		wantErr bool
	}{
		{
			data:    []byte{0x02, 0x02},
			want:    []byte{0x02},
			wantErr: false,
		},
		{
			data:    []byte{0x04, 0x03, 0xFF},
			want:    []byte{0x03, 0xFF},
			wantErr: false,
		},
		{
			data:    []byte{0x08, 0xEC, 0xAB, 0x44, 0x00},
			want:    []byte{0xEC, 0xAB, 0x44, 0x00},
			wantErr: false,
		},
		{
			data:    []byte{0x0C, 0xAC, 0xDC, 0x01, 0x00, 0x10, 0x0F},
			want:    []byte{0xAC, 0xDC, 0x01, 0x00, 0x10, 0x0F},
			wantErr: false,
		},
		{
			data:    []byte(nil), // io.EOF no length
			want:    nil,
			wantErr: true,
		},
		{
			data:    []byte{0x05, 0x03, 0xFF, 0x0A}, // Invalid bytes length
			want:    nil,
			wantErr: true,
		},
		{
			data:    []byte{0x08, 0xFF}, // io.EOF length greater then data
			want:    nil,
			wantErr: true,
		},
	}

	for _, test := range tests {
		r := avro.NewReader(bytes.NewReader(test.data), 10)

		got := r.ReadBytes()

		if test.wantErr {
			assert.Error(t, r.Error)
			continue
		}

		require.NoError(t, r.Error)
		assert.Equal(t, test.want, got)
	}
}

func TestReader_ReadString(t *testing.T) {
	tests := []struct {
		data    []byte
		want    string
		wantErr bool
	}{
		{
			data:    []byte{0x00},
			want:    "",
			wantErr: false,
		},
		{
			data:    []byte{0x06, 0x66, 0x6F, 0x6F},
			want:    "foo",
			wantErr: false,
		},
		{
			data:    []byte{0x08, 0x61, 0x76, 0x72, 0x6F},
			want:    "avro",
			wantErr: false,
		},
		{
			data:    []byte{0x0C, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65},
			want:    "apache",
			wantErr: false,
		},
		{
			data:    []byte{0x28, 0x6F, 0x70, 0x70, 0x61, 0x6E, 0x20, 0x67, 0x61, 0x6E, 0x67, 0x6E, 0x61, 0x6D, 0x20, 0x73, 0x74, 0x79, 0x6C, 0x65, 0x21},
			want:    "oppan gangnam style!",
			wantErr: false,
		},
		{
			data:    []byte{0x36, 0xD1, 0x87, 0xD0, 0xB5, 0x2D, 0xD1, 0x82, 0xD0, 0xBE, 0x20, 0xD0, 0xBF, 0xD0, 0xBE, 0x20, 0xD1, 0x80, 0xD1, 0x83, 0xD1, 0x81, 0xD1, 0x81, 0xD0, 0xBA, 0xD0, 0xB8},
			want:    "че-то по русски",
			wantErr: false,
		},
		{
			data:    []byte{0x0C, 0xE4, 0xB8, 0x96, 0xE7, 0x95, 0x8C},
			want:    "世界",
			wantErr: false,
		},
		{
			data:    []byte{0x22, 0x21, 0xE2, 0x84, 0x96, 0x3B, 0x25, 0x3A, 0x3F, 0x2A, 0x22, 0x28, 0x29, 0x40, 0x23, 0x24, 0x5E, 0x26},
			want:    "!№;%:?*\"()@#$^&",
			wantErr: false,
		},
		{
			data:    []byte(nil), // io.EOF no length
			want:    "",
			wantErr: true,
		},
		{
			data:    []byte{0x05, 0x66, 0x6F, 0x6F, 0x6F}, // Invalid string length
			want:    "",
			wantErr: true,
		},
		{
			data:    []byte{0x08, 0x66}, // io.EOF length greater then data
			want:    "",
			wantErr: true,
		},
	}

	for _, test := range tests {
		r := avro.NewReader(bytes.NewReader(test.data), 10)

		got := r.ReadString()

		if test.wantErr {
			assert.Error(t, r.Error)
			continue
		}

		require.NoError(t, r.Error)
		assert.Equal(t, test.want, got)
	}
}

func TestReader_ReadStringFastPathIsntBoundToBuffer(t *testing.T) {
	data := []byte{0x06, 0x66, 0x6F, 0x6F, 0x08, 0x61, 0x76, 0x72, 0x6F}
	r := avro.NewReader(bytes.NewReader(data), 4)

	got1 := r.ReadString()
	got2 := r.ReadString()

	require.NoError(t, r.Error)
	assert.Equal(t, "foo", got1)
	assert.Equal(t, "avro", got2)
}

func TestReader_ReadBlockHeader(t *testing.T) {
	tests := []struct {
		data []byte
		len  int64
		size int64
	}{
		{
			data: []byte{0x80, 0x01},
			len:  64,
			size: 0,
		},
		{
			data: []byte{0x7F, 0x80, 0x01},
			len:  64,
			size: 64,
		},
	}

	for _, test := range tests {
		r := avro.NewReader(bytes.NewReader(test.data), 10)

		gotLen, gotSize := r.ReadBlockHeader()

		require.NoError(t, r.Error)
		assert.Equal(t, test.len, gotLen)
		assert.Equal(t, test.size, gotSize)
	}
}

type delayedReader struct {
	count int
	b     []byte
}

func (r *delayedReader) Read(p []byte) (n int, err error) {
	if r.count == 0 {
		r.count++
		return 0, nil
	}

	return copy(p, r.b), nil
}
