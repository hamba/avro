package avro_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriter_Reset(t *testing.T) {
	var buf bytes.Buffer
	w := avro.NewWriter(nil, 10)
	w.Reset(&buf)
	_, _ = w.Write([]byte("test"))

	err := w.Flush()

	require.NoError(t, err)
	assert.Equal(t, []byte("test"), buf.Bytes())
}

func TestWriter_Buffer(t *testing.T) {
	w := avro.NewWriter(nil, 10)
	_, _ = w.Write([]byte("test"))

	assert.Equal(t, 4, w.Buffered())
	assert.Equal(t, []byte("test"), w.Buffer())
}

func TestWriter_Flush(t *testing.T) {
	var buf bytes.Buffer
	w := avro.NewWriter(&buf, 10)
	_, _ = w.Write([]byte("test"))

	err := w.Flush()

	require.NoError(t, err)
	assert.Equal(t, []byte("test"), buf.Bytes())
}

func TestWriter_FlushNoWriter(t *testing.T) {
	w := avro.NewWriter(nil, 10)
	_, _ = w.Write([]byte("test"))

	err := w.Flush()

	assert.NoError(t, err)
}

func TestWriter_FlushReturnsWriterError(t *testing.T) {
	var buf bytes.Buffer
	w := avro.NewWriter(&buf, 10)
	w.Error = errors.New("test")

	err := w.Flush()

	assert.Error(t, err)
}

func TestWriter_FlushReturnsUnderlyingWriterError(t *testing.T) {
	w := avro.NewWriter(&errorWriter{}, 10)
	_, _ = w.Write([]byte("test"))

	err := w.Flush()

	assert.Error(t, err)
	assert.Error(t, w.Error)
}

func TestWriter_Write(t *testing.T) {
	w := avro.NewWriter(nil, 50)

	_, _ = w.Write([]byte{0xBC, 0xDC, 0x06, 0x00, 0x10, 0x0A})

	assert.Equal(t, []byte{0xBC, 0xDC, 0x06, 0x00, 0x10, 0x0A}, w.Buffer())
}

func TestWriter_WriteBool(t *testing.T) {
	tests := []struct {
		data bool
		want []byte
	}{
		{
			data: false,
			want: []byte{0x00},
		},
		{
			data: true,
			want: []byte{0x01},
		},
	}

	for _, test := range tests {
		w := avro.NewWriter(nil, 50)

		w.WriteBool(test.data)

		assert.Equal(t, test.want, w.Buffer())
	}
}

func TestWriter_WriteInt(t *testing.T) {
	tests := []struct {
		data int32
		want []byte
	}{
		{
			data: 27,
			want: []byte{0x36},
		},
		{
			data: -8,
			want: []byte{0x0F},
		},
		{
			data: -1,
			want: []byte{0x01},
		},
		{
			data: 0,
			want: []byte{0x00},
		},
		{
			data: 1,
			want: []byte{0x02},
		},
		{
			data: -64,
			want: []byte{0x7F},
		},
		{
			data: 64,
			want: []byte{0x80, 0x01},
		},
		{
			data: 123456789,
			want: []byte{0xAA, 0xB4, 0xDE, 0x75},
		},
		{
			data: 987654321,
			want: []byte{0xE2, 0xA2, 0xF3, 0xAD, 0x07},
		},
	}

	for _, test := range tests {
		w := avro.NewWriter(nil, 50)

		w.WriteInt(test.data)

		assert.Equal(t, test.want, w.Buffer())
	}
}

func TestWriter_WriteLong(t *testing.T) {
	tests := []struct {
		data int64
		want []byte
	}{
		{
			data: 27,
			want: []byte{0x36},
		},
		{
			data: -8,
			want: []byte{0x0F},
		},
		{
			data: -1,
			want: []byte{0x01},
		},
		{
			data: 0,
			want: []byte{0x00},
		},
		{
			data: 1,
			want: []byte{0x02},
		},
		{
			data: -64,
			want: []byte{0x7F},
		},
		{
			data: 64,
			want: []byte{0x80, 0x01},
		},
		{
			data: 123456789,
			want: []byte{0xAA, 0xB4, 0xDE, 0x75},
		},
		{
			data: 987654321,
			want: []byte{0xE2, 0xA2, 0xF3, 0xAD, 0x07},
		},
		{
			data: 9223372036854775807,
			want: []byte{0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		},
		{
			data: -9223372036854775808,
			want: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
		},
		{
			data: -5468631321897454687,
			want: []byte{0xBD, 0xB1, 0xAE, 0xD4, 0xD2, 0xCD, 0xBD, 0xE4, 0x97, 0x01},
		},
	}

	for _, test := range tests {
		w := avro.NewWriter(nil, 50)

		w.WriteLong(test.data)

		assert.Equal(t, test.want, w.Buffer())
	}
}

func TestWriter_WriteFloat(t *testing.T) {
	tests := []struct {
		data float32
		want []byte
	}{
		{
			data: 0.0,
			want: []byte{0x00, 0x00, 0x00, 0x00},
		},
		{
			data: 1.0,
			want: []byte{0x00, 0x00, 0x80, 0x3F},
		},
		{
			data: 1.15,
			want: []byte{0x33, 0x33, 0x93, 0x3F},
		},
		{
			data: -53.964,
			want: []byte{0x23, 0xDB, 0x57, 0xC2},
		},
		{
			data: -123456789.123,
			want: []byte{0xA3, 0x79, 0xEB, 0xCC},
		},
		{
			data: 987654.111115,
			want: []byte{0x62, 0x20, 0x71, 0x49},
		},
	}

	for _, test := range tests {
		w := avro.NewWriter(nil, 50)

		w.WriteFloat(test.data)

		assert.Equal(t, test.want, w.Buffer())
	}
}

func TestWriter_WriteDouble(t *testing.T) {
	tests := []struct {
		data float64
		want []byte
	}{
		{
			data: 0.0,
			want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		{
			data: 1.0,
			want: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F},
		},
		{
			data: 1.15,
			want: []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0xF2, 0x3F},
		},
		{
			data: -53.964,
			want: []byte{0x08, 0xAC, 0x1C, 0x5A, 0x64, 0xFB, 0x4A, 0xC0},
		},
		{
			data: -123456789.123,
			want: []byte{0xB6, 0xF3, 0x7D, 0x54, 0x34, 0x6F, 0x9D, 0xC1},
		},
		{
			data: 987654.111115,
			want: []byte{0xB6, 0x10, 0xE4, 0x38, 0x0C, 0x24, 0x2E, 0x41},
		},
		{
			data: 123456789.123456789,
			want: []byte{0x75, 0x6B, 0x7E, 0x54, 0x34, 0x6F, 0x9D, 0x41},
		},
		{
			data: 9999999.99999999999999999999999,
			want: []byte{0x00, 0x00, 0x00, 0x00, 0xD0, 0x12, 0x63, 0x41},
		},
		{
			data: 916734926348163.01973408746523,
			want: []byte{0x18, 0xFC, 0x1A, 0xDD, 0x1F, 0x0E, 0x0A, 0x43},
		},
		{
			data: -267319348967891263.1928357138913857,
			want: []byte{0x0A, 0x8F, 0xA6, 0x40, 0xAC, 0xAD, 0x8D, 0xC3},
		},
	}

	for _, test := range tests {
		w := avro.NewWriter(nil, 50)

		w.WriteDouble(test.data)

		assert.Equal(t, test.want, w.Buffer())
	}
}

func TestWriter_WriteBytes(t *testing.T) {
	tests := []struct {
		data []byte
		want []byte
	}{
		{
			data: []byte{0x02},
			want: []byte{0x02, 0x02},
		},
		{
			data: []byte{0x03, 0xFF},
			want: []byte{0x04, 0x03, 0xFF},
		},
		{
			data: []byte{0xEC, 0xAB, 0x44, 0x00},
			want: []byte{0x08, 0xEC, 0xAB, 0x44, 0x00},
		},
		{
			data: []byte{0xAC, 0xDC, 0x01, 0x00, 0x10, 0x0F},
			want: []byte{0x0C, 0xAC, 0xDC, 0x01, 0x00, 0x10, 0x0F},
		},
	}

	for _, test := range tests {
		w := avro.NewWriter(nil, 50)

		w.WriteBytes(test.data)

		assert.Equal(t, test.want, w.Buffer())
	}
}

func TestWriter_WriteString(t *testing.T) {
	tests := []struct {
		data string
		want []byte
	}{
		{
			data: "",
			want: []byte{0x00},
		},
		{
			data: "foo",
			want: []byte{0x06, 0x66, 0x6F, 0x6F},
		},
		{
			data: "avro",
			want: []byte{0x08, 0x61, 0x76, 0x72, 0x6F},
		},
		{
			data: "apache",
			want: []byte{0x0C, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65},
		},
		{
			data: "oppan gangnam style!",
			want: []byte{0x28, 0x6F, 0x70, 0x70, 0x61, 0x6E, 0x20, 0x67, 0x61, 0x6E, 0x67, 0x6E, 0x61, 0x6D, 0x20, 0x73, 0x74, 0x79, 0x6C, 0x65, 0x21},
		},
		{
			data: "че-то по русски",
			want: []byte{0x36, 0xD1, 0x87, 0xD0, 0xB5, 0x2D, 0xD1, 0x82, 0xD0, 0xBE, 0x20, 0xD0, 0xBF, 0xD0, 0xBE, 0x20, 0xD1, 0x80, 0xD1, 0x83, 0xD1, 0x81, 0xD1, 0x81, 0xD0, 0xBA, 0xD0, 0xB8},
		},
		{
			data: "世界",
			want: []byte{0x0C, 0xE4, 0xB8, 0x96, 0xE7, 0x95, 0x8C},
		},
		{
			data: "!№;%:?*\"()@#$^&",
			want: []byte{0x22, 0x21, 0xE2, 0x84, 0x96, 0x3B, 0x25, 0x3A, 0x3F, 0x2A, 0x22, 0x28, 0x29, 0x40, 0x23, 0x24, 0x5E, 0x26},
		},
		{
			data: `{"field":"value"}`,
			want: []byte{0x22, 0x7b, 0x22, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x22, 0x3a, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x7d},
		},
		{
			data: `{"field": {"complex": 1}}`,
			want: []byte{0x32, 0x7b, 0x22, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x22, 0x3a, 0x20, 0x7b, 0x22, 0x63, 0x6f, 0x6d, 0x70, 0x6c, 0x65, 0x78, 0x22, 0x3a, 0x20, 0x31, 0x7d, 0x7d},
		},
	}

	for _, test := range tests {
		w := avro.NewWriter(nil, 50)

		w.WriteString(test.data)

		assert.Equal(t, test.want, w.Buffer())
	}
}

func TestWriter_WriteBlockHeader(t *testing.T) {
	tests := []struct {
		len  int64
		size int64
		want []byte
	}{
		{
			len:  64,
			size: 0,
			want: []byte{0x80, 0x01},
		},
		{
			len:  64,
			size: 64,
			want: []byte{0x7F, 0x80, 0x01},
		},
	}

	for _, test := range tests {
		w := avro.NewWriter(nil, 50)

		w.WriteBlockHeader(test.len, test.size)

		assert.Equal(t, test.want, w.Buffer())
	}
}

func TestWriter_WriteBlockCB(t *testing.T) {
	w := avro.NewWriter(nil, 50)

	wrote := w.WriteBlockCB(func(w *avro.Writer) int64 {
		w.WriteString("foo")
		w.WriteString("avro")

		return 2
	})

	assert.Equal(t, []byte{0x03, 0x12, 0x06, 0x66, 0x6F, 0x6F, 0x08, 0x61, 0x76, 0x72, 0x6F}, w.Buffer())
	assert.Equal(t, int64(2), wrote)
}

type errorWriter struct{}

func (errorWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("test error")
}
