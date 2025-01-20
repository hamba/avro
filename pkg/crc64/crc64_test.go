package crc64

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGolden(t *testing.T) {
	tests := []struct {
		in   string
		want uint64
	}{
		{
			in:   `"null"`,
			want: 7195948357588979594,
		},
		{
			in:   `{"name":"foo","type":"fixed","size":15}`,
			want: 1756455273707447556,
		},
		{
			in:   `{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}`,
			want: 7843277075252814651,
		},
	}

	hash := New()

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			hash.Reset()
			_, _ = hash.Write([]byte(test.in))

			got := hash.Sum64()

			assert.Equal(t, test.want, got)
		})
	}
}

func TestGoldenBytes(t *testing.T) {
	tests := []struct {
		in   string
		want []byte
	}{
		{
			in:   `"null"`,
			want: []byte{0x63, 0xdd, 0x24, 0xe7, 0xcc, 0x25, 0x8f, 0x8a},
		},
		{
			in:   `{"name":"foo","type":"fixed","size":15}`,
			want: []byte{0x18, 0x60, 0x2e, 0xc3, 0xed, 0x31, 0xa5, 0x4},
		},
		{
			in:   `{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}`,
			want: []byte{0x6c, 0xd8, 0xea, 0xf1, 0xc9, 0x68, 0xa3, 0x3b},
		},
	}

	hash := New()

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			hash.Reset()
			_, _ = hash.Write([]byte(test.in))

			got := make([]byte, 0, hash.Size())
			got = hash.Sum(got)

			assert.Equal(t, test.want, got)
		})
	}
}

func TestDigest_BlockSize(t *testing.T) {
	hash := New()

	assert.Equal(t, 1, hash.BlockSize())
}

func TestGoldenSum(t *testing.T) {
	tests := []struct {
		in  string
		out []byte
	}{
		{
			in:  `"null"`,
			out: []byte{0x63, 0xdd, 0x24, 0xe7, 0xcc, 0x25, 0x8f, 0x8a},
		},
		{
			in:  `{"name":"foo","type":"fixed","size":15}`,
			out: []byte{0x18, 0x60, 0x2e, 0xc3, 0xed, 0x31, 0xa5, 0x04},
		},
		{
			in:  `{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}`,
			out: []byte{0x6c, 0xd8, 0xea, 0xf1, 0xc9, 0x68, 0xa3, 0x3b},
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			got := Sum([]byte(test.in))
			assert.Equal(t, test.out, got[:])
		})
	}
}

func TestGoldenSumLittleEndian(t *testing.T) {
	tests := []struct {
		in  string
		out []byte
	}{
		{
			in:  `"null"`,
			out: []byte{0x8a, 0x8f, 0x25, 0xcc, 0xe7, 0x24, 0xdd, 0x63},
		},
		{
			in:  `{"name":"foo","type":"fixed","size":15}`,
			out: []byte{0x04, 0xa5, 0x31, 0xed, 0xc3, 0x2e, 0x60, 0x18},
		},
		{
			in:  `{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}`,
			out: []byte{0x3b, 0xa3, 0x68, 0xc9, 0xf1, 0xea, 0xd8, 0x6c},
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			got := SumLittleEndian([]byte(test.in))
			assert.Equal(t, test.out, got[:])
		})
	}
}

func bench(b *testing.B, size int64) {
	b.SetBytes(size)

	h := New()
	in := make([]byte, 0, h.Size())

	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Reset()
		_, _ = h.Write(data)
		h.Sum(in)

		in = in[:0]
	}
}

func BenchmarkCrc64(b *testing.B) {
	b.Run("64KB", func(b *testing.B) {
		bench(b, 64<<10)
	})
	b.Run("4KB", func(b *testing.B) {
		bench(b, 4<<10)
	})
	b.Run("1KB", func(b *testing.B) {
		bench(b, 1<<10)
	})
}

func BenchmarkSum(b *testing.B) {
	data := make([]byte, 4<<10)
	for i := range data {
		data[i] = byte(i)
	}

	b.Run("BigEndian", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = Sum(data)
		}
	})
	b.Run("LittleEndian", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = SumLittleEndian(data)
		}
	})
}
