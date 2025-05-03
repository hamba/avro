package ocf

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZstdEncodeDecodeLowEntropyLong(t *testing.T) {
	input := makeTestData(8762, func() byte { return 'a' })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeLowEntropyShort(t *testing.T) {
	input := makeTestData(7, func() byte { return 'a' })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeHighEntropyLong(t *testing.T) {
	input := makeTestData(8762, func() byte { return byte(rand.Uint32()) })

	verifyZstdEncodeDecode(t, input)
}

func TestZstdEncodeDecodeHighEntropyShort(t *testing.T) {
	input := makeTestData(7, func() byte { return byte(rand.Uint32()) })

	verifyZstdEncodeDecode(t, input)
}

/*
benchmark results always creating a new zstd encoder/decoder

goos: linux
goarch: amd64
pkg: github.com/hamba/avro/v2/ocf
cpu: AMD Ryzen 5 3550H with Radeon Vega Mobile Gfx


BenchmarkZstdEncodeDecodeLowEntropyLong
BenchmarkZstdEncodeDecodeLowEntropyLong-8    	     289	   3523847 ns/op	10891887 B/op	      40 allocs/op
BenchmarkZstdEncodeDecodeHighEntropyLong
BenchmarkZstdEncodeDecodeHighEntropyLong-8   	     298	   3390952 ns/op	10894703 B/op	      40 allocs/op


benchmark results reusing an existing zstd encoder/decoder

BenchmarkZstdEncodeDecodeLowEntropyLong
BenchmarkZstdEncodeDecodeLowEntropyLong-8    	   55628	     22883 ns/op	   19220 B/op	       2 allocs/op
BenchmarkZstdEncodeDecodeHighEntropyLong
BenchmarkZstdEncodeDecodeHighEntropyLong-8   	   47652	     25064 ns/op	   31553 B/op	       3 allocs/op
*/

func BenchmarkZstdEncodeDecodeLowEntropyLong(b *testing.B) {
	input := makeTestData(8762, func() byte { return 'a' })

	codec, err := resolveCodec(ZStandard, codecOptions{})
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed := codec.Encode(input)
		_, decodeErr := codec.Decode(compressed)
		require.NoError(b, decodeErr)
	}
}

func BenchmarkZstdEncodeDecodeHighEntropyLong(b *testing.B) {
	input := makeTestData(8762, func() byte { return byte(rand.Uint32()) })

	codec, err := resolveCodec(ZStandard, codecOptions{})
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compressed := codec.Encode(input)
		_, decodeErr := codec.Decode(compressed)
		require.NoError(b, decodeErr)
	}
}

func verifyZstdEncodeDecode(t *testing.T, input []byte) {
	codec, err := resolveCodec(ZStandard, codecOptions{})
	require.NoError(t, err)

	compressed := codec.Encode(input)
	actual, decodeErr := codec.Decode(compressed)

	require.NoError(t, decodeErr)
	assert.Equal(t, input, actual)
}

func makeTestData(length int, charMaker func() byte) []byte {
	input := make([]byte, length)
	for i := 0; i < length; i++ {
		input[i] = charMaker()
	}
	return input
}
