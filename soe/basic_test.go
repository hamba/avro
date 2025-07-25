package soe_test

import (
	"testing"

	"github.com/hamba/avro/v2/soe"
	"github.com/hamba/avro/v2/soe/internal/testdata"
	"github.com/stretchr/testify/require"
)

func newCodec(t *testing.T) *soe.Codec {
	t.Helper()

	codec, err := soe.NewCodec(testdata.StringIntSchema)
	require.NoError(t, err)

	return codec
}

// Used to test over all decoder functions.
func decoderFuncs(codec *soe.Codec) map[string]func([]byte, any) error {
	return map[string]func([]byte, any) error {
		"Decode": codec.Decode,
		"DecodeUnverified": codec.DecodeUnverified,
	}
}

func TestCodec_Roundtrip(t *testing.T) {
	codec := newCodec(t)

	v0 := testdata.StringInt{
		StringVal: "abc",
		IntVal:    123,
	}

	// Encode
	data, err := codec.Encode(v0)
	require.NoError(t, err)

	// Test all decoders behave the same.
	for name, decoderFunc := range decoderFuncs(codec) {
		t.Run(name, func(t *testing.T) {
			var v1 testdata.StringInt
			err := decoderFunc(data, &v1)

			// All decoders should successfully decode good data.
			require.NoError(t, err)
			require.Equal(t, v0, v1)
		})
	}
}

func TestCodec_DecodeShortHeader(t *testing.T) {
	codec := newCodec(t)

	// At least 10 bytes header required
	data := []byte{
		0xc3, 0x01,
	}

	// Test all decoders behave the same.
	for name, decoderFunc := range decoderFuncs(codec) {
		t.Run(name, func(t *testing.T) {
			var v1 testdata.StringInt
			err := decoderFunc(data, &v1)

			// All decoders should validate length.
			require.ErrorContains(t, err, "too short")
		})
	}
}

func TestCodec_DecodeBadMagic(t *testing.T) {
	codec := newCodec(t)

	data := []byte{
		// Invalid magic
		0x00, 0x00,
		// Faux schema ID
		0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
		// No data payload
	}

	// Test all decoders behave the same.
	for name, decoderFunc := range decoderFuncs(codec) {
		t.Run(name, func(t *testing.T) {
			var v1 testdata.StringInt
			err := decoderFunc(data, &v1)

			// All decoders should validate the magic
			require.ErrorContains(t, err, "invalid magic")

		})
	}
}

func TestCodec_DecodeBadFingerprint(t *testing.T) {
	codec := newCodec(t)

	data := []byte{
		// Good magic
		0xc3, 0x01,
		// Faux schema ID
		0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
		// No data payload
	}

	t.Run("Decode", func(t *testing.T) {
		// Decode fails due to fingerprint mismatch
		var v1 testdata.StringInt
		err := codec.Decode(data, &v1)

		require.ErrorContains(t, err, "bad fingerprint")
	})
	t.Run("DecodeUnverified", func(t *testing.T) {
		// DecodeUnverified does not validate the fingerprint, and
		// successfully decodes empty payload.
		var v1 testdata.StringInt
		err := codec.DecodeUnverified(data, &v1)

		require.NoError(t, err)
		require.Equal(t, testdata.StringInt{}, v1)
	})
}

func TestCodec_HeaderFormat(t *testing.T) {
	codec := newCodec(t)

	// Build an expected header from magic + schema fingerprint
	expectedHeader, err := soe.BuildHeader(testdata.StringIntSchema)
	require.NoError(t, err)

	// Encode an arbitrary value
	v0 := testdata.StringInt{}
	data, err := codec.Encode(v0)
	require.NoError(t, err)

	// Extract as much of SOE header as is available from payload.
	var header []byte
	if len(data) < 10 {
		header = data
	} else {
		header = data[:10]
	}

	// Compare to the actual header
	require.Equal(t, expectedHeader, header)
}
