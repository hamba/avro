package soe_test

import (
	"testing"

	"github.com/hamba/avro/v2/soe"
	"github.com/hamba/avro/v2/soe/testdata"
	"github.com/stretchr/testify/require"
)

func newTypedCodec(t *testing.T) *soe.TypedCodec[*testdata.Generated] {
	t.Helper()

	codec, err := soe.NewTypedCodec[*testdata.Generated]()
	require.NoError(t, err)

	return codec
}

func TestTypedRoundtrip(t *testing.T) {
	codec := newTypedCodec(t)

	v0 := testdata.Generated{
		Name: "bob",
		Age:  14,
	}

	data, err := codec.Encode(&v0)
	require.NoError(t, err)

	var v1 testdata.Generated
	err = codec.Decode(data, &v1)
	require.NoError(t, err)
	require.Equal(t, v0, v1)
}

func TestTypedShortHeader(t *testing.T) {
	codec := newTypedCodec(t)

	// At least 10 bytes header required
	data := []byte{
		0xc3, 0x01,
	}

	// Both Decode and DecodeUnverified validate the length
	var v1 testdata.Generated
	err := codec.Decode(data, &v1)
	require.ErrorContains(t, err, "too short")

	err = codec.DecodeUnverified(data, &v1)
	require.ErrorContains(t, err, "too short")
}

func TestTypedBadMagic(t *testing.T) {
	codec := newTypedCodec(t)

	data := []byte{
		// Invalid magic
		0x00, 0x00,
		// Faux schema ID
		0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
		// No data payload
	}

	// Both Decode and DecodeUnverified validate the magic
	var v1 testdata.Generated
	err := codec.Decode(data, &v1)
	require.ErrorContains(t, err, "invalid magic")

	err = codec.DecodeUnverified(data, &v1)
	require.ErrorContains(t, err, "invalid magic")
}

func TestTypedBadFingerprint(t *testing.T) {
	codec := newTypedCodec(t)

	data := []byte{
		// Good magic
		0xc3, 0x01,
		// Faux schema ID
		0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
		// No data payload
	}

	// DecodeUnverified does not validate the fingerprint, and successfully
	// decodes empty payload.
	var v1 testdata.Generated
	err := codec.DecodeUnverified(data, &v1)
	require.NoError(t, err)

	// Decode fails earlier due to fingerprint mismatch
	err = codec.Decode(data, &v1)
	require.ErrorContains(t, err, "bad fingerprint")
}

func TestTypedHeaderFormat(t *testing.T) {
	codec := newTypedCodec(t)

	v0 := testdata.Generated{}
	data, err := codec.Encode(&v0)
	require.NoError(t, err)

	var header []byte
	if len(data) < 10 {
		header = data
	} else {
		header = data[:10]
	}

	// Build an expected header from magic + schema fingerprint
	schema := soe.GetSchema[*testdata.Generated]()
	expectedFingerprint, err := soe.ComputeFingerprint(schema)
	require.NoError(t, err)
	expectedHeader := append(soe.Magic, expectedFingerprint...)

	// Compare to the actual header
	require.Equal(t, expectedHeader, header)
}
