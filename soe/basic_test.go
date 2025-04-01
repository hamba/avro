package soe_test

import (
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/soe"
	"github.com/stretchr/testify/require"
)

type Value struct {
	StringVal string `avro:"stringval"`
	IntVal    int    `avro:"intval"`
}

var schema = avro.MustParse(`{"name":"value","type":"record","fields":[{"name":"stringval","type":"string"},{"name":"intval","type":"int"}]}`)

func newCodec(t *testing.T) *soe.Codec {
	t.Helper()

	codec, err := soe.NewCodecWithAPI(schema, avro.DefaultConfig)
	require.NoError(t, err)

	return codec
}

func TestRoundtrip(t *testing.T) {
	codec := newCodec(t)

	v0 := Value{
		StringVal: "abc",
		IntVal:    123,
	}

	data, err := codec.Encode(v0)
	require.NoError(t, err)

	var v1 Value
	err = codec.Decode(data, &v1)
	require.NoError(t, err)
	require.Equal(t, v0, v1)
}

func TestShortHeader(t *testing.T) {
	codec := newCodec(t)

	// At least 10 bytes header required
	data := []byte{
		0xc3, 0x01,
	}

	// Both Decode and DecodeStrict validate the length
	var v1 Value
	err := codec.Decode(data, &v1)
	require.ErrorContains(t, err, "too short")

	err = codec.DecodeStrict(data, &v1)
	require.ErrorContains(t, err, "too short")
}

func TestBadMagic(t *testing.T) {
	codec := newCodec(t)

	data := []byte{
		// Invalid magic
		0x00, 0x00,
		// Faux schema ID
		0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
		// No data payload
	}

	// Both Decode and DecodeStrict validate the magic
	var v1 Value
	err := codec.Decode(data, &v1)
	require.ErrorContains(t, err, "invalid magic")

	err = codec.DecodeStrict(data, &v1)
	require.ErrorContains(t, err, "invalid magic")
}

func TestBadFingerprint(t *testing.T) {
	codec := newCodec(t)

	data := []byte{
		// Good magic
		0xc3, 0x01,
		// Faux schema ID
		0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
		// No data payload
	}

	// Decode does not validate the fingerprint, and successfully decodes
	// empty payload.
	var v1 Value
	err := codec.Decode(data, &v1)
	require.NoError(t, err)

	// DecodeStrict fails earlier due to fingerprint mismatch
	err = codec.DecodeStrict(data, &v1)
	require.ErrorContains(t, err, "bad fingerprint")
}

func TestHeaderFormat(t *testing.T) {
	codec := newCodec(t)

	v0 := Value{}
	data, err := codec.Encode(v0)
	require.NoError(t, err)

	var header []byte
	if len(data) < 10 {
		header = data
	} else {
		header = data[:10]
	}

	// Build an expected header from magic + schema fingerprint
	expectedFingerprint, err := soe.ComputeFingerprint(schema)
	require.NoError(t, err)
	expectedHeader := append(soe.Magic, expectedFingerprint...)

	// Compare to the actual header
	require.Equal(t, expectedHeader, header)
}
