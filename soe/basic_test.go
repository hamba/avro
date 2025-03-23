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

func newCoders(t *testing.T) (*soe.Encoder, *soe.Decoder) {
	t.Helper()

	encoder, err := soe.NewEncoder(avro.DefaultConfig, schema)
	require.NoError(t, err)

	decoder, err := soe.NewDecoder(avro.DefaultConfig, schema)
	require.NoError(t, err)

	return encoder, decoder
}

func TestRoundtrip(t *testing.T) {
	encoder, decoder := newCoders(t)

	v0 := Value{
		StringVal: "abc",
		IntVal:    123,
	}

	data, err := encoder.Encode(v0)
	require.NoError(t, err)

	var v1 Value
	err = decoder.Decode(data, &v1)
	require.NoError(t, err)
	require.Equal(t, v0, v1)
}

func TestShortHeader(t *testing.T) {
	_, decoder := newCoders(t)

	// At least 10 bytes header required
	data := []byte{
		0xc3, 0x01,
	}

	// Both Decode and DecodeStrict validate the length
	var v1 Value
	err := decoder.Decode(data, &v1)
	require.ErrorContains(t, err, "too short")

	err = decoder.DecodeStrict(data, &v1)
	require.ErrorContains(t, err, "too short")
}

func TestBadMagic(t *testing.T) {
	_, decoder := newCoders(t)

	data := []byte{
		// Invalid magic
		0x00, 0x00,
		// Faux schema ID
		0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
		// No data payload
	}

	// Both Decode and DecodeStrict validate the magic
	var v1 Value
	err := decoder.Decode(data, &v1)
	require.ErrorContains(t, err, "invalid magic")

	err = decoder.DecodeStrict(data, &v1)
	require.ErrorContains(t, err, "invalid magic")
}

func TestBadFingerprint(t *testing.T) {
	_, decoder := newCoders(t)

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
	err := decoder.Decode(data, &v1)
	require.NoError(t, err)

	// DecodeStrict fails earlier due to fingerprint mismatch
	err = decoder.DecodeStrict(data, &v1)
	require.ErrorContains(t, err, "bad fingerprint")
}

func TestHeaderFormat(t *testing.T) {
	encoder, _ := newCoders(t)

	v0 := Value{}
	data, err := encoder.Encode(v0)
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
