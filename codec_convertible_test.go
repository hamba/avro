package avro_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_ConvertiblePtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x90, 0xb2, 0xae, 0xc3, 0xec, 0x5b}
	schema := `{"type": "long", "logicalType": "timestamp-millis"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var ts TestTimestampPtr
	err = dec.Decode(&ts)

	require.NoError(t, err)
	want := TestTimestampPtr(time.Date(2020, 01, 02, 03, 04, 05, 00, time.UTC))
	assert.Equal(t, want, ts)
}

func TestDecoder_ConvertiblePtrPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x2, 0x90, 0xb2, 0xae, 0xc3, 0xec, 0x5b}
	schema := `{"type" : ["null", {"type": "long", "logicalType": "timestamp-millis"}]}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var ts *TestTimestampPtr
	err = dec.Decode(&ts)

	require.NoError(t, err)
	assert.NotNil(t, ts)
	want := TestTimestampPtr(time.Date(2020, 01, 02, 03, 04, 05, 00, time.UTC))
	assert.Equal(t, want, *ts)
}

func TestDecoder_ConvertibleError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x90, 0xb2, 0xae, 0xc3, 0xec, 0x5b}
	schema := `{"type": "long", "logicalType": "timestamp-millis"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var ts *TestTimestampError
	err = dec.Decode(&ts)

	assert.Error(t, err)
}

func TestEncoder_Convertible(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type": "long", "logicalType": "timestamp-millis"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	ts := TestTimestamp(time.Date(2020, 01, 02, 03, 04, 05, 00, time.UTC))

	err = enc.Encode(ts)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x90, 0xb2, 0xae, 0xc3, 0xec, 0x5b}, buf.Bytes())
}

func TestEncoder_ConvertiblePtr(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type" : ["null", {"type": "long", "logicalType": "timestamp-millis"}]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	ts := TestTimestampPtr(time.Date(2020, 01, 02, 03, 04, 05, 00, time.UTC))

	err = enc.Encode(&ts)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x2, 0x90, 0xb2, 0xae, 0xc3, 0xec, 0x5b}, buf.Bytes())
}

func TestEncoder_ConvertiblePtrNil(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type" : ["null", {"type": "long", "logicalType": "timestamp-millis"}]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	var ts *TestTimestampPtr

	err = enc.Encode(ts)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_ConvertibleError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type": "long", "logicalType": "timestamp-millis"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)
	ts := TestTimestampError{}

	err = enc.Encode(&ts)

	assert.Error(t, err)
}
