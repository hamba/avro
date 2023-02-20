package registry

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//series of tests for decoding

func TestDeserialize_extractSchemaIDFromPayload(t *testing.T) {
	id := 42
	payload := make([]byte, 5)
	payload[0] = 0

	binary.BigEndian.PutUint32(payload[1:5], uint32(id))

	extracted_id, err := extractSchemaIDFromPayload(payload)

	require.NoError(t, err)

	assert.Equal(t, extracted_id, id)
}

func TestDeserializeError_extractSchemaIDFromPayload(t *testing.T) {
	payload := make([]byte, 4)
	payload[0] = 0

	_, err := extractSchemaIDFromPayload(payload)

	assert.Equal(t, err.Error(), "payload too short to contain avro header")

	payload = make([]byte, 5)
	payload[0] = 1

	_, err = extractSchemaIDFromPayload(payload)

	assert.Equal(t, err.Error(), fmt.Sprintf("magic byte value is %d, different from 0", payload[0]))
}
