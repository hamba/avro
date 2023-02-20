package registry

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"
)

// DecoderFunc is a function used to customize the Decoder.
type DecoderFunc func(*Decoder)

// WithAPI generates a new decoder with the same client as the one on
// which the method is invoked and as api the one provided in input.
func WithAPI(api avro.API) DecoderFunc {
	return func(d *Decoder) {
		d.api = api
	}
}

// Decoder is entitled to receive raw messages as bytes, obtain the
// related schema and then use it to deserialize the remaining content.
type Decoder struct {
	client *Client
	api    avro.API
}

// NewDecoder shall return a new decoder given a client.
func NewDecoder(client *Client, opts ...DecoderFunc) *Decoder {
	d := &Decoder{
		client: client,
		api:    avro.DefaultConfig,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Decode takes in input a payload to be deserialized, extrapolates
// its schema id, gets the related schema and uses it to unmarshal the payload.
// The payload shall be formatted according to:
// https://docs.confluent.io/3.2.0/schema-registry/docs/serializer-formatter.html#wire-format .
func (d *Decoder) Decode(
	ctx context.Context,
	payload []byte,
	target interface{},
) error {
	if len(payload) < 6 {
		return fmt.Errorf("payload not containing data")
	}

	id, err := extractSchemaIDFromPayload(payload)
	if err != nil {
		return fmt.Errorf("unable to extract schema id from payload, error: %w", err)
	}

	schema, err := d.client.GetSchema(ctx, id)
	if err != nil {
		return fmt.Errorf("unable to obtain schema, error: %w", err)
	}

	return d.api.Unmarshal(schema, payload[5:], target)
}

// ExtractSchemaIDFromPayload extrapolates the schema id from a payload composed
// of raw bytes containing a magic bytes, 4 bytes representing the schema encoding,
// and the remaining payload being encoded with avro, as described in
// https://docs.confluent.io/3.2.0/schema-registry/docs/serializer-formatter.html#wire-format .
func extractSchemaIDFromPayload(payload []byte) (int, error) {
	if len(payload) < 5 {
		return 0, fmt.Errorf("payload too short to contain avro header")
	}
	if payload[0] != 0 {
		return 0, fmt.Errorf("magic byte value is %d, different from 0", payload[0])
	}
	return int(binary.BigEndian.Uint32(payload[1:5])), nil
}
