package registry

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/hamba/avro/v2"
)

// DecoderFunc is a function used to customize the Decoder.
type DecoderFunc func(*Decoder)

// WithAPI sets the avro configuration on the decoder.
func WithAPI(api avro.API) DecoderFunc {
	return func(d *Decoder) {
		d.api = api
	}
}

// Decoder decodes confluent wire formatted avro payloads.
type Decoder struct {
	client *Client
	api    avro.API
}

// NewDecoder returns a decoder that will get schemas from client.
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

// Decode decodes data into v.
// The data must be formatted using the Confluent wire format, otherwise
// and error will be returned.
// See:
// https://docs.confluent.io/3.2.0/schema-registry/docs/serializer-formatter.html#wire-format.
func (d *Decoder) Decode(ctx context.Context, data []byte, v any) error {
	if len(data) < 6 {
		return fmt.Errorf("data too short")
	}

	id, err := extractSchemaID(data)
	if err != nil {
		return fmt.Errorf("extracting schema id: %w", err)
	}

	schema, err := d.client.GetSchema(ctx, id)
	if err != nil {
		return fmt.Errorf("getting schema: %w", err)
	}

	return d.api.Unmarshal(schema, data[5:], v)
}

func extractSchemaID(data []byte) (int, error) {
	if len(data) < 5 {
		return 0, fmt.Errorf("data too short")
	}
	if data[0] != 0 {
		return 0, fmt.Errorf("invalid magic byte: %x", data[0])
	}
	return int(binary.BigEndian.Uint32(data[1:5])), nil
}
