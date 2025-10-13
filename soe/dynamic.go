package soe

import (
	"context"
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
)

// ErrUnknownSchema is an API error that must be returned if the requested
// schema is not known to a resolver.
var ErrUnknownSchema = errors.New("unknown schema")

// SchemaResolver implementations are expected to be callable for every record,
// so should cache expensive schema lookups.
type SchemaResolver interface {
	// GetSchema must return ErrUnknownSchema if no schema is found for
	// fingerprint. All other errors are unexpected.
	GetSchema(ctx context.Context, fingerprint []byte) (avro.Schema, error)
}

// DynamicDecoder unmarshals SOE-framed records. It will use a schema resolver
// to lookup schemas for every record, and deserialize using exactly the writer
// schema. This makes it possible to decode records from entirely disparate
// schemas into the same Go type.
type DynamicDecoder struct {
	api      avro.API
	resolver SchemaResolver
}

// NewDynamicDecoder returns a new DynamicDecoder for a resolver the default
// config.
func NewDynamicDecoder(resolver SchemaResolver) *DynamicDecoder {
	return NewDynamicDecoderWithAPI(resolver, avro.DefaultConfig)
}

// NewDynamicDecoderWithAPI returns a new DynamicDecoder for the given resolver
// and API.
func NewDynamicDecoderWithAPI(resolver SchemaResolver, api avro.API) *DynamicDecoder {
	return &DynamicDecoder{
		api:      api,
		resolver: resolver,
	}
}

// Decode unmarshals a value from SOE-encoded Avro binary using the schema
// specified in the SOE header. Fails if schema is not known to resolver.
func (d *DynamicDecoder) Decode(ctx context.Context, data []byte, v any) error {
	fingerprint, data, err := ParseHeader(data)
	if err != nil {
		return err
	}
	schema, err := d.resolver.GetSchema(ctx, fingerprint)
	if err != nil {
		return fmt.Errorf("resolver: %w", err)
	}
	return d.api.Unmarshal(schema, data, v)
}
