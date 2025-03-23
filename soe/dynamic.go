package soe

import (
	"context"
	"fmt"

	"github.com/hamba/avro/v2"
)

// DynamicDecoder unmarshals SOE-framed records. It will use a schema resolver
// to lookup schemas for every record, and deserialize using exactly the writer
// schema. This makes it possible to decode records from entirely disparate
// schemas into the same Go type.
type DynamicDecoder struct {
	api      avro.API
	resolver SchemaResolver
}

func NewDynamicDecoder(api avro.API, resolver SchemaResolver) *DynamicDecoder {
	return &DynamicDecoder{
		api:      api,
		resolver: resolver,
	}
}

func (d *DynamicDecoder) Decode(ctx context.Context, data []byte, v any) error {
	fingerprint, data, err := ParseHeader(data)
	if err != nil {
		return err
	}
	schema, err := d.resolver.GetSchema(ctx, fingerprint)
	if err != nil {
		return fmt.Errorf("getting schema: %w", err)
	}
	return d.api.Unmarshal(schema, data, v)
}
