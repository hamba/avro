package soe

import (
	"context"

	"github.com/hamba/avro/v2"
)

// SchemaResolver implementations are expected to be callable for every record,
// so should cache expensive schema lookups.
type SchemaResolver interface {
	GetSchema(ctx context.Context, fingerprint []byte) (avro.Schema, error)
}
