package soe

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
)

// SchemaResolver implementations are expected to be callable for every record,
// so should cache expensive schema lookups.
type SchemaResolver interface {
	GetSchema(ctx context.Context, fingerprint []byte) (avro.Schema, error)
}

// ErrUnknownSchema is an API error that must be returned if the requested
// schema is not known to a resolver.
var ErrUnknownSchema = errors.New("unknown schema")

// MemorySchemaRegistry is an in-memory cache, intended for testing. It is not
// safe for multithreaded use.
type MemorySchemaRegistry struct {
	schemas map[uint64]avro.Schema
}

// NewMemorySchemaRegistry creates a new MemorySchemaRegistry.
func NewMemorySchemaRegistry() *MemorySchemaRegistry {
	return &MemorySchemaRegistry{
		schemas: make(map[uint64]avro.Schema),
	}
}

// AddSchema puts a schema into the registry.
func (r *MemorySchemaRegistry) AddSchema(s avro.Schema) error {
	fp, err := ComputeFingerprint(s)
	if err != nil {
		return err
	}

	key := keyFromFingerprint(fp)
	r.schemas[key] = s
	return nil
}

// GetSchema implements SchemaResolver.
func (r *MemorySchemaRegistry) GetSchema(_ context.Context, fingerprint []byte) (avro.Schema, error) {
	key := keyFromFingerprint(fingerprint)
	schema, ok := r.schemas[key]
	if !ok {
		return nil, fmt.Errorf("%w: %x", ErrUnknownSchema, fingerprint)
	}
	return schema, nil
}

// keyFromFingerprint creates an arbitrary uint64 encoding of a schema
// fingerprint, suitable as a map key.
func keyFromFingerprint(fingerprint []byte) uint64 {
	return binary.LittleEndian.Uint64(fingerprint)
}
