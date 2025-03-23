package resolvers

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/soe"
)

// MemorySchemaStore is a basic in-memory schema store and resolver
// implementation.
type MemorySchemaStore struct {
	schemas sync.Map // map[uint64]avro.Schema
}

// NewMemorySchemaStore creates a new MemorySchemaStore.
func NewMemorySchemaStore() *MemorySchemaStore {
	return &MemorySchemaStore{}
}

// AddSchema puts a schema into the registry.
func (s *MemorySchemaStore) AddSchema(schema avro.Schema) error {
	fp, err := soe.ComputeFingerprint(schema)
	if err != nil {
		return err
	}

	key := keyFromFingerprint(fp)
	s.schemas.Store(key, schema)
	return nil
}

// GetSchema implements SchemaResolver.
func (s *MemorySchemaStore) GetSchema(_ context.Context, fingerprint []byte) (avro.Schema, error) {
	key := keyFromFingerprint(fingerprint)
	schema, ok := s.schemas.Load(key)
	if !ok {
		return nil, fmt.Errorf("%w: %x", soe.ErrUnknownSchema, fingerprint)
	}
	return schema.(avro.Schema), nil
}

// keyFromFingerprint creates an arbitrary uint64 encoding of a schema
// fingerprint, suitable as a map key.
func keyFromFingerprint(fingerprint []byte) uint64 {
	return binary.LittleEndian.Uint64(fingerprint)
}
