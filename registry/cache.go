package registry

import (
	"github.com/hamba/avro"
	"github.com/modern-go/concurrent"
)

// CacheClient is a cache registry client.
type CacheClient struct {
	client Registry

	idCache     *concurrent.Map // map[int]avro.Schema
	schemaCache *concurrent.Map // map[string]int
}

// NewCacheClient creates a cached registry client that falls back to the
// underlying Registry.
func NewCacheClient(client Registry) *CacheClient {
	return &CacheClient{
		client:      client,
		idCache:     concurrent.NewMap(),
		schemaCache: concurrent.NewMap(),
	}
}

// GetSchema returns the schema with the given id.
func (c *CacheClient) GetSchema(id int) (avro.Schema, error) {
	if schema, ok := c.idCache.Load(id); ok {
		return schema.(avro.Schema), nil
	}

	schema, err := c.client.GetSchema(id)
	if err != nil {
		return nil, err
	}

	c.idCache.Store(id, schema)

	return schema, nil
}

// GetSubjects gets the registry subjects.
func (c *CacheClient) GetSubjects() ([]string, error) {
	return c.client.GetSubjects()
}

// GetVersions gets the schema versions for a subject.
func (c *CacheClient) GetVersions(subject string) ([]int, error) {
	return c.client.GetVersions(subject)
}

// GetSchemaByVersion gets the schema by version.
func (c *CacheClient) GetSchemaByVersion(subject string, version int) (avro.Schema, error) {
	return c.client.GetSchemaByVersion(subject, version)
}

// GetLatestSchema gets the latest schema for a subject.
func (c *CacheClient) GetLatestSchema(subject string) (avro.Schema, error) {
	return c.client.GetLatestSchema(subject)
}

// CreateSchema creates a schema in the registry, returning the schema id.
func (c *CacheClient) CreateSchema(subject string, schema string) (int, avro.Schema, error) {
	id, sch, err := c.client.CreateSchema(subject, schema)
	if err != nil {
		return 0, nil, err
	}

	c.idCache.Store(id, sch)
	c.schemaCache.Store(schema, id)

	return id, sch, err
}

// IsRegistered determines of the schema is registered.
func (c *CacheClient) IsRegistered(subject string, schema string) (int, avro.Schema, error) {
	key, ok := c.schemaCache.Load(schema)
	if ok {
		// At this point both caches must have the schema
		id := key.(int)
		key, _ := c.idCache.Load(id)
		return id, key.(avro.Schema), nil
	}

	id, sch, err := c.client.IsRegistered(subject, schema)
	if err != nil {
		return 0, nil, err
	}

	c.idCache.Store(id, sch)
	c.schemaCache.Store(schema, id)

	return id, sch, err
}
