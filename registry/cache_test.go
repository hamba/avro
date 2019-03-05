package registry_test

import (
	"errors"
	"testing"

	"github.com/hamba/avro"
	"github.com/hamba/avro/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewCacheClient(t *testing.T) {
	m := new(MockRegistry)
	c := registry.NewCacheClient(m)

	assert.Implements(t, (*registry.Registry)(nil), c)
}

func TestCacheClient_GetSchema(t *testing.T) {
	schema, _ := avro.Parse(`["null"]`)
	m := new(MockRegistry)
	m.On("GetSchema", 5).Once().Return(schema, nil)

	client := registry.NewCacheClient(m)

	_, err := client.GetSchema(5)
	assert.NoError(t, err)

	_, err = client.GetSchema(5)
	assert.NoError(t, err)
}

func TestCacheClient_GetSchemaParentError(t *testing.T) {
	m := new(MockRegistry)
	m.On("GetSchema", 5).Once().Return(nil, errors.New("test"))

	client := registry.NewCacheClient(m)

	_, err := client.GetSchema(5)

	assert.Error(t, err)
}

func TestCacheClient_GetSubjects(t *testing.T) {
	m := new(MockRegistry)
	m.On("GetSubjects").Return([]string{}, nil)
	r := registry.NewCacheClient(m)

	_, err := r.GetSubjects()

	assert.NoError(t, err)
}

func TestCacheClient_GetVersions(t *testing.T) {
	m := new(MockRegistry)
	m.On("GetVersions", "foobar").Return([]int{}, nil)
	r := registry.NewCacheClient(m)

	_, err := r.GetVersions("foobar")

	assert.NoError(t, err)
}

func TestCacheClient_GetSchemaByVersion(t *testing.T) {
	schema, _ := avro.Parse(`["null"]`)
	m := new(MockRegistry)
	m.On("GetSchemaByVersion", "foobar", 5).Return(schema, nil)
	r := registry.NewCacheClient(m)

	_, err := r.GetSchemaByVersion("foobar", 5)

	assert.NoError(t, err)
}

func TestCacheClient_GetLatestSchema(t *testing.T) {
	schema, _ := avro.Parse(`["null"]`)
	m := new(MockRegistry)
	m.On("GetLatestSchema", "foobar").Return(schema, nil)
	r := registry.NewCacheClient(m)

	_, err := r.GetLatestSchema("foobar")

	assert.NoError(t, err)
}

func TestCacheClient_CreateSchema(t *testing.T) {
	schema, _ := avro.Parse(`["null"]`)
	m := new(MockRegistry)
	m.On("CreateSchema", "foobar", `["null"]`).Return(1, schema, nil)
	r := registry.NewCacheClient(m)

	id, _, err := r.CreateSchema("foobar", `["null"]`)

	assert.NoError(t, err)
	assert.Equal(t, id, 1)
}

func TestCacheClient_CreateSchemaParentError(t *testing.T) {
	m := new(MockRegistry)
	m.On("CreateSchema", "foobar", `["null"]`).Return(0, nil, errors.New("test"))
	r := registry.NewCacheClient(m)

	_, _, err := r.CreateSchema("foobar", `["null"]`)

	assert.Error(t, err)
}

func TestCacheClient_IsRegistered(t *testing.T) {
	schema, _ := avro.Parse(`["null"]`)
	m := new(MockRegistry)
	m.On("IsRegistered", "foobar", `["null"]`).Return(1, schema, nil)
	r := registry.NewCacheClient(m)

	id, _, err := r.IsRegistered("foobar", `["null"]`)

	assert.NoError(t, err)
	assert.Equal(t, id, 1)
}

func TestCacheClient_IsRegisteredParentError(t *testing.T) {
	m := new(MockRegistry)
	m.On("IsRegistered", "foobar", `["null"]`).Return(0, nil, errors.New("test"))
	r := registry.NewCacheClient(m)

	_, _, err := r.IsRegistered("foobar", `["null"]`)

	assert.Error(t, err)
}

func TestCacheClient_IsRegisteredAlreadyCached(t *testing.T) {
	schema, _ := avro.Parse(`["null"]`)
	m := new(MockRegistry)
	m.On("CreateSchema", "foobar", `["null"]`).Return(1, schema, nil)
	r := registry.NewCacheClient(m)
	_, _, _ = r.CreateSchema("foobar", `["null"]`)

	_, _, err := r.IsRegistered("foobar", `["null"]`)

	assert.NoError(t, err)
	m.AssertExpectations(t)
}

type MockRegistry struct {
	mock.Mock
}

func (m *MockRegistry) GetSchema(id int) (avro.Schema, error) {
	args := m.Called(id)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(avro.Schema), args.Error(1)
}

func (m *MockRegistry) GetSubjects() ([]string, error) {
	args := m.Called()
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockRegistry) GetVersions(subject string) ([]int, error) {
	args := m.Called(subject)
	return args.Get(0).([]int), args.Error(1)
}

func (m *MockRegistry) GetSchemaByVersion(subject string, version int) (avro.Schema, error) {
	args := m.Called(subject, version)
	return args.Get(0).(avro.Schema), args.Error(1)
}

func (m *MockRegistry) GetLatestSchema(subject string) (avro.Schema, error) {
	args := m.Called(subject)
	return args.Get(0).(avro.Schema), args.Error(1)
}

func (m *MockRegistry) CreateSchema(subject, schema string) (int, avro.Schema, error) {
	args := m.Called(subject, schema)

	var codec avro.Schema
	if c := args.Get(1); c != nil {
		codec = c.(avro.Schema)
	}

	return args.Int(0), codec, args.Error(2)
}

func (m *MockRegistry) IsRegistered(subject, schema string) (int, avro.Schema, error) {
	args := m.Called(subject, schema)

	var codec avro.Schema
	if c := args.Get(1); c != nil {
		codec = c.(avro.Schema)
	}

	return args.Int(0), codec, args.Error(2)
}
