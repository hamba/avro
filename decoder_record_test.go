package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestDecoder_RecordStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got TestRecord
	err = dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, TestRecord{A: 27, B: "foo"}, got)
}

func TestDecoder_RecordStructPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	got := &TestRecord{}
	err = dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, &TestRecord{A: 27, B: "foo"}, got)
}

func TestDecoder_RecordStructPtrNil(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got *TestRecord
	err = dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, &TestRecord{A: 27, B: "foo"}, got)
}

func TestDecoder_RecordPartialStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_RecordStructInvalidData(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got TestRecord
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RecordMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got map[string]interface{}
	err = dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"a": int64(27), "b": "foo"}, got)
}

func TestDecoder_RecordMapInvalidKey(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got map[int]interface{}
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RecordMapInvalidElem(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got map[string]string
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RecordMapInvalidData(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got map[string]interface{}
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RecordInterface(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got TestInterface = &TestRecord{}
	err = dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, &TestRecord{A: 27, B: "foo"}, got)
}

func TestDecoder_RecordEmptyInterface(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got TestInterface
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RefStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "parent",
	"fields" : [
		{"name": "a", "type": {
			"type": "record",
			"name": "test",
			"fields" : [
				{"name": "a", "type": "long"},
	    		{"name": "b", "type": "string"}
			]}
		},
	    {"name": "b", "type": "test"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var got TestNestedRecord
	err = dec.Decode(&got)

	want := TestNestedRecord{
		A: TestRecord{A: 27, B: "foo"},
		B: TestRecord{A: 27, B: "foo"},
	}
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}
