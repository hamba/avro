package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_SkipBool(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "boolean"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipInt(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "int"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipLong(t *testing.T) {
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
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipFloat(t *testing.T) {
	data := []byte{0x0, 0x0, 0x0, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "float"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipDouble(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "double"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipBytes(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "bytes"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipString(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "string"},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipRecord(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "record", "name": "test2", "fields": [{"name": "c", "type": "string"}]}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipRef(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "record", "name": "test2", "fields": [{"name": "c", "type": "string"}]}},
	    {"name": "b", "type": "string"},
		{"name": "c", "type": "test2"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipEnum(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "enum", "name": "test2", "symbols": ["sym1", "sym2"]}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipArray(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "array", "items": "int"}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipArrayBlocks(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x03, 0x04, 0x36, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "array", "items": "int"}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x02, 0x66, 0x36, 0x02, 0x6f, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "map", "values": "int"}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipMapBlocks(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x03, 0x0C, 0x02, 0x66, 0x36, 0x02, 0x6f, 0x36, 0x0, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "map", "values": "int"}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipUnion(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x02, 0x66, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"]},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipUnionNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"]},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}

func TestDecoder_SkipUnionInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x03, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"]},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_SkipFixed(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x02, 0x66, 0x36, 0x02, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": {"type": "fixed", "name": "test2", "size": 7}},
	    {"name": "b", "type": "string"}
	]
}`

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestPartialRecord{B: "foo"}, got)
}
