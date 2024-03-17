package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)

	var got TestRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
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
	require.NoError(t, err)

	got := TestRecord{}
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestRecord{A: 27, B: "foo"}, got)
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
	require.NoError(t, err)

	var got *TestRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, &TestRecord{A: 27, B: "foo"}, got)
}

func TestDecoder_RecordStructWithFieldAlias(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "c", "aliases": ["a"], "type": "long"},
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
	require.NoError(t, err)

	var got TestPartialRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
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
	require.NoError(t, err)

	var got TestRecord
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_RecordEmbeddedStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestEmbeddedRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestEmbeddedRecord{TestEmbed: TestEmbed{A: 27, B: "foo"}, C: "bar"}, got)
}

func TestDecoder_RecordEmbeddedPtrStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestEmbeddedPtrRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestEmbeddedPtrRecord{TestEmbed: &TestEmbed{A: 27, B: "foo"}, C: "bar"}, got)
}

func TestDecoder_RecordEmbeddedPtrStructNew(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestEmbeddedPtrRecord
	got.C = "nonzero" // non-zero value here triggers bug in allocating TestEmbed
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestEmbeddedPtrRecord{TestEmbed: &TestEmbed{A: 27, B: "foo"}, C: "bar"}, got)
}

func TestDecoder_RecordEmbeddedIntStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got TestEmbeddedIntRecord
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, TestEmbeddedIntRecord{B: "foo"}, got)
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
	require.NoError(t, err)

	var got map[string]any
	err = dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{"a": int64(27), "b": "foo"}, got)
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
	require.NoError(t, err)

	var got map[int]any
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
	require.NoError(t, err)

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
	require.NoError(t, err)

	var got map[string]any
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
	require.NoError(t, err)

	var got TestInterface = &TestRecord{}
	err = dec.Decode(&got)

	require.NoError(t, err)
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
	require.NoError(t, err)

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
	require.NoError(t, err)

	var got TestNestedRecord
	err = dec.Decode(&got)

	want := TestNestedRecord{
		A: TestRecord{A: 27, B: "foo"},
		B: TestRecord{A: 27, B: "foo"},
	}
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestDecoder_RefStructRecursiveUnion(t *testing.T) {
	defer ConfigTeardown()

	type TestRecord struct {
		A int64       `avro:"a"`
		B string      `avro:"b"`
		C *TestRecord `avro:"c"`
	}
	schema := `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}, {"name": "c", "type": ["test", "null"]}]}`
	data := []byte{0x18, 0x6, 0x61, 0x61, 0x61, 0x0, 0x1a, 0x6, 0x62, 0x62, 0x62, 0x0, 0x58, 0x6, 0x63, 0x63, 0x63, 0x0, 0x6e, 0x6, 0x64, 0x64, 0x64, 0x2}
	recordOrigin := TestRecord{A: 12, B: "aaa", C: &TestRecord{A: 13, B: "bbb", C: &TestRecord{A: 44, B: "ccc", C: &TestRecord{A: 55, B: "ddd", C: nil}}}}
	var record TestRecord

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	err = dec.Decode(&record)
	require.NoError(t, err)
	assert.Equal(t, record, recordOrigin)

}

func TestDecoder_RefStructNestedRecursiveUnion(t *testing.T) {
	defer ConfigTeardown()

	type Record struct {
		D string `avro:"d"`
		E any    `avro:"e"`
	}

	type TestRecordNested struct {
		A int64   `avro:"a"`
		B string  `avro:"b"`
		C *Record `avro:"c"`
	}
	//avro.Register("test", &TestRecordNested{})
	schema := `
	{
	  "type": "record",
	  "name": "test",
	  "fields": [
        {"name": "a", "type": "long"}, 
        {"name": "b","type": "string"},
		{"name": "c", "type": {"type": "record", "name": "nested", "fields": [{"name": "d", "type": "string"}, {"name": "e", "type": ["test", "null"]}]}}
	  ]
	}
	`
	//rec := &TestRecordNested{A: 12, B: "aaa", C: &Record{D: "bbb", E: &TestRecordNested{A: 44, B: "ccc", C: &Record{D: "ddd", E: &TestRecordNested{A: 66, B: "eee", C: &Record{D: "fff", E: nil}}}}}}

	data := []byte{0x18, 0x6, 0x61, 0x61, 0x61, 0x6, 0x62, 0x62, 0x62, 0x0, 0x58, 0x6, 0x63, 0x63, 0x63, 0x6, 0x64, 0x64, 0x64, 0x0, 0x84, 0x1, 0x6, 0x65, 0x65, 0x65, 0x6, 0x66, 0x66, 0x66, 0x2}
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)
	rec := map[string]interface{}{"a": int64(12), "b": "aaa", "c": map[string]interface{}{"d": "bbb", "e": map[string]interface{}{"a": int64(44), "b": "ccc", "c": map[string]interface{}{"d": "ddd", "e": map[string]interface{}{"a": int64(66), "b": "eee", "c": map[string]interface{}{"d": "fff", "e": nil}}}}}}

	got := map[string]interface{}{}
	err = dec.Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, rec, got)
}
