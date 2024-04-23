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

	got := &TestRecord{}
	err = dec.Decode(&got)

	require.NoError(t, err)
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

func TestDecoder_RecordFieldEOF(t *testing.T) {
	defer ConfigTeardown()

	data := []byte("010\xf8\xf1\xc600000\xbcֱ00000\U00014e5800000\xb0\xa6\xfd\xbf0000000000\xcf00000\xa6܇\xd200000\x96\x9b\xf9\xae00000\xf4\xf3\xce\xf600000\xa8\xe3\xe1\xad00000\x86\xd8Ԛ00000\xde00000\xf200000\xa50000000000\xd600000\x9e\xeb\xf8\xfa00000\x96\xa4\xe3\x9500000\xf2\xa9\xcc\xfb00000\xa0\xb8\xe1\xad00000\xf8\xb3\xac\xaa00000\xaa\xc3\xf6\xcc00000\x88\x87\xc800000\xf2\xef00000\xc3000009000000\xbe\x80\xdd\xce00000Ȋ\xea\xee00000\xba\xa4\x8a\xe600000\x86\xca\xc0\xc70000000000\xff00000ܮ\x8c\xf500000\xac\x97Ď00000\xb0\xac\x97\xad00000\xb4\xb8\xc0\x8900000\xa6\xb5\xf9\xce0000000000\xbc00000\xf4\xf8\x98\xc500000\x9a\x82\xa6\xc500000\xb2뵿00000\xea\xa3ݍ00000\xa8\xa5\xd1\xdf00000آ\xbe\xb500000\x900000000000\x9c\xea\x90\xdc00000\xdc\xe8\xba\xed00000\xfeɻ\xb700000溂\xa000000\xe6\xf4\x95\x8700000\x86\x96\xc9\xd2000000000000000000000000000000000000000000000000000000000000000000000000000000000\xf1\xeb00000\xf6ˑ\xf0")
	schema := `{
		"items": {
		  "items": {
			"fields": [
			  {
				"name": "A",
				"type": {
					"type": "int"
				}
			  },
			  {
				"name": "A",
				"tYpe": {
					"type": "float"
				}
			  }
			],
			"name": "A",
			"type": "record"
		  },
		  "type": "array"
		},
		"type": "array"
	  }`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got any
	err := dec.Decode(&got)

	assert.Error(t, err)
}
