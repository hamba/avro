package avro_test

import (
	"bytes"
	"log"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_RecordStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestRecord{A: 27, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructPtr(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := &TestRecord{A: 27, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructPtrNil(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	var obj *TestRecord
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordStructMissingRequiredField(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestPartialRecord{B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordStructWithDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long", "default": 27},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestPartialRecord{B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructPartialWithNullDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestPartialRecord{B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructPartialWithSubRecordDefault(t *testing.T) {
	defer ConfigTeardown()

	_, err := avro.Parse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
		]
	}`)
	require.NoError(t, err)

	schema := `{
		"type": "record",
		"name": "parent",
		"fields" : [
			{
				"name": "a",
				"type": "test",
				"default": {"a": 1000, "b": "def b"}
			},
			{"name": "b", "type": "string"}
		]
	}`
	obj := TestPartialRecord{B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	assert.Equal(t, []byte{0xd0, 0xf, 0xa, 0x64, 0x65, 0x66, 0x20, 0x62, 0x6, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructWithNullDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "null", "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestPartialRecord{B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordStructFieldError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "string"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestRecord{A: 27, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordEmbeddedStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	obj := TestEmbeddedRecord{TestEmbed: TestEmbed{A: 27, B: "foo"}, C: "bar"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}, buf.Bytes())
}

func TestEncoder_RecordEmbeddedPtrStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	obj := TestEmbeddedPtrRecord{TestEmbed: &TestEmbed{A: 27, B: "foo"}, C: "bar"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x06, 0x62, 0x61, 0x72}, buf.Bytes())
}

func TestEncoder_RecordEmbeddedPtrStructNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"},
	    {"name": "c", "type": "string"}
	]
}`
	obj := TestEmbeddedPtrRecord{C: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordEmbeddedIntStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestEmbeddedIntRecord{TestEmbedInt: 27, B: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordUnexportedStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := TestUnexportedRecord{A: 27, b: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordMap(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"a": int64(27), "b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapNested(t *testing.T) {
	defer ConfigTeardown()

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
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"a": map[string]any{
		"a": int64(27),
		"b": "bar",
	}, "b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x6, 0x62, 0x61, 0x72, 0x6, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapNilValue(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"a": int64(27), "b": nil}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordMapMissingRequiredField(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordMapWithDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "long", "default": 27},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapWithSubRecordDefault(t *testing.T) {
	defer ConfigTeardown()

	_, err := avro.Parse(`{
		"type": "record",
		"name": "test",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
		]
	}`)
	require.NoError(t, err)

	schema := `{
		"type": "record",
		"name": "parent",
		"fields" : [
			{
				"name": "a",
				"type": "test",
				"default": {"a": 1000, "b": "def b"}
			},
			{"name": "b", "type": "string"}
		]
	}`

	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)
	require.NoError(t, err)

	assert.Equal(t, []byte{0xd0, 0xf, 0xa, 0x64, 0x65, 0x66, 0x20, 0x62, 0x6, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapWithNullDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "null", "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapWithUnionNullDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "string"], "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapWithUnionStringDefault(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["string", "null"], "default": "test"},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]any{"b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x0, 0x8, 0x74, 0x65, 0x73, 0x74, 0x6, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RecordMapInvalidKeyType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "null", "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[int]any{1: int64(27), 2: "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RecordMapInvalidValueType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": "null", "default": null},
	    {"name": "b", "type": "string"}
	]
}`
	obj := map[string]string{"a": "test", "b": "foo"}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	assert.Error(t, err)
}

func TestEncoder_RefStruct(t *testing.T) {
	defer ConfigTeardown()

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
	obj := TestNestedRecord{
		A: TestRecord{A: 27, B: "foo"},
		B: TestRecord{A: 27, B: "foo"},
	}
	buf := &bytes.Buffer{}
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(obj)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f}, buf.Bytes())
}

func TestEncoder_RefStructRecursiveUnion(t *testing.T) {
	defer ConfigTeardown()

	type TestRecord struct {
		A int64       `avro:"a"`
		B string      `avro:"b"`
		C *TestRecord `avro:"c"`
	}
	schema := `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}, {"name": "c", "type": ["test", "null"]}]}`
	buf := bytes.NewBuffer([]byte{})

	enc, err := avro.NewEncoder(schema, buf)
	slice := TestRecord{A: 12, B: "aaa", C: &TestRecord{A: 13, B: "bbb", C: &TestRecord{A: 44, B: "ccc", C: &TestRecord{A: 55, B: "ddd", C: nil}}}}

	err = enc.Encode(slice)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x18, 0x6, 0x61, 0x61, 0x61, 0x0, 0x1a, 0x6, 0x62, 0x62, 0x62, 0x0, 0x58, 0x6, 0x63, 0x63, 0x63, 0x0, 0x6e, 0x6, 0x64, 0x64, 0x64, 0x2}, buf.Bytes())

}

func TestEncoder_RefStructRecursiveUnion2(t *testing.T) {
	defer ConfigTeardown()

	type TestRecordNested struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
		C any    `avro:"c"`
	}

	type Record struct {
		D string            `avro:"d"`
		E *TestRecordNested `avro:"e"`
	}

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
	buf := bytes.NewBuffer([]byte{})
	// {'a': 12, 'b': 'aaa', 'c': {'d': 'bbb', 'e': {'a': 44, 'b': 'ccc', 'c': {'d': 'ddd', 'e': {'a': 66, 'b': 'eee', 'c': {'d': 'fff', 'e': None}}}}}}
	rec1 := map[string]interface{}{
		"d": "bbb",
		"e": &TestRecordNested{A: 44, B: "ccc", C: map[string]interface{}{
			"d": "ddd", "e": &TestRecordNested{A: 66, B: "eee", C: map[string]interface{}{"d": "fff", "e": nil}},
		}},
	}
	enc, err := avro.NewEncoder(schema, buf)
	slice := &TestRecordNested{A: 12, B: "aaa", C: rec1}

	err = enc.Encode(slice)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x18, 0x6, 0x61, 0x61, 0x61, 0x6, 0x62, 0x62, 0x62, 0x0, 0x58, 0x6, 0x63, 0x63, 0x63, 0x6, 0x64, 0x64, 0x64, 0x0, 0x84, 0x1, 0x6, 0x65, 0x65, 0x65, 0x6, 0x66, 0x66, 0x66, 0x2}, buf.Bytes())
	data := []byte{0x18, 0x6, 0x61, 0x61, 0x61, 0x6, 0x62, 0x62, 0x62, 0x0, 0x58, 0x6, 0x63, 0x63, 0x63, 0x6, 0x64, 0x64, 0x64, 0x0, 0x84, 0x1, 0x6, 0x65, 0x65, 0x65, 0x6, 0x66, 0x66, 0x66, 0x2}
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	got := &TestRecordNested{}
	err = dec.Decode(&got)
	log.Println(got)
	require.NoError(t, err)
	//assert.Equal(t, slice, got)
}
