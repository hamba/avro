package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_ArrayInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x38, 0x0}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	assert.Error(t, err)
}

func TestDecoder_ArraySlice(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x38, 0x0}
	schema := `{"type":"array", "items": "int"}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got []int
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, []int{27, 28}, got)
}

func TestDecoder_ArraySliceOfStruct(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}
	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got []TestRecord
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, []TestRecord{{A: 27, B: "foo"}, {A: 27, B: "foo"}}, got)
}

func TestDecoder_ArraySliceError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []int
	err = dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_ArraySliceOfStructRecursive(t *testing.T) {
	defer ConfigTeardown()

	type TestRecord struct {
		A int64        `avro:"a"`
		B string       `avro:"b"`
		C []TestRecord `avro:"c"`
	}
	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}, {"name": "c", "type": { "type" : "array", "items" : "test" }}]}}`
	data := []byte{0x3, 0x28, 0x18, 0x6, 0x66, 0x6f, 0x6f, 0x1, 0xc, 0x1a, 0x6, 0x62, 0x62, 0x62, 0x0, 0x0, 0x36, 0x6, 0x62, 0x61, 0x7a, 0x0, 0x0}
	objS := []TestRecord{{A: 12, B: "foo", C: []TestRecord{{A: 13, B: "bbb", C: nil}}}, {A: 27, B: "baz", C: nil}}
	var str []TestRecord

	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	err = dec.Decode(&str)
	assert.NoError(t, err)
	assert.Equal(t, objS, str)

}

func TestDecoder_ArraySliceOfStructRecursiveUnion(t *testing.T) {
	defer ConfigTeardown()

	type TestRecord struct {
		A int64       `avro:"a"`
		B string      `avro:"b"`
		C *TestRecord `avro:"c"`
	}
	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}, {"name": "c", "type": ["test", "null"]}]}}`
	buf := bytes.NewBuffer([]byte{})
	slice := []TestRecord{{A: 12, B: "aaa", C: &TestRecord{A: 13, B: "bbb", C: &TestRecord{A: 44, B: "ccc", C: nil}}}, {A: 27, B: "ddd", C: &TestRecord{A: 13, B: "eee", C: nil}}}

	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(slice)
	require.NoError(t, err)

	data := []byte{0x3, 0x3c, 0x18, 0x6, 0x61, 0x61, 0x61, 0x0, 0x1a, 0x6, 0x62, 0x62, 0x62, 0x0, 0x58, 0x6, 0x63, 0x63, 0x63, 0x2, 0x36, 0x6, 0x64, 0x64, 0x64, 0x0, 0x1a, 0x6, 0x65, 0x65, 0x65, 0x2, 0x0}
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var s []TestRecord
	err = dec.Decode(&s)
	assert.NoError(t, err)
	//log.Print(s)
	assert.Equal(t, slice, s)
}

func TestDecoder_ArraySliceItemError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD, 0xAD, 0xE2, 0xA2, 0xF3, 0xAD, 0xAD}
	schema := `{"type":"array", "items": "int"}`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

	var got []int
	err = dec.Decode(&got)

	assert.Error(t, err)
}
