package avro_test

import (
	"bytes"
	"github.com/modern-go/reflect2"
	"log"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_ArrayInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode("test")

	assert.Error(t, err)
}

func TestEncoder_ArrayRecursive(t *testing.T) {
	type TestRecord struct {
		A int64        `avro:"a"`
		B string       `avro:"b"`
		C []TestRecord `avro:"c"`
	}
	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}, {"name": "c", "type": { "type" : "array", "items" : "test" }}]}}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	log.Print(buf.Bytes())
	objS := []TestRecord{{A: 12, B: "foo", C: []TestRecord{{A: 13, B: "bbb", C: []TestRecord{}}}}, {A: 27, B: "baz", C: []TestRecord{}}}
	err = enc.Encode(objS)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x3, 0x28, 0x18, 0x6, 0x66, 0x6f, 0x6f, 0x1, 0xc, 0x1a, 0x6, 0x62, 0x62, 0x62, 0x0, 0x0, 0x36, 0x6, 0x62, 0x61, 0x7a, 0x0, 0x0}, buf.Bytes())
	var str []TestRecord
	dec, err := avro.NewDecoder(schema, bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	err = dec.Decode(&str)
	log.Println(reflect2.TypeOf(str[0].C))
	log.Println(str[0])

	log.Println(str[0].C[0].B)
	log.Println(str[1])
	log.Println(str[1].C)
	//assert.Equal(t, slice, str)
	assert.NoError(t, err)
}
func TestEncoder_ArrayRecursiveStruct2(t *testing.T) {
	type TestRecord struct {
		A int64       `avro:"a"`
		B string      `avro:"b"`
		C *TestRecord `avro:"c"`
	}
	schema := `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}, {"name": "c", "type": ["test", "null"]}]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	log.Print(buf.Bytes())
	slice := TestRecord{A: 12, B: "aaa", C: &TestRecord{A: 13, B: "bbb", C: &TestRecord{A: 44, B: "ccc", C: &TestRecord{A: 55, B: "ddd", C: nil}}}}
	err = enc.Encode(slice)
	require.NoError(t, err)
	p := buf.Bytes()
	assert.Equal(t, []byte{0x18, 0x6, 0x61, 0x61, 0x61, 0x0, 0x1a, 0x6, 0x62, 0x62, 0x62, 0x0, 0x58, 0x6, 0x63, 0x63, 0x63, 0x0, 0x6e, 0x6, 0x64, 0x64, 0x64, 0x2}, p)

	dec, err := avro.NewDecoder(schema, bytes.NewReader(p))
	require.NoError(t, err)

	var s TestRecord
	log.Println(reflect2.TypeOf(dec))
	err = dec.Decode(&s)
	log.Println(reflect2.TypeOf(s))
	log.Println(s.C.C.B)
	//assert.Equal(t, slice, str)
	assert.NoError(t, err)
}
func TestEncoder_ArrayRecursiveStruct(t *testing.T) {
	type TestRecord struct {
		A int64       `avro:"a"`
		B string      `avro:"b"`
		C *TestRecord `avro:"c"`
	}
	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}, {"name": "c", "type": ["test", "null"]}]}}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	log.Print(buf.Bytes())
	slice := []TestRecord{{A: 12, B: "aaa", C: &TestRecord{A: 13, B: "bbb", C: &TestRecord{A: 44, B: "ccc", C: nil}}}, {A: 27, B: "ddd", C: &TestRecord{A: 13, B: "eee", C: nil}}}
	err = enc.Encode(slice)
	require.NoError(t, err)
	p := buf.Bytes()
	assert.Equal(t, []byte{0x3, 0x3c, 0x18, 0x6, 0x61, 0x61, 0x61, 0x0, 0x1a, 0x6, 0x62, 0x62, 0x62, 0x0, 0x58, 0x6, 0x63, 0x63, 0x63, 0x2, 0x36, 0x6, 0x64, 0x64, 0x64, 0x0, 0x1a, 0x6, 0x65, 0x65, 0x65, 0x2, 0x0}, p)

	dec, err := avro.NewDecoder(schema, bytes.NewReader(p))
	require.NoError(t, err)

	var s []TestRecord
	log.Println(reflect2.TypeOf(dec))
	err = dec.Decode(&s)
	log.Println(reflect2.TypeOf(s))
	log.Println(s)
	aa := s[0].C
	log.Println(aa.C.B)
	//assert.Equal(t, slice, str)
	assert.NoError(t, err)
}

func TestEncoder_Array(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([]int{27, 28})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x04, 0x36, 0x38, 0x0}, buf.Bytes())
}

func TestEncoder_ArrayEmpty(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([]int{})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x0}, buf.Bytes())
}

func TestEncoder_ArrayOfStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([]TestRecord{{A: 27, B: "foo"}, {A: 27, B: "foo"}})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x03, 0x14, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}, buf.Bytes())
}

func TestEncoder_ArrayError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"array", "items": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode([]string{"foo", "bar"})

	assert.Error(t, err)
}
