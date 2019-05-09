package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestDecoder_UnionInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["null", "string"]`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	assert.NoError(t, err)

	var str string
	err = dec.Decode(&str)

	assert.Error(t, err)
}

func TestDecoder_UnionMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"string": "foo"}, got)
}

func TestDecoder_UnionMapNamed(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x02}
	schema := `["null", {"type":"enum", "name": "test", "symbols": ["foo", "bar"]}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"test": "bar"}, got)
}

func TestDecoder_UnionMapNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}(nil), got)
}

func TestDecoder_UnionMapInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_UnionMapInvalidMap(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]string
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_UnionPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *string
	err := dec.Decode(&got)

	want := "foo"
	assert.NoError(t, err)
	assert.Equal(t, &want, got)
}

func TestDecoder_UnionPtrNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *string
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Nil(t, got)
}

func TestDecoder_UnionPtrInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *string
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_UnionPtrNotNullable(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["null", "string", "int"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *string
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_UnionInterface(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36}
	schema := `["null", "int"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, 27, got)
}

func TestDecoder_UnionInterfaceInRecord(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36}
	schema := `{
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["null", "int"]}
	]
}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	got := &TestUnion{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, 27, got.A)
}

func TestDecoder_UnionInterfaceMap(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("map:int", map[string]int{})

	data := []byte{0x02, 0x01, 0x0a, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x00}
	schema := `["int", {"type": "map", "values": "int"}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, map[string]int{"foo": 27}, got)
}

func TestDecoder_UnionInterfaceMapNamed(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("map:test", map[string]string{})

	data := []byte{0x02, 0x01, 0x0a, 0x06, 0x66, 0x6f, 0x6f, 0x02, 0x00}
	schema := `["int", {"type": "map", "values": {"type":"enum", "name": "test", "symbols": ["A", "B"]}}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"foo": "B"}, got)
}

func TestDecoder_UnionInterfaceArray(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("array:int", []int{})

	data := []byte{0x02, 0x01, 0x02, 0x36, 0x00}
	schema := `["int", {"type": "array", "items": "int"}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, []int{27}, got)
}

func TestDecoder_UnionInterfaceArrayNamed(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("array:test", []string{})

	data := []byte{0x02, 0x01, 0x02, 0x02, 0x00}
	schema := `["int", {"type": "array", "items": {"type":"enum", "name": "test", "symbols": ["A", "B"]}}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, []string{"B"}, got)
}

func TestDecoder_UnionInterfaceNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, nil, got)
}

func TestDecoder_UnionInterfaceNamed(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", "")

	data := []byte{0x02, 0x02}
	schema := `["null", {"type":"enum", "name": "test", "symbols": ["A", "B"]}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, "B", got)
}

func TestDecoder_UnionInterfaceRecord(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", &TestRecord{})

	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.IsType(t, &TestRecord{}, got)
	rec := got.(*TestRecord)
	assert.Equal(t, int64(27), rec.A)
	assert.Equal(t, "foo", rec.B)
}

func TestDecoder_UnionInterfaceRecordReused(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", &TestRecord{})

	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{} = &TestRecord{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.IsType(t, &TestRecord{}, got)
	rec := got.(*TestRecord)
	assert.Equal(t, int64(27), rec.A)
	assert.Equal(t, "foo", rec.B)
}

func TestDecoder_UnionInterfaceUnresolvableType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.IsType(t, map[string]interface{}{}, got)
	m := got.(map[string]interface{})
	assert.IsType(t, map[string]interface{}{}, m["test"])
	assert.Equal(t, int64(27), m["test"].(map[string]interface{})["a"])
	assert.Equal(t, "foo", m["test"].(map[string]interface{})["b"])
}

func TestDecoder_UnionInterfaceInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `["null", "int"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.Error(t, err)
}
