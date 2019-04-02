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

func TestDecoder_UnionTyped(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x36}
	schema := `["null", "int"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	got := &TestUnionType{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, 27, got.Val)
}

func TestDecoder_UnionTypedInRecord(t *testing.T) {
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

	got := &TestUnionRecord{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, 27, got.A.Val)
}

func TestDecoder_UnionTypedNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	got := &TestUnionType{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, nil, got.Val)
}

func TestDecoder_UnionTypedNamed(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x02}
	schema := `["null", {"type":"enum", "name": "test", "symbols": ["A", "B"]}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	got := &TestUnionType{}
	err := dec.Decode(&got)

	assert.NoError(t, err)
	assert.Equal(t, "B", got.Val)
}

func TestDecoder_UnionTypedNilPointer(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `["null", "long"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	got := &TestUnionType{}
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_UnionTypedSetTypeError(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `["null", "boolean"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	got := &TestUnionType{}
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_UnionTypedSetTypeNilPtr(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	got := &TestUnionType{}
	err := dec.Decode(&got)

	assert.Error(t, err)
}

func TestDecoder_UnionTypedInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x04}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	got := &TestUnionType{}
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
