package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestEncoder_UnionInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode("foo")

	assert.Error(t, err)
}

func TestEncoder_UnionMap(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"string": "foo"})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionMapNamed(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", {"type":"enum", "name": "test", "symbols": ["foo", "bar"]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"test": "bar"})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x02}, buf.Bytes())
}

func TestEncoder_UnionMapNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var m map[string]interface{}
	err = enc.Encode(m)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_UnionMapMultipleEntries(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string", "int"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"string": "foo", "int": 27})

	assert.Error(t, err)
}

func TestEncoder_UnionMapInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"long": 27})

	assert.Error(t, err)
}

func TestEncoder_UnionMapInvalidMap(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(map[string]string{})

	assert.Error(t, err)
}

func TestEncoder_UnionTyped(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "int"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(&TestUnionType{Val: 27})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36}, buf.Bytes())
}

func TestEncoder_UnionTypedNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(&TestUnionType{Val: nil})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_UnionTypedNamed(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", {"type":"enum", "name": "test", "symbols": ["A", "B"]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(&TestUnionType{Val: "B"})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x02}, buf.Bytes())
}

func TestEncoder_UnionTypedNilPointer(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "long"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(&TestUnionType{Val: int64(1)})

	assert.Error(t, err)
}

func TestEncoder_UnionTypedGetTypeError(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "long"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(&TestUnionType{Val: int64(1)})

	assert.Error(t, err)
}

func TestEncoder_UnionTypedInvalidSchema(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "long"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(&TestUnionType{Val: 1})

	assert.Error(t, err)
}

func TestEncoder_UnionPtr(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	str := "foo"
	err = enc.Encode(&str)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionPtrNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var str *string
	err = enc.Encode(str)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_UnionPtrNotNullable(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string", "int"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	str := "test"
	err = enc.Encode(&str)

	assert.Error(t, err)
}
