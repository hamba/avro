package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

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

func TestEncoder_UnionPtrReversed(t *testing.T) {
	defer ConfigTeardown()

	schema := `["string", "null"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	str := "foo"
	err = enc.Encode(&str)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
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

func TestEncoder_UnionPtrReversedNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `["string", "null"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var str *string
	err = enc.Encode(str)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02}, buf.Bytes())
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

func TestEncoder_UnionInterface(t *testing.T) {
	defer ConfigTeardown()

	schema := `["int", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var val interface{} = "foo"
	err = enc.Encode(val)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionInterfaceRecord(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", &TestRecord{})

	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var val interface{} = &TestRecord{A: 27, B: "foo"}
	err = enc.Encode(val)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionInterfaceRecordNonPtr(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", TestRecord{})

	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var val interface{} = TestRecord{A: 27, B: "foo"}
	err = enc.Encode(val)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionInterfaceMap(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("map:int", map[string]int{})

	schema := `["int", {"type": "map", "values": "int"}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var val interface{} = map[string]int{"foo": 27}
	err = enc.Encode(val)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x01, 0x0a, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x00}, buf.Bytes())
}

func TestEncoder_UnionInterfaceArray(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("array:int", []int{})

	schema := `["int", {"type": "array", "items": "int"}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var val interface{} = []int{27}
	err = enc.Encode(val)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x01, 0x02, 0x36, 0x00}, buf.Bytes())
}

func TestEncoder_UnionInterfaceNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": ["null", "string", "int"]}]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	err = enc.Encode(&TestUnion{A: nil})

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_UnionInterfaceNamed(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", "")

	schema := `["null", {"type":"enum", "name": "test", "symbols": ["A", "B"]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var val interface{} = "B"
	err = enc.Encode(val)

	assert.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x02}, buf.Bytes())
}

func TestEncoder_UnionInterfaceUnregisteredType(t *testing.T) {
	defer ConfigTeardown()

	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var val interface{} = &TestRecord{}
	err = enc.Encode(val)

	assert.Error(t, err)
}

func TestEncoder_UnionInterfaceNotInSchema(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", &TestRecord{})

	schema := `["int", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	assert.NoError(t, err)

	var val interface{} = &TestRecord{}
	err = enc.Encode(val)

	assert.Error(t, err)
}
