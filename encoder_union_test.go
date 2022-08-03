package avro_test

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_UnionMap(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"string": "foo"})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionMapRecord(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", {
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": ["string", "null"], "default": "test"},
	    {"name": "b", "type": "string"}
	]
}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"test": map[string]interface{}{"b": "foo"}})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x00, 0x08, 0x74, 0x65, 0x73, 0x74, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionMapJSON(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", {
	"type": "record",
	"name": "test",
	"fields" : [
		{"name": "a", "type": [{"type": "string", "sqlType": "JSON"}, "null"], "default": "{\"test\": 1}"},
	    {"name": "b", "type": {"type": "string", "sqlType": "JSON"}}
	]
}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"test": map[string]interface{}{"b": "{\"foo\": null}"}})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x2, 0x0, 0x16, 0x7b, 0x22, 0x74, 0x65, 0x73, 0x74, 0x22, 0x3a, 0x20, 0x31, 0x7d, 0x1a, 0x7b, 0x22, 0x66, 0x6f, 0x6f, 0x22, 0x3a, 0x20, 0x6e, 0x75, 0x6c, 0x6c, 0x7d}, buf.Bytes())
}

func TestEncoder_UnionMapNamed(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", {"type":"enum", "name": "test", "symbols": ["foo", "bar"]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"test": "bar"})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x02}, buf.Bytes())
}

func TestEncoder_UnionMapNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var m map[string]interface{}
	err = enc.Encode(m)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_UnionMapMultipleEntries(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string", "int"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"string": "foo", "int": 27})

	assert.Error(t, err)
}

func TestEncoder_UnionMapWithTime(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", {"type": "long", "logicalType": "timestamp-micros"}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	m := map[string]interface{}{
		"long.timestamp-micros": time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	err = enc.Encode(m)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x80, 0xCD, 0xB7, 0xA2, 0xEE, 0xC7, 0xCD, 0x05}, buf.Bytes())
}

func TestEncoder_UnionMapWithDuration(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", {"type": "int", "logicalType": "time-millis"}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	m := map[string]interface{}{
		"int.time-millis": 123456789 * time.Millisecond,
	}
	err = enc.Encode(m)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0xAA, 0xB4, 0xDE, 0x75}, buf.Bytes())
}

func TestEncoder_UnionMapWithDecimal(t *testing.T) {
	defer ConfigTeardown()

	t.Run("low scale", func(t *testing.T) {
		schema := `["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}]`
		buf := bytes.NewBuffer([]byte{})
		enc, err := avro.NewEncoder(schema, buf)
		require.NoError(t, err)

		m := map[string]interface{}{
			"bytes.decimal": big.NewRat(1734, 5),
		}
		err = enc.Encode(m)

		require.NoError(t, err)
		assert.Equal(t, []byte{0x02, 0x6, 0x00, 0x87, 0x78}, buf.Bytes())
	})

	t.Run("high scale", func(t *testing.T) {
		schema := `["null", {"type": "bytes", "logicalType": "decimal", "precision": 77, "scale": 38}]`
		buf := bytes.NewBuffer([]byte{})
		enc, err := avro.NewEncoder(schema, buf)
		require.NoError(t, err)

		m := map[string]interface{}{
			"bytes.decimal": big.NewRat(1734, 5),
		}
		err = enc.Encode(m)

		require.NoError(t, err)
		assert.Equal(t, []byte{0x2, 0x22, 0x65, 0xea, 0x55, 0xc, 0x11, 0x8, 0xf7, 0xc3, 0xb8, 0xec, 0x53, 0xff, 0x80, 0x0, 0x0, 0x0, 0x0}, buf.Bytes())
	})
}

func TestEncoder_UnionMapInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"long": 27})

	assert.Error(t, err)
}

func TestEncoder_UnionMapInvalidMap(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]string{})

	assert.Error(t, err)
}

func TestEncoder_UnionPtr(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	str := "foo"
	err = enc.Encode(&str)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionPtrReversed(t *testing.T) {
	defer ConfigTeardown()

	schema := `["string", "null"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	str := "foo"
	err = enc.Encode(&str)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionPtrNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var str *string
	err = enc.Encode(str)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_UnionPtrReversedNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `["string", "null"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var str *string
	err = enc.Encode(str)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02}, buf.Bytes())
}

func TestEncoder_UnionPtrNotNullable(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", "string", "int"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	str := "test"
	err = enc.Encode(&str)

	assert.Error(t, err)
}

func TestEncoder_UnionInterface(t *testing.T) {
	defer ConfigTeardown()

	schema := `["int", "string"]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var val interface{} = "foo"
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionInterfaceRecord(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", &TestRecord{})

	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var val interface{} = &TestRecord{A: 27, B: "foo"}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionInterfaceRecordNonPtr(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", TestRecord{})

	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var val interface{} = TestRecord{A: 27, B: "foo"}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}, buf.Bytes())
}

func TestEncoder_UnionInterfaceMap(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("map:int", map[string]int{})

	schema := `["int", {"type": "map", "values": "int"}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var val interface{} = map[string]int{"foo": 27}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x01, 0x0a, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x00}, buf.Bytes())
}

func TestEncoder_UnionInterfaceInMapWithBool(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": ["null", "boolean"]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]interface{}{"foo": true})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x0c, 0x06, 0x66, 0x6F, 0x6F, 0x02, 0x01, 0x00}, buf.Bytes())
}

func TestEncoder_UnionInterfaceArray(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("array:int", []int{})

	schema := `["int", {"type": "array", "items": "int"}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var val interface{} = []int{27}
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x01, 0x02, 0x36, 0x00}, buf.Bytes())
}

func TestEncoder_UnionInterfaceNull(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": ["null", "string", "int"]}]}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(&TestUnion{A: nil})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_UnionInterfaceNamed(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", "")

	schema := `["null", {"type":"enum", "name": "test", "symbols": ["A", "B"]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var val interface{} = "B"
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x02}, buf.Bytes())
}

func TestEncoder_UnionInterfaceWithTime(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", {"type": "long", "logicalType": "timestamp-micros"}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var val interface{} = time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0x80, 0xCD, 0xB7, 0xA2, 0xEE, 0xC7, 0xCD, 0x05}, buf.Bytes())
}

func TestEncoder_UnionInterfaceWithDuration(t *testing.T) {
	defer ConfigTeardown()

	schema := `["null", {"type": "int", "logicalType": "time-millis"}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	var val interface{} = 123456789 * time.Millisecond
	err = enc.Encode(val)

	require.NoError(t, err)
	assert.Equal(t, []byte{0x02, 0xAA, 0xB4, 0xDE, 0x75}, buf.Bytes())
}

func TestEncoder_UnionInterfaceWithDecimal(t *testing.T) {
	defer ConfigTeardown()

	t.Run("low scale", func(t *testing.T) {
		schema := `["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}]`
		buf := bytes.NewBuffer([]byte{})
		enc, err := avro.NewEncoder(schema, buf)
		require.NoError(t, err)

		var val interface{} = big.NewRat(1734, 5)
		err = enc.Encode(val)

		require.NoError(t, err)
		assert.Equal(t, []byte{0x02, 0x6, 0x00, 0x87, 0x78}, buf.Bytes())
	})

	t.Run("high scale", func(t *testing.T) {
		schema := `["null", {"type": "bytes", "logicalType": "decimal", "precision": 77, "scale": 38}]`
		buf := bytes.NewBuffer([]byte{})
		enc, err := avro.NewEncoder(schema, buf)
		require.NoError(t, err)

		var val interface{} = big.NewRat(1734, 5)
		err = enc.Encode(val)

		require.NoError(t, err)
		assert.Equal(t, []byte{0x2, 0x22, 0x65, 0xea, 0x55, 0xc, 0x11, 0x8, 0xf7, 0xc3, 0xb8, 0xec, 0x53, 0xff, 0x80, 0x0, 0x0, 0x0, 0x0}, buf.Bytes())
	})
}

func TestEncoder_UnionInterfaceUnregisteredType(t *testing.T) {
	defer ConfigTeardown()

	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

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
	require.NoError(t, err)

	var val interface{} = &TestRecord{}
	err = enc.Encode(val)

	assert.Error(t, err)
}
