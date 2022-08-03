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

func TestDecoder_UnionInvalidType(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["null", "string"]`
	dec, err := avro.NewDecoder(schema, bytes.NewReader(data))
	require.NoError(t, err)

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

	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"string": "foo"}, got)
}

func TestDecoder_UnionMapJSON(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x2, 0x1a, 0x7b, 0x22, 0x66, 0x6f, 0x6f, 0x22, 0x3a, 0x20, 0x6e, 0x75, 0x6c, 0x6c, 0x7d}
	schema := `["null", {"type": "string", "sqlType": "JSON"}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"string": `{"foo": null}`}, got)
}

func TestDecoder_UnionMapNamed(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x02}
	schema := `["null", {"type":"enum", "name": "test", "symbols": ["foo", "bar"]}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"test": "bar"}, got)
}

func TestDecoder_UnionMapNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}(nil), got)
}

func TestDecoder_UnionMapWithTime(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x80, 0xCD, 0xB7, 0xA2, 0xEE, 0xC7, 0xCD, 0x05}
	schema := `["null", {"type": "long", "logicalType": "timestamp-micros"}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), got["long.timestamp-micros"])
}

func TestDecoder_UnionMapWithDuration(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0xAA, 0xB4, 0xDE, 0x75}
	schema := `["null", {"type": "int", "logicalType": "time-millis"}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, 123456789*time.Millisecond, got["int.time-millis"])
}

func TestDecoder_UnionMapWithDecimal(t *testing.T) {
	defer ConfigTeardown()

	t.Run("low scale", func(t *testing.T) {
		data := []byte{0x02, 0x6, 0x00, 0x87, 0x78}
		schema := `["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}]`
		dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

		var got map[string]interface{}
		err := dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, big.NewRat(1734, 5), got["bytes.decimal"])
	})

	t.Run("high scale", func(t *testing.T) {
		data := []byte{0x2, 0x22, 0x65, 0xea, 0x55, 0xc, 0x11, 0x8, 0xf7, 0xc3, 0xb8, 0xec, 0x53, 0xff, 0x80, 0x0, 0x0, 0x0, 0x0}
		schema := `["null", {"type": "bytes", "logicalType": "decimal", "precision": 77, "scale": 38}]`
		dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

		var got map[string]interface{}
		err := dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, big.NewRat(1734, 5), got["bytes.decimal"])
	})
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
	require.NoError(t, err)
	assert.Equal(t, &want, got)
}

func TestDecoder_UnionPtrReversed(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["string", "null"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *string
	err := dec.Decode(&got)

	want := "foo"
	require.NoError(t, err)
	assert.Equal(t, &want, got)
}

func TestDecoder_UnionPtrReuseInstance(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", &TestRecord{})

	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["null", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	got := &TestRecord{}
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.IsType(t, &TestRecord{}, got)
	assert.Equal(t, int64(27), got.A)
	assert.Equal(t, "foo", got.B)
}

func TestDecoder_UnionPtrNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *string
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestDecoder_UnionPtrReversedNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02}
	schema := `["string", "null"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got *string
	err := dec.Decode(&got)

	require.NoError(t, err)
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

	require.NoError(t, err)
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

	require.NoError(t, err)
	assert.Equal(t, 27, got.A)
}

func TestDecoder_UnionInterfaceInMap(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("map:int", map[string]int{})

	data := []byte{0x01, 0x0c, 0x06, 0x66, 0x6f, 0x6f, 0x00, 0x36, 0x00}
	schema := `{"type": "map", "values": ["int", "string"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"foo": 27}, got)
}

func TestDecoder_UnionInterfaceInMapWithBool(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x01, 0x0c, 0x06, 0x66, 0x6F, 0x6F, 0x02, 0x01, 0x00}
	schema := `{"type":"map", "values": ["null", "boolean"]}`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got map[string]interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"foo": true}, got)
}

func TestDecoder_UnionInterfaceMap(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("map:int", map[string]int{})

	data := []byte{0x02, 0x01, 0x0a, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x00}
	schema := `["int", {"type": "map", "values": "int"}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
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

	require.NoError(t, err)
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

	require.NoError(t, err)
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

	require.NoError(t, err)
	assert.Equal(t, []string{"B"}, got)
}

func TestDecoder_UnionInterfaceNull(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x00}
	schema := `["null", "string"]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
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

	require.NoError(t, err)
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

	require.NoError(t, err)
	assert.IsType(t, &TestRecord{}, got)
	rec := got.(*TestRecord)
	assert.Equal(t, int64(27), rec.A)
	assert.Equal(t, "foo", rec.B)
}

func TestDecoder_UnionInterfaceRecordNotReused(t *testing.T) {
	defer ConfigTeardown()

	avro.Register("test", &TestRecord{})

	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{} = ""
	err := dec.Decode(&got)

	require.NoError(t, err)
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

	require.NoError(t, err)
	assert.IsType(t, map[string]interface{}{}, got)
	m := got.(map[string]interface{})
	assert.IsType(t, map[string]interface{}{}, m["test"])
	assert.Equal(t, int64(27), m["test"].(map[string]interface{})["a"])
	assert.Equal(t, "foo", m["test"].(map[string]interface{})["b"])
}

func TestDecoder_UnionInterfaceWithTime(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0x80, 0xCD, 0xB7, 0xA2, 0xEE, 0xC7, 0xCD, 0x05}
	schema := `["null", {"type": "long", "logicalType": "timestamp-micros"}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), got)
}

func TestDecoder_UnionInterfaceWithDuration(t *testing.T) {
	defer ConfigTeardown()

	data := []byte{0x02, 0xAA, 0xB4, 0xDE, 0x75}
	schema := `["null", {"type": "int", "logicalType": "time-millis"}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	require.NoError(t, err)
	assert.Equal(t, 123456789*time.Millisecond, got)
}

func TestDecoder_UnionInterfaceWithDecimal(t *testing.T) {
	defer ConfigTeardown()

	t.Run("low scale", func(t *testing.T) {
		data := []byte{0x02, 0x6, 0x00, 0x87, 0x78}
		schema := `["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}]`
		dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

		var got interface{}
		err := dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, big.NewRat(1734, 5), got)
	})

	t.Run("high scale", func(t *testing.T) {
		data := []byte{0x2, 0x22, 0x65, 0xea, 0x55, 0xc, 0x11, 0x8, 0xf7, 0xc3, 0xb8, 0xec, 0x53, 0xff, 0x80, 0x0, 0x0, 0x0, 0x0}
		schema := `["null", {"type": "bytes", "logicalType": "decimal", "precision": 77, "scale": 38}]`
		dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

		var got interface{}
		err := dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, big.NewRat(1734, 5), got)
	})
}

func TestDecoder_UnionInterfaceWithDecimal_Negative(t *testing.T) {
	defer ConfigTeardown()

	t.Run("low scale", func(t *testing.T) {
		data := []byte{0x02, 0x6, 0xFF, 0x78, 0x88}
		schema := `["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}]`
		dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

		var got interface{}
		err := dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, big.NewRat(-1734, 5), got)
	})
	t.Run("high scale", func(t *testing.T) {
		data := []byte{0x2, 0x22, 0x9a, 0x15, 0xaa, 0xf3, 0xee, 0xf7, 0x8, 0x3c, 0x47, 0x13, 0xac, 0x0, 0x80, 0x0, 0x0, 0x0, 0x0}
		schema := `["null", {"type": "bytes", "logicalType": "decimal", "precision": 77, "scale": 38}]`
		dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

		var got interface{}
		err := dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, big.NewRat(-1734, 5), got)
	})
}

func TestDecoder_UnionInterfaceUnresolvableTypeWithError(t *testing.T) {
	defer ConfigTeardown()

	avro.DefaultConfig = avro.Config{UnionResolutionError: true}.Freeze()

	data := []byte{0x02, 0x36, 0x06, 0x66, 0x6F, 0x6F}
	schema := `["int", {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}]`
	dec, _ := avro.NewDecoder(schema, bytes.NewReader(data))

	var got interface{}
	err := dec.Decode(&got)

	assert.Error(t, err)
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
