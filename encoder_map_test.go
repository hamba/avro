package avro_test

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncoder_MapInvalidType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode("test")

	assert.Error(t, err)
}

func TestEncoder_Map(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]string{})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x00}, buf.Bytes())
}

func TestEncoder_MapEmpty(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]string{"foo": "foo"})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x10, 0x06, 0x66, 0x6F, 0x6F, 0x06, 0x66, 0x6F, 0x6F, 0x00}, buf.Bytes())
}

func TestEncoder_MapOfStruct(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": {"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]TestRecord{"foo": {A: 27, B: "foo"}})

	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x12, 0x06, 0x66, 0x6F, 0x6F, 0x36, 0x06, 0x66, 0x6f, 0x6f, 0x0}, buf.Bytes())
}

func TestEncoder_MapInvalidKeyType(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[int]string{1: "foo"})

	assert.Error(t, err)
}

func TestEncoder_MapError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]int{"foo": 1})

	assert.Error(t, err)
}

func TestEncoder_MapWithMoreThanBlockLengthKeys(t *testing.T) {
	avro.DefaultConfig = avro.Config{
		TagKey:               "avro",
		BlockLength:          1,
		UnionResolutionError: true,
	}.Freeze()

	schema := `{"type":"map", "values": "int"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[string]int{"foo": 1, "bar": 2})

	require.NoError(t, err)
	assert.Condition(t, func() bool {
		// {"foo": 1, "bar": 2}
		foobar := bytes.Equal([]byte{0x01, 0x0a, 0x06, 0x66, 0x6F, 0x6F, 0x02, 0x01, 0x0a, 0x06, 0x62, 0x61, 0x72, 0x04, 0x0}, buf.Bytes())
		// {"bar": 2, "foo": 1}
		barfoo := bytes.Equal([]byte{0x01, 0x0a, 0x06, 0x62, 0x61, 0x72, 0x04, 0x01, 0x0a, 0x06, 0x66, 0x6F, 0x6F, 0x02, 0x0}, buf.Bytes())
		return (foobar || barfoo)
	})
}

type textMarshallerInt int

func (t textMarshallerInt) MarshalText() (text []byte, err error) {
	return []byte(strconv.Itoa(int(t))), nil
}

type textMarshallerError int

func (t textMarshallerError) MarshalText() (text []byte, err error) {
	return nil, errors.New("test")
}

func TestEncoder_MapMarshaller(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[textMarshallerInt]string{
		1: "test",
	})

	require.NoError(t, err)
	want := []byte{0x1, 0xe, 0x2, 0x31, 0x8, 0x74, 0x65, 0x73, 0x74, 0x0}
	assert.Equal(t, want, buf.Bytes())
}

func TestEncoder_MapMarshallerNil(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[*textMarshallerError]int{
		nil: 1,
	})

	require.Error(t, err)
}

func TestEncoder_MapMarshallerKeyError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[textMarshallerError]int{
		1: 1,
	})

	require.Error(t, err)
}

func TestEncoder_MapMarshallerError(t *testing.T) {
	defer ConfigTeardown()

	schema := `{"type":"map", "values": "string"}`
	buf := bytes.NewBuffer([]byte{})
	enc, err := avro.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(map[textMarshallerInt]int{
		1: 1,
	})

	require.Error(t, err)
}
