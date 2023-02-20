package registry_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_WithAPI(t *testing.T) {
	client, err := registry.NewClient("http://example.com")
	require.NoError(t, err)

	registry.NewDecoder(client, registry.WithAPI(avro.DefaultConfig))
}

func TestDecoder_DeserializePayload(t *testing.T) {
	//i declare a schema id and a struct to be deserialized
	schema_id := 42
	type Person struct {
		Name string `avro:"name"`
		Age  int    `avro:"age"`
	}
	john := &Person{
		Name: "john",
		Age:  28,
	}

	//i declare the string schema to be returned by the httptest server
	var person_schema_string string = `{\"type\":\"record\",\"name\":\"person\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}`

	//i declare the string schema to be parsed
	var person_schema_string_to_parse string = `{"type":"record","name":"person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`

	schema, err := avro.Parse(person_schema_string_to_parse)

	//i marshal the payload
	john_payload, err := avro.Marshal(schema, john)
	require.NoError(t, err)

	//i then add the magic byte to the payload, together with the encoded id
	payload := make([]byte, 0, len(john_payload)+5)
	payload = append(payload, 0)
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(schema_id))
	payload = append(payload, binarySchemaId...)
	payload = append(payload, john_payload...)

	//instantiating the test server
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, fmt.Sprintf("/schemas/ids/%d", schema_id), r.URL.Path)

		//_, _ = w.Write([]byte(fmt.Sprintf(`{"schema":""}`)))
		_, _ = w.Write([]byte(fmt.Sprintf(`{"schema":"%s"}`, person_schema_string)))
	}))
	t.Cleanup(s.Close)

	//creating the test client
	client, _ := registry.NewClient(s.URL)
	decoder := registry.NewDecoder(client)

	target_john := &Person{}
	err = decoder.DeserializePayload(context.Background(), payload, target_john)

	require.NoError(t, err)
	assert.Equal(t, john.Name, target_john.Name)
	assert.Equal(t, john.Age, target_john.Age)
}

func TestError_DeserializePayload(t *testing.T) {
	schema_id := 42

	//instantiating the test server
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, fmt.Sprintf("/schemas/ids/%d", schema_id), r.URL.Path)

		//_, _ = w.Write([]byte(fmt.Sprintf(`{"schema":""}`)))
		_, _ = w.Write([]byte(`{"schema":"boh"}`))
	}))
	t.Cleanup(s.Close)

	//creating the test client
	client, _ := registry.NewClient(s.URL)
	decoder := registry.NewDecoder(client)

	err := decoder.DeserializePayload(context.Background(), []byte{0}, nil)

	assert.Equal(t, "payload not containing data", err.Error())

	err = decoder.DeserializePayload(context.Background(), []byte{1, 1, 1, 1, 1, 1, 1, 1, 1}, nil)

	assert.Equal(t, "unable to extract schema id from payload, error: magic byte value is 1, different from 0", err.Error())

	payload := make([]byte, 0, 6)
	payload = append(payload, 0)
	binarySchemaId := make([]byte, 4)
	binary.BigEndian.PutUint32(binarySchemaId, uint32(schema_id))
	payload = append(payload, binarySchemaId...)
	payload = append(payload, []byte{0}...)

	err = decoder.DeserializePayload(context.Background(), payload, nil)

	assert.Equal(t, "unable to obtain schema, error: avro: unknown type: boh", err.Error())
}
