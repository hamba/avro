package avro_test

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hamba/avro/v2"
)

func ExampleParse() {
	schema, err := avro.Parse(`{
	    "type": "record",
	    "name": "simple",
	    "namespace": "org.hamba.avro",
	    "fields" : [
	        {"name": "a", "type": "long"},
	        {"name": "b", "type": "string"}
	    ]
	}`)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(schema.Type())
	// Outputs: record
}

func ExampleNewDecoder() {
	schema := `{
	    "type": "record",
	    "name": "simple",
	    "namespace": "org.hamba.avro",
	    "fields" : [
	        {"name": "a", "type": "long"},
	        {"name": "b", "type": "string"}
	    ]
	}`

	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	r := bytes.NewReader([]byte{}) // Your reader goes here
	decoder, err := avro.NewDecoder(schema, r)
	if err != nil {
		fmt.Println("error:", err)
	}

	simple := SimpleRecord{}
	if err := decoder.Decode(&simple); err != nil {
		fmt.Println("error:", err)
	}

	fmt.Printf("%+v", simple)
}

func ExampleNewDecoderForSchema() {
	schema := avro.MustParse(`{
	    "type": "record",
	    "name": "simple",
	    "namespace": "org.hamba.avro",
	    "fields" : [
	        {"name": "a", "type": "long"},
	        {"name": "b", "type": "string"}
	    ]
	}`)

	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	r := bytes.NewReader([]byte{0x36, 0x06, 0x66, 0x6F, 0x6F}) // Your reader goes here
	decoder := avro.NewDecoderForSchema(schema, r)

	simple := SimpleRecord{}
	if err := decoder.Decode(&simple); err != nil {
		fmt.Println("error:", err)
	}

	fmt.Printf("%+v", simple)

	// Output: {A:27 B:foo}
}

func ExampleUnmarshal() {
	schema := avro.MustParse(`{
	    "type": "record",
	    "name": "simple",
	    "namespace": "org.hamba.avro",
	    "fields" : [
	        {"name": "a", "type": "long"},
	        {"name": "b", "type": "string"}
	    ]
	}`)

	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	data := []byte{0x36, 0x06, 0x66, 0x6F, 0x6F} // Your Avro data here
	simple := SimpleRecord{}
	if err := avro.Unmarshal(schema, data, &simple); err != nil {
		fmt.Println("error:", err)
	}

	fmt.Printf("%+v", simple)

	// Output: {A:27 B:foo}
}

func ExampleNewEncoder() {
	schema := `{
	    "type": "record",
	    "name": "simple",
	    "namespace": "org.hamba.avro",
	    "fields" : [
	        {"name": "a", "type": "long"},
	        {"name": "b", "type": "string"}
	    ]
	}`

	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	w := &bytes.Buffer{}
	encoder, err := avro.NewEncoder(schema, w)
	if err != nil {
		fmt.Println("error:", err)
	}

	simple := SimpleRecord{A: 27, B: "foo"}
	if err := encoder.Encode(simple); err != nil {
		fmt.Println("error:", err)
	}

	fmt.Println(w.Bytes())

	// Output: [54 6 102 111 111]
}

func ExampleNewEncoderForSchema() {
	schema := avro.MustParse(`{
	    "type": "record",
	    "name": "simple",
	    "namespace": "org.hamba.avro",
	    "fields" : [
	        {"name": "a", "type": "long"},
	        {"name": "b", "type": "string"}
	    ]
	}`)

	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	w := &bytes.Buffer{}
	encoder := avro.NewEncoderForSchema(schema, w)

	simple := SimpleRecord{A: 27, B: "foo"}
	if err := encoder.Encode(simple); err != nil {
		fmt.Println("error:", err)
	}

	fmt.Println(w.Bytes())

	// Output: [54 6 102 111 111]
}

func ExampleMarshal() {
	schema := avro.MustParse(`{
	    "type": "record",
	    "name": "simple",
	    "namespace": "org.hamba.avro",
	    "fields" : [
	        {"name": "a", "type": "long"},
	        {"name": "b", "type": "string"}
	    ]
	}`)

	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	simple := SimpleRecord{A: 27, B: "foo"}
	b, err := avro.Marshal(schema, simple)
	if err != nil {
		fmt.Println("error:", err)
	}

	fmt.Println(b)

	// Output: [54 6 102 111 111]
}

func TestEncoderDecoder_Concurrency(t *testing.T) {
	schema := avro.MustParse(`{
	    "type": "record",
	    "name": "simple",
	    "namespace": "org.hamba.avro",
	    "fields" : [
	        {"name": "a", "type": "long"},
	        {"name": "b", "type": "string"}
	    ]
	}`)

	var ops atomic.Uint32

	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(schema avro.Schema, wg *sync.WaitGroup, idx int64) {
			defer wg.Done()
			in := SimpleRecord{A: idx, B: fmt.Sprintf("foo-%d", idx)}

			data, err := avro.Marshal(schema, in)
			require.NoError(t, err)

			out := SimpleRecord{}
			err = avro.Unmarshal(schema, data, &out)

			require.NoError(t, err)
			assert.Equal(t, idx, out.A)
			assert.Equal(t, fmt.Sprintf("foo-%d", idx), out.B)
			ops.Add(1)
		}(schema, wg, int64(i))
	}
	wg.Wait()

	assert.Equal(t, uint32(1000), ops.Load())
}

func TestEncoderDecoder_UnionMarshalUnmarshalInterface(t *testing.T) {
	defer ConfigTeardown()

	schema := avro.MustParse(`{
	    "type": "record",
	    "name": "Payload",
	    "fields" : [
			{
				"name": "union", 
				"type": [
					"int", 
					{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}
				]
			}
		]
	}`)

	avro.Register("test", TestRecord{})

	type Payload struct {
		Union *UnionRecord `avro:"union"`
	}

	intValue := 1
	payload1 := Payload{Union: &UnionRecord{Int: &intValue}}
	testValue := TestRecord{A: 5, B: "foo"}
	payload2 := Payload{Union: &UnionRecord{Test: &testValue}}

	// encode
	b1, err := avro.Marshal(schema, &payload1)
	require.NoError(t, err)
	b2, err := avro.Marshal(schema, &payload2)
	require.NoError(t, err)

	// decode
	var res1 Payload
	err = avro.Unmarshal(schema, b1, &res1)
	require.NoError(t, err)
	var res2 Payload
	err = avro.Unmarshal(schema, b2, &res2)
	require.NoError(t, err)

	// assert
	require.NotNil(t, res1.Union)
	require.NotNil(t, res1.Union.Int)
	assert.Equal(t, intValue, *res1.Union.Int)
	require.NotNil(t, res2.Union)
	require.NotNil(t, res2.Union.Test)
	assert.Equal(t, testValue, *res2.Union.Test)
}
