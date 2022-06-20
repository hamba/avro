package avro_test

import (
	"bytes"
	"fmt"
	"log"

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
