package avro_test

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/hamba/avro"
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

	r := bytes.NewReader([]byte{}) // Your reader goes here
	decoder := avro.NewDecoderForSchema(schema, r)

	simple := SimpleRecord{}
	if err := decoder.Decode(&simple); err != nil {
		fmt.Println("error:", err)
	}

	fmt.Printf("%+v", simple)
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

	data := []byte{} // Your Avro data here
	simple := SimpleRecord{}
	if err := avro.Unmarshal(schema, data, &simple); err != nil {
		fmt.Println("error:", err)
	}

	fmt.Printf("%+v", simple)
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

	simple := SimpleRecord{}
	if err := encoder.Encode(simple); err != nil {
		fmt.Println("error:", err)
	}

	os.Stdout.Write(w.Bytes())
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

	simple := SimpleRecord{}
	if err := encoder.Encode(simple); err != nil {
		fmt.Println("error:", err)
	}

	os.Stdout.Write(w.Bytes())
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

	simple := SimpleRecord{}
	b, err := avro.Marshal(schema, simple)
	if err != nil {
		fmt.Println("error:", err)
	}

	os.Stdout.Write(b)
}
