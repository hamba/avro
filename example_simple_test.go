package avro_test

import (
	"fmt"
	"log"

	"github.com/hamba/avro"
)

var Schema = `{
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

func Example_usage() {
	schema, err := avro.Parse(Schema)
	if err != nil {
		log.Fatal(err)
	}

	in := SimpleRecord{A: 27, B: "foo"}

	data, err := avro.Marshal(schema, in)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v\n", data)
	// Outputs: [54 6 102 111 111]

	out := SimpleRecord{}
	err = avro.Unmarshal(schema, data, &out)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v\n", out)
	// Outputs: {A:27 B:foo}
}
