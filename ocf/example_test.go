package ocf_test

import (
	"log"
	"os"

	"github.com/hamba/avro/v2/ocf"
)

func ExampleNewDecoder() {
	type SimpleRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	f, err := os.Open("/your/avro/file.avro")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	dec, err := ocf.NewDecoder(f)
	if err != nil {
		log.Fatal(err)
	}

	for dec.HasNext() {
		var record SimpleRecord
		err = dec.Decode(&record)
		if err != nil {
			log.Fatal(err)
		}

		// Do something with the data
	}

	if dec.Error() != nil {
		log.Fatal(err)
	}
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

	f, err := os.Open("/your/avro/file.avro")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	enc, err := ocf.NewEncoder(schema, f)
	if err != nil {
		log.Fatal(err)
	}

	var record SimpleRecord
	err = enc.Encode(record)
	if err != nil {
		log.Fatal(err)
	}

	if err := enc.Flush(); err != nil {
		log.Fatal(err)
	}

	if err := f.Sync(); err != nil {
		log.Fatal(err)
	}
}
