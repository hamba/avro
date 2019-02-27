/*
Package avro implements encoding and decoding of Avro as defined by the Avro specification.

See the Avro specification for an understanding of Avro: https://avro.apache.org/docs/1.8.1/spec.html

Usage Example:
	type TestRecord struct {
		A int64  `avro:"a"`
		B string `avro:"b"`
	}

	schema, err := avro.Parse(`{
		"type": "record",
		"name": "test",
		"namespace": "org.hamba.avro",
		"fields" : [
			{"name": "a", "type": "long"},
			{"name": "b", "type": "string"}
		]
	}`)
	if err != nil {
		log.Fatal(err)
	}

	in := TestRecord{A: 27, B: "foo"}

	data, err := avro.Marshal(schema, in)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(data)
	// Outputs: [54 6 102 111 111]

	out := TestRecord{}
	err = avro.Unmarshal(schema, data, &out)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(out)
	// Outputs: {27 foo}
*/
package avro
