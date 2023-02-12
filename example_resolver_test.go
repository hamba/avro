package avro_test

import (
	"fmt"
	"log"

	"github.com/hamba/avro/v2"
)

func ExampleRegister() {
	data := []byte{0x02, 0x02} // Your Avro data here
	schema := avro.MustParse(`["null", {"type":"enum", "name": "test", "symbols": ["A", "B"]}]`)

	avro.Register("test", "") // Register the name test as a string type

	var result any
	err := avro.Unmarshal(schema, data, &result)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result)

	// Output: B
}
