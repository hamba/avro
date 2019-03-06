package registry_test

import (
	"fmt"
	"log"

	"github.com/hamba/avro/registry"
)

func Example() {
	reg, err := registry.NewClient("http://example.com")
	if err != nil {
		log.Fatal(err)
	}

	schema, err := reg.GetSchema(5)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("schema: ", schema)
}
