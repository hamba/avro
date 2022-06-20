package registry_test

import (
	"context"
	"fmt"
	"log"

	"github.com/hamba/avro/v2/registry"
)

func Example() {
	reg, err := registry.NewClient("http://example.com")
	if err != nil {
		log.Fatal(err)
	}

	schema, err := reg.GetSchema(context.Background(), 5)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("schema: ", schema)

	schemaRaw := `["null","string","int"]`
	id, schema, err := reg.IsRegistered(context.Background(), "foobar", schemaRaw)
	if err != nil {
		id, schema, err = reg.CreateSchema(context.Background(), "foobar", schemaRaw)
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("id: ", id)
	fmt.Println("schema: ", schema)
}
