package avro_test

import (
	"fmt"
	"os"

	"github.com/hamba/avro"
)

type SimpleUnionType struct {
	Val interface{}
}

func (u *SimpleUnionType) Value() *interface{} {
	return &u.Val
}

func (u *SimpleUnionType) SetType(typ string) error {
	switch typ {
	case string(avro.String):
		u.Val = ""

	case string(avro.Int):
		u.Val = int(0)

	default:
		return fmt.Errorf("unknown type %s", typ)
	}

	return nil
}

func (u *SimpleUnionType) GetType() (string, error) {
	switch u.Val.(type) {
	case string:
		return string(avro.String), nil

	case int:
		return string(avro.Int), nil
	}

	return "", fmt.Errorf("unknown type %T", u.Val)
}

type SimpleRecord struct {
	Items map[string]*SimpleUnionType `avro:"items"`
}

func ExampleUnionType() {
	schema := avro.MustParse(`{
	    "type": "record",
	    "name": "simple",
	    "namespace": "org.hamba.avro",
	    "fields" : [
	        {"name": "items", "type": {"type": "map", "values": ["string", "int"]}}
	    ]
	}`)

	simple := SimpleRecord{}
	b, err := avro.Marshal(schema, simple)
	if err != nil {
		fmt.Println("error:", err)
	}

	os.Stdout.Write(b)
}
