package avro_test

import (
	"fmt"

	"github.com/hamba/avro"
)

type TestInterface interface {
	SomeFunc() int
}

type TestRecord struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

func (*TestRecord) SomeFunc() int {
	return 0
}

type TestPartialRecord struct {
	B string `avro:"b"`
}

type TestNestedRecord struct {
	A TestRecord `avro:"a"`
	B TestRecord `avro:"b"`
}

type TestUnionRecord struct {
	A *TestUnionType `avro:"a"`
}

type TestUnionType struct {
	Val interface{}
}

func (u *TestUnionType) Value() *interface{} {
	return &u.Val
}

func (u *TestUnionType) SetType(typ string) error {
	switch typ {
	case string(avro.Null):
		u.Val = nil

	case "test":
		u.Val = ""

	case string(avro.Int):
		u.Val = int(0)

	case string(avro.String):
		u.Val = ""

	default:
		return fmt.Errorf("unknown type %s", typ)
	}

	return nil
}

func (u *TestUnionType) GetType() (string, error) {
	switch u.Val.(type) {
	case nil:
		return string(avro.Null), nil

	case string:
		return "test", nil

	case int:
		return string(avro.Int), nil
	}

	return "", fmt.Errorf("unknown type %T", u.Val)
}
