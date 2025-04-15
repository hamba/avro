package avro_test

import "errors"

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

type UnionRecord struct {
	Int  *int
	Test *TestRecord
}

func (u *UnionRecord) MarshalUnion() (any, error) {
	if u.Int != nil {
		return *u.Int, nil
	} else if u.Test != nil {
		return *u.Test, nil
	}

	return nil, errors.New("no value to encode")
}

func (u *UnionRecord) UnmarshalUnion(payload any) error {
	switch t := payload.(type) {
	case int:
		u.Int = &t
	case *TestRecord:
		u.Test = t
	default:
		return errors.New("unknown type during decode of union")
	}

	return nil
}

type TestPartialRecord struct {
	B string `avro:"b"`
}

type TestNestedRecord struct {
	A TestRecord `avro:"a"`
	B TestRecord `avro:"b"`
}

type TestUnion struct {
	A any `avro:"a"`
}

type TestEmbeddedRecord struct {
	C string `avro:"c"`

	TestEmbed // tests not-first position
}

type TestEmbeddedPtrRecord struct {
	C string `avro:"c"`

	*TestEmbed // tests not-first position
}

type TestEmbed struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

type TestEmbedInt int

type TestEmbeddedIntRecord struct {
	B string `avro:"b"`

	TestEmbedInt // tests not-first position
}

type TestUnexportedRecord struct {
	A int64  `avro:"a"`
	b string `avro:"b"`
}
