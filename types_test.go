package avro_test

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
