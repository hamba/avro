// Code generated by avro/gen. DO NOT EDIT.
package something


type TestUnionType struct {
		Field1 int64 `avro:"Field1"`
		Field2 int `avro:"Field2"`
}

type TestMain struct {
		TestUnion *TestUnionType `avro:"TestUnion"`
}
