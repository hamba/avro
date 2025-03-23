package testdata

import (
	"github.com/hamba/avro/v2"
)

// Basic annotated type
type StringInt struct {
	StringVal string `avro:"stringval"`
	IntVal    int    `avro:"intval"`
}

var StringIntSchema = avro.MustParse(`{"name":"stringint","type":"record","fields":[{"name":"stringval","type":"string"},{"name":"intval","type":"int"}]}`)

// Faux avrogen-generated type
type Generated struct {
	Name string `avro:"name"`
	Age  int    `avro:"age"`
}

var generatedSchema = avro.MustParse(`{"name":"generated","type":"record","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`)

// Schema returns the schema for Generated.
func (o *Generated) Schema() avro.Schema {
	return generatedSchema
}

// Unmarshal decodes b into the receiver.
func (o *Generated) Unmarshal(b []byte) error {
	return avro.Unmarshal(o.Schema(), b, o)
}

// Marshal encodes the receiver.
func (o *Generated) Marshal() ([]byte, error) {
	return avro.Marshal(o.Schema(), o)
}

// Types used for testing the dynamic decoder.

// Dynamic1 is a basic type used for testing.
type Dynamic1 struct {
	Name string `avro:"name"`
	Age  int    `avro:"age"`
}

var Dynamic1Schema = avro.MustParse(`{"name":"dynamic1","type":"record","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`)

// Dynamic2 is a basic type used for testing, incompatible with Dynamic1.
type Dynamic2 struct {
	Key     string `avro:"key"`
	Enabled bool   `avro:"enabled"`
}

var Dynamic2Schema = avro.MustParse(`{"name":"dynamic2","type":"record","fields":[{"name":"key","type":"string"},{"name":"enabled","type":"boolean"}]}`)

// Dynamic3 is a basic type used for testing, an extension of Dynamic1.
type Dynamic3 struct {
	Name  string `avro:"name"`
	Age   int    `avro:"age"`
	Hobby string `avro:"hobby"`
}

var Dynamic3Schema = avro.MustParse(`{"name":"dynamic2","type":"record","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"},{"name":"hobby","type":"string"}]}`)
