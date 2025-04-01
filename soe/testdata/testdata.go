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
