package avro_test

import (
	"strconv"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test cases are taken from the reference implementation here:
// https://github.com/apache/avro/blob/master/share/test/data/schema-tests.txt

func TestSchema_Canonical(t *testing.T) {
	tests := []struct {
		input     string
		canonical string
	}{
		{
			input:     `"null"`,
			canonical: `"null"`,
		},
		{
			input:     `{"type":"null"}`,
			canonical: `"null"`,
		},
		{
			input:     `"boolean"`,
			canonical: `"boolean"`,
		},
		{
			input:     `{"type":"boolean"}`,
			canonical: `"boolean"`,
		},
		{
			input:     `"int"`,
			canonical: `"int"`,
		},
		{
			input:     `{"type":"int"}`,
			canonical: `"int"`,
		},
		{
			input:     `{"type":"int","logicalType":"date"}`,
			canonical: `{"type":"int","logicalType":"date"}`,
		},
		{
			input:     `{"type":"int","logicalType":"time-millis"}`,
			canonical: `{"type":"int","logicalType":"time-millis"}`,
		},
		{
			input:     `{"type":"int"}`,
			canonical: `"int"`,
		},
		{
			input:     `"long"`,
			canonical: `"long"`,
		},
		{
			input:     `{"type":"long"}`,
			canonical: `"long"`,
		},
		{
			input:     `{"type":"long","logicalType":"time-micros"}`,
			canonical: `{"type":"long","logicalType":"time-micros"}`,
		},
		{
			input:     `{"type":"long","logicalType":"timestamp-millis"}`,
			canonical: `{"type":"long","logicalType":"timestamp-millis"}`,
		},
		{
			input:     `{"type":"long","logicalType":"timestamp-millis"}`,
			canonical: `{"type":"long","logicalType":"timestamp-millis"}`,
		},
		{
			input:     `"float"`,
			canonical: `"float"`,
		},
		{
			input:     `{"type":"float"}`,
			canonical: `"float"`,
		},
		{
			input:     `"double"`,
			canonical: `"double"`,
		},
		{
			input:     `{"type":"double"}`,
			canonical: `"double"`,
		},
		{
			input:     `"bytes"`,
			canonical: `"bytes"`,
		},
		{
			input:     `{"type":"bytes"}`,
			canonical: `"bytes"`,
		},
		{
			input:     `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			canonical: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		},
		{
			input:     `{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}`,
			canonical: `{"type":"bytes","logicalType":"decimal","precision":4}`,
		},
		{
			input:     `"string"`,
			canonical: `"string"`,
		},
		{
			input:     `{"type":"string"}`,
			canonical: `"string"`,
		},
		{
			input:     `{"type":"string","logicalType":"uuid"}`,
			canonical: `{"type":"string","logicalType":"uuid"}`,
		},
		{
			input:     `{"type":"string","sqlType":"JSON"}`,
			canonical: `{"type":"string","sqlType":"JSON"}`,
		},
		{
			input:     `[  ]`,
			canonical: `[]`,
		},
		{
			input:     `[ "int"  ]`,
			canonical: `["int"]`,
		},
		{
			input:     `[ "int" , {"type":"boolean"} ]`,
			canonical: `["int","boolean"]`,
		},
		{
			input:     `{"fields":[], "type":"error", "name":"foo"}`,
			canonical: `{"name":"foo","type":"error","fields":[]}`,
		},
		{
			input:     `{"fields":[], "type":"record", "name":"foo"}`,
			canonical: `{"name":"foo","type":"record","fields":[]}`,
		},
		{
			input:     `{"fields":[], "type":"record", "name":"foo", "namespace":"x.y"}`,
			canonical: `{"name":"x.y.foo","type":"record","fields":[]}`,
		},
		{
			input:     `{"fields":[], "type":"record", "name":"a.b.foo", "namespace":"x.y"}`,
			canonical: `{"name":"a.b.foo","type":"record","fields":[]}`,
		},
		{
			input:     `{"fields":[], "type":"record", "name":"foo", "doc":"Useful info"}`,
			canonical: `{"name":"foo","type":"record","fields":[]}`,
		},
		{
			input:     `{"fields":[], "type":"record", "name":"foo", "aliases":["foo","bar"]}`,
			canonical: `{"name":"foo","type":"record","fields":[]}`,
		},
		{
			input:     `{"fields":[], "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}`,
			canonical: `{"name":"foo","type":"record","fields":[]}`,
		},
		{
			input:     `{"fields":[{"type":{"type":"boolean"}, "name":"f1"}], "type":"record", "name":"foo"}`,
			canonical: `{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}`,
		},
		{
			input: `
{ "fields":[{"type":"boolean", "aliases":[], "name":"f1", "default":true},
           {"order":"descending","name":"f2","doc":"Hello","type":"int"}],
 "type":"record", "name":"foo"
}`,
			canonical: `{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"},{"name":"f2","type":"int"}]}`,
		},
		{
			input:     `{"type":"enum", "name":"foo", "symbols":["A1"]}`,
			canonical: `{"name":"foo","type":"enum","symbols":["A1"]}`,
		},
		{
			input:     `{"namespace":"x.y.z", "type":"enum", "name":"foo", "doc":"foo bar", "symbols":["A1", "A2"]}`,
			canonical: `{"name":"x.y.z.foo","type":"enum","symbols":["A1","A2"]}`,
		},
		{
			input:     `{"name":"foo","type":"fixed","size":15}`,
			canonical: `{"name":"foo","type":"fixed","size":15}`,
		},
		{
			input:     `{"name":"foo","type":"fixed","logicalType":"duration","size":12}`,
			canonical: `{"name":"foo","type":"fixed","size":12,"logicalType":"duration"}`,
		},
		{
			input:     `{"name":"foo","type":"fixed","logicalType":"decimal","size":12,"precision":4,"scale":2}`,
			canonical: `{"name":"foo","type":"fixed","size":12,"logicalType":"decimal","precision":4,"scale":2}`,
		},
		{
			input:     `{"name":"foo","type":"fixed","logicalType":"decimal","size":12,"precision":4,"scale":2,"someProp":"foobar"}`,
			canonical: `{"name":"foo","type":"fixed","size":12,"logicalType":"decimal","precision":4,"scale":2}`,
		},
		{
			input:     `{"name":"foo","type":"fixed","logicalType":"decimal","size":12,"precision":4,"scale":0}`,
			canonical: `{"name":"foo","type":"fixed","size":12,"logicalType":"decimal","precision":4}`,
		},
		{
			input:     `{"namespace":"x.y.z", "type":"fixed", "name":"foo", "doc":"foo bar", "size":32}`,
			canonical: `{"name":"x.y.z.foo","type":"fixed","size":32}`,
		},
		{
			input:     `{ "items":{"type":"null"}, "type":"array"}`,
			canonical: `{"type":"array","items":"null"}`,
		},
		{
			input:     `{ "values":"string", "type":"map"}`,
			canonical: `{"type":"map","values":"string"}`,
		},
		{
			input: `

 {"name":"PigValue","type":"record",
  "fields":[{"name":"value", "type":["null", "int", "long", "PigValue"]}]}
`,
			canonical: `{"name":"PigValue","type":"record","fields":[{"name":"value","type":["null","int","long","PigValue"]}]}`,
		},
		{
			input: `{
				"type":"record",
				"namespace": "org.hamba.avro",
				"name":"X",
  				"fields":[
					{"name":"value", "type":{
						"type":"record",
						"name":"Y",
						"fields":[
							{"name":"value", "type":"string"}
						]
					}}
				]
			}`,
			canonical: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"value","type":{"name":"org.hamba.avro.Y","type":"record","fields":[{"name":"value","type":"string"}]}}]}`,
		},
		{
			input: `{
				"type":"record",
				"namespace": "org.hamba.avro",
				"name":"X",
  				"fields":[
					{"name":"value", "type":{
						"type":"enum",
						"name":"Y",
						"symbols":["TEST"]
					}}
				]
			}`,
			canonical: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"value","type":{"name":"org.hamba.avro.Y","type":"enum","symbols":["TEST"]}}]}`,
		},
		{
			input: `{
				"type":"record",
				"namespace": "org.hamba.avro",
				"name":"X",
  				"fields":[
					{"name":"value", "type":{
						"type":"fixed",
						"name":"Y",
						"size":15
					}}
				]
			}`,
			canonical: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"value","type":{"name":"org.hamba.avro.Y","type":"fixed","size":15}}]}`,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			s, err := avro.Parse(test.input)

			require.NoError(t, err)
			assert.Equal(t, test.canonical, s.String())
		})
	}
}
