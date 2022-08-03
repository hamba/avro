package avro_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchema_JSON(t *testing.T) {
	tests := []struct {
		input string
		json  string
	}{
		{
			input: `"null"`,
			json:  `"null"`,
		},
		{
			input: `{"type":"null"}`,
			json:  `"null"`,
		},
		{
			input: `"boolean"`,
			json:  `"boolean"`,
		},
		{
			input: `{"type":"boolean"}`,
			json:  `"boolean"`,
		},
		{
			input: `"int"`,
			json:  `"int"`,
		},
		{
			input: `{"type":"int"}`,
			json:  `"int"`,
		},
		{
			input: `{"type":"int","logicalType":"date"}`,
			json:  `{"type":"int","logicalType":"date"}`,
		},
		{
			input: `{"type":"int","logicalType":"time-millis"}`,
			json:  `{"type":"int","logicalType":"time-millis"}`,
		},
		{
			input: `{"type":"int"}`,
			json:  `"int"`,
		},
		{
			input: `"long"`,
			json:  `"long"`,
		},
		{
			input: `{"type":"long"}`,
			json:  `"long"`,
		},
		{
			input: `{"type":"long","logicalType":"time-micros"}`,
			json:  `{"type":"long","logicalType":"time-micros"}`,
		},
		{
			input: `{"type":"long","logicalType":"timestamp-millis"}`,
			json:  `{"type":"long","logicalType":"timestamp-millis"}`,
		},
		{
			input: `{"type":"long","logicalType":"timestamp-millis"}`,
			json:  `{"type":"long","logicalType":"timestamp-millis"}`,
		},
		{
			input: `"float"`,
			json:  `"float"`,
		},
		{
			input: `{"type":"float"}`,
			json:  `"float"`,
		},
		{
			input: `"double"`,
			json:  `"double"`,
		},
		{
			input: `{"type":"double"}`,
			json:  `"double"`,
		},
		{
			input: `"bytes"`,
			json:  `"bytes"`,
		},
		{
			input: `{"type":"bytes"}`,
			json:  `"bytes"`,
		},
		{
			input: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
			json:  `{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}`,
		},
		{
			input: `{"type":"bytes","logicalType":"decimal","precision":4,"scale":0}`,
			json:  `{"type":"bytes","logicalType":"decimal","precision":4}`,
		},
		{
			input: `"string"`,
			json:  `"string"`,
		},
		{
			input: `{"type":"string"}`,
			json:  `"string"`,
		},
		{
			input: `{"type":"string","logicalType":"uuid"}`,
			json:  `{"type":"string","logicalType":"uuid"}`,
		},
		{
			input: `{"type":"string","sqlType":"JSON"}`,
			json:  `{"type":"string","sqlType":"JSON"}`,
		},
		{
			input: `[  ]`,
			json:  `[]`,
		},
		{
			input: `[ "int"  ]`,
			json:  `["int"]`,
		},
		{
			input: `[ "int" , {"type":"boolean"} ]`,
			json:  `["int","boolean"]`,
		},
		{
			input: `{"fields":[], "type":"error", "name":"foo"}`,
			json:  `{"name":"foo","type":"error","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo"}`,
			json:  `{"name":"foo","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo", "namespace":"x.y"}`,
			json:  `{"name":"x.y.foo","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"a.b.foo", "namespace":"x.y"}`,
			json:  `{"name":"a.b.foo","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo", "doc":"Useful info"}`,
			json:  `{"name":"foo","doc":"Useful info","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo", "aliases":["foo","bar"]}`,
			json:  `{"name":"foo","aliases":["foo","bar"],"type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[], "type":"record", "name":"foo", "doc":"foo", "aliases":["foo","bar"]}`,
			json:  `{"name":"foo","aliases":["foo","bar"],"doc":"foo","type":"record","fields":[]}`,
		},
		{
			input: `{"fields":[{"type":{"type":"boolean"}, "name":"f1"}], "type":"record", "name":"foo"}`,
			json:  `{"name":"foo","type":"record","fields":[{"name":"f1","type":"boolean"}]}`,
		},
		{
			input: `
{ "fields":[{"type":"boolean", "aliases":["foo"], "name":"f1", "default":true},
           {"order":"descending","name":"f2","doc":"Hello","type":"int"}],
 "type":"record", "name":"foo"
}`,
			json: `{"name":"foo","type":"record","fields":[{"name":"f1","aliases":["foo"],"type":"boolean","default":true},{"name":"f2","doc":"Hello","type":"int","order":"descending"}]}`,
		},
		{
			input: `{"type":"enum", "name":"foo", "symbols":["A1"]}`,
			json:  `{"name":"foo","type":"enum","symbols":["A1"]}`,
		},
		{
			input: `{"namespace":"x.y.z", "type":"enum", "name":"foo", "doc":"foo bar", "symbols":["A1", "A2"]}`,
			json:  `{"name":"x.y.z.foo","doc":"foo bar","type":"enum","symbols":["A1","A2"]}`,
		},
		{
			input: `{"name":"foo","type":"fixed","size":15}`,
			json:  `{"name":"foo","type":"fixed","size":15}`,
		},
		{
			input: `{"name":"foo","type":"fixed","logicalType":"duration","size":12}`,
			json:  `{"name":"foo","type":"fixed","size":12,"logicalType":"duration"}`,
		},
		{
			input: `{"name":"foo","type":"fixed","logicalType":"decimal","size":12,"precision":4,"scale":2}`,
			json:  `{"name":"foo","type":"fixed","size":12,"logicalType":"decimal","precision":4,"scale":2}`,
		},
		{
			input: `{"name":"foo","type":"fixed","logicalType":"decimal","size":12,"precision":4,"scale":0}`,
			json:  `{"name":"foo","type":"fixed","size":12,"logicalType":"decimal","precision":4}`,
		},
		{
			input: `{"namespace":"x.y.z", "type":"fixed", "name":"foo", "size":32}`,
			json:  `{"name":"x.y.z.foo","type":"fixed","size":32}`,
		},
		{
			input: `{ "items":{"type":"null"}, "type":"array"}`,
			json:  `{"type":"array","items":"null"}`,
		},
		{
			input: `{ "values":"string", "type":"map"}`,
			json:  `{"type":"map","values":"string"}`,
		},
		{
			input: `

 {"name":"PigValue","type":"record",
  "fields":[{"name":"value", "type":["null", "int", "long", "PigValue"]}]}
`,
			json: `{"name":"PigValue","type":"record","fields":[{"name":"value","type":["null","int","long","PigValue"]}]}`,
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
			json: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"value","type":{"name":"org.hamba.avro.Y","type":"record","fields":[{"name":"value","type":"string"}]}}]}`,
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
			json: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"value","type":{"name":"org.hamba.avro.Y","type":"enum","symbols":["TEST"]}}]}`,
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
			json: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"value","type":{"name":"org.hamba.avro.Y","type":"fixed","size":15}}]}`,
		},
		{
			input: `{
				"type":"record",
				"namespace": "org.hamba.avro",
				"name":"X",
  				"fields":[
					{"name":"union_no_def","type":["null", "int"]},
					{"name":"union_with_def","type":["null", "string"],"default": null}
				]
			}`,
			json: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"union_no_def","type":["null","int"]},{"name":"union_with_def","type":["null","string"],"default":null}]}`,
		},
		{
			input: `{
				"type":"record",
				"namespace": "org.hamba.avro",
				"name":"X",
  				"fields":[
					{"name":"union","type":["null", "string"]},
					{"name":"union_with_json_string","type":["null", {"type": "string", "sqlType": "JSON"}]}
				]
			}`,
			json: `{"name":"org.hamba.avro.X","type":"record","fields":[{"name":"union","type":["null","string"]},{"name":"union_with_json_string","type":["null",{"type":"string","sqlType":"JSON"}]}]}`,
		},
	}

	for i, test := range tests {
		test := test
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			s, err := avro.Parse(test.input)
			require.NoError(t, err)

			b, err := json.Marshal(s)

			require.NoError(t, err)
			assert.Equal(t, test.json, string(b))
		})
	}
}
