package gen_test

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"

	"github.com/hamba/avro/gen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidSchemaYieldsErr(t *testing.T) {
	require.Error(t, gen.Struct(`asd`, &bytes.Buffer{}, gen.Config{}))
}

func TestNonRecordSchemasAreNotSupported(t *testing.T) {
	err := gen.Struct(`{"type": "string"}`, &bytes.Buffer{}, gen.Config{})
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "only")
	assert.Contains(t, strings.ToLower(err.Error()), "record schema")
}

func TestAvroStyleCannotBeOverriden(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`
	gc := gen.Config{
		PackageName: "Something",
		Tags: map[string]gen.TagStyle{
			"avro": gen.Kebab,
		},
	}

	goCode, lines := generate(t, schema, gc)
	fmt.Printf("Generated code:\n%s\n", string(goCode))

	for _, expected := range []string{
		"package something",
		"type Test struct {",
		"SomeString string `avro:\"someString\"`",
		"}",
	} {
		assert.Contains(t, lines, expected, "avro tags should not be configurable, they need to match the schema")
	}
}

func TestConfigurableFieldTags(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`

	for _, tc := range []struct {
		tagStyle    gen.TagStyle
		expectedTag string
	}{
		{tagStyle: gen.Camel, expectedTag: "json:\"someString\""},
		{tagStyle: gen.Snake, expectedTag: "json:\"some_string\""},
		{tagStyle: gen.Kebab, expectedTag: "json:\"some-string\""},
		{tagStyle: gen.UpperCamel, expectedTag: "json:\"SomeString\""},
	} {
		t.Run(fmt.Sprintf("%s", tc.tagStyle), func(t *testing.T) {
			gc := gen.Config{PackageName: "Something", Tags: map[string]gen.TagStyle{
				"json": tc.tagStyle,
			}}
			goCode, lines := generate(t, schema, gc)

			fmt.Printf("Generated code:\n%s\n", string(goCode))

			for _, expected := range []string{
				"package something",
				"type Test struct {",
				fmt.Sprintf("SomeString string `avro:\"someString\" %s`", tc.expectedTag),
				"}",
			} {
				assert.Contains(t, lines, expected)
			}
		})
	}
}

func TestGenFromRecordSchema(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "test",
  "fields": [
    {
      "name": "aString",
      "type": "string"
    },
    {
      "name": "aBoolean",
      "type": "boolean"
    },
    {
      "name": "anInt",
      "type": "int"
    },
    {
      "name": "aFloat",
      "type": "float"
    },
    {
      "name": "aDouble",
      "type": "double"
    },
    {
      "name": "aLong",
      "type": "long"
    },
    {
      "name": "justBytes",
      "type": "bytes"
    },
    {
      "name": "primitiveNullableArrayUnion",
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "default": null
    },
    {
      "name": "innerRecord",
      "type": {
        "name": "InnerRecord",
        "type": "record",
        "fields": [
          {
            "name": "innerJustBytes",
            "type": "bytes"
          },
          {
            "name": "innerPrimitiveNullableArrayUnion",
            "type": [
              "null",
              {
                "type": "array",
                "items": "string"
              }
            ],
            "default": null
          }
        ]
      }
    },
    {
      "name": "anEnum",
      "type": {
        "type": "enum",
        "name": "Cards",
        "symbols": [
          "SPADES",
          "HEARTS",
          "DIAMONDS",
          "CLUBS"
        ]
      }
    },
    {
      "name": "aFixed",
      "type": {
        "type": "fixed",
        "name": "fixedField",
        "size": 7
      }
    },
    {
      "name": "mapOfStrings",
      "type": {
        "name": "aMapOfStrings",
        "type": "map",
        "values": "string"
      }
    },
    {
      "name": "mapOfRecords",
      "type": {
        "name": "aMapOfRecords",
        "type": "map",
        "values": {
          "name": "RecordInMap",
          "type": "record",
          "fields": [
            {
              "type": "string",
              "name": "name"
            }
          ]
        }
      }
    },
    {
      "name": "aDate",
      "type": "int",
      "logicalType": "date"
    },
    {
      "name": "aDuration",
      "type": "int",
      "logicalType": "time-millis"
    },
    {
      "name": "aLongTimeMicros",
      "type": "long",
      "logicalType": "time-micros"
    },
    {
      "name": "aLongTimestampMillis",
      "type": "long",
      "logicalType": "timestamp-millis"
    },
    {
      "name": "aLongTimestampMicro",
      "type": "long",
      "logicalType": "timestamp-micros"
    },
    {
      "name": "aBytesDecimal",
      "type": "bytes",
      "logicalType": "decimal",
      "precision": 4,
      "scale": 2
    },
    {
      "name": "aRecordArray",
      "type": {
        "name": "someRecordArray",
        "type": "array",
        "items": {
          "name": "recordInArray",
          "type": "record",
          "fields": [
            {
              "type": "string",
              "name": "aString"
            }
          ]
        }
      }
    },
    {
      "name": "nullableRecordUnion",
      "type": [
        "null",
        {
          "name": "recordInNullableUnion",
          "type": "record",
          "fields": [
            {
              "name": "aString",
              "type": "string"
            }
          ]
        }
      ],
      "default": null
    },
    {
      "name": "nonNullableRecordUnion",
      "type": [
        {
          "name": "record1InNonNullableUnion",
          "type": "record",
          "fields": [
            {
              "name": "aString",
              "type": "string"
            }
          ]
        },
        {
          "name": "record2InNonNullableUnion",
          "type": "record",
          "fields": [
            {
              "name": "aString",
              "type": "string"
            }
          ]
        }
      ]
    },
    {
      "name": "nullableRecordUnionWith3Options",
      "type": [
        "null",
        {
          "name": "record1InNullableUnion",
          "type": "record",
          "fields": [
            {
              "name": "aString",
              "type": "string"
            }
          ]
        },
        {
          "name": "record2InNullableUnion",
          "type": "record",
          "fields": [
            {
              "name": "aString",
              "type": "string"
            }
          ]
        }
      ],
      "default": null
    },
    {"name": "ref", "type": "record2InNullableUnion"}
  ]
}`

	gc := gen.Config{PackageName: "Something"}
	goCode, lines := generate(t, schema, gc)

	fmt.Printf("Generated code:\n%s\n", string(goCode))

	for _, expected := range []string{
		"package something",
		"import (",
		"\"math/big\"",
		"\"time\"",
		")",
		"type Test struct {",
		"AString string `avro:\"aString\"`",
		"ABoolean bool `avro:\"aBoolean\"`",
		"AnInt int `avro:\"anInt\"`",
		"AFloat float32 `avro:\"aFloat\"`",
		"ADouble float64 `avro:\"aDouble\"`",
		"ALong int64 `avro:\"aLong\"`",
		"JustBytes []byte `avro:\"justBytes\"`",
		"PrimitiveNullableArrayUnion []string `avro:\"primitiveNullableArrayUnion\"`",
		"InnerRecord InnerRecord `avro:\"innerRecord\"`",
		"AnEnum string `avro:\"anEnum\"`",
		"AFixed [7]byte `avro:\"aFixed\"`",
		"MapOfStrings map[string]string `avro:\"mapOfStrings\"`",
		"MapOfRecords map[string]RecordInMap `avro:\"mapOfRecords\"`",
		"ADate time.Time `avro:\"aDate\"`",
		"ADuration time.Duration `avro:\"aDuration\"`",
		"ALongTimeMicros time.Duration `avro:\"aLongTimeMicros\"`",
		"ALongTimestampMillis time.Time `avro:\"aLongTimestampMillis\"`",
		"ALongTimestampMicro time.Time `avro:\"aLongTimestampMicro\"`",
		"ABytesDecimal *big.Rat `avro:\"aBytesDecimal\"`",
		"NullableRecordUnion *RecordInNullableUnion `avro:\"nullableRecordUnion\"`",
		"NonNullableRecordUnion interface{} `avro:\"nonNullableRecordUnion\"`",
		"NullableRecordUnionWith3Options interface{} `avro:\"nullableRecordUnionWith3Options\"`",
		"Ref Record2InNullableUnion `avro:\"ref\"`",
		"}",
		"type InnerRecord struct {",
		"InnerJustBytes []byte `avro:\"innerJustBytes\"`",
		"InnerPrimitiveNullableArrayUnion []string `avro:\"innerPrimitiveNullableArrayUnion\"`",
		"}",
		"type RecordInMap struct {",
		"Name string `avro:\"name\"`",
		"}",
		"type RecordInArray struct {",
		"AString string `avro:\"aString\"`",
		"}",
		"type Record1InNonNullableUnion struct {",
		"AString string `avro:\"aString\"`",
		"}",
		"type Record2InNonNullableUnion struct {",
		"AString string `avro:\"aString\"`",
		"}",
		"type RecordInNullableUnion struct {",
		"AString string `avro:\"aString\"`",
		"}",
		"type Record1InNullableUnion struct {",
		"AString string `avro:\"aString\"`",
		"}",
		"type Record2InNullableUnion struct {",
		"AString string `avro:\"aString\"`",
		"}",
	} {
		assert.Contains(t, lines, expected)
	}
}

// generate is a utility to run the generation and return the result as a tuple
func generate(t *testing.T, schema string, gc gen.Config) ([]byte, []string) {
	buf := &bytes.Buffer{}
	err := gen.Struct(schema, buf, gc)
	require.NoError(t, err)
	goCode, err := io.ReadAll(buf)
	require.NoError(t, err)
	return goCode, removeSpaceAndEmptyLines(goCode)
}

func removeSpaceAndEmptyLines(goCode []byte) []string {
	var lines []string
	for _, lineBytes := range bytes.Split(goCode, []byte("\n")) {
		if len(lineBytes) == 0 {
			continue
		}
		trimmed := removeMoreThanOneConsecutiveSpaces(lineBytes)
		lines = append(lines, trimmed)
	}
	return lines
}

// removeMoreThanOneConsecutiveSpaces replaces all sequences of more than one space, with a single one
func removeMoreThanOneConsecutiveSpaces(lineBytes []byte) string {
	return strings.Join(regexp.MustCompile("\\s+|\\t+").Split(strings.TrimSpace(string(lineBytes)), -1), " ")
}
