package avro_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenFromSchema(t *testing.T) {
	defer ConfigTeardown()
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
    }
  ]
}`

	gc := avro.GenConf{PackageName: "Something"}
	goCodeReader, err := avro.GenerateFrom(gc, schema)
	require.NoError(t, err)
	goCode, err := io.ReadAll(goCodeReader)
	require.NoError(t, err)

	fmt.Printf("Generated code:\n%s\n", string(goCode))

	var lines []string
	for _, lineBytes := range bytes.Split(goCode, []byte("\n")) {
		if len(lineBytes) == 0 {
			continue
		}
		trimmed := removeMoreThanOneConsecutiveSpaces(lineBytes)
		lines = append(lines, trimmed)
	}

	for _, expected := range []string{
		"package something",
		"type Test struct {",
		"AString string",
		"ABoolean bool",
		"AnInt int",
		"AFloat float32",
		"ADouble float64",
		"ALong int64",
		"JustBytes []byte",
		"PrimitiveNullableArrayUnion []string",
		"InnerRecord InnerRecord",
		"AnEnum string",
		"AFixed [7]byte",
		"MapOfStrings map[string]string",
		"MapOfRecords map[string]RecordInMap",
		"ADate time.Time",
		"ADuration time.Duration",
		"ALongTimeMicros time.Duration",
		"ALongTimestampMillis time.Time",
		"ALongTimestampMicro time.Time",
		"ABytesDecimal *big.Rat",
		"NullableRecordUnion *RecordInNullableUnion",
		"NonNullableRecordUnion {}interface",
		"NullableRecordUnionWith3Options {}interface",
		"}",
		"type InnerRecord struct {",
		"InnerJustBytes []byte",
		"InnerPrimitiveNullableArrayUnion []string",
		"}",
		"type RecordInMap struct {",
		"Name string",
		"}",
		"type RecordInArray struct {",
		"AString string",
		"}",
		"type Record1InNonNullableUnion struct {",
		"AString string",
		"}",
		"type Record2InNonNullableUnion struct {",
		"AString string",
		"}",
		"type RecordInNullableUnion struct {",
		"AString string",
		"}",
		"type Record1InNullableUnion struct {",
		"AString string",
		"}",
		"type Record2InNullableUnion struct {",
		"AString string",
		"}",
	} {
		assert.Contains(t, lines, expected)
	}
}

func removeMoreThanOneConsecutiveSpaces(lineBytes []byte) string {
	trimmed := strings.TrimSpace(string(lineBytes))
	var line []string
	words := strings.Split(trimmed, " ")
	for _, word := range words {
		if word != "" {
			line = append(line, word)
		}
	}
	return strings.Join(line, " ")
}
