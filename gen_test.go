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
  "type": "record", "name": "test", "fields": [
  {"name": "aString", "type": "string"},
  {"name": "aBoolean", "type": "boolean"},
  {"name": "anInt", "type": "int"},
  {"name": "aFloat", "type": "float"},
  {"name": "aDouble", "type": "double"},
  {"name": "aLong", "type": "long"},
  {"name": "justBytes", "type": "bytes"},
  {
    "name": "primitiveNullableArrayUnion", "type": [
    "null",
    {"type": "array", "items": "string"}
  ], "default": null
  },
  {
    "name": "innerRecord", "type": {
    "name": "InnerRecord", "type": "record", "fields": [
      {"name": "innerJustBytes", "type": "bytes"},
      {
        "name": "innerPrimitiveNullableArrayUnion", "type": [
        "null",
        {"type": "array", "items": "string"}
      ], "default": null
      }
    ]
  }
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
		"}",
		"type InnerRecord struct {",
		"InnerJustBytes []byte",
		"InnerPrimitiveNullableArrayUnion []string",
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
