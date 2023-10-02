package gen_test

import (
	"bytes"
	"flag"
	"go/format"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/gen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "Update golden files")

func TestStruct_InvalidSchemaYieldsErr(t *testing.T) {
	err := gen.Struct(`asd`, &bytes.Buffer{}, gen.Config{})

	assert.Error(t, err)
}

func TestStruct_NonRecordSchemasAreNotSupported(t *testing.T) {
	err := gen.Struct(`{"type": "string"}`, &bytes.Buffer{}, gen.Config{})

	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "only")
	assert.Contains(t, strings.ToLower(err.Error()), "record schema")
}

func TestStruct_AvroStyleCannotBeOverridden(t *testing.T) {
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

	_, lines := generate(t, schema, gc)

	for _, expected := range []string{
		"package something",
		"type Test struct {",
		"SomeString string `avro:\"someString\"`",
		"}",
	} {
		assert.Contains(t, lines, expected, "avro tags should not be configurable, they need to match the schema")
	}
}

func TestStruct_HandlesGoInitialisms(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "httpRecord",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`
	gc := gen.Config{
		PackageName: "Something",
	}

	_, lines := generate(t, schema, gc)

	assert.Contains(t, lines, "type HTTPRecord struct {")
}

func TestStruct_HandlesAdditionalInitialisms(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "CidOverHttpRecord",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`
	gc := gen.Config{
		PackageName: "Something",
		Initialisms: []string{"CID"},
	}

	_, lines := generate(t, schema, gc)

	assert.Contains(t, lines, "type CIDOverHTTPRecord struct {")
}

func TestStruct_ConfigurableFieldTags(t *testing.T) {
	schema := `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "someSTRING", "type": "string" }
  ]
}`

	tests := []struct {
		tagStyle    gen.TagStyle
		expectedTag string
	}{
		{tagStyle: gen.Camel, expectedTag: "json:\"someString\""},
		{tagStyle: gen.Snake, expectedTag: "json:\"some_string\""},
		{tagStyle: gen.Kebab, expectedTag: "json:\"some-string\""},
		{tagStyle: gen.UpperCamel, expectedTag: "json:\"SomeString\""},
		{tagStyle: gen.Original, expectedTag: "json:\"someSTRING\""},
		{tagStyle: gen.TagStyle(""), expectedTag: "json:\"someSTRING\""},
	}

	for _, test := range tests {
		test := test
		t.Run(string(test.tagStyle), func(t *testing.T) {
			gc := gen.Config{
				PackageName: "Something",
				Tags: map[string]gen.TagStyle{
					"json": test.tagStyle,
				},
			}
			_, lines := generate(t, schema, gc)

			for _, expected := range []string{
				"package something",
				"type Test struct {",
				"SomeString string `avro:\"someSTRING\" " + test.expectedTag + "`",
				"}",
			} {
				assert.Contains(t, lines, expected)
			}
		})
	}
}

func TestStruct_GenFromRecordSchema(t *testing.T) {
	schema, err := os.ReadFile("testdata/golden.avsc")
	require.NoError(t, err)

	gc := gen.Config{PackageName: "Something"}
	file, _ := generate(t, string(schema), gc)

	if *update {
		err = os.WriteFile("testdata/golden.go", file, 0600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden.go")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(file))
}

func TestStruct_GenFromRecordSchemaWithFullName(t *testing.T) {
	schema, err := os.ReadFile("testdata/golden.avsc")
	require.NoError(t, err)

	gc := gen.Config{PackageName: "Something", FullName: true}
	file, _ := generate(t, string(schema), gc)

	if *update {
		err = os.WriteFile("testdata/golden_fullname.go", file, 0600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_fullname.go")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(file))
}

func TestStruct_GenFromRecordSchemaWithEncoders(t *testing.T) {
	schema, err := os.ReadFile("testdata/golden.avsc")
	require.NoError(t, err)

	gc := gen.Config{PackageName: "Something", Encoders: true}
	file, _ := generate(t, string(schema), gc)

	if *update {
		err = os.WriteFile("testdata/golden_encoders.go", file, 0600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_encoders.go")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(file))
}

func TestGenerator(t *testing.T) {
	unionSchema, err := avro.ParseFiles("testdata/uniontype.avsc")
	require.NoError(t, err)

	mainSchema, err := avro.ParseFiles("testdata/main.avsc")
	require.NoError(t, err)

	g := gen.NewGenerator("something", map[string]gen.TagStyle{})
	g.Parse(unionSchema)
	g.Parse(mainSchema)

	var buf bytes.Buffer
	err = g.Write(&buf)
	require.NoError(t, err)

	formatted, err := format.Source(buf.Bytes())
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_multiple.go", formatted, 0600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_multiple.go")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(formatted))
}

// generate is a utility to run the generation and return the result as a tuple
func generate(t *testing.T, schema string, gc gen.Config) ([]byte, []string) {
	t.Helper()

	buf := &bytes.Buffer{}
	err := gen.Struct(schema, buf, gc)
	require.NoError(t, err)

	b := make([]byte, buf.Len())
	copy(b, buf.Bytes())

	return buf.Bytes(), removeSpaceAndEmptyLines(b)
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
	lines := strings.TrimSpace(string(lineBytes))
	return strings.Join(regexp.MustCompile("\\s+|\\t+").Split(lines, -1), " ")
}
