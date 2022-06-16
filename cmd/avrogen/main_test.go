package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAvroGenRequiredFlags(t *testing.T) {
	t.Run("package is required", func(t *testing.T) {
		_, err := runWithArgs(rawOpts{
			Package: "", // fails here
			OutFile: "out.go",
			Schema:  "in.avsc",
			Tags:    "json:camel",
		})
		require.Error(t, err)
	})
	t.Run("out file is required", func(t *testing.T) {
		_, err := runWithArgs(rawOpts{
			Package: "something",
			OutFile: "", // fails here
			Schema:  "in.avsc",
			Tags:    "json:camel",
		})
		require.Error(t, err)
	})
	t.Run("schema is required", func(t *testing.T) {
		_, err := runWithArgs(rawOpts{
			Package: "something",
			OutFile: "out.go",
			Schema:  "", // fails here
			Tags:    "json:camel",
		})
		require.Error(t, err)
	})
}

func TestAvroGenOptionalFields(t *testing.T) {
	t.Run("package is required", func(t *testing.T) {
		_, err := runWithArgs(rawOpts{
			Package: "something",
			OutFile: "out.go",
			Schema: `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`,
			Tags: "", // its fine, tags are optional
		})
		require.NoError(t, err)
	})
}

func TestAvroGenTagsParsingInvalid(t *testing.T) {
	for _, tc := range []struct {
		desc string
		tags string
	}{
		{desc: "gibberish", tags: "asdklasd"},
		{desc: "trailing comma", tags: "json:snake,"},
		{desc: "close enough, but unknown style", tags: "json:camel."},
		{desc: "unknown style", tags: "json:uber"},
		{desc: "missing tag name", tags: ":snake"},
		{desc: "missing style", tags: "json:snake,yaml:"},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := runWithArgs(rawOpts{
				Package: "something",
				OutFile: "out.go",
				Schema: `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`,
				Tags: tc.tags,
			})

			require.Error(t, err)
			for _, expectedWord := range []string{"tags", "colon", "comma", "separated"} {
				assert.Contains(t, err.Error(), expectedWord)
			}
		})
	}
}

func TestAvroGenTagsParsingValid(t *testing.T) {
	for _, tc := range []struct {
		desc string
		tags string
	}{
		{desc: "snake case", tags: "json:snake"},
		{desc: "camel case", tags: "json:camel"},
		{desc: "upper camel case", tags: "json:upper-camel"},
		{desc: "kebab case", tags: "json:kebab"},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := runWithArgs(rawOpts{
				Package: "something",
				OutFile: "out.go",
				Schema: `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`,
				Tags: tc.tags,
			})

			require.NoError(t, err)
		})
	}
}

func TestAvroGen(t *testing.T) {
	cr, err := runWithArgs(rawOpts{
		Package: "something",
		OutFile: "out.go",
		Schema: `{
  "type": "record",
  "name": "test",
  "fields": [
    { "name": "someString", "type": "string" }
  ]
}`,
		Tags: "json:snake",
	})

	require.NoError(t, err)
	cr.HasLineWith(t, []string{"package", "something"})
	cr.HasLineWith(t, []string{"type", "Test", "struct", "{"})
	cr.HasLineWith(t, []string{"SomeString", "string", "avro:\"someString\"", "json:\"some_string\""})
	cr.HasLineWith(t, []string{"}"})
}

func runWithArgs(args rawOpts) (*cmdResult, error) {
	outBuf := &bytes.Buffer{}
	err := execute(args.Schema, outBuf, args)
	return &cmdResult{outStream: outBuf}, err
}

type cmdResult struct {
	outStream io.Reader
	outText   string
}

func (cr *cmdResult) out(t *testing.T) string {
	if cr.outText == "" {
		all, err := io.ReadAll(cr.outStream)
		require.NoError(t, err)
		cr.outText = string(all)
	}
	return cr.outText
}

func (cr *cmdResult) HasLineWith(t *testing.T, words []string) {
	for _, line := range strings.Split(cr.out(t), "\n") {
		for j, w := range words {
			if !strings.Contains(line, w) {
				break
			}

			if j == len(words)-1 {
				return // all words are contained in the line
			}
		}
	}
	t.Fatalf("did not find any line with any of these words: %s. Output was: %s", strings.Join(words, ","), cr.out(t))
}
