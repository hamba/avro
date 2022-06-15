package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAvroGenHelp(t *testing.T) {
	t.Run("help is always given priority", func(t *testing.T) {
		cr := runWithArgs(rawOpts{
			Package: "something",
			OutFile: "out.go",
			Tags:    "json:camel",
			Help:    true,
		})
		cr.ThereIsNoError(t)
		cr.HasLineWith(t, []string{"Example"})
		cr.HasLineWith(t, []string{"-o"})
		cr.HasLineWith(t, []string{"-pkg"})
		cr.HasLineWith(t, []string{"-tags"})
		cr.HasLineWith(t, []string{"-schema"})
		cr.HasLineWith(t, []string{"$ avrogen"})
	})
	t.Run("help shows the options and an example", func(t *testing.T) {
		cr := runWithArgs(rawOpts{Help: true})
		cr.ThereIsNoError(t)
		cr.HasLineWith(t, []string{"Example"})
		cr.HasLineWith(t, []string{"-o"})
		cr.HasLineWith(t, []string{"-pkg"})
		cr.HasLineWith(t, []string{"-tags"})
		cr.HasLineWith(t, []string{"-schema"})
		cr.HasLineWith(t, []string{"$ avrogen"})
	})
}

func TestAvroGenRequiredFlags(t *testing.T) {
	t.Run("package is required", func(t *testing.T) {
		cr := runWithArgs(rawOpts{
			Package: "", // fails here
			OutFile: "out.go",
			Schema:  "in.avsc",
			Tags:    "json:camel",
		})
		cr.ThereIsAnError(t)
	})
	t.Run("out file is required", func(t *testing.T) {
		cr := runWithArgs(rawOpts{
			Package: "something",
			OutFile: "", // fails here
			Schema:  "in.avsc",
			Tags:    "json:camel",
		})
		cr.ThereIsAnError(t)
	})
	t.Run("schema is required", func(t *testing.T) {
		cr := runWithArgs(rawOpts{
			Package: "something",
			OutFile: "out.go",
			Schema:  "", // fails here
			Tags:    "json:camel",
		})
		cr.ThereIsAnError(t)
	})
}

func TestAvroGenOptionalFields(t *testing.T) {
	t.Run("package is required", func(t *testing.T) {
		cr := runWithArgs(rawOpts{
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
		cr.ThereIsNoError(t)
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
			cr := runWithArgs(rawOpts{
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

			cr.ThereIsAnErrorWithWords(t, []string{"tags", "valid", "colon", "comma", "separated"})
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
			cr := runWithArgs(rawOpts{
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

			cr.ThereIsNoError(t)
		})
	}
}

func TestAvroGen(t *testing.T) {
	cr := runWithArgs(rawOpts{
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

	cr.ThereIsNoError(t)
	cr.HasLineWith(t, []string{"package", "something"})
	cr.HasLineWith(t, []string{"type", "Test", "struct", "{"})
	cr.HasLineWith(t, []string{"SomeString", "string", "avro:\"someString\"", "json:\"some_string\""})
	cr.HasLineWith(t, []string{"}"})
}

func runWithArgs(args rawOpts) *cmdResult {
	outBuf := &bytes.Buffer{}
	erBuf := &bytes.Buffer{}
	_ = execute(args.Schema, outBuf, erBuf, args)
	return &cmdResult{outStream: outBuf, errStream: erBuf}
}

type cmdResult struct {
	outStream io.Reader
	errStream io.Reader

	errText string
	outText string
}

func (cr *cmdResult) err(t *testing.T) string {
	if cr.errText == "" {
		all, err := io.ReadAll(cr.errStream)
		require.NoError(t, err)
		cr.errText = string(all)
	}
	return cr.errText
}

func (cr *cmdResult) out(t *testing.T) string {
	if cr.outText == "" {
		all, err := io.ReadAll(cr.outStream)
		require.NoError(t, err)
		cr.outText = string(all)
	}
	return cr.outText
}

func (cr *cmdResult) ThereIsNoError(t *testing.T) {
	require.Empty(t, cr.err(t))
}

func (cr *cmdResult) ThereIsAnError(t *testing.T) {
	require.NotEmpty(t, cr.err(t))
}

func (cr *cmdResult) ThereIsAnErrorWithWords(t *testing.T, words []string) {
	require.NotEmpty(t, cr.err(t))
	for _, w := range words {
		assert.Contains(t, strings.ToLower(cr.err(t)), strings.ToLower(w))
	}
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
