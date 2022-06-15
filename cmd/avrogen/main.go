package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	g "github.com/hamba/avro/gen"
)

type rawOpts struct {
	Package string
	OutFile string
	Schema  string
	Tags    string
	Help    bool
}

func main() {
	var ro rawOpts
	flag.StringVar(&ro.Package, "pkg", "", "-pkg <file-name>")
	flag.StringVar(&ro.OutFile, "o", "", "-o <file-name>")
	flag.StringVar(&ro.Schema, "schema", "", "-schema <file-name>")
	flag.BoolVar(&ro.Help, "h", false, "-h")
	flag.Parse()

	outFile, err := os.OpenFile(ro.OutFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0x644)
	if err != nil {
		_, _ = os.Stderr.Write([]byte("Could not open output file for writing"))
		os.Exit(1)
	}
	defer func() { _ = outFile.Close() }()

	schemaFile, err := os.ReadFile(ro.Schema)
	if err != nil {
		_, _ = os.Stderr.Write([]byte("Could not open schema file"))
		os.Exit(2)
	}

	schema, err := io.ReadAll(bytes.NewReader(schemaFile))
	if err != nil {
		_, _ = os.Stderr.Write([]byte("Could not read the schema file"))
		os.Exit(3)
	}

	if err := execute(string(schema), outFile, os.Stderr, ro); err != nil {
		os.Exit(4) // already wrote the error
	}
}

func execute(schema string, out io.Writer, er io.Writer, rawOpts rawOpts) error {
	if rawOpts.Help {
		if err := help(out); err != nil {
			_, _ = er.Write([]byte(fmt.Errorf(`failed to run -help: %w`, err).Error()))
			return err
		}
		return nil
	}

	for _, x := range []struct {
		value   string
		flagVal string
	}{
		{rawOpts.OutFile, "-o"},
		{rawOpts.Package, "-pkg"},
		{rawOpts.Schema, "-schema"},
	} {
		if x.value == "" {
			err := errors.New(x.flagVal + " is required")
			_, _ = er.Write([]byte(err.Error()))
			return err
		}
	}

	parsedTags, err := parseTags(rawOpts.Tags)
	if err != nil {
		_, _ = er.Write([]byte(err.Error()))
		return err
	}

	err = g.Struct(schema, out, g.Config{
		PackageName: rawOpts.Package,
		Tags:        parsedTags,
	})
	if err != nil {
		_, _ = er.Write([]byte(err.Error()))
		return err
	}

	return nil
}

const invalidTagsMsg = "tags should be a comma separated list of key value pairs separated by colon (':'). Valid styles: camel, upper-camel, snake, kebab"

func parseTags(tags string) (map[string]g.TagStyle, error) {
	result := make(map[string]g.TagStyle)
	if tags == "" {
		return result, nil
	}

	commaCount, colonCount := 0, 0
	for _, c := range tags {
		if c == ':' {
			colonCount += 1
		}

		if c == ',' {
			commaCount += 1
		}
	}

	if colonCount == 0 || colonCount != commaCount+1 {
		return nil, errors.New(invalidTagsMsg)
	}

	for _, kvp := range strings.Split(tags, ",") {
		kv := strings.Split(kvp, ":")
		if kv[0] == "" || kv[1] == "" {
			return nil, errors.New(invalidTagsMsg)
		}
		style := g.TagStyle("")
		switch strings.ToLower(kv[1]) {
		case string(g.UpperCamel):
			style = g.UpperCamel
		case string(g.Camel):
			style = g.Camel
		case string(g.Kebab):
			style = g.Kebab
		case string(g.Snake):
			style = g.Snake
		}

		if style == "" {
			return nil, errors.New(invalidTagsMsg)
		}
		result[kv[0]] = style
	}
	return result, nil
}

func help(out io.Writer) error {
	_, err := out.Write([]byte(`avrogen - Generate Golang structs from avro schemas

Run it with: avrogen [options]

Options:
  -pkg   , REQUIRED - the file to read from. Defaults to stdin if missing
  -o     , REQUIRED - the name of the output file to which write the generated structs
  -tags  , OPT - a list of key-value pairs: <tag-name>:casing[,...]. example: -tags json:camel,yaml:snake
         , available casings are: camel, snake, upper-camel, kebab
  -schema, REQUIRED - the schema file from which to generate the structs

Example:
  $ avrogen -pkg somepackage -tags json:camel,yaml:snake -o my/file.go -schema schema.avsc`))
	return err
}
