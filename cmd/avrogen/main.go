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
	os.Exit(realMain(os.Args))
}

func realMain(args []string) int {
	var ro rawOpts
	flgs := flag.NewFlagSet("avrogen", flag.ExitOnError)
	flgs.StringVar(&ro.Package, "pkg", "", "-pkg <package-name-on-the-generated-file>")
	flgs.StringVar(&ro.OutFile, "o", "", "-o <file-name>")
	flgs.StringVar(&ro.Tags, "tags", "", "-tags <tag-name>:{snake|camel|upper-camel|kebab}>[,...]")
	if err := flgs.Parse(args[1:]); err != nil {
		return 1
	}

	trailing := flgs.Args()
	if len(trailing) > 0 {
		ro.Schema = trailing[len(trailing)-1]
	}

	outFile, err := os.OpenFile(ro.OutFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Could not open output file for writing")
		return 2
	}
	defer func() { _ = outFile.Close() }()

	schemaFile, err := os.ReadFile(ro.Schema)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Could not open schema file")
		return 3
	}

	schema, err := io.ReadAll(bytes.NewReader(schemaFile))
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Could not read the schema file")
		return 4
	}

	if err = execute(string(schema), outFile, ro); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
		return 5
	}
	return 0
}

func execute(schema string, out io.Writer, rawOpts rawOpts) error {
	for _, x := range []struct {
		value   string
		flagVal string
	}{
		{rawOpts.OutFile, "-o"},
		{rawOpts.Package, "-pkg"},
		{rawOpts.Schema, "-schema"},
	} {
		if x.value == "" {
			return errors.New(x.flagVal + " is required")
		}
	}

	parsedTags, err := parseTags(rawOpts.Tags)
	if err != nil {
		return err
	}

	err = g.Struct(schema, out, g.Config{
		PackageName: rawOpts.Package,
		Tags:        parsedTags,
	})
	if err != nil {
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
			colonCount++
		}

		if c == ',' {
			commaCount++
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
