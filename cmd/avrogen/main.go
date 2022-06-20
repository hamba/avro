package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/hamba/avro/v2/gen"
)

type rawOpts struct {
	Pkg  string
	Out  string
	Tags string
}

func main() {
	os.Exit(realMain(os.Args, os.Stderr))
}

func realMain(args []string, out io.Writer) int {
	var ro rawOpts
	flgs := flag.NewFlagSet("avrogen", flag.ExitOnError)
	flgs.SetOutput(out)
	flgs.StringVar(&ro.Pkg, "pkg", "", "The package name of the output file.")
	flgs.StringVar(&ro.Out, "o", "", "The output file path.")
	flgs.StringVar(&ro.Tags, "tags", "", "The additional field tags <tag-name>:{snake|camel|upper-camel|kebab}>[,...]")
	flgs.Usage = func() {
		_, _ = fmt.Fprintln(out, "Usage: avrogen [options] schema")
		_, _ = fmt.Fprintln(out, "Options:")
		flgs.PrintDefaults()
	}

	if err := flgs.Parse(args[1:]); err != nil {
		return 1
	}
	if err := validateOpts(flgs.Args(), ro); err != nil {
		_, _ = fmt.Fprintln(out, "Error: "+err.Error())

		return 1
	}
	tags, err := parseTags(ro.Tags)
	if err != nil {
		_, _ = fmt.Fprintln(out, "Error: "+err.Error())
		return 1
	}

	schema, err := readSchema(flgs.Args()[0])
	if err != nil {
		_, _ = fmt.Fprintf(out, "Error: %v\n", err)
		return 2
	}

	var buf bytes.Buffer
	cfg := gen.Config{PackageName: ro.Pkg, Tags: tags}
	if err = gen.Struct(schema, &buf, cfg); err != nil {
		_, _ = fmt.Fprintln(out, err.Error())
		return 3
	}

	if err = os.WriteFile(ro.Out, buf.Bytes(), 0o600); err != nil {
		_, _ = fmt.Fprintf(out, "Error: could write file: %v\n", err)
		return 4
	}
	return 0
}

func validateOpts(args []string, ro rawOpts) error {
	if len(args) != 1 {
		return fmt.Errorf("schema is required")
	}

	if ro.Pkg == "" {
		return fmt.Errorf("a package is required")
	}

	if ro.Out == "" {
		return fmt.Errorf("an output file is reqired")
	}

	return nil
}

func parseTags(raw string) (map[string]gen.TagStyle, error) {
	if raw == "" {
		return map[string]gen.TagStyle{}, nil
	}

	result := map[string]gen.TagStyle{}
	for _, tag := range strings.Split(raw, ",") {
		parts := strings.Split(tag, ":")
		switch {
		case len(parts) != 2:
			return nil, fmt.Errorf("%q is not a valid tag, should be in the formet \"tag:style\"", tag)
		case parts[0] == "":
			return nil, fmt.Errorf("tag name is required in %q", tag)
		}

		var style gen.TagStyle
		switch strings.ToLower(parts[1]) {
		case string(gen.UpperCamel):
			style = gen.UpperCamel
		case string(gen.Camel):
			style = gen.Camel
		case string(gen.Kebab):
			style = gen.Kebab
		case string(gen.Snake):
			style = gen.Snake
		default:
			return nil, fmt.Errorf("style %q is invalid in %q", parts[1], tag)
		}

		result[parts[0]] = style
	}
	return result, nil
}

func readSchema(path string) (string, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("could not open schema file: %w", err)
	}
	return string(b), nil
}
