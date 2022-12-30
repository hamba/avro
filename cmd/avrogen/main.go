package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/gen"
)

type config struct {
	Pkg  string
	Out  string
	Tags string
}

func main() {
	os.Exit(realMain(os.Args, os.Stderr))
}

func realMain(args []string, out io.Writer) int {
	var cfg config
	flgs := flag.NewFlagSet("avrogen", flag.ExitOnError)
	flgs.SetOutput(out)
	flgs.StringVar(&cfg.Pkg, "pkg", "", "The package name of the output file.")
	flgs.StringVar(&cfg.Out, "o", "", "The output file path.")
	flgs.StringVar(&cfg.Tags, "tags", "", "The additional field tags <tag-name>:{snake|camel|upper-camel|kebab}>[,...]")
	flgs.Usage = func() {
		_, _ = fmt.Fprintln(out, "Usage: avrogen [options] schemas")
		_, _ = fmt.Fprintln(out, "Options:")
		flgs.PrintDefaults()
	}
	if err := flgs.Parse(args[1:]); err != nil {
		return 1
	}

	if err := validateOpts(flgs.NArg(), cfg); err != nil {
		_, _ = fmt.Fprintln(out, "Error: "+err.Error())
		return 1
	}
	tags, err := parseTags(cfg.Tags)
	if err != nil {
		_, _ = fmt.Fprintln(out, "Error: "+err.Error())
		return 1
	}

	g := gen.NewGenerator(cfg.Pkg, tags)
	for _, file := range flgs.Args() {
		schema, err := avro.ParseFiles(filepath.Clean(file))
		if err != nil {
			_, _ = fmt.Fprintf(out, "Error: %v\n", err)
			return 2
		}
		g.Parse(schema)
	}

	var buf bytes.Buffer
	if err = g.Write(&buf); err != nil {
		_, _ = fmt.Fprintf(out, "Error: could not generate code: %v\n", err)
		return 3
	}
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		_, _ = fmt.Fprintf(out, "Error: could format code: %v\n", err)
		return 3
	}
	if err = os.WriteFile(cfg.Out, formatted, 0o600); err != nil {
		_, _ = fmt.Fprintf(out, "Error: could write file: %v\n", err)
		return 4
	}
	return 0
}

func validateOpts(nargs int, cfg config) error {
	if nargs < 1 {
		return fmt.Errorf("at least one schema is required")
	}

	if cfg.Pkg == "" {
		return fmt.Errorf("a package is required")
	}

	if cfg.Out == "" {
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
