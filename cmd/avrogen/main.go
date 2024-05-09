package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/gen"
	"golang.org/x/tools/imports"
)

type config struct {
	TemplateFileName string

	Pkg         string
	Out         string
	Tags        string
	FullName    bool
	Encoders    bool
	StrictTypes bool
	Initialisms string
}

func main() {
	os.Exit(realMain(os.Args, os.Stdout, os.Stderr))
}

func realMain(args []string, stdout, stderr io.Writer) int {
	var cfg config
	flgs := flag.NewFlagSet("avrogen", flag.ExitOnError)
	flgs.SetOutput(stderr)
	flgs.StringVar(&cfg.Pkg, "pkg", "", "The package name of the output file.")
	flgs.StringVar(&cfg.Out, "o", "", "The output file path to write to instead of stdout.")
	flgs.StringVar(&cfg.Tags, "tags", "", "The additional field tags <tag-name>:{snake|camel|upper-camel|kebab}>[,...]")
	flgs.BoolVar(&cfg.FullName, "fullname", false, "Use the full name of the Record schema to create the struct name.")
	flgs.BoolVar(&cfg.Encoders, "encoders", false, "Generate encoders for the structs.")
	flgs.BoolVar(&cfg.StrictTypes, "strict-types", false, "Use strict type sizes (e.g. int32) during generation.")
	flgs.StringVar(&cfg.Initialisms, "initialisms", "", "Custom initialisms <VAL>[,...] for struct and field names.")
	flgs.StringVar(&cfg.TemplateFileName, "template-filename", "", "Override output template with one loaded from file.")
	flgs.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: avrogen [options] schemas")
		_, _ = fmt.Fprintln(stderr, "Options:")
		flgs.PrintDefaults()
	}
	if err := flgs.Parse(args[1:]); err != nil {
		return 1
	}

	if err := validateOpts(flgs.NArg(), cfg); err != nil {
		_, _ = fmt.Fprintln(stderr, "Error: "+err.Error())
		return 1
	}

	tags, err := parseTags(cfg.Tags)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "Error: "+err.Error())
		return 1
	}

	initialisms, err := parseInitialisms(cfg.Initialisms)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "Error: "+err.Error())
		return 1
	}

	template, err := loadTemplate(cfg.TemplateFileName)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "Error: "+err.Error())
		return 1
	}

	opts := []gen.OptsFunc{
		gen.WithFullName(cfg.FullName),
		gen.WithEncoders(cfg.Encoders),
		gen.WithInitialisms(initialisms),
		gen.WithTemplate(string(template)),
		gen.WithStrictTypes(cfg.StrictTypes),
	}
	g := gen.NewGenerator(cfg.Pkg, tags, opts...)
	for _, file := range flgs.Args() {
		schema, err := avro.ParseFiles(filepath.Clean(file))
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
			return 2
		}
		g.Parse(schema)
	}

	var buf bytes.Buffer
	if err = g.Write(&buf); err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: could not generate code: %v\n", err)
		return 3
	}
	formatted, err := imports.Process("", buf.Bytes(), nil)
	if err != nil {
		_ = writeOut(cfg.Out, stdout, buf.Bytes())
		_, _ = fmt.Fprintf(stderr, "Error: generated code could not be formatted: %v\n", err)
		return 3
	}

	err = writeOut(cfg.Out, stdout, formatted)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "Error: %v\n", err)
		return 4
	}
	return 0
}

func writeOut(filename string, stdout io.Writer, bytes []byte) error {
	writer := stdout
	if filename != "" {
		file, err := os.Create(filepath.Clean(filename))
		if err != nil {
			return fmt.Errorf("could not create output file: %w", err)
		}
		defer func() { _ = file.Close() }()

		writer = file
	}

	if _, err := writer.Write(bytes); err != nil {
		return fmt.Errorf("could not write code: %w", err)
	}
	return nil
}

func validateOpts(nargs int, cfg config) error {
	if nargs < 1 {
		return errors.New("at least one schema is required")
	}

	if cfg.Pkg == "" {
		return errors.New("a package is required")
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
		case string(gen.Original):
			style = gen.Original
		default:
			return nil, fmt.Errorf("style %q is invalid in %q", parts[1], tag)
		}
		result[parts[0]] = style
	}
	return result, nil
}

func parseInitialisms(raw string) ([]string, error) {
	if raw == "" {
		return []string{}, nil
	}

	result := []string{}
	for _, initialism := range strings.Split(raw, ",") {
		if initialism != strings.ToUpper(initialism) {
			return nil, fmt.Errorf("initialism %q must be fully in upper case", initialism)
		}
		result = append(result, initialism)
	}

	return result, nil
}

func loadTemplate(templateFileName string) ([]byte, error) {
	if templateFileName == "" {
		return nil, nil
	}
	return os.ReadFile(filepath.Clean(templateFileName))
}
