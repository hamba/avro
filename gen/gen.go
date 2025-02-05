// Package gen allows generating Go structs from avro schemas.
package gen

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"text/template"
	"unicode/utf8"

	"github.com/ettle/strcase"
	"github.com/hamba/avro/v2"
	"golang.org/x/tools/imports"
)

// Config configures the code generation.
type Config struct {
	PackageName  string
	Tags         map[string]TagStyle
	FullName     bool
	Encoders     bool
	FullSchema   bool
	StrictTypes  bool
	Initialisms  []string
	LogicalTypes []LogicalType
	Metadata     *SchemaMetadata
}

// SchemaMetadata contains schema registry metadata
type SchemaMetadata struct {
	Subject string
	Version int
}

// TagStyle defines the styling for a tag.
type TagStyle string

const (
	// Original is a style like whAtEVer_IS_InthEInpuT.
	Original TagStyle = "original"
	// Snake is a style like im_written_in_snake_case.
	Snake TagStyle = "snake"
	// Camel is a style like imWrittenInCamelCase.
	Camel TagStyle = "camel"
	// Kebab is a style like im-written-in-kebab-case.
	Kebab TagStyle = "kebab"
	// UpperCamel is a style like ImWrittenInUpperCamel.
	UpperCamel TagStyle = "upper-camel"
)

//go:embed output_template.tmpl
var outputTemplate string

var (
	primitiveMappings = map[avro.Type]string{
		"string":  "string",
		"bytes":   "[]byte",
		"int":     "int",
		"long":    "int64",
		"float":   "float32",
		"double":  "float64",
		"boolean": "bool",
	}
	strictTypeMappings = map[string]string{
		"int": "int32",
	}
)

// Struct generates Go structs based on the schema and writes them to w.
func Struct(s string, w io.Writer, cfg Config) error {
	schema, err := avro.Parse(s)
	if err != nil {
		return err
	}
	return StructFromSchema(schema, w, cfg)
}

// StructFromSchema generates Go structs based on the schema and writes them to w.
func StructFromSchema(schema avro.Schema, w io.Writer, cfg Config) error {
	rec, ok := schema.(*avro.RecordSchema)
	if !ok {
		return errors.New("can only generate Go code from Record Schemas")
	}

	opts := []OptsFunc{
		WithFullName(cfg.FullName),
		WithEncoders(cfg.Encoders),
		WithInitialisms(cfg.Initialisms),
		WithStrictTypes(cfg.StrictTypes),
		WithFullSchema(cfg.FullSchema),
		WithMetadata(cfg.Metadata),
	}
	for _, opt := range cfg.LogicalTypes {
		opts = append(opts, WithLogicalType(opt))
	}
	g := NewGenerator(strcase.ToSnake(cfg.PackageName), cfg.Tags, opts...)
	g.Parse(rec)

	buf := &bytes.Buffer{}
	if err := g.Write(buf); err != nil {
		return err
	}

	formatted, err := imports.Process("", buf.Bytes(), nil)
	if err != nil {
		_, _ = w.Write(buf.Bytes())
		return fmt.Errorf("generated code could not be formatted: %w", err)
	}

	_, err = w.Write(formatted)
	return err
}

// OptsFunc is a function that configures a generator.
type OptsFunc func(*Generator)

// WithFullName configures the generator to use the full name of a record
// when creating the struct name.
func WithFullName(b bool) OptsFunc {
	return func(g *Generator) {
		g.fullName = b
	}
}

// WithEncoders configures the generator to generate schema and encoders on
// all objects.
func WithEncoders(b bool) OptsFunc {
	return func(g *Generator) {
		g.encoders = b
		if b {
			g.thirdPartyImports = append(g.thirdPartyImports, "github.com/hamba/avro/v2")
		}
	}
}

// WithInitialisms configures the generator to use additional custom initialisms
// when styling struct and field names.
func WithInitialisms(ss []string) OptsFunc {
	return func(g *Generator) {
		g.initialisms = ss
	}
}

// WithTemplate configures the generator to use a custom template provided by the user.
func WithTemplate(template string) OptsFunc {
	return func(g *Generator) {
		if template == "" {
			return
		}
		g.template = template
	}
}

// WithStrictTypes configures the generator to use strict type sizes.
func WithStrictTypes(b bool) OptsFunc {
	return func(g *Generator) {
		g.strictTypes = b
	}
}

// WithPackageDoc configures the generator to output the given text as a package doc comment.
func WithPackageDoc(text string) OptsFunc {
	return func(g *Generator) {
		g.pkgdoc = ensureTrailingPeriod(text)
	}
}

// WithFullSchema configures the generator to store the full schema within the generation context.
func WithFullSchema(b bool) OptsFunc {
	return func(g *Generator) {
		g.fullSchema = b
	}
}

// WithMetadata configures the generator to store the metadata within the generation context.
func WithMetadata(m *SchemaMetadata) OptsFunc {
	return func(g *Generator) {
		g.metadata = m
	}
}

// LogicalType used when the name of the "LogicalType" field in the Avro schema matches the Name attribute.
type LogicalType struct {
	// Name of the LogicalType
	Name string
	// Typ returned, has to be a valid Go type
	Typ string
	// Import added as import (if not empty)
	Import string
	// ThirdPartyImport added as import (if not empty)
	ThirdPartyImport string
}

// WithLogicalType registers a LogicalType which takes precedence over the default logical types
// defined by this package.
func WithLogicalType(logicalType LogicalType) OptsFunc {
	return func(g *Generator) {
		if g.logicalTypes == nil {
			g.logicalTypes = map[avro.LogicalType]LogicalType{}
		}
		g.logicalTypes[avro.LogicalType(logicalType.Name)] = logicalType
	}
}

func ensureTrailingPeriod(text string) string {
	if text == "" {
		return text
	}
	if last, _ := utf8.DecodeLastRuneInString(text); last == '.' {
		return text
	}
	return text + "."
}

// Generator generates Go structs from schemas.
type Generator struct {
	template     string
	pkg          string
	pkgdoc       string
	tags         map[string]TagStyle
	fullName     bool
	encoders     bool
	fullSchema   bool
	strictTypes  bool
	initialisms  []string
	logicalTypes map[avro.LogicalType]LogicalType
	metadata     *SchemaMetadata

	imports           []string
	thirdPartyImports []string
	typedefs          []typedef

	nameCaser *strcase.Caser
}

// NewGenerator returns a generator.
func NewGenerator(pkg string, tags map[string]TagStyle, opts ...OptsFunc) *Generator {
	clonedTags := maps.Clone(tags)
	delete(clonedTags, "avro")

	g := &Generator{
		template: outputTemplate,
		pkg:      pkg,
		tags:     clonedTags,
	}

	for _, opt := range opts {
		opt(g)
	}

	initialisms := map[string]bool{}
	for _, v := range g.initialisms {
		initialisms[v] = true
	}

	g.nameCaser = strcase.NewCaser(
		true, // use standard Golint's initialisms
		initialisms,
		nil, // use default word split function
	)

	return g
}

// Reset reset the generator.
func (g *Generator) Reset() {
	g.imports = g.imports[:0]
	g.thirdPartyImports = g.thirdPartyImports[:0]
	g.typedefs = g.typedefs[:0]
}

// Parse parses an avro schema into Go types.
func (g *Generator) Parse(schema avro.Schema) {
	_ = g.generate(schema)
}

func (g *Generator) generate(schema avro.Schema) string {
	switch s := schema.(type) {
	case *avro.RefSchema:
		return g.resolveRefSchema(s)
	case *avro.RecordSchema:
		return g.resolveRecordSchema(s)
	case *avro.PrimitiveSchema:
		typ := primitiveMappings[s.Type()]
		if ls := s.Logical(); ls != nil {
			typ = g.resolveLogicalSchema(ls.Type())
		}
		if g.strictTypes {
			if newTyp, ok := strictTypeMappings[typ]; ok {
				typ = newTyp
			}
		}
		return typ
	case *avro.ArraySchema:
		return "[]" + g.generate(s.Items())
	case *avro.EnumSchema:
		return "string"
	case *avro.FixedSchema:
		typ := fmt.Sprintf("[%d]byte", s.Size())
		if ls := s.Logical(); ls != nil {
			typ = g.resolveLogicalSchema(ls.Type())
		}
		return typ
	case *avro.MapSchema:
		return "map[string]" + g.generate(s.Values())
	case *avro.UnionSchema:
		return g.resolveUnionTypes(s)
	default:
		return ""
	}
}

func (g *Generator) resolveTypeName(s avro.NamedSchema) string {
	if g.fullName {
		return g.nameCaser.ToPascal(s.FullName())
	}
	return g.nameCaser.ToPascal(s.Name())
}

func (g *Generator) resolveRecordSchema(schema *avro.RecordSchema) string {
	fields := make([]field, len(schema.Fields()))
	for i, f := range schema.Fields() {
		typ := g.generate(f.Type())
		fields[i] = g.newField(g.nameCaser.ToPascal(f.Name()), typ, f.Doc(), f.Name(), f.Props())
	}

	typeName := g.resolveTypeName(schema)
	if !g.hasTypeDef(typeName) {
		g.typedefs = append(g.typedefs, newType(typeName, schema.Doc(), fields, g.rawSchema(schema), schema.Props()))
	}
	return typeName
}

func (g *Generator) rawSchema(schema *avro.RecordSchema) string {
	if g.fullSchema {
		schemaJSON, err := schema.MarshalJSON()
		if err != nil {
			panic(fmt.Errorf("failed to marshal raw schema for '%s': %w", schema.FullName(), err))
		}
		return string(schemaJSON)
	}
	return schema.String()
}

func (g *Generator) hasTypeDef(name string) bool {
	for _, def := range g.typedefs {
		if def.Name != name {
			continue
		}
		return true
	}
	return false
}

func (g *Generator) resolveRefSchema(s *avro.RefSchema) string {
	if sx, ok := s.Schema().(*avro.RecordSchema); ok {
		return g.resolveTypeName(sx)
	}
	return g.generate(s.Schema())
}

func (g *Generator) resolveUnionTypes(s *avro.UnionSchema) string {
	types := make([]string, 0, len(s.Types()))
	for _, elem := range s.Types() {
		if _, ok := elem.(*avro.NullSchema); ok {
			continue
		}
		types = append(types, g.generate(elem))
	}
	if s.Nullable() {
		return "*" + types[0]
	}
	return "any"
}

func (g *Generator) resolveLogicalSchema(logicalType avro.LogicalType) string {
	if g.logicalTypes != nil {
		if typ, ok := g.logicalTypes[logicalType]; ok {
			if val := typ.Import; val != "" {
				g.addImport(val)
			}
			if val := typ.ThirdPartyImport; val != "" {
				g.addThirdPartyImport(val)
			}

			return typ.Typ
		}
	}

	var typ string
	switch logicalType {
	case "date", "timestamp-millis", "timestamp-micros":
		typ = "time.Time"
	case "time-millis", "time-micros":
		typ = "time.Duration"
	case "decimal":
		typ = "*big.Rat"
	case "duration":
		typ = "avro.LogicalDuration"
	case "uuid":
		typ = "string"
	}
	if strings.Contains(typ, "time") {
		g.addImport("time")
	}
	if strings.Contains(typ, "big") {
		g.addImport("math/big")
	}
	if strings.Contains(typ, "avro") {
		g.addThirdPartyImport("github.com/hamba/avro/v2")
	}
	return typ
}

func (g *Generator) newField(name, typ, doc, avroFieldName string, props map[string]any) field {
	return field{
		Name:          name,
		Type:          typ,
		AvroFieldName: avroFieldName,
		Doc:           ensureTrailingPeriod(doc),
		Tags:          g.tags,
		Props:         props,
	}
}

func (g *Generator) addImport(pkg string) {
	for _, p := range g.imports {
		if p == pkg {
			return
		}
	}
	g.imports = append(g.imports, pkg)
}

func (g *Generator) addThirdPartyImport(pkg string) {
	for _, p := range g.thirdPartyImports {
		if p == pkg {
			return
		}
	}
	g.thirdPartyImports = append(g.thirdPartyImports, pkg)
}

// Write writes Go code from the parsed schemas.
func (g *Generator) Write(w io.Writer) error {
	parsed, err := template.New("out").
		Funcs(template.FuncMap{
			"kebab":      strcase.ToKebab,
			"upperCamel": strcase.ToPascal,
			"camel":      strcase.ToCamel,
			"snake":      strcase.ToSnake,
			"replace":    strings.Replace,
		}).
		Parse(g.template)
	if err != nil {
		return err
	}

	data := struct {
		WithEncoders      bool
		PackageName       string
		PackageDoc        string
		Imports           []string
		ThirdPartyImports []string
		Typedefs          []typedef
		Metadata          *SchemaMetadata
	}{
		WithEncoders: g.encoders,
		PackageName:  g.pkg,
		PackageDoc:   g.pkgdoc,
		Imports:      append(g.imports, g.thirdPartyImports...),
		Typedefs:     g.typedefs,
		Metadata:     g.metadata,
	}
	return parsed.Execute(w, data)
}

type typedef struct {
	Name   string
	Doc    string
	Fields []field
	Schema string
	Props  map[string]any
}

func newType(name, doc string, fields []field, schema string, props map[string]any) typedef {
	return typedef{
		Name:   name,
		Doc:    ensureTrailingPeriod(doc),
		Fields: fields,
		Schema: schema,
		Props:  props,
	}
}

type field struct {
	Name          string
	Type          string
	Doc           string
	AvroFieldName string
	Tags          map[string]TagStyle
	Props         map[string]any
}
