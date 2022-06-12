package gen

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"io"
	"strings"
	"text/template"

	"github.com/hamba/avro"
	"github.com/iancoleman/strcase"
	"golang.org/x/tools/go/ast/astutil"
)

type Conf struct {
	PackageName string
}

var primitiveMappings = map[avro.Type]string{
	"string":  "string",
	"bytes":   "[]byte",
	"int":     "int",
	"long":    "int64",
	"float":   "float32",
	"double":  "float64",
	"boolean": "bool",
}

func Struct(s string, dst io.Writer, gc Conf) error {
	schema, err := avro.Parse(s)
	if err != nil {
		return err
	}

	rSchema, ok := schema.(*avro.RecordSchema)
	if !ok {
		return errors.New("can only generate Go code from Record Schemas")
	}
	file := ast.File{
		Name: &ast.Ident{Name: strcase.ToSnake(gc.PackageName)},
	}

	_ = generateFrom(rSchema, &file)
	buf := &bytes.Buffer{}
	if err = writeFile(buf, &file); err != nil {
		return err
	}
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed formatting. %w", err)
	}

	_, err = dst.Write(formatted)
	return err
}

func generateFrom(schema avro.Schema, acc *ast.File) string {
	switch t := schema.(type) {
	case *avro.RecordSchema:
		typeName := strcase.ToCamel(t.Name())
		fields := make([]*ast.Field, len(t.Fields()))
		for i, f := range t.Fields() {
			fSchema := f.Type()
			fieldName := strcase.ToCamel(f.Name())
			typ := resolveType(fSchema, f.Prop("logicalType"), acc)
			tag := f.Name()
			fields[i] = newField(fieldName, typ, tag)
		}
		acc.Decls = append(acc.Decls, newType(typeName, fields))
		return typeName
	default:
		return resolveType(schema, nil, acc)
	}
}

func resolveType(fieldSchema avro.Schema, logicalType interface{}, acc *ast.File) string {
	var typ string
	switch s := fieldSchema.(type) {
	case *avro.RefSchema:
		typ = resolveRefSchema(s)
	case *avro.RecordSchema:
		typ = generateFrom(s, acc)
	case *avro.PrimitiveSchema:
		typ = resolvePrimitiveLogicalType(logicalType, typ, s)
		if strings.Contains(typ, "time") {
			addImport(acc, "time")
		}
		if strings.Contains(typ, "big") {
			addImport(acc, "math/big")
		}
	case *avro.ArraySchema:
		typ = fmt.Sprintf("[]%s", generateFrom(s.Items(), acc))
	case *avro.EnumSchema:
		typ = "string"
	case *avro.FixedSchema:
		typ = fmt.Sprintf("[%d]byte", +s.Size())
	case *avro.MapSchema:
		typ = "map[string]" + resolveType(s.Values(), nil, acc)
	case *avro.UnionSchema:
		typ = resolveUnionTypes(s, acc)
	}
	return typ
}

func resolveRefSchema(s *avro.RefSchema) string {
	typ := ""
	switch sx := s.Schema().(type) {
	case *avro.RecordSchema:
		typ = sx.Name()
	case avro.NamedSchema:
		typ = sx.Name()
	}
	return strcase.ToCamel(typ)
}

func resolveUnionTypes(unionSchema *avro.UnionSchema, acc *ast.File) string {
	nullIsAllowed := false
	typesInUnion := make([]string, 0)
	for _, elementSchema := range unionSchema.Types() {
		if _, ok := elementSchema.(*avro.NullSchema); ok {
			nullIsAllowed = true
		} else {
			typesInUnion = append(typesInUnion, generateFrom(elementSchema, acc))
		}
	}
	if nullIsAllowed && len(typesInUnion) == 1 {
		typ := typesInUnion[0]
		if strings.HasPrefix(typ, "[]") {
			return typ
		}
		return "*" + typ
	}
	return "interface{}"
}

func resolvePrimitiveLogicalType(logicalType interface{}, typ string, s avro.Schema) string {
	switch logicalType {
	case "", nil:
		typ = primitiveMappings[s.Type()]
	case "date", "timestamp-millis", "timestamp-micros":
		typ = "time.Time"
	case "time-millis", "time-micros":
		typ = "time.Duration"
	case "decimal":
		typ = "*big.Rat"
	}
	return typ
}

func newType(name string, fields []*ast.Field) *ast.GenDecl {
	return &ast.GenDecl{
		Tok: token.TYPE,
		Specs: []ast.Spec{
			&ast.TypeSpec{
				Name: &ast.Ident{Name: name},
				Type: &ast.StructType{
					Fields: &ast.FieldList{
						List: fields,
					},
				},
			},
		},
	}
}

func newField(name string, typ string, tag string) *ast.Field {
	return &ast.Field{
		Names: []*ast.Ident{{Name: name}},
		Type: &ast.Ident{
			Name: typ,
		},
		Tag: &ast.BasicLit{
			Value: "`avro:\"" + tag + "\"`",
			Kind:  token.STRING,
		},
	}
}

func addImport(acc *ast.File, statement string) {
	astutil.AddImport(token.NewFileSet(), acc, statement)
}

const outputTemplate = `package {{ .PackageName }}

{{ if len .Imports }}
import (
    {{- range .Imports }}
		{{ . }}
	{{- end }}
)
{{ end }}

{{- range .Typedefs }}
type {{ .Name }} struct {
	{{- range .Fields }}
		{{ .Name }} {{ .Type }} {{ .Tag }}
	{{- end }}
}
{{ end }}`

type data struct {
	PackageName string
	Imports     []string
	Typedefs    []typedef
}

type typedef struct {
	Name   string
	Fields []field
}

type field struct {
	Name string
	Type string
	Tag  string
}

func writeFile(w io.Writer, f *ast.File) error {
	parsed, err := template.New("out").Parse(outputTemplate)
	if err != nil {
		return err
	}

	return parsed.Execute(w, data{
		PackageName: packageName(f),
		Imports:     imports(f),
		Typedefs:    types(f),
	})
}

func packageName(f *ast.File) string {
	return f.Name.Name
}

func imports(f *ast.File) []string {
	result := make([]string, len(f.Imports))
	for i, imp := range f.Imports {
		result[i] = imp.Path.Value
	}
	return result
}

func types(f *ast.File) []typedef {
	var result []typedef

	for _, decl := range f.Decls {
		x, _ := decl.(*ast.GenDecl)

		for _, spec := range x.Specs {
			s, isType := spec.(*ast.TypeSpec)
			if !isType {
				continue
			}

			st, isStruct := s.Type.(*ast.StructType)
			if !isStruct {
				continue
			}

			td := typedef{
				Name:   s.Name.Name,
				Fields: make([]field, 0),
			}
			for _, fld := range st.Fields.List {
				typ, _ := fld.Type.(*ast.Ident)
				td.Fields = append(td.Fields, field{
					Name: fld.Names[0].Name,
					Type: typ.Name,
					Tag:  fld.Tag.Value,
				})
			}

			result = append(result, td)
		}
	}

	return result
}
