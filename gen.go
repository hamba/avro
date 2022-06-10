package avro

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"io"
	"strings"

	"github.com/iancoleman/strcase"
	"golang.org/x/tools/go/ast/astutil"
)

type GenConf struct {
	PackageName string
	OutFileName string
}

var primitiveMappings = map[Type]string{
	"string":  "string",
	"bytes":   "[]byte",
	"int":     "int",
	"long":    "int64",
	"float":   "float32",
	"double":  "float64",
	"boolean": "bool",
}

func GenerateFrom(gc GenConf, s string) (io.Reader, error) {
	schema, err := Parse(s)
	if err != nil {
		return nil, err
	}

	rSchema, ok := schema.(*RecordSchema)
	if !ok {
		return nil, errors.New("can only generate Go code from Record Schemas")
	}
	file := ast.File{
		Name: &ast.Ident{Name: strcase.ToSnake(gc.PackageName)},
	}
	generateFrom(rSchema, &file)

	buf := &bytes.Buffer{}
	if err = writeFile(buf, &file); err != nil {
		return nil, err
	}
	return buf, nil
}

func generateFrom(schema Schema, acc *ast.File) string {
	switch t := schema.(type) {
	case *RecordSchema:
		typeName := strcase.ToCamel(t.Name())
		fields := make([]*ast.Field, len(t.fields))
		for i, f := range t.fields {
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

func resolveType(fieldSchema Schema, logicalType interface{}, acc *ast.File) string {
	var typ string
	switch s := fieldSchema.(type) {
	case *RefSchema:
		typ = strcase.ToCamel(s.actual.Name())
	case *RecordSchema:
		typ = generateFrom(s, acc)
	case *PrimitiveSchema:
		typ = resolvePrimitiveLogicalType(logicalType, typ, s)
		if strings.Contains(typ, "time") {
			addImport(acc, "time")
		}
		if strings.Contains(typ, "big") {
			addImport(acc, "math/big")
		}
	case *ArraySchema:
		typ = fmt.Sprintf("[]%s", generateFrom(s.Items(), acc))
	case *EnumSchema:
		typ = "string"
	case *FixedSchema:
		typ = fmt.Sprintf("[%d]byte", +s.Size())
	case *MapSchema:
		typ = "map[string]" + resolveType(s.Values(), nil, acc)
	case *UnionSchema:
		typ = resolveUnionTypes(s, acc)
	}
	return typ
}

func resolveUnionTypes(unionSchema *UnionSchema, acc *ast.File) string {
	nullIsAllowed := false
	typesInUnion := make([]string, 0)
	for _, elementSchema := range unionSchema.Types() {
		if _, ok := elementSchema.(*NullSchema); ok {
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

func resolvePrimitiveLogicalType(logicalType interface{}, typ string, s Schema) string {
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

func writeFile(w io.Writer, f *ast.File) error {
	if _, err := w.Write([]byte("package " + f.Name.Name + "\n\nimport (\n")); err != nil {
		return err
	}

	for _, imp := range f.Imports {
		if _, err := w.Write([]byte("\t" + imp.Path.Value + "\n")); err != nil {
			return err
		}
	}

	if _, err := w.Write([]byte(")\n\n")); err != nil {
		return err
	}

	for _, decl := range f.Decls {
		x, _ := decl.(*ast.GenDecl)

		for _, spec := range x.Specs {
			s, isType := spec.(*ast.TypeSpec)
			if !isType {
				continue
			}
			if _, err := w.Write([]byte("type " + s.Name.Name + " struct {\n")); err != nil {
				return err
			}

			st, isStruct := s.Type.(*ast.StructType)
			if !isStruct {
				continue
			}

			for _, field := range st.Fields.List {
				typ, _ := field.Type.(*ast.Ident)
				_, err := w.Write([]byte("\t" + field.Names[0].Name + " " + typ.Name + " " + field.Tag.Value + "\n"))
				if err != nil {
					return err
				}
			}

			if _, err := w.Write([]byte("}\n\n")); err != nil {
				return err
			}
		}
	}

	return nil
}
