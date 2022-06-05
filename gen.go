package avro

import (
	"bytes"
	"errors"
	"go/token"
	"io"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
)

type GenConf struct {
	PackageName string
	OutFileName string
}

func GenerateFrom(gc GenConf, s string) (io.Reader, error) {
	// Ideas
	/*
		- We get an avro schema
		- We get a config, optional, so lets ignore for now...
		- We can choose what file to output the generated code
		- Generated code is tagged in comments, so we can infer if we can change it or not
		- The above step means we can output to a file that already contains go code
		- A clean up can be requested to remove all generated code (Nice to have)
	*/

	/*
		1. Read and parse the schema into a suitable internal representation...
			that representation should already exist in this project
		2. Generate go code based on the representation
			- Template based representation would be faster
			- Go Code based representation would be more powerful
	*/
	schema, err := Parse(s)
	if err != nil {
		return nil, err
	}

	rSchema, ok := schema.(*RecordSchema)
	if !ok {
		return nil, errors.New("can only generate Go code from Record Schemas")
	}

	result := dst.File{
		// TODO should be to_snake_case
		Name: &dst.Ident{Name: strings.ToLower(gc.PackageName)}, // For some reason this is the package name
	}
	generateFrom(gc, rSchema, &result)

	buf := &bytes.Buffer{}
	if err = decorator.Fprint(buf, &result); err != nil {
		return nil, err
	}
	return buf, nil
}

func generateFrom(gc GenConf, schema *RecordSchema, acc *dst.File) string {
	// TODO inner records
	// TODO union types
	// TODO union types at root
	fields := make([]*dst.Field, len(schema.fields))
	for i, f := range schema.fields {
		fieldName, typ := genField(gc, f, acc)
		fields[i] = newField(fieldName, typ)
	}
	typeName := capitalize(schema.name.name)
	acc.Decls = append(acc.Decls, newType(typeName, fields))
	return typeName
}

func genField(gc GenConf, f *Field, acc *dst.File) (string, string) {
	fieldName := capitalize(f.name)
	var typ string
	switch s := f.Type().(type) {
	case *RecordSchema:
		typ = generateFrom(gc, s, acc)
	case *PrimitiveSchema:
		typ = resolvePrimitiveType(s.Type())
	case *ArraySchema:
		// TODO missing arrays of unions
		typ = "[]" + resolvePrimitiveType(s.Items().Type())
	case *UnionSchema:
		nullIsAllowed := false // TODO assumes null is always first
		for _, schemaInUnion := range s.Types() {
			switch schemaAsType := schemaInUnion.(type) {
			case *RecordSchema:
				typ = generateFrom(gc, schemaAsType, acc)
				if !nullIsAllowed || len(s.Types()) != 2 {
					typ = "interface{}"
				}
			case *ArraySchema:
				typ = "[]" + resolvePrimitiveType(schemaAsType.Items().Type())
			case *NullSchema:
				nullIsAllowed = true
			}
		}
	}
	return fieldName, typ
}

func resolvePrimitiveType(x Type) string {
	switch x {
	case "record":
		panic("impl")
	case "error":
		panic("impl")
	case "<ref>":
		panic("impl")
	case "enum":
		panic("impl")
	case "array":
		panic("impl")
	case "map":
		panic("impl")
	case "union":
		panic("impl")
	case "fixed":
		panic("impl")
	case "string":
		return "string"
	case "bytes":
		return "[]byte"
	case "int":
		return "int"
	case "long":
		return "int64"
	case "float":
		return "float32"
	case "double":
		return "float64"
	case "boolean":
		return "bool"
	case "null":
		panic("impl")
	}
	panic("impl")
}

func newType(name string, fields []*dst.Field) *dst.GenDecl {
	return &dst.GenDecl{
		Tok: token.TYPE,
		Specs: []dst.Spec{
			&dst.TypeSpec{
				Name: &dst.Ident{Name: name},
				Type: &dst.StructType{
					Fields: &dst.FieldList{
						List: fields,
					},
				},
			},
		},
	}
}

func newField(name string, typ string) *dst.Field {
	return &dst.Field{
		Names: []*dst.Ident{{Name: name}},
		Type: &dst.Ident{
			Name: typ,
		},
	}
}

func capitalize(s string) string {
	if s == "" || s[0] < 'a' {
		return s
	}
	return strings.ToUpper(string(s[0])) + s[1:]
}
