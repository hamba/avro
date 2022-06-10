package avro

import (
	"bytes"
	"errors"
	"fmt"
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
	generateFrom(rSchema, &result)

	buf := &bytes.Buffer{}
	if err = decorator.Fprint(buf, &result); err != nil {
		return nil, err
	}
	return buf, nil
}

func generateFrom(schema Schema, acc *dst.File) string {
	// TODO union types at root
	switch t := schema.(type) {
	case *RecordSchema:
		typeName := capitalize(t.Name())
		fields := make([]*dst.Field, len(t.fields))
		for i, f := range t.fields {
			fSchema := f.Type()
			fieldName := capitalize(f.Name())
			typ := resolveType(fSchema, f.Prop("logicalType"), acc)
			fields[i] = newField(fieldName, typ)
		}
		acc.Decls = append(acc.Decls, newType(typeName, fields))
		return typeName
	default:
		return resolveType(schema, nil, acc)
	}
}

func resolveType(fieldSchema Schema, logicalType interface{}, acc *dst.File) string {
	var typ string
	switch s := fieldSchema.(type) {
	case *RefSchema:
		panic("impl")
	case *RecordSchema:
		typ = generateFrom(s, acc)
	case *PrimitiveSchema:
		typ = resolvePrimitiveLogicalType(logicalType, typ, s)
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

func resolveUnionTypes(unionSchema *UnionSchema, acc *dst.File) string {
	nullIsAllowed := false // TODO assumes null is always first
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
	return "{}interface"
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
