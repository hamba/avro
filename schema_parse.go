package avro

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/json-iterator/go"
)

var (
	schemaReserved = []string{"doc", "fields", "items", "name", "namespace", "size", "symbols", "values", "type", "aliases"}
	fieldReserved = []string{"default", "doc", "name", "order", "type", "aliases"}
)

// Parse parses a schema string.
func Parse(schema string) (Schema, error) {
	var json interface{}
	if err := jsoniter.Unmarshal([]byte(schema), &json); err != nil {
		json = schema
	}

	return parseType("", json)
}

// MustParse parses a schema string, panicing if there is an error.
func MustParse(schema string) Schema {
	parsed, err := Parse(schema)
	if err != nil {
		panic(err)
	}

	return parsed
}

// ParseFiles parses the schemas in the files, in the order they appear, returning the last schema.
//
// This is useful when your schemas rely on other schemas.
func ParseFiles(paths ...string) (Schema, error) {
	var schema Schema
	for _, path := range paths {
		s, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}

		schema, err = Parse(string(s))
		if err != nil {
			return nil, err
		}
	}

	return schema, nil
}

func parseType(namespace string, v interface{}) (Schema, error) {
	switch val := v.(type) {
	case nil:
		return &NullSchema{}, nil

	case string:
		return parsePrimitive(namespace, val)

	case map[string]interface{}:
		return parseComplex(namespace, val)

	case []interface{}:
		return parseUnion(namespace, val)
	}

	return nil, fmt.Errorf("avro: unknown type: %v", v)
}

func parsePrimitive(namespace, s string) (Schema, error) {
	typ := Type(s)
	switch typ {
	case Null:
		return &NullSchema{}, nil

	case String, Bytes, Int, Long, Float, Double, Boolean:
		return NewPrimitiveSchema(typ), nil

	default:
		schema := schemaConfig.getSchemaFromCache(fullName(namespace, s))
		if schema != nil {
			return schema, nil
		}

		return nil, fmt.Errorf("avro: unknown type: %s", s)
	}
}

func parseComplex(namespace string, m map[string]interface{}) (Schema, error) {
	if val, ok := m["type"].([]interface{}); ok {
		return parseUnion(namespace, val)
	}

	str, ok := m["type"].(string)
	if !ok {
		return nil, fmt.Errorf("avro: unknown type: %+v", m)
	}
	typ := Type(str)

	switch typ {
	case Null:
		return &NullSchema{}, nil

	case String, Bytes, Int, Long, Float, Double, Boolean:
		return NewPrimitiveSchema(typ), nil

	case Record:
		return parseRecord(namespace, m)

	case Enum:
		return parseEnum(namespace, m)

	case Array:
		return parseArray(namespace, m)

	case Map:
		return parseMap(namespace, m)

	case Fixed:
		return parseFixed(namespace, m)

	default:
		return parseType(namespace, string(typ))
	}
}

func parseRecord(namespace string, m map[string]interface{}) (Schema, error) {
	name, newNamespace, err := resolveFullName(m)
	if err != nil {
		return nil, err
	}
	if newNamespace != "" {
		namespace = newNamespace
	}

	fields, ok := m["fields"].([]interface{})
	if !ok {
		return nil, errors.New("avro: record must have an array of fields")
	}

	rec, err := NewRecordSchema(name, namespace)
	if err != nil {
		return nil, err
	}

	schemaConfig.addSchemaToCache(rec.FullName(), NewRefSchema(rec))

	for k, v := range m {
		rec.AddProp(k, v)
	}

	for _, f := range fields {
		field, err := parseField(namespace, f)
		if err != nil {
			return nil, err
		}

		rec.AddField(field)
	}

	return rec, nil
}

func parseField(namespace string, v interface{}) (*Field, error) {
	m, ok := v.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("avro: invalid field: %+v", v)
	}

	name, err := resolveName(m)
	if err != nil {
		return nil, err
	}

	if _, ok := m["type"]; !ok {
		return nil, errors.New("avro: field requires a type")
	}

	typ, err := parseType(namespace, m["type"])
	if err != nil {
		return nil, err
	}

	field, err := NewField(name, typ, m["default"])
	if err != nil {
		return nil, err
	}

	for k, v := range m {
		field.AddProp(k, v)
	}

	return field, nil
}

func parseEnum(namespace string, m map[string]interface{}) (Schema, error) {
	name, newNamespace, err := resolveFullName(m)
	if err != nil {
		return nil, err
	}
	if newNamespace != "" {
		namespace = newNamespace
	}

	syms, ok := m["symbols"].([]interface{})
	if !ok {
		return nil, errors.New("avro: enum must have a non-empty array of symbols")
	}

	symbols := make([]string, len(syms))
	for i, sym := range syms {
		str, ok := sym.(string)
		if !ok {
			return nil, fmt.Errorf("avro: invalid symbol: %+v", sym)
		}

		symbols[i] = str
	}

	enum, err := NewEnumSchema(name, namespace, symbols)
	if err != nil {
		return nil, err
	}

	schemaConfig.addSchemaToCache(enum.FullName(), enum)

	for k, v := range m {
		enum.AddProp(k, v)
	}

	return enum, nil
}

func parseArray(namespace string, m map[string]interface{}) (Schema, error) {
	items, ok := m["items"]
	if !ok {
		return nil, errors.New("avro: array must have an items key")
	}

	schema, err := parseType(namespace, items)
	if err != nil {
		return nil, err
	}

	arr := NewArraySchema(schema)

	for k, v := range m {
		arr.AddProp(k, v)
	}

	return arr, nil
}

func parseMap(namespace string, m map[string]interface{}) (Schema, error) {
	values, ok := m["values"]
	if !ok {
		return nil, errors.New("avro: map must have an values key")
	}

	schema, err := parseType(namespace, values)
	if err != nil {
		return nil, err
	}

	ms := NewMapSchema(schema)

	for k, v := range m {
		ms.AddProp(k, v)
	}

	return ms, nil
}

func parseUnion(namespace string, v []interface{}) (Schema, error) {
	var err error
	types := make([]Schema, len(v))
	for i := range v {
		types[i], err = parseType(namespace, v[i])
		if err != nil {
			return nil, err
		}
	}

	return NewUnionSchema(types)
}

func parseFixed(namespace string, m map[string]interface{}) (Schema, error) {
	name, newNamespace, err := resolveFullName(m)
	if err != nil {
		return nil, err
	}
	if newNamespace != "" {
		namespace = newNamespace
	}

	size, ok := m["size"].(float64)
	if !ok {
		return nil, errors.New("avro: fixed must have a size")
	}

	fixed, err := NewFixedSchema(name, namespace, int(size))
	if err != nil {
		return nil, err
	}

	schemaConfig.addSchemaToCache(fixed.FullName(), fixed)

	for k, v := range m {
		fixed.AddProp(k, v)
	}

	return fixed, nil
}

func fullName(namespace, name string) string {
	if len(namespace) == 0 || strings.ContainsRune(name, '.') {
		return name
	}

	return namespace + "." + name
}

func resolveName(m map[string]interface{}) (string, error) {
	name, ok := m["name"].(string)
	if !ok {
		return "", errors.New("avro: name key required")
	}

	return name, nil
}

func resolveFullName(m map[string]interface{}) (string, string, error) {
	name, err := resolveName(m)
	if err != nil {
		return "", "", err
	}

	namespace, ok := m["namespace"].(string)
	if !ok {
		return name, "", nil
	}
	if namespace == "" {
		return "", "", errors.New("avro: namespace key must be non-empty or omitted")
	}

	return name, namespace, nil
}
