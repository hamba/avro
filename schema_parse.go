package avro

import (
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/json-iterator/go"
)

var nameRegexp = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*?(\.[A-Za-z_][A-Za-z0-9_]*?)*$`)

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

	rec := &RecordSchema{
		name:   fullName(namespace, name),
		fields: make([]*Field, len(fields)),
	}

	schemaConfig.addSchemaToCache(rec.name, &RefSchema{actual: rec})

	for i, f := range fields {
		field, err := parseField(namespace, f)
		if err != nil {
			return nil, err
		}

		rec.fields[i] = field
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

	field := &Field{
		name: name,
		typ:  typ,
		def:  m["default"],
	}

	// TODO: check default types
	// if the type is a union, the default must be of the same type if the first type
	// Sanitise the default
	if f, ok := field.def.(float64); ok {
		switch typ.Type() {
		case Int:
			field.def = int32(f)

		case Long:
			field.def = int64(f)

		case Float:
			field.def = float32(f)
		}
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

		if err := validateName(str); err != nil {
			return nil, err
		}

		symbols[i] = str
	}

	if len(symbols) == 0 {
		return nil, errors.New("avro: enum must have a non-empty array of symbols")
	}

	enum := &EnumSchema{
		name:    fullName(namespace, name),
		symbols: symbols,
	}

	schemaConfig.addSchemaToCache(enum.name, enum)

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

	return &ArraySchema{
		items: schema,
	}, nil
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

	return &MapSchema{
		values: schema,
	}, nil
}

func parseUnion(namespace string, v []interface{}) (Schema, error) {
	types := make([]Schema, len(v))

	var err error
	for i := range v {
		types[i], err = parseType(namespace, v[i])
		if err != nil {
			return nil, err
		}

		// TODO: types[i] cannot be a union
		// No dup types, except if named and names are different
	}
	return &UnionSchema{
		types: Schemas(types),
	}, nil
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

	fixed := &FixedSchema{
		name: fullName(namespace, name),
		size: int(size),
	}

	schemaConfig.addSchemaToCache(fixed.name, fixed)

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

	if name == "" {
		return "", errors.New("avro: name must be a non-empty")
	}

	if err := validateName(name); err != nil {
		return "", err
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

	if err := validateNamespace(namespace); err != nil {
		return "", "", err
	}

	return name, namespace, nil
}

func validateName(name string) error {
	if !nameRegexp.MatchString(name) {
		return fmt.Errorf("avro: name is invalid: %s", name)
	}

	return nil
}

func validateNamespace(namespace string) error {
	if !nameRegexp.MatchString(namespace) {
		return fmt.Errorf("avro: namespace is invalid: %s", namespace)
	}

	return nil
}
