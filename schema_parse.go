package avro

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"strings"

	"github.com/json-iterator/go"
)

var (
	schemaReserved = []string{"doc", "fields", "items", "name", "namespace", "size", "symbols", "values", "type",
		"aliases", "logicalType", "precision", "scale"}
	fieldReserved = []string{"default", "doc", "name", "order", "type", "aliases"}
)

// DefaultSchemaCache is the default cache for schemas.
var DefaultSchemaCache = &SchemaCache{}

// Parse parses a schema string.
func Parse(schema string) (Schema, error) {
	return ParseWithCache(schema, "", DefaultSchemaCache)
}

// ParseWithCache parses a schema string using the given namespace and  schema cache.
func ParseWithCache(schema, namespace string, cache *SchemaCache) (Schema, error) {
	var json interface{}
	if err := jsoniter.Unmarshal([]byte(schema), &json); err != nil {
		json = schema
	}

	return parseType(namespace, json, cache)
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

func parseType(namespace string, v interface{}, cache *SchemaCache) (Schema, error) {
	switch val := v.(type) {
	case nil:
		return &NullSchema{}, nil

	case string:
		return parsePrimitiveType(namespace, val, cache)

	case map[string]interface{}:
		return parseComplexType(namespace, val, cache)

	case []interface{}:
		return parseUnion(namespace, val, cache)
	}

	return nil, fmt.Errorf("avro: unknown type: %v", v)
}

func parsePrimitiveType(namespace, s string, cache *SchemaCache) (Schema, error) {
	typ := Type(s)
	switch typ {
	case Null:
		return &NullSchema{}, nil

	case String, Bytes, Int, Long, Float, Double, Boolean:
		return parsePrimitive(typ, nil)

	default:
		schema := cache.Get(fullName(namespace, s))
		if schema != nil {
			return schema, nil
		}

		return nil, fmt.Errorf("avro: unknown type: %s", s)
	}
}

func parseComplexType(namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
	if val, ok := m["type"].([]interface{}); ok {
		return parseUnion(namespace, val, cache)
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
		return parsePrimitive(typ, m)

	case Record, Error:
		return parseRecord(typ, namespace, m, cache)

	case Enum:
		return parseEnum(namespace, m, cache)

	case Array:
		return parseArray(namespace, m, cache)

	case Map:
		return parseMap(namespace, m, cache)

	case Fixed:
		return parseFixed(namespace, m, cache)

	default:
		return parseType(namespace, string(typ), cache)
	}
}

func parsePrimitive(typ Type, m map[string]interface{}) (Schema, error) {
	logical := parsePrimitiveLogicalType(typ, m)

	return NewPrimitiveSchema(typ, logical), nil
}

func parsePrimitiveLogicalType(typ Type, m map[string]interface{}) LogicalSchema {
	if m == nil {
		return nil
	}

	lt, ok := m["logicalType"].(string)
	if !ok {
		return nil
	}

	ltyp := LogicalType(lt)
	if (typ == String && ltyp == UUID) ||
		(typ == Int && ltyp == Date) ||
		(typ == Int && ltyp == TimeMillis) ||
		(typ == Long && ltyp == TimeMicros) ||
		(typ == Long && ltyp == TimestampMillis) ||
		(typ == Long && ltyp == TimestampMicros) {
		return NewPrimitiveLogicalSchema(ltyp)
	}

	if typ == Bytes && ltyp == Decimal {
		return parseDecimalLogicalType(-1, m)
	}

	return nil
}

func parseRecord(typ Type, namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
	name, newNamespace, err := resolveFullName(m)
	if err != nil {
		return nil, err
	}
	if newNamespace != "" {
		namespace = newNamespace
	}

	fs, ok := m["fields"].([]interface{})
	if !ok {
		return nil, errors.New("avro: record must have an array of fields")
	}
	fields := make([]*Field, len(fs))

	var rec *RecordSchema
	switch typ {
	case Record:
		rec, err = NewRecordSchema(name, namespace, fields)

	case Error:
		rec, err = NewErrorRecordSchema(name, namespace, fields)
	}
	if err != nil {
		return nil, err
	}

	cache.Add(rec.FullName(), NewRefSchema(rec))

	for k, v := range m {
		rec.AddProp(k, v)
	}

	for i, f := range fs {
		field, err := parseField(namespace, f, cache)
		if err != nil {
			return nil, err
		}

		fields[i] = field
	}

	return rec, nil
}

func parseField(namespace string, v interface{}, cache *SchemaCache) (*Field, error) {
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

	typ, err := parseType(namespace, m["type"], cache)
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

func parseEnum(namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
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

	cache.Add(enum.FullName(), enum)

	for k, v := range m {
		enum.AddProp(k, v)
	}

	return enum, nil
}

func parseArray(namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
	items, ok := m["items"]
	if !ok {
		return nil, errors.New("avro: array must have an items key")
	}

	schema, err := parseType(namespace, items, cache)
	if err != nil {
		return nil, err
	}

	arr := NewArraySchema(schema)

	for k, v := range m {
		arr.AddProp(k, v)
	}

	return arr, nil
}

func parseMap(namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
	values, ok := m["values"]
	if !ok {
		return nil, errors.New("avro: map must have an values key")
	}

	schema, err := parseType(namespace, values, cache)
	if err != nil {
		return nil, err
	}

	ms := NewMapSchema(schema)

	for k, v := range m {
		ms.AddProp(k, v)
	}

	return ms, nil
}

func parseUnion(namespace string, v []interface{}, cache *SchemaCache) (Schema, error) {
	var err error
	types := make([]Schema, len(v))
	for i := range v {
		types[i], err = parseType(namespace, v[i], cache)
		if err != nil {
			return nil, err
		}
	}

	return NewUnionSchema(types)
}

func parseFixed(namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
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

	logical := parseFixedLogicalType(int(size), m)

	fixed, err := NewFixedSchema(name, namespace, int(size), logical)
	if err != nil {
		return nil, err
	}

	cache.Add(fixed.FullName(), fixed)

	for k, v := range m {
		fixed.AddProp(k, v)
	}

	return fixed, nil
}

func parseFixedLogicalType(size int, m map[string]interface{}) LogicalSchema {
	lt, ok := m["logicalType"].(string)
	if !ok {
		return nil
	}

	ltyp := LogicalType(lt)
	if ltyp == Duration && size == 12 {
		return NewPrimitiveLogicalSchema(Duration)
	}

	if ltyp == Decimal {
		return parseDecimalLogicalType(size, m)
	}

	return nil
}

func parseDecimalLogicalType(size int, m map[string]interface{}) LogicalSchema {
	prec, ok := m["precision"].(float64)
	if !ok || prec <= 0 {
		return nil
	}

	if size > 0 {
		maxPrecision := math.Round(math.Floor(math.Log10(2) * (8*float64(size) - 1)))
		if prec > maxPrecision {
			return nil
		}
	}

	scale, _ := m["scale"].(float64)
	if scale < 0 {
		return nil
	}

	// Scale may not be bigger than precision
	if scale > prec {
		return nil
	}

	return NewDecimalLogicalSchema(int(prec), int(scale))
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
