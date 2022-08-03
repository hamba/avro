package avro

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/mitchellh/mapstructure"
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
		s, err := os.ReadFile(filepath.Clean(path))
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

type primitiveSchema struct {
	LogicalType string                 `mapstructure:"logicalType"`
	SqlType     string                 `mapstructure:"sqlType"`
	Precision   int                    `mapstructure:"precision"`
	Scale       int                    `mapstructure:"scale"`
	Props       map[string]interface{} `mapstructure:",remain"`
}

func parsePrimitive(typ Type, m map[string]interface{}) (Schema, error) {
	if m == nil {
		return NewPrimitiveSchema(typ, nil, nil), nil
	}

	var (
		p    primitiveSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &p, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding primitive: %w", err)
	}
	if p.LogicalType != "" && p.SqlType != "" {
		return nil, fmt.Errorf("avro: error decoding primitive: provided logical and sql type at the same time")
	}

	var logical LogicalSchema
	if p.LogicalType != "" {
		logical = parsePrimitiveLogicalType(typ, p.LogicalType, p.Precision, p.Scale)
	}

	var sql SqlSchema
	if p.SqlType != "" {
		sql = parsePrimitiveSqlType(typ, p.SqlType)
	}

	return NewPrimitiveSchema(typ, logical, sql, WithProps(p.Props)), nil
}

func parsePrimitiveLogicalType(typ Type, lt string, prec, scale int) LogicalSchema {
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
		return parseDecimalLogicalType(-1, prec, scale)
	}

	return nil
}

func parsePrimitiveSqlType(typ Type, st string) SqlSchema {
	styp := SqlType(st)
	if typ == String && styp == Json {
		return NewPrimitiveSqlSchema(styp)
	}

	return nil
}

type recordSchema struct {
	Type      string                   `mapstructure:"type"`
	Name      string                   `mapstructure:"name"`
	Namespace string                   `mapstructure:"namespace"`
	Aliases   []string                 `mapstructure:"aliases"`
	Doc       string                   `mapstructure:"doc"`
	Fields    []map[string]interface{} `mapstructure:"fields"`
	Props     map[string]interface{}   `mapstructure:",remain"`
}

func parseRecord(typ Type, namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
	var (
		r    recordSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &r, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding record: %w", err)
	}

	if err := checkParsedName(r.Name, r.Namespace, hasKey(meta.Keys, "namespace")); err != nil {
		return nil, err
	}
	if r.Namespace == "" {
		r.Namespace = namespace
	}

	if !hasKey(meta.Keys, "fields") {
		return nil, errors.New("avro: record must have an array of fields")
	}
	fields := make([]*Field, len(r.Fields))

	var (
		rec *RecordSchema
		err error
	)
	switch typ {
	case Record:
		rec, err = NewRecordSchema(r.Name, r.Namespace, fields,
			WithAliases(r.Aliases), WithDoc(r.Doc), WithProps(r.Props),
		)
	case Error:
		rec, err = NewErrorRecordSchema(r.Name, r.Namespace, fields,
			WithAliases(r.Aliases), WithDoc(r.Doc), WithProps(r.Props),
		)
	}
	if err != nil {
		return nil, err
	}

	ref := NewRefSchema(rec)
	cache.Add(rec.FullName(), ref)
	for _, alias := range rec.Aliases() {
		cache.Add(alias, ref)
	}

	for i, f := range r.Fields {
		field, err := parseField(r.Namespace, f, cache)
		if err != nil {
			return nil, err
		}
		fields[i] = field
	}

	return rec, nil
}

type fieldSchema struct {
	Name    string                 `mapstructure:"name"`
	Aliases []string               `mapstructure:"aliases"`
	Type    interface{}            `mapstructure:"type"`
	Doc     string                 `mapstructure:"doc"`
	Default interface{}            `mapstructure:"default"`
	Order   Order                  `mapstructure:"order"`
	Props   map[string]interface{} `mapstructure:",remain"`
}

func parseField(namespace string, m map[string]interface{}, cache *SchemaCache) (*Field, error) {
	var (
		f    fieldSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &f, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding field: %w", err)
	}

	if err := checkParsedName(f.Name, "", false); err != nil {
		return nil, err
	}

	if !hasKey(meta.Keys, "type") {
		return nil, errors.New("avro: field requires a type")
	}
	typ, err := parseType(namespace, f.Type, cache)
	if err != nil {
		return nil, err
	}

	if !hasKey(meta.Keys, "default") {
		f.Default = NoDefault
	}

	field, err := NewField(f.Name, typ,
		WithDefault(f.Default), WithAliases(f.Aliases), WithDoc(f.Doc), WithOrder(f.Order), WithProps(f.Props),
	)
	if err != nil {
		return nil, err
	}

	return field, nil
}

type enumSchema struct {
	Name      string                 `mapstructure:"name"`
	Namespace string                 `mapstructure:"namespace"`
	Aliases   []string               `mapstructure:"aliases"`
	Type      string                 `mapstructure:"type"`
	Doc       string                 `mapstructure:"doc"`
	Symbols   []string               `mapstructure:"symbols"`
	Default   string                 `mapstructure:"default"`
	Props     map[string]interface{} `mapstructure:",remain"`
}

func parseEnum(namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
	var (
		e    enumSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &e, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding enum: %w", err)
	}

	if err := checkParsedName(e.Name, e.Namespace, hasKey(meta.Keys, "namespace")); err != nil {
		return nil, err
	}
	if e.Namespace == "" {
		e.Namespace = namespace
	}

	enum, err := NewEnumSchema(e.Name, e.Namespace, e.Symbols,
		WithDefault(e.Default), WithAliases(e.Aliases), WithDoc(e.Doc), WithProps(e.Props),
	)
	if err != nil {
		return nil, err
	}

	cache.Add(enum.FullName(), enum)
	for _, alias := range enum.Aliases() {
		cache.Add(alias, enum)
	}

	return enum, nil
}

type arraySchema struct {
	Items interface{}            `mapstructure:"items"`
	Props map[string]interface{} `mapstructure:",remain"`
}

func parseArray(namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
	var (
		a    arraySchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &a, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding array: %w", err)
	}

	if !hasKey(meta.Keys, "items") {
		return nil, errors.New("avro: array must have an items key")
	}
	schema, err := parseType(namespace, a.Items, cache)
	if err != nil {
		return nil, err
	}

	return NewArraySchema(schema, WithProps(a.Props)), nil
}

type mapSchema struct {
	Values interface{}            `mapstructure:"values"`
	Props  map[string]interface{} `mapstructure:",remain"`
}

func parseMap(namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
	var (
		ms   mapSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &ms, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding map: %w", err)
	}

	if !hasKey(meta.Keys, "values") {
		return nil, errors.New("avro: map must have an values key")
	}
	schema, err := parseType(namespace, ms.Values, cache)
	if err != nil {
		return nil, err
	}

	return NewMapSchema(schema, WithProps(ms.Props)), nil
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

type fixedSchema struct {
	Name        string                 `mapstructure:"name"`
	Namespace   string                 `mapstructure:"namespace"`
	Aliases     []string               `mapstructure:"aliases"`
	Type        string                 `mapstructure:"type"`
	Size        int                    `mapstructure:"size"`
	LogicalType string                 `mapstructure:"logicalType"`
	Precision   int                    `mapstructure:"precision"`
	Scale       int                    `mapstructure:"scale"`
	Props       map[string]interface{} `mapstructure:",remain"`
}

func parseFixed(namespace string, m map[string]interface{}, cache *SchemaCache) (Schema, error) {
	var (
		f    fixedSchema
		meta mapstructure.Metadata
	)
	if err := decodeMap(m, &f, &meta); err != nil {
		return nil, fmt.Errorf("avro: error decoding fixed: %w", err)
	}

	if err := checkParsedName(f.Name, f.Namespace, hasKey(meta.Keys, "namespace")); err != nil {
		return nil, err
	}
	if f.Namespace == "" {
		f.Namespace = namespace
	}

	if !hasKey(meta.Keys, "size") {
		return nil, errors.New("avro: fixed must have a size")
	}

	var logical LogicalSchema
	if f.LogicalType != "" {
		logical = parseFixedLogicalType(f.Size, f.LogicalType, f.Precision, f.Scale)
	}

	fixed, err := NewFixedSchema(f.Name, f.Namespace, f.Size, logical, WithAliases(f.Aliases), WithProps(f.Props))
	if err != nil {
		return nil, err
	}

	cache.Add(fixed.FullName(), fixed)
	for _, alias := range fixed.Aliases() {
		cache.Add(alias, fixed)
	}

	return fixed, nil
}

func parseFixedLogicalType(size int, lt string, prec, scale int) LogicalSchema {
	ltyp := LogicalType(lt)
	switch {
	case ltyp == Duration && size == 12:
		return NewPrimitiveLogicalSchema(Duration)
	case ltyp == Decimal:
		return parseDecimalLogicalType(size, prec, scale)
	}

	return nil
}

func parseDecimalLogicalType(size, prec, scale int) LogicalSchema {
	if prec <= 0 {
		return nil
	}

	if size > 0 {
		maxPrecision := int(math.Round(math.Floor(math.Log10(2) * (8*float64(size) - 1))))
		if prec > maxPrecision {
			return nil
		}
	}

	if scale < 0 {
		return nil
	}

	// Scale may not be bigger than precision
	if scale > prec {
		return nil
	}

	return NewDecimalLogicalSchema(prec, scale)
}

func fullName(namespace, name string) string {
	if len(namespace) == 0 || strings.ContainsRune(name, '.') {
		return name
	}

	return namespace + "." + name
}

func checkParsedName(name, ns string, hasNS bool) error {
	if name == "" {
		return errors.New("avro: non-empty name key required")
	}
	if hasNS && ns == "" {
		return errors.New("avro: namespace key must be non-empty or omitted")
	}
	return nil
}

func hasKey(keys []string, k string) bool {
	for _, key := range keys {
		if key == k {
			return true
		}
	}
	return false
}

func decodeMap(in, v interface{}, meta *mapstructure.Metadata) error {
	cfg := &mapstructure.DecoderConfig{
		ZeroFields: true,
		Metadata:   meta,
		Result:     v,
	}

	decoder, _ := mapstructure.NewDecoder(cfg)
	return decoder.Decode(in)
}
