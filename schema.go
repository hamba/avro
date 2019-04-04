package avro

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/modern-go/concurrent"
)

var emptyFgpt = [32]byte{}

// Type is a schema type
type Type string

// Schema type constants.
const (
	Record  Type = "record"
	Ref     Type = "<ref>"
	Enum    Type = "enum"
	Array   Type = "array"
	Map     Type = "map"
	Union   Type = "union"
	Fixed   Type = "fixed"
	String  Type = "string"
	Bytes   Type = "bytes"
	Int     Type = "int"
	Long    Type = "long"
	Float   Type = "float"
	Double  Type = "double"
	Boolean Type = "boolean"
	Null    Type = "null"
)

type frozenSchemaConfig struct {
	schemaCache concurrent.Map // map[string]Schema
}

func (c *frozenSchemaConfig) addSchemaToCache(name string, schema Schema) {
	c.schemaCache.Store(name, schema)
}

// getSchemaFromCache returns the Schema if it exists.
func (c *frozenSchemaConfig) getSchemaFromCache(name string) Schema {
	if v, ok := c.schemaCache.Load(name); ok {
		return v.(Schema)
	}

	return nil
}

var schemaConfig = frozenSchemaConfig{}

// Schemas is a slice of Schemas.
type Schemas []Schema

// Get gets a schema and position by type or name if it is a named schema.
func (s Schemas) Get(name string) (Schema, int) {
	for i, schema := range s {
		if string(schema.Type()) == name {
			return schema, i
		}

		if namedSchema, ok := schema.(NamedSchema); ok && namedSchema.FullName() == name {
			return schema, i
		}
	}

	return nil, -1
}

// Schema represents an Avro schema
type Schema interface {
	// Type returns the type of the schema.
	Type() Type

	// String returns the canonical form of the schema.
	String() string

	// Fingerprint returns the SHA256 fingerprint of the schema.
	Fingerprint() [32]byte
}

// NamedSchema represents a schema with a name.
type NamedSchema interface {
	Schema

	// Name returns the name of the schema.
	Name() string

	// Namespace returns the namespace of a schema.
	Namespace() string

	// FullName returns the full qualified name of a schema.
	FullName() string
}

type name struct {
	name  string
	space string
	full  string
}

func newName(n, s string) (name, error) {
	if idx := strings.LastIndexByte(n, '.'); idx > -1 {
		s = n[:idx]
		n = n[idx+1:]
	}

	full := n
	if s != "" {
		full = s + "." + n
	}

	for _, part := range strings.Split(full, ".") {
		if err := validateName(part); err != nil {
			return name{}, err
		}
	}

	return name{
		name:  n,
		space: s,
		full:  full,
	}, nil
}

// Name returns the name of a schema.
func (n name) Name() string {
	return n.name
}

// Namespace returns the namespace of a schema.
func (n name) Namespace() string {
	return n.space
}

// FullName returns the full qualified name of a schema.
func (n name) FullName() string {
	return n.full
}

type fingerprinter struct {
	fingerprint [32]byte
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (f *fingerprinter) Fingerprint(stringer fmt.Stringer) [32]byte {
	if f.fingerprint != emptyFgpt {
		return f.fingerprint
	}

	f.fingerprint = sha256.Sum256([]byte(stringer.String()))
	return f.fingerprint
}

// PrimitiveSchema is an Avro primitive type schema.
type PrimitiveSchema struct {
	fingerprinter

	typ Type
}

// NewPrimitiveSchema creates a new PrimitiveSchema.
func NewPrimitiveSchema(t Type) *PrimitiveSchema {
	return &PrimitiveSchema{
		typ: t,
	}
}

// Type returns the type of the schema.
func (s *PrimitiveSchema) Type() Type {
	return s.typ
}

// String returns the canonical form of the schema.
func (s *PrimitiveSchema) String() string {
	return `"` + string(s.typ) + `"`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *PrimitiveSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// RecordSchema is an Avro record type schema.
type RecordSchema struct {
	name
	fingerprinter

	fields []*Field
}

// NewRecordSchema creates a new record schema instance.
func NewRecordSchema(name, space string) (*RecordSchema, error) {
	n, err := newName(name, space)
	if err != nil {
		return nil, err
	}

	return &RecordSchema{
		name: n,
	}, nil
}

// Type returns the type of the schema.
func (s *RecordSchema) Type() Type {
	return Record
}

// AddField adds a field to the record.
func (s *RecordSchema) AddField(field *Field) {
	// Clear the fingerprint cache
	s.fingerprint = emptyFgpt

	s.fields = append(s.fields, field)
}

// Fields returns the fields of a record.
func (s *RecordSchema) Fields() []*Field {
	return s.fields
}

// String returns the canonical form of the schema.
func (s *RecordSchema) String() string {
	fields := ""
	for _, f := range s.fields {
		fields += f.String() + ","
	}
	if len(fields) > 0 {
		fields = fields[:len(fields)-1]
	}

	return `{"name":"` + s.FullName() + `","type":"record","fields":[` + fields + `]}`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *RecordSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// Field is an Avro record type field.
type Field struct {
	name string
	typ  Schema
	def  interface{}
}

// NewField creates a new field instance.
func NewField(name string, typ Schema, def interface{}) (*Field, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}

	def, err := validateDefault(name, typ, def)
	if err != nil {
		return nil, err
	}

	return &Field{
		name: name,
		typ:  typ,
		def:  def,
	}, nil
}

// Name returns the name of a field.
func (s *Field) Name() string {
	return s.name
}

// Type returns the schema of a field.
func (s *Field) Type() Schema {
	return s.typ
}

// Default returns the default of a field or nil.
//
// The only time a nil default is valid is for a Null Type.
func (s *Field) Default() interface{} {
	return s.def
}

// String returns the canonical form of a field.
func (s *Field) String() string {
	return `{"name":"` + s.name + `","type":` + s.typ.String() + `}`
}

// EnumSchema is an Avro enum type schema.
type EnumSchema struct {
	name
	fingerprinter

	symbols []string
}

// NewEnumSchema creates a new enum schema instance.
func NewEnumSchema(name, namespace string, symbols []string) (*EnumSchema, error) {
	n, err := newName(name, namespace)
	if err != nil {
		return nil, err
	}

	if len(symbols) == 0 {
		return nil, errors.New("avro: enum must have a non-empty array of symbols")
	}
	for _, symbol := range symbols {
		if err := validateName(symbol); err != nil {
			return nil, fmt.Errorf("avro: invalid symnol %s", symbol)
		}
	}

	return &EnumSchema{
		name:    n,
		symbols: symbols,
	}, nil
}

// Type returns the type of the schema.
func (s *EnumSchema) Type() Type {
	return Enum
}

// Symbols returns the symbols of an enum.
func (s *EnumSchema) Symbols() []string {
	return s.symbols
}

// String returns the canonical form of the schema.
func (s *EnumSchema) String() string {
	symbols := ""
	for _, sym := range s.symbols {
		symbols += `"` + sym + `",`
	}
	if len(symbols) > 0 {
		symbols = symbols[:len(symbols)-1]
	}

	return `{"name":"` + s.FullName() + `","type":"enum","symbols":[` + symbols + `]}`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *EnumSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// ArraySchema is an Avro array type schema.
type ArraySchema struct {
	fingerprinter

	items Schema
}

// NewArraySchema creates an array schema instance.
func NewArraySchema(items Schema) *ArraySchema {
	return &ArraySchema{
		items: items,
	}
}

// Type returns the type of the schema.
func (s *ArraySchema) Type() Type {
	return Array
}

// Items returns the items schema of an array.
func (s *ArraySchema) Items() Schema {
	return s.items
}

// String returns the canonical form of the schema.
func (s *ArraySchema) String() string {
	return `{"type":"array","items":` + s.items.String() + `}`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *ArraySchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// MapSchema is an Avro map type schema.
type MapSchema struct {
	fingerprinter

	values Schema
}

// NewMapSchema creates a map schema instance.
func NewMapSchema(values Schema) *MapSchema {
	return &MapSchema{
		values: values,
	}
}

// Type returns the type of the schema.
func (s *MapSchema) Type() Type {
	return Map
}

// Values returns the values schema of a map.
func (s *MapSchema) Values() Schema {
	return s.values
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *MapSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// String returns the canonical form of the schema.
func (s *MapSchema) String() string {
	return `{"type":"map","values":` + s.values.String() + `}`
}

// UnionSchema is an Avro union type schema.
type UnionSchema struct {
	fingerprinter

	types Schemas
}

// NewUnionSchema creates a union schema instance.
func NewUnionSchema(types []Schema) (*UnionSchema, error) {
	seen := map[string]bool{}
	for _, schema := range types {
		if schema.Type() == Union {
			return nil, errors.New("avro: union type cannot be a union")
		}

		strType := string(schema.Type())
		if named, ok := schema.(NamedSchema); ok {
			strType = named.FullName()
		}

		if seen[strType] {
			return nil, errors.New("avro: union type must be unique")
		}
		seen[strType] = true
	}

	return &UnionSchema{
		types: Schemas(types),
	}, nil
}

// Type returns the type of the schema.
func (s *UnionSchema) Type() Type {
	return Union
}

// Types returns the types of a union.
func (s *UnionSchema) Types() Schemas {
	return s.types
}

// Nullable returns the Schema if the union is nullable, otherwise nil.
func (s *UnionSchema) Nullable() bool {
	if len(s.types) != 2 || s.types[0].Type() != Null {
		return false
	}

	return true
}

// String returns the canonical form of the schema.
func (s *UnionSchema) String() string {
	types := ""
	for _, typ := range s.types {
		types += typ.String() + ","
	}
	if len(types) > 0 {
		types = types[:len(types)-1]
	}

	return `[` + types + `]`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *UnionSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FixedSchema is an Avro fixed type schema.
type FixedSchema struct {
	name
	fingerprinter

	size int
}

// NewFixedSchema creates a new fixed schema instance.
func NewFixedSchema(name, namespace string, size int) (*FixedSchema, error) {
	n, err := newName(name, namespace)
	if err != nil {
		return nil, err
	}

	return &FixedSchema{
		name: n,
		size: size,
	}, nil
}

// Type returns the type of the schema.
func (s *FixedSchema) Type() Type {
	return Fixed
}

// Size returns the number of bytes of the fixed schema.
func (s *FixedSchema) Size() int {
	return s.size
}

// String returns the canonical form of the schema.
func (s *FixedSchema) String() string {
	size := strconv.Itoa(s.size)
	return `{"name":"` + s.FullName() + `","type":"fixed","size":` + size + `}`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *FixedSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// NullSchema is an Avro null type schema.
type NullSchema struct{}

// Type returns the type of the schema.
func (s *NullSchema) Type() Type {
	return Null
}

// String returns the canonical form of the schema.
func (s *NullSchema) String() string {
	return `"null"`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *NullSchema) Fingerprint() [32]byte {
	return [32]byte{0xf0, 0x72, 0xcb, 0xec, 0x3b, 0xf8, 0x84, 0x18, 0x71, 0xd4, 0x28, 0x42, 0x30, 0xc5, 0xe9, 0x83, 0xdc, 0x21, 0x1a, 0x56, 0x83, 0x7a, 0xed, 0x86, 0x24, 0x87, 0x14, 0x8f, 0x94, 0x7d, 0x1a, 0x1f}
}

// RefSchema is a reference to a named Avro schema.
type RefSchema struct {
	actual NamedSchema
}

// NewRefSchema creates a ref schema instance.
func NewRefSchema(schema NamedSchema) *RefSchema {
	return &RefSchema{
		actual: schema,
	}
}

// Type returns the type of the schema.
func (s *RefSchema) Type() Type {
	return Ref
}

// Schema returns the schema being referenced.
func (s *RefSchema) Schema() Schema {
	return s.actual
}

// String returns the canonical form of the schema.
func (s *RefSchema) String() string {
	return `"` + s.actual.FullName() + `"`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *RefSchema) Fingerprint() [32]byte {
	return s.actual.Fingerprint()
}

func invalidNameFirstChar(r rune) bool {
	return (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && r != '_'
}

func invalidNameOtherChar(r rune) bool {
	return invalidNameFirstChar(r) && (r < '0' || r > '9')
}

func validateName(name string) error {
	if len(name) == 0 {
		return errors.New("avro: name must be a non-empty")
	}

	if strings.IndexFunc(name[:1], invalidNameFirstChar) > -1 {
		return fmt.Errorf("avro: invalid name %s", name)
	}
	if strings.IndexFunc(name[1:], invalidNameOtherChar) > -1 {
		return fmt.Errorf("avro: invalid name %s", name)
	}

	return nil
}

func validateDefault(name string, schema Schema, def interface{}) (interface{}, error) {
	if def == nil {
		return nil, nil
	}

	def, ok := isValidDefault(schema, def)
	if !ok {
		return nil, fmt.Errorf("avro: invalid default for field %s. %+v not a %s", name, def, schema.Type())
	}

	return def, nil
}

func isValidDefault(schema Schema, def interface{}) (interface{}, bool) {
	switch schema.Type() {
	case Null:
		return nil, def == nil

	case String, Bytes, Enum, Fixed:
		if _, ok := def.(string); ok {
			return def, true
		}

	case Boolean:
		if _, ok := def.(bool); ok {
			return def, true
		}

	case Int:
		if i, ok := def.(int8); ok {
			return int(i), true
		}
		if i, ok := def.(int16); ok {
			return int(i), true
		}
		if i, ok := def.(int32); ok {
			return int(i), true
		}
		if _, ok := def.(int); ok {
			return def, true
		}
		if f, ok := def.(float64); ok {
			return int(f), true
		}

	case Long:
		if _, ok := def.(int64); ok {
			return def, true
		}
		if f, ok := def.(float64); ok {
			return int64(f), true
		}

	case Float:
		if _, ok := def.(float32); ok {
			return def, true
		}
		if f, ok := def.(float64); ok {
			return float32(f), true
		}

	case Double:
		if _, ok := def.(float64); ok {
			return def, true
		}

	case Array:
		arr, ok := def.([]interface{})
		if !ok {
			return nil, false
		}

		arrSchema := schema.(*ArraySchema)
		for i, v := range arr {
			v, ok := isValidDefault(arrSchema.Items(), v)
			if !ok {
				return nil, false
			}
			arr[i] = v
		}

		return arr, true

	case Map:
		m, ok := def.(map[string]interface{})
		if !ok {
			return nil, false
		}

		mapSchema := schema.(*MapSchema)
		for k, v := range m {
			v, ok := isValidDefault(mapSchema.Values(), v)
			if !ok {
				return nil, false
			}

			m[k] = v
		}

		return m, true

	case Union:
		unionSchema := schema.(*UnionSchema)
		return isValidDefault(unionSchema.Types()[0], def)

	case Record:
		m, ok := def.(map[string]interface{})
		if !ok {
			return nil, false
		}

		recordSchema := schema.(*RecordSchema)
		for _, field := range recordSchema.Fields() {
			fieldDef := field.Default()
			if newDef, ok := m[field.Name()]; ok {
				fieldDef = newDef
			}

			v, ok := isValidDefault(field.Type(), fieldDef)
			if !ok {
				return nil, false
			}

			m[field.Name()] = v
		}

		return m, true
	}

	return nil, false
}
