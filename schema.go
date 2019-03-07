package avro

import (
	"crypto/sha256"
	"strconv"

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

		if namedSchema, ok := schema.(NamedSchema); ok && namedSchema.Name() == name {
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

	// Name returns the fill name of the schema.
	Name() string
}

// PrimitiveSchema is an Avro primitive type schema.
type PrimitiveSchema struct {
	typ Type

	fingerprint [32]byte
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
	if s.fingerprint != emptyFgpt {
		return s.fingerprint
	}

	s.fingerprint = sha256.Sum256([]byte(s.String()))
	return s.fingerprint
}

// RecordSchema is an Avro record type schema.
type RecordSchema struct {
	name   string
	fields []*Field

	fingerprint [32]byte
}

// Type returns the type of the schema.
func (s *RecordSchema) Type() Type {
	return Record
}

// Name implements the NamedSchema interface.
func (s *RecordSchema) Name() string {
	return s.name
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

	return  `{"name":"` + s.name + `","type":"record","fields":[` + fields + `]}`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *RecordSchema) Fingerprint() [32]byte {
	if s.fingerprint != emptyFgpt {
		return s.fingerprint
	}

	s.fingerprint = sha256.Sum256([]byte(s.String()))
	return s.fingerprint
}

// Field is an Avro record type field.
type Field struct {
	name string
	typ  Schema
	def  interface{}
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
	name    string
	symbols []string

	canonical   string
	fingerprint [32]byte
}

// Type returns the type of the schema.
func (s *EnumSchema) Type() Type {
	return Enum
}

// Name implements the NamedSchema interface.
func (s *EnumSchema) Name() string {
	return s.name
}

// Symbols returns the symbols of an enum.
func (s *EnumSchema) Symbols() []string {
	return s.symbols
}

// String returns the canonical form of the schema.
func (s *EnumSchema) String() string {
	if s.canonical != "" {
		return s.canonical
	}

	symbols := ""
	for _, sym := range s.symbols {
		symbols += `"` + sym + `",`
	}
	if len(symbols) > 0 {
		symbols = symbols[:len(symbols)-1]
	}

	s.canonical = `{"name":"` + s.name + `","type":"enum","symbols":[` + symbols + `]}`
	return s.canonical
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *EnumSchema) Fingerprint() [32]byte {
	if s.fingerprint != emptyFgpt {
		return s.fingerprint
	}

	s.fingerprint = sha256.Sum256([]byte(s.String()))
	return s.fingerprint
}

// ArraySchema is an Avro array type schema.
type ArraySchema struct {
	items Schema

	fingerprint [32]byte
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
	if s.fingerprint != emptyFgpt {
		return s.fingerprint
	}

	s.fingerprint = sha256.Sum256([]byte(s.String()))
	return s.fingerprint
}

// MapSchema is an Avro map type schema.
type MapSchema struct {
	values Schema

	fingerprint [32]byte
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
	if s.fingerprint != emptyFgpt {
		return s.fingerprint
	}

	s.fingerprint = sha256.Sum256([]byte(s.String()))
	return s.fingerprint
}

// String returns the canonical form of the schema.
func (s *MapSchema) String() string {
	return `{"type":"map","values":` + s.values.String() + `}`
}

// UnionSchema is an Avro union type schema.
type UnionSchema struct {
	types Schemas

	fingerprint [32]byte
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
	if s.fingerprint != emptyFgpt {
		return s.fingerprint
	}

	s.fingerprint = sha256.Sum256([]byte(s.String()))
	return s.fingerprint
}

// FixedSchema is an Avro fixed type schema.
type FixedSchema struct {
	name string
	size int

	fingerprint [32]byte
}

// Type returns the type of the schema.
func (s *FixedSchema) Type() Type {
	return Fixed
}

// Name implements the NamedSchema interface.
func (s *FixedSchema) Name() string {
	return s.name
}

// Size returns the number of bytes of the fixed schema.
func (s *FixedSchema) Size() int {
	return s.size
}

// String returns the canonical form of the schema.
func (s *FixedSchema) String() string {
	size := strconv.Itoa(s.size)
	return `{"name":"` + s.name + `","type":"fixed","size":` + size + `}`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *FixedSchema) Fingerprint() [32]byte {
	if s.fingerprint != emptyFgpt {
		return s.fingerprint
	}

	s.fingerprint = sha256.Sum256([]byte(s.String()))
	return s.fingerprint
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
	return `"` + s.actual.Name() + `"`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *RefSchema) Fingerprint() [32]byte {
	return s.actual.Fingerprint()
}
