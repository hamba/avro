package avro

import (
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"

	"github.com/hamba/avro/pkg/crc64"
	"github.com/modern-go/concurrent"
)

var emptyFgpt = [32]byte{}
var nullDefault = struct{}{}

// Type is a schema type
type Type string

// Schema type constants.
const (
	Record  Type = "record"
	Error   Type = "error"
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

// LogicalType is a schema logical type.
type LogicalType string

// Schema logical type constants.
const (
	Decimal         LogicalType = "decimal"
	UUID            LogicalType = "uuid"
	Date            LogicalType = "date"
	TimeMillis      LogicalType = "time-millis"
	TimeMicros      LogicalType = "time-micros"
	TimestampMillis LogicalType = "timestamp-millis"
	TimestampMicros LogicalType = "timestamp-micros"
	Duration        LogicalType = "duration"
)

// FingerprintType is a fingerprinting algorithm.
type FingerprintType string

// Fingerprint type constants.
const (
	CRC64Avro FingerprintType = "CRC64-AVRO"
	MD5       FingerprintType = "MD5"
	SHA256    FingerprintType = "SHA256"
)

var fingerprinters = map[FingerprintType]hash.Hash{
	CRC64Avro: crc64.New(),
	MD5:       md5.New(),
	SHA256:    sha256.New(),
}

// SchemaCache is a cache of schemas.
type SchemaCache struct {
	cache concurrent.Map // map[string]Schema
}

// Add adds a schema to the cache with the given name.
func (c *SchemaCache) Add(name string, schema Schema) {
	c.cache.Store(name, schema)
}

// Get returns the Schema if it exists.
func (c *SchemaCache) Get(name string) Schema {
	if v, ok := c.cache.Load(name); ok {
		return v.(Schema)
	}

	return nil
}

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

	// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
	FingerprintUsing(FingerprintType) ([]byte, error)
}

// LogicalSchema represents an Avro schema with a logical type.
type LogicalSchema interface {
	// Type returns the type of the logical schema.
	Type() LogicalType

	// String returns the canonical form of the logical schema.
	String() string
}

// PropertySchema represents a schema with properties.
type PropertySchema interface {
	// AddProp adds a property to the schema.
	//
	// AddProp will not overwrite existing properties.
	AddProp(name string, value interface{})

	// Prop gets a property from the schema.
	Prop(string) interface{}
}

// NamedSchema represents a schema with a name.
type NamedSchema interface {
	Schema
	PropertySchema

	// Name returns the name of the schema.
	Name() string

	// Namespace returns the namespace of a schema.
	Namespace() string

	// FullName returns the full qualified name of a schema.
	FullName() string
}

// LogicalTypeSchema represents a schema that can contain a logical type.
type LogicalTypeSchema interface {
	// Logical returns the logical schema or nil.
	Logical() LogicalSchema
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
	cache       map[FingerprintType][]byte
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (f *fingerprinter) Fingerprint(stringer fmt.Stringer) [32]byte {
	if f.fingerprint != emptyFgpt {
		return f.fingerprint
	}

	f.fingerprint = sha256.Sum256([]byte(stringer.String()))
	return f.fingerprint
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (f *fingerprinter) FingerprintUsing(typ FingerprintType, stringer fmt.Stringer) ([]byte, error) {
	if f.cache == nil {
		f.cache = map[FingerprintType][]byte{}
	}

	if b, ok := f.cache[typ]; ok {
		return b, nil
	}

	h, ok := fingerprinters[typ]
	if !ok {
		return nil, fmt.Errorf("avro: unknown fingerprint alogrithm %s", typ)
	}

	h.Reset()
	_, _ = h.Write([]byte(stringer.String()))
	fingerprint := h.Sum(make([]byte, 0, h.Size()))
	f.cache[typ] = fingerprint
	return fingerprint, nil
}

type properties struct {
	reserved []string
	props    map[string]interface{}
}

// AddProp adds a property to the schema.
//
// AddProp will not overwrite existing properties.
func (p *properties) AddProp(name string, value interface{}) {
	// Create the props is needed.
	if p.props == nil {
		p.props = map[string]interface{}{}
	}

	// Dont allow reserved properties
	for _, res := range p.reserved {
		if name == res {
			return
		}
	}

	// Dont overwrite a property
	if _, ok := p.props[name]; ok {
		return
	}

	p.props[name] = value
}

// Prop gets a property from the schema.
func (p *properties) Prop(name string) interface{} {
	if p.props == nil {
		return nil
	}

	return p.props[name]
}

// PrimitiveSchema is an Avro primitive type schema.
type PrimitiveSchema struct {
	fingerprinter

	typ     Type
	logical LogicalSchema
}

// NewPrimitiveSchema creates a new PrimitiveSchema.
func NewPrimitiveSchema(t Type, l LogicalSchema) *PrimitiveSchema {
	return &PrimitiveSchema{
		typ:     t,
		logical: l,
	}
}

// Type returns the type of the schema.
func (s *PrimitiveSchema) Type() Type {
	return s.typ
}

// Logical returns the logical schema or nil.
func (s *PrimitiveSchema) Logical() LogicalSchema {
	return s.logical
}

// String returns the canonical form of the schema.
func (s *PrimitiveSchema) String() string {
	if s.logical == nil {
		return `"` + string(s.typ) + `"`
	}

	return `{"type":"` + string(s.typ) + `",` + s.logical.String() + `}`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *PrimitiveSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *PrimitiveSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// RecordSchema is an Avro record type schema.
type RecordSchema struct {
	name
	properties
	fingerprinter

	isError bool
	fields  []*Field
}

// NewRecordSchema creates a new record schema instance.
func NewRecordSchema(name, space string, fields []*Field) (*RecordSchema, error) {
	n, err := newName(name, space)
	if err != nil {
		return nil, err
	}

	return &RecordSchema{
		name:       n,
		properties: properties{reserved: schemaReserved},
		fields:     fields,
	}, nil
}

// NewErrorRecordSchema creates a new error record schema instance.
func NewErrorRecordSchema(name, space string, fields []*Field) (*RecordSchema, error) {
	n, err := newName(name, space)
	if err != nil {
		return nil, err
	}

	return &RecordSchema{
		name:       n,
		properties: properties{reserved: schemaReserved},
		isError:    true,
		fields:     fields,
	}, nil
}

// Type returns the type of the schema.
func (s *RecordSchema) Type() Type {
	return Record
}

// IsError determines is this is an error record.
func (s *RecordSchema) IsError() bool {
	return s.isError
}

// Fields returns the fields of a record.
func (s *RecordSchema) Fields() []*Field {
	return s.fields
}

// String returns the canonical form of the schema.
func (s *RecordSchema) String() string {
	typ := "record"
	if s.isError {
		typ = "error"
	}

	fields := ""
	for _, f := range s.fields {
		fields += f.String() + ","
	}
	if len(fields) > 0 {
		fields = fields[:len(fields)-1]
	}

	return `{"name":"` + s.FullName() + `","type":"` + typ + `","fields":[` + fields + `]}`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *RecordSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *RecordSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// Field is an Avro record type field.
type Field struct {
	properties

	name   string
	typ    Schema
	hasDef bool
	def    interface{}
}

type noDef struct{}

// NoDefault is used when no default exists for a field.
var NoDefault = noDef{}

// NewField creates a new field instance.
func NewField(name string, typ Schema, def interface{}) (*Field, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}

	f := &Field{
		properties: properties{reserved: fieldReserved},
		name:       name,
		typ:        typ,
	}

	if def != NoDefault {
		def, err := validateDefault(name, typ, def)
		if err != nil {
			return nil, err
		}
		f.def = def
		f.hasDef = true
	}

	return f, nil
}

// Name returns the name of a field.
func (s *Field) Name() string {
	return s.name
}

// Type returns the schema of a field.
func (s *Field) Type() Schema {
	return s.typ
}

// HasDefault determines if the field has a default value.
func (s *Field) HasDefault() bool {
	return s.hasDef
}

// Default returns the default of a field or nil.
//
// The only time a nil default is valid is for a Null Type.
func (s *Field) Default() interface{} {
	if s.def == nullDefault {
		return nil
	}

	return s.def
}

// String returns the canonical form of a field.
func (s *Field) String() string {
	return `{"name":"` + s.name + `","type":` + s.typ.String() + `}`
}

// EnumSchema is an Avro enum type schema.
type EnumSchema struct {
	name
	properties
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
		name:       n,
		properties: properties{reserved: schemaReserved},
		symbols:    symbols,
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

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *EnumSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// ArraySchema is an Avro array type schema.
type ArraySchema struct {
	properties
	fingerprinter

	items Schema
}

// NewArraySchema creates an array schema instance.
func NewArraySchema(items Schema) *ArraySchema {
	return &ArraySchema{
		properties: properties{reserved: schemaReserved},
		items:      items,
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

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *ArraySchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// MapSchema is an Avro map type schema.
type MapSchema struct {
	properties
	fingerprinter

	values Schema
}

// NewMapSchema creates a map schema instance.
func NewMapSchema(values Schema) *MapSchema {
	return &MapSchema{
		properties: properties{reserved: schemaReserved},
		values:     values,
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

// String returns the canonical form of the schema.
func (s *MapSchema) String() string {
	return `{"type":"map","values":` + s.values.String() + `}`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *MapSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *MapSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
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

		strType := schemaTypeName(schema)

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
	if len(s.types) != 2 || s.types[0].Type() != Null && s.types[1].Type() != Null {
		return false
	}

	return true
}

// Indices returns the index of the null and type schemas for a
// nullable schema. For non-nullable schemas 0 is returned for
// both.
func (s *UnionSchema) Indices() (null int, typ int) {
	if !s.Nullable() {
		return 0, 0
	}
	if s.types[0].Type() == Null {
		return 0, 1
	}
	return 1, 0
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

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *UnionSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// FixedSchema is an Avro fixed type schema.
type FixedSchema struct {
	name
	properties
	fingerprinter

	size    int
	logical LogicalSchema
}

// NewFixedSchema creates a new fixed schema instance.
func NewFixedSchema(name, namespace string, size int, logical LogicalSchema) (*FixedSchema, error) {
	n, err := newName(name, namespace)
	if err != nil {
		return nil, err
	}

	return &FixedSchema{
		name:       n,
		properties: properties{reserved: schemaReserved},
		size:       size,
		logical:    logical,
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

// Logical returns the logical schema or nil.
func (s *FixedSchema) Logical() LogicalSchema {
	return s.logical
}

// String returns the canonical form of the schema.
func (s *FixedSchema) String() string {
	size := strconv.Itoa(s.size)

	var logical string
	if s.logical != nil {
		logical = "," + s.logical.String()
	}

	return `{"name":"` + s.FullName() + `","type":"fixed","size":` + size + logical + `}`
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *FixedSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *FixedSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// NullSchema is an Avro null type schema.
type NullSchema struct {
	fingerprinter
}

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
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *NullSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
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

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *RefSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.actual.FingerprintUsing(typ)
}

// PrimitiveLogicalSchema is a logical type with no properties.
type PrimitiveLogicalSchema struct {
	typ LogicalType
}

// NewPrimitiveLogicalSchema creates a new primitive logical schema instance.
func NewPrimitiveLogicalSchema(typ LogicalType) *PrimitiveLogicalSchema {
	return &PrimitiveLogicalSchema{
		typ: typ,
	}
}

// Type returns the type of the logical schema.
func (s *PrimitiveLogicalSchema) Type() LogicalType {
	return s.typ
}

// String returns the canonical form of the logical schema.
func (s *PrimitiveLogicalSchema) String() string {
	return `"logicalType":"` + string(s.typ) + `"`
}

// DecimalLogicalSchema is a decimal logical type.
type DecimalLogicalSchema struct {
	prec  int
	scale int
}

// NewDecimalLogicalSchema creates a new decimal logical schema instance.
func NewDecimalLogicalSchema(prec, scale int) *DecimalLogicalSchema {
	return &DecimalLogicalSchema{
		prec:  prec,
		scale: scale,
	}
}

// Type returns the type of the logical schema.
func (s *DecimalLogicalSchema) Type() LogicalType {
	return Decimal
}

// Precision returns the precision of the decimal logical schema.
func (s *DecimalLogicalSchema) Precision() int {
	return s.prec
}

// Scale returns the scale of the decimal logical schema.
func (s *DecimalLogicalSchema) Scale() int {
	return s.scale
}

// String returns the canonical form of the logical schema.
func (s *DecimalLogicalSchema) String() string {
	var scale string
	if s.scale > 0 {
		scale = `,"scale":` + strconv.Itoa(s.scale)
	}
	precision := strconv.Itoa(s.prec)

	return `"logicalType":"` + string(Decimal) + `","precision":` + precision + scale
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
		if schema.Type() != Null && !(schema.Type() == Union && schema.(*UnionSchema).Nullable()) {
			// This is an empty default value.
			return nil, nil
		}
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
		return nullDefault, def == nil

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

func schemaTypeName(schema Schema) string {
	if n, ok := schema.(NamedSchema); ok {
		return n.FullName()
	}

	return string(schema.Type())
}
