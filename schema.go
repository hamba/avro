package avro

import (
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hamba/avro/pkg/crc64"
	jsoniter "github.com/json-iterator/go"
)

var nullDefault = struct{}{}

var (
	schemaReserved = []string{
		"doc", "fields", "items", "name", "namespace", "size", "symbols",
		"values", "type", "aliases", "logicalType", "precision", "scale",
	}
	fieldReserved = []string{"default", "doc", "name", "order", "type", "aliases"}
)

// Type is a schema type.
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

// Order is a field order.
type Order string

// Field orders.
const (
	Asc    Order = "ascending"
	Desc   Order = "descending"
	Ignore Order = "ignore"
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
	cache sync.Map // map[string]Schema
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
		if schemaTypeName(schema) == name {
			return schema, i
		}
	}

	return nil, -1
}

// Schema represents an Avro schema.
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
	name      string
	namespace string
	full      string
	aliases   []string
}

func newName(n, ns string, aliases []string) (name, error) {
	if idx := strings.LastIndexByte(n, '.'); idx > -1 {
		ns = n[:idx]
		n = n[idx+1:]
	}

	full := n
	if ns != "" {
		full = ns + "." + n
	}

	for _, part := range strings.Split(full, ".") {
		if err := validateName(part); err != nil {
			return name{}, fmt.Errorf("avro: invalid name part %q in name %q: %w", full, part, err)
		}
	}

	a := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		if !strings.Contains(alias, ".") {
			if err := validateName(alias); err != nil {
				return name{}, fmt.Errorf("avro: invalid name %q: %w", alias, err)
			}
			if ns == "" {
				a = append(a, alias)
				continue
			}
			a = append(a, ns+"."+alias)
			continue
		}

		for _, part := range strings.Split(alias, ".") {
			if err := validateName(part); err != nil {
				return name{}, fmt.Errorf("avro: invalid name part %q in name %q: %w", full, part, err)
			}
		}
		a = append(a, alias)
	}

	return name{
		name:      n,
		namespace: ns,
		full:      full,
		aliases:   a,
	}, nil
}

// Name returns the name of a schema.
func (n name) Name() string {
	return n.name
}

// Namespace returns the namespace of a schema.
func (n name) Namespace() string {
	return n.namespace
}

// FullName returns the fully qualified name of a schema.
func (n name) FullName() string {
	return n.full
}

// Aliases returns the fully qualified aliases of a schema.
func (n name) Aliases() []string {
	return n.aliases
}

type fingerprinter struct {
	fingerprint atomic.Value // [32]byte
	cache       sync.Map     // map[FingerprintType][]byte
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (f *fingerprinter) Fingerprint(stringer fmt.Stringer) [32]byte {
	if v := f.fingerprint.Load(); v != nil {
		return v.([32]byte)
	}

	fingerprint := sha256.Sum256([]byte(stringer.String()))
	f.fingerprint.Store(fingerprint)
	return fingerprint
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (f *fingerprinter) FingerprintUsing(typ FingerprintType, stringer fmt.Stringer) ([]byte, error) {
	if v, ok := f.cache.Load(typ); ok {
		return v.([]byte), nil
	}

	h, ok := fingerprinters[typ]
	if !ok {
		return nil, fmt.Errorf("avro: unknown fingerprint algorithm %s", typ)
	}

	h.Reset()
	_, _ = h.Write([]byte(stringer.String()))
	fingerprint := h.Sum(make([]byte, 0, h.Size()))
	f.cache.Store(typ, fingerprint)
	return fingerprint, nil
}

type properties struct {
	props map[string]interface{}
}

func newProperties(props map[string]interface{}, res []string) properties {
	p := properties{props: map[string]interface{}{}}
	for k, v := range props {
		if isReserved(res, k) {
			continue
		}
		p.props[k] = v
	}
	return p
}

func isReserved(res []string, k string) bool {
	for _, r := range res {
		if k == r {
			return true
		}
	}
	return false
}

// Prop gets a property from the schema.
func (p properties) Prop(name string) interface{} {
	if p.props == nil {
		return nil
	}

	return p.props[name]
}

type schemaConfig struct {
	aliases []string
	doc     string
	def     interface{}
	order   Order
	props   map[string]interface{}
}

// SchemaOption is a function that sets a schema option.
type SchemaOption func(*schemaConfig)

// WithAliases sets the aliases on a schema.
func WithAliases(aliases []string) SchemaOption {
	return func(opts *schemaConfig) {
		opts.aliases = aliases
	}
}

// WithDoc sets the doc on a schema.
func WithDoc(doc string) SchemaOption {
	return func(opts *schemaConfig) {
		opts.doc = doc
	}
}

// WithDefault sets the default on a schema.
func WithDefault(def interface{}) SchemaOption {
	return func(opts *schemaConfig) {
		opts.def = def
	}
}

// WithOrder sets the order on a schema.
func WithOrder(order Order) SchemaOption {
	return func(opts *schemaConfig) {
		opts.order = order
	}
}

// WithProps sets the properties on a schema.
func WithProps(props map[string]interface{}) SchemaOption {
	return func(opts *schemaConfig) {
		opts.props = props
	}
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

// MarshalJSON marshals the schema to json.
func (s *PrimitiveSchema) MarshalJSON() ([]byte, error) {
	return []byte(s.String()), nil
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

	doc string
}

// NewRecordSchema creates a new record schema instance.
func NewRecordSchema(name, namespace string, fields []*Field, opts ...SchemaOption) (*RecordSchema, error) {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	n, err := newName(name, namespace, cfg.aliases)
	if err != nil {
		return nil, err
	}

	return &RecordSchema{
		name:       n,
		properties: newProperties(cfg.props, schemaReserved),
		fields:     fields,
		doc:        cfg.doc,
	}, nil
}

// NewErrorRecordSchema creates a new error record schema instance.
func NewErrorRecordSchema(name, namespace string, fields []*Field, opts ...SchemaOption) (*RecordSchema, error) {
	rec, err := NewRecordSchema(name, namespace, fields, opts...)
	if err != nil {
		return nil, err
	}

	rec.isError = true

	return rec, nil
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

// Doc returns the record doc.
func (s *RecordSchema) Doc() string {
	return s.doc
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

// MarshalJSON marshals the schema to json.
func (s *RecordSchema) MarshalJSON() ([]byte, error) {
	typ := "record"
	if s.isError {
		typ = "error"
	}

	ss := struct {
		Name    string   `json:"name"`
		Aliases []string `json:"aliases,omitempty"`
		Doc     string   `json:"doc,omitempty"`
		Type    string   `json:"type"`
		Fields  []*Field `json:"fields"`
	}{
		Name:    s.full,
		Aliases: s.aliases,
		Doc:     s.doc,
		Type:    typ,
		Fields:  s.fields,
	}

	return jsoniter.Marshal(ss)
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

	name    string
	aliases []string
	doc     string
	typ     Schema
	hasDef  bool
	def     interface{}
	order   Order
}

type noDef struct{}

// NoDefault is used when no default exists for a field.
var NoDefault = noDef{}

// NewField creates a new field instance.
func NewField(name string, typ Schema, opts ...SchemaOption) (*Field, error) {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	if err := validateName(name); err != nil {
		return nil, err
	}
	for _, a := range cfg.aliases {
		if err := validateName(a); err != nil {
			return nil, err
		}
	}

	switch cfg.order {
	case "":
		cfg.order = Asc
	case Asc, Desc, Ignore:
	default:
		return nil, fmt.Errorf("avro: field %q order %q is invalid", name, cfg.order)
	}

	f := &Field{
		properties: newProperties(cfg.props, fieldReserved),
		name:       name,
		aliases:    cfg.aliases,
		doc:        cfg.doc,
		typ:        typ,
		order:      cfg.order,
	}

	if cfg.def != NoDefault {
		def, err := validateDefault(name, typ, cfg.def)
		if err != nil {
			return nil, err
		}
		f.def = def
		f.hasDef = true
	}

	return f, nil
}

// Name returns the name of a field.
func (f *Field) Name() string {
	return f.name
}

// Aliases return the field aliases.
func (f *Field) Aliases() []string {
	return f.aliases
}

// Type returns the schema of a field.
func (f *Field) Type() Schema {
	return f.typ
}

// Doc returns the field doc.
func (f *Field) Doc() string {
	return f.doc
}

// HasDefault determines if the field has a default value.
func (f *Field) HasDefault() bool {
	return f.hasDef
}

// Default returns the default of a field or nil.
//
// The only time a nil default is valid is for a Null Type.
func (f *Field) Default() interface{} {
	if f.def == nullDefault {
		return nil
	}

	return f.def
}

// Order returns the field order.
func (f *Field) Order() Order {
	return f.order
}

// String returns the canonical form of a field.
func (f *Field) String() string {
	return `{"name":"` + f.name + `","type":` + f.typ.String() + `}`
}

// MarshalJSON marshals the schema to json.
func (f *Field) MarshalJSON() ([]byte, error) {
	s := struct {
		Name    string      `json:"name"`
		Aliases []string    `json:"aliases,omitempty"`
		Doc     string      `json:"doc,omitempty"`
		Type    Schema      `json:"type"`
		Default interface{} `json:"default,omitempty"`
		Order   string      `json:"order,omitempty"`
	}{
		Name:    f.name,
		Aliases: f.aliases,
		Doc:     f.doc,
		Type:    f.typ,
	}
	if f.hasDef {
		s.Default = f.def
	}
	if f.order != Asc {
		s.Order = string(f.order)
	}
	return jsoniter.Marshal(s)
}

// EnumSchema is an Avro enum type schema.
type EnumSchema struct {
	name
	properties
	fingerprinter

	symbols []string
	def     string

	doc string
}

// NewEnumSchema creates a new enum schema instance.
func NewEnumSchema(name, namespace string, symbols []string, opts ...SchemaOption) (*EnumSchema, error) {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	n, err := newName(name, namespace, cfg.aliases)
	if err != nil {
		return nil, err
	}

	if len(symbols) == 0 {
		return nil, errors.New("avro: enum must have a non-empty array of symbols")
	}
	for _, sym := range symbols {
		if err = validateName(sym); err != nil {
			return nil, fmt.Errorf("avro: invalid symnol %q", sym)
		}
	}

	var def string
	if d, ok := cfg.def.(string); ok && d != "" {
		if !hasSymbol(symbols, d) {
			return nil, fmt.Errorf("avro: symbol default %q must be a symbol", d)
		}
		def = d
	}

	return &EnumSchema{
		name:       n,
		properties: newProperties(cfg.props, schemaReserved),
		symbols:    symbols,
		def:        def,
		doc:        cfg.doc,
	}, nil
}

func hasSymbol(symbols []string, sym string) bool {
	for _, s := range symbols {
		if s == sym {
			return true
		}
	}
	return false
}

// Type returns the type of the schema.
func (s *EnumSchema) Type() Type {
	return Enum
}

// Doc returns the schema doc.
func (s *EnumSchema) Doc() string {
	return s.doc
}

// Symbols returns the symbols of an enum.
func (s *EnumSchema) Symbols() []string {
	return s.symbols
}

// Default returns the default of an enum or an empty string.
func (s *EnumSchema) Default() string {
	return s.def
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

// MarshalJSON marshals the schema to json.
func (s *EnumSchema) MarshalJSON() ([]byte, error) {
	ss := struct {
		Name    string   `json:"name"`
		Aliases []string `json:"aliases,omitempty"`
		Doc     string   `json:"doc,omitempty"`
		Type    string   `json:"type"`
		Symbols []string `json:"symbols"`
		Default string   `json:"default,omitempty"`
	}{
		Name:    s.full,
		Aliases: s.aliases,
		Doc:     s.doc,
		Type:    "enum",
		Symbols: s.symbols,
		Default: s.def,
	}
	return jsoniter.Marshal(ss)
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
func NewArraySchema(items Schema, opts ...SchemaOption) *ArraySchema {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	return &ArraySchema{
		properties: newProperties(cfg.props, schemaReserved),
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

// MarshalJSON marshals the schema to json.
func (s *ArraySchema) MarshalJSON() ([]byte, error) {
	ss := struct {
		Type  string `json:"type"`
		Items Schema `json:"items"`
	}{
		Type:  "array",
		Items: s.items,
	}
	return jsoniter.Marshal(ss)
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
func NewMapSchema(values Schema, opts ...SchemaOption) *MapSchema {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	return &MapSchema{
		properties: newProperties(cfg.props, schemaReserved),
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

// MarshalJSON marshals the schema to json.
func (s *MapSchema) MarshalJSON() ([]byte, error) {
	ss := struct {
		Type   string `json:"type"`
		Values Schema `json:"values"`
	}{
		Type:   "map",
		Values: s.values,
	}
	return jsoniter.Marshal(ss)
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
		types: types,
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
func (s *UnionSchema) Indices() (null, typ int) {
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

// MarshalJSON marshals the schema to json.
func (s *UnionSchema) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal(s.types)
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
func NewFixedSchema(
	name, namespace string,
	size int,
	logical LogicalSchema,
	opts ...SchemaOption,
) (*FixedSchema, error) {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	n, err := newName(name, namespace, cfg.aliases)
	if err != nil {
		return nil, err
	}

	return &FixedSchema{
		name:       n,
		properties: newProperties(cfg.props, schemaReserved),
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

// MarshalJSON marshals the schema to json.
func (s *FixedSchema) MarshalJSON() ([]byte, error) {
	ss := struct {
		Name    string   `json:"name"`
		Aliases []string `json:"aliases,omitempty"`
		Type    string   `json:"type"`
		Size    int      `json:"size"`
	}{
		Name:    s.full,
		Aliases: s.aliases,
		Type:    "fixed",
		Size:    s.size,
	}
	json, err := jsoniter.MarshalToString(ss)
	if err != nil {
		return nil, err
	}

	var logical string
	if s.logical != nil {
		logical = "," + s.logical.String()
	}

	return []byte(json[:len(json)-1] + logical + "}"), nil
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

// MarshalJSON marshals the schema to json.
func (s *NullSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"null"`), nil
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

// MarshalJSON marshals the schema to json.
func (s *RefSchema) MarshalJSON() ([]byte, error) {
	return []byte(`"` + s.actual.FullName() + `"`), nil
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
	if name == "" {
		return errors.New("name must be a non-empty")
	}

	if strings.IndexFunc(name[:1], invalidNameFirstChar) > -1 {
		return fmt.Errorf("invalid name %s", name)
	}
	if strings.IndexFunc(name[1:], invalidNameOtherChar) > -1 {
		return fmt.Errorf("invalid name %s", name)
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
	if schema.Type() == Ref {
		schema = schema.(*RefSchema).Schema()
	}

	if n, ok := schema.(NamedSchema); ok {
		return n.FullName()
	}

	name := string(schema.Type())
	if lt := getLogicalType(schema); lt != "" {
		name += "." + string(lt)
	}

	return name
}
