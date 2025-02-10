package avro

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hamba/avro/v2/pkg/crc64"
	jsoniter "github.com/json-iterator/go"
)

type nullDefaultType struct{}

func (nullDefaultType) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

var nullDefault nullDefaultType = struct{}{}

var (
	// Note: order matches the order of properties as they are named in the spec.
	// 	https://avro.apache.org/docs/1.12.0/specification
	recordReserved               = []string{"type", "name", "namespace", "doc", "aliases", "fields"}
	fieldReserved                = []string{"name", "doc", "type", "order", "aliases", "default"}
	enumReserved                 = []string{"type", "name", "namespace", "aliases", "doc", "symbols", "default"}
	arrayReserved                = []string{"type", "items"}
	mapReserved                  = []string{"type", "values"}
	fixedReserved                = []string{"type", "name", "namespace", "aliases", "size"}
	fixedWithLogicalTypeReserved = []string{"type", "name", "namespace", "aliases", "size", "logicalType"}
	fixedWithDecimalTypeReserved = []string{
		"type", "name", "namespace", "aliases", "size", "logicalType", "precision", "scale",
	}
	primitiveReserved                = []string{"type"}
	primitiveWithLogicalTypeReserved = []string{"type", "logicalType"}
	primitiveWithDecimalTypeReserved = []string{"type", "logicalType", "precision", "scale"}
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
	Decimal              LogicalType = "decimal"
	UUID                 LogicalType = "uuid"
	Date                 LogicalType = "date"
	TimeMillis           LogicalType = "time-millis"
	TimeMicros           LogicalType = "time-micros"
	TimestampMillis      LogicalType = "timestamp-millis"
	TimestampMicros      LogicalType = "timestamp-micros"
	LocalTimestampMillis LogicalType = "local-timestamp-millis"
	LocalTimestampMicros LogicalType = "local-timestamp-micros"
	Duration             LogicalType = "duration"
)

// Action is a field action used during decoding process.
type Action string

// Action type constants.
const (
	FieldIgnore     Action = "ignore"
	FieldSetDefault Action = "set_default"
)

// FingerprintType is a fingerprinting algorithm.
type FingerprintType string

// Fingerprint type constants.
const (
	CRC64Avro   FingerprintType = "CRC64-AVRO"
	CRC64AvroLE FingerprintType = "CRC64-AVRO-LE"
	MD5         FingerprintType = "MD5"
	SHA256      FingerprintType = "SHA256"
)

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

// AddAll adds all schemas from the given cache to the current cache.
func (c *SchemaCache) AddAll(cache *SchemaCache) {
	if cache == nil {
		return
	}
	cache.cache.Range(func(key, value interface{}) bool {
		c.cache.Store(key, value)
		return true
	})
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

	// CacheFingerprint returns the unique identity of the schema.
	// This returns a unique identity for schemas resolved from a writer schema, otherwise it returns
	// the schemas Fingerprint.
	CacheFingerprint() [32]byte
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
	Prop(string) any
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

	// Aliases returns the full qualified aliases of a schema.
	Aliases() []string
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

	data := []byte(stringer.String())

	var fingerprint []byte
	switch typ {
	case CRC64Avro:
		h := crc64.Sum(data)
		fingerprint = h[:]
	case CRC64AvroLE:
		h := crc64.SumWithByteOrder(data, crc64.LittleEndian)
		fingerprint = h[:]
	case MD5:
		h := md5.Sum(data)
		fingerprint = h[:]
	case SHA256:
		h := sha256.Sum256(data)
		fingerprint = h[:]
	default:
		return nil, fmt.Errorf("avro: unknown fingerprint algorithm %s", typ)
	}

	f.cache.Store(typ, fingerprint)
	return fingerprint, nil
}

type cacheFingerprinter struct {
	writerFingerprint *[32]byte

	cache atomic.Value // [32]byte
}

// CacheFingerprint returns the SHA256 identity of the schema.
func (i *cacheFingerprinter) CacheFingerprint(schema Schema, fn func() []byte) [32]byte {
	if v := i.cache.Load(); v != nil {
		return v.([32]byte)
	}

	if i.writerFingerprint == nil {
		fp := schema.Fingerprint()
		i.cache.Store(fp)
		return fp
	}

	fp := schema.Fingerprint()
	d := append([]byte{}, fp[:]...)
	d = append(d, (*i.writerFingerprint)[:]...)
	if fn != nil {
		d = append(d, fn()...)
	}
	ident := sha256.Sum256(d)
	i.cache.Store(ident)
	return ident
}

type properties struct {
	props map[string]any
}

func newProperties(props map[string]any, res []string) properties {
	p := properties{props: map[string]any{}}
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
func (p properties) Prop(name string) any {
	if p.props == nil {
		return nil
	}

	return p.props[name]
}

// Props returns a map that contains all schema custom properties.
// Any accidental change to the returned map will directly modify the schema custom properties.
func (p properties) Props() map[string]any {
	return p.props
}

func (p properties) marshalPropertiesToJSON(buf *bytes.Buffer) error {
	sortedPropertyKeys := make([]string, 0, len(p.props))
	for k := range p.props {
		sortedPropertyKeys = append(sortedPropertyKeys, k)
	}
	sort.Strings(sortedPropertyKeys)
	for _, k := range sortedPropertyKeys {
		vv, err := jsoniter.Marshal(p.props[k])
		if err != nil {
			return err
		}
		kk, err := jsoniter.Marshal(k)
		if err != nil {
			return err
		}
		buf.WriteString(`,`)
		buf.Write(kk)
		buf.WriteString(`:`)
		buf.Write(vv)
	}
	return nil
}

type schemaConfig struct {
	aliases []string
	doc     string
	def     any
	order   Order
	props   map[string]any
	wfp     *[32]byte
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
func WithDefault(def any) SchemaOption {
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
func WithProps(props map[string]any) SchemaOption {
	return func(opts *schemaConfig) {
		opts.props = props
	}
}

func withWriterFingerprint(fp [32]byte) SchemaOption {
	return func(opts *schemaConfig) {
		opts.wfp = &fp
	}
}

func withWriterFingerprintIfResolved(fp [32]byte, resolved bool) SchemaOption {
	return func(opts *schemaConfig) {
		if resolved {
			opts.wfp = &fp
		}
	}
}

// PrimitiveSchema is an Avro primitive type schema.
type PrimitiveSchema struct {
	properties
	fingerprinter
	cacheFingerprinter

	typ     Type
	logical LogicalSchema

	// encodedType is the type of the encoded value, if it is different from the typ.
	// It's only used in the context of write-read schema resolution.
	encodedType Type
}

// NewPrimitiveSchema creates a new PrimitiveSchema.
func NewPrimitiveSchema(t Type, l LogicalSchema, opts ...SchemaOption) *PrimitiveSchema {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	reservedProps := primitiveReserved
	if l != nil {
		if l.Type() == Decimal {
			reservedProps = primitiveWithDecimalTypeReserved
		} else {
			reservedProps = primitiveWithLogicalTypeReserved
		}
	}
	return &PrimitiveSchema{
		properties:         newProperties(cfg.props, reservedProps),
		cacheFingerprinter: cacheFingerprinter{writerFingerprint: cfg.wfp},
		typ:                t,
		logical:            l,
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
	if s.logical == nil && len(s.props) == 0 {
		return jsoniter.Marshal(s.typ)
	}

	buf := new(bytes.Buffer)
	buf.WriteString(`{"type":"` + string(s.typ) + `"`)
	if s.logical != nil {
		buf.WriteString(`,"logicalType":"` + string(s.logical.Type()) + `"`)
		if d, ok := s.logical.(*DecimalLogicalSchema); ok {
			buf.WriteString(`,"precision":` + strconv.Itoa(d.prec))
			if d.scale > 0 {
				buf.WriteString(`,"scale":` + strconv.Itoa(d.scale))
			}
		}
	}
	if err := s.marshalPropertiesToJSON(buf); err != nil {
		return nil, err
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *PrimitiveSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *PrimitiveSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// CacheFingerprint returns unique identity of the schema.
func (s *PrimitiveSchema) CacheFingerprint() [32]byte {
	return s.cacheFingerprinter.CacheFingerprint(s, nil)
}

// RecordSchema is an Avro record type schema.
type RecordSchema struct {
	name
	properties
	fingerprinter
	cacheFingerprinter
	isError bool
	fields  []*Field
	doc     string
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
		name:               n,
		properties:         newProperties(cfg.props, recordReserved),
		cacheFingerprinter: cacheFingerprinter{writerFingerprint: cfg.wfp},
		fields:             fields,
		doc:                cfg.doc,
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

// Doc returns the documentation of a record.
func (s *RecordSchema) Doc() string {
	return s.doc
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

// MarshalJSON marshals the schema to json.
func (s *RecordSchema) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	buf.WriteString(`{"name":"` + s.full + `"`)
	if len(s.aliases) > 0 {
		aliasesJSON, err := jsoniter.Marshal(s.aliases)
		if err != nil {
			return nil, err
		}
		buf.WriteString(`,"aliases":`)
		buf.Write(aliasesJSON)
	}
	if s.doc != "" {
		docJSON, err := jsoniter.Marshal(s.doc)
		if err != nil {
			return nil, err
		}
		buf.WriteString(`,"doc":`)
		buf.Write(docJSON)
	}
	if s.isError {
		buf.WriteString(`,"type":"error"`)
	} else {
		buf.WriteString(`,"type":"record"`)
	}
	fieldsJSON, err := jsoniter.Marshal(s.fields)
	if err != nil {
		return nil, err
	}
	buf.WriteString(`,"fields":`)
	buf.Write(fieldsJSON)
	if err := s.marshalPropertiesToJSON(buf); err != nil {
		return nil, err
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *RecordSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *RecordSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// CacheFingerprint returns unique identity of the schema.
func (s *RecordSchema) CacheFingerprint() [32]byte {
	return s.cacheFingerprinter.CacheFingerprint(s, func() []byte {
		var defs []any
		for _, field := range s.fields {
			if !field.HasDefault() {
				continue
			}
			defs = append(defs, field.Default())
		}
		b, _ := jsoniter.Marshal(defs)
		return b
	})
}

// Field is an Avro record type field.
type Field struct {
	properties

	name    string
	aliases []string
	doc     string
	typ     Schema
	hasDef  bool
	def     any
	order   Order

	// action mainly used when decoding data that lack the field for schema evolution purposes.
	action Action
	// encodedDef mainly used when decoding data that lack the field for schema evolution purposes.
	// Its value remains empty unless the field's encodeDefault function is called.
	encodedDef atomic.Value
}

type noDef struct{}

// NoDefault is used when no default exists for a field.
var NoDefault = noDef{}

// NewField creates a new field instance.
func NewField(name string, typ Schema, opts ...SchemaOption) (*Field, error) {
	cfg := schemaConfig{def: NoDefault}
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

// HasDefault determines if the field has a default value.
func (f *Field) HasDefault() bool {
	return f.hasDef
}

// Default returns the default of a field or nil.
//
// The only time a nil default is valid is for a Null Type.
func (f *Field) Default() any {
	if f.def == nullDefault {
		return nil
	}

	return f.def
}

func (f *Field) encodeDefault(encode func(any) ([]byte, error)) ([]byte, error) {
	if v := f.encodedDef.Load(); v != nil {
		return v.([]byte), nil
	}
	if !f.hasDef {
		return nil, fmt.Errorf("avro: '%s' field must have a non-empty default value", f.name)
	}
	if encode == nil {
		return nil, fmt.Errorf("avro: failed to encode '%s' default value", f.name)
	}
	b, err := encode(f.Default())
	if err != nil {
		return nil, err
	}
	f.encodedDef.Store(b)

	return b, nil
}

// Doc returns the documentation of a field.
func (f *Field) Doc() string {
	return f.doc
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
	buf := new(bytes.Buffer)
	buf.WriteString(`{"name":"` + f.name + `"`)
	if len(f.aliases) > 0 {
		aliasesJSON, err := jsoniter.Marshal(f.aliases)
		if err != nil {
			return nil, err
		}
		buf.WriteString(`,"aliases":`)
		buf.Write(aliasesJSON)
	}
	if f.doc != "" {
		docJSON, err := jsoniter.Marshal(f.doc)
		if err != nil {
			return nil, err
		}
		buf.WriteString(`,"doc":`)
		buf.Write(docJSON)
	}
	typeJSON, err := jsoniter.Marshal(f.typ)
	if err != nil {
		return nil, err
	}
	buf.WriteString(`,"type":`)
	buf.Write(typeJSON)
	if f.hasDef {
		defaultValueJSON, err := jsoniter.Marshal(f.Default())
		if err != nil {
			return nil, err
		}
		buf.WriteString(`,"default":`)
		buf.Write(defaultValueJSON)
	}
	if f.order != "" && f.order != Asc {
		buf.WriteString(`,"order":"` + string(f.order) + `"`)
	}
	if err := f.marshalPropertiesToJSON(buf); err != nil {
		return nil, err
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

// EnumSchema is an Avro enum type schema.
type EnumSchema struct {
	name
	properties
	fingerprinter
	cacheFingerprinter

	symbols []string
	def     string
	doc     string

	// encodedSymbols is the symbols of the encoded value.
	// It's only used in the context of write-read schema resolution.
	encodedSymbols []string
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
			return nil, fmt.Errorf("avro: invalid symbol %q", sym)
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
		name:               n,
		properties:         newProperties(cfg.props, enumReserved),
		cacheFingerprinter: cacheFingerprinter{writerFingerprint: cfg.wfp},
		symbols:            symbols,
		def:                def,
		doc:                cfg.doc,
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

// Symbol returns the symbol for the given index.
// It might return the default value in the context of write-read schema resolution.
func (s *EnumSchema) Symbol(i int) (string, bool) {
	resolv := len(s.encodedSymbols) > 0
	symbols := s.symbols
	if resolv {
		// A different set of symbols is encoded.
		symbols = s.encodedSymbols
	}

	if i < 0 || i >= len(symbols) {
		return "", false
	}

	symbol := symbols[i]
	if resolv && !hasSymbol(s.symbols, symbol) {
		if !s.HasDefault() {
			return "", false
		}
		return s.Default(), true
	}
	return symbol, true
}

// Default returns the default of an enum or an empty string.
func (s *EnumSchema) Default() string {
	return s.def
}

// HasDefault determines if the schema has a default value.
func (s *EnumSchema) HasDefault() bool {
	return s.def != ""
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
	buf := new(bytes.Buffer)
	buf.WriteString(`{"name":"` + s.full + `"`)
	if len(s.aliases) > 0 {
		aliasesJSON, err := jsoniter.Marshal(s.aliases)
		if err != nil {
			return nil, err
		}
		buf.WriteString(`,"aliases":`)
		buf.Write(aliasesJSON)
	}
	if s.doc != "" {
		docJSON, err := jsoniter.Marshal(s.doc)
		if err != nil {
			return nil, err
		}
		buf.WriteString(`,"doc":`)
		buf.Write(docJSON)
	}
	buf.WriteString(`,"type":"enum"`)
	symbolsJSON, err := jsoniter.Marshal(s.symbols)
	if err != nil {
		return nil, err
	}
	buf.WriteString(`,"symbols":`)
	buf.Write(symbolsJSON)
	if s.def != "" {
		buf.WriteString(`,"default":"` + s.def + `"`)
	}
	if err := s.marshalPropertiesToJSON(buf); err != nil {
		return nil, err
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *EnumSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *EnumSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// CacheFingerprint returns unique identity of the schema.
func (s *EnumSchema) CacheFingerprint() [32]byte {
	return s.cacheFingerprinter.CacheFingerprint(s, func() []byte {
		if !s.HasDefault() {
			return []byte{}
		}
		return []byte(s.Default())
	})
}

// ArraySchema is an Avro array type schema.
type ArraySchema struct {
	properties
	fingerprinter
	cacheFingerprinter

	items Schema
}

// NewArraySchema creates an array schema instance.
func NewArraySchema(items Schema, opts ...SchemaOption) *ArraySchema {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	return &ArraySchema{
		properties:         newProperties(cfg.props, arrayReserved),
		cacheFingerprinter: cacheFingerprinter{writerFingerprint: cfg.wfp},
		items:              items,
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
	buf := new(bytes.Buffer)
	buf.WriteString(`{"type":"array"`)
	itemsJSON, err := jsoniter.Marshal(s.items)
	if err != nil {
		return nil, err
	}
	buf.WriteString(`,"items":`)
	buf.Write(itemsJSON)
	if err = s.marshalPropertiesToJSON(buf); err != nil {
		return nil, err
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *ArraySchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *ArraySchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// CacheFingerprint returns unique identity of the schema.
func (s *ArraySchema) CacheFingerprint() [32]byte {
	return s.cacheFingerprinter.CacheFingerprint(s, nil)
}

// MapSchema is an Avro map type schema.
type MapSchema struct {
	properties
	fingerprinter
	cacheFingerprinter

	values Schema
}

// NewMapSchema creates a map schema instance.
func NewMapSchema(values Schema, opts ...SchemaOption) *MapSchema {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	return &MapSchema{
		properties:         newProperties(cfg.props, mapReserved),
		cacheFingerprinter: cacheFingerprinter{writerFingerprint: cfg.wfp},
		values:             values,
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
	buf := new(bytes.Buffer)
	buf.WriteString(`{"type":"map"`)
	valuesJSON, err := jsoniter.Marshal(s.values)
	if err != nil {
		return nil, err
	}
	buf.WriteString(`,"values":`)
	buf.Write(valuesJSON)
	if err := s.marshalPropertiesToJSON(buf); err != nil {
		return nil, err
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *MapSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *MapSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// CacheFingerprint returns unique identity of the schema.
func (s *MapSchema) CacheFingerprint() [32]byte {
	return s.cacheFingerprinter.CacheFingerprint(s, nil)
}

// UnionSchema is an Avro union type schema.
type UnionSchema struct {
	fingerprinter
	cacheFingerprinter

	types Schemas
}

// NewUnionSchema creates a union schema instance.
func NewUnionSchema(types []Schema, opts ...SchemaOption) (*UnionSchema, error) {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}

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
		cacheFingerprinter: cacheFingerprinter{writerFingerprint: cfg.wfp},
		types:              types,
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

// CacheFingerprint returns unique identity of the schema.
func (s *UnionSchema) CacheFingerprint() [32]byte {
	return s.cacheFingerprinter.CacheFingerprint(s, nil)
}

// FixedSchema is an Avro fixed type schema.
type FixedSchema struct {
	name
	properties
	fingerprinter
	cacheFingerprinter

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

	reservedProps := fixedReserved
	if logical != nil {
		if logical.Type() == Decimal {
			reservedProps = fixedWithDecimalTypeReserved
		} else {
			reservedProps = fixedWithLogicalTypeReserved
		}
	}
	return &FixedSchema{
		name:               n,
		properties:         newProperties(cfg.props, reservedProps),
		cacheFingerprinter: cacheFingerprinter{writerFingerprint: cfg.wfp},
		size:               size,
		logical:            logical,
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
	buf := new(bytes.Buffer)
	buf.WriteString(`{"name":"` + s.full + `"`)
	if len(s.aliases) > 0 {
		aliasesJSON, err := jsoniter.Marshal(s.aliases)
		if err != nil {
			return nil, err
		}
		buf.WriteString(`,"aliases":`)
		buf.Write(aliasesJSON)
	}
	buf.WriteString(`,"type":"fixed"`)
	buf.WriteString(`,"size":` + strconv.Itoa(s.size))
	if s.logical != nil {
		buf.WriteString(`,"logicalType":"` + string(s.logical.Type()) + `"`)
		if d, ok := s.logical.(*DecimalLogicalSchema); ok {
			buf.WriteString(`,"precision":` + strconv.Itoa(d.prec))
			if d.scale > 0 {
				buf.WriteString(`,"scale":` + strconv.Itoa(d.scale))
			}
		}
	}
	if err := s.marshalPropertiesToJSON(buf); err != nil {
		return nil, err
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *FixedSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *FixedSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// CacheFingerprint returns unique identity of the schema.
func (s *FixedSchema) CacheFingerprint() [32]byte {
	return s.cacheFingerprinter.CacheFingerprint(s, nil)
}

// NullSchema is an Avro null type schema.
type NullSchema struct {
	properties
	fingerprinter
}

// NewNullSchema creates a new NullSchema.
func NewNullSchema(opts ...SchemaOption) *NullSchema {
	var cfg schemaConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	return &NullSchema{
		properties: newProperties(cfg.props, primitiveReserved),
	}
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
	if len(s.props) == 0 {
		return []byte(`"null"`), nil
	}
	buf := new(bytes.Buffer)
	buf.WriteString(`{"type":"null"`)
	if err := s.marshalPropertiesToJSON(buf); err != nil {
		return nil, err
	}
	buf.WriteString("}")
	return buf.Bytes(), nil
}

// Fingerprint returns the SHA256 fingerprint of the schema.
func (s *NullSchema) Fingerprint() [32]byte {
	return s.fingerprinter.Fingerprint(s)
}

// FingerprintUsing returns the fingerprint of the schema using the given algorithm or an error.
func (s *NullSchema) FingerprintUsing(typ FingerprintType) ([]byte, error) {
	return s.fingerprinter.FingerprintUsing(typ, s)
}

// CacheFingerprint returns unique identity of the schema.
func (s *NullSchema) CacheFingerprint() [32]byte {
	return s.Fingerprint()
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
func (s *RefSchema) Schema() NamedSchema {
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

// CacheFingerprint returns unique identity of the schema.
func (s *RefSchema) CacheFingerprint() [32]byte {
	return s.actual.CacheFingerprint()
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

	if SkipNameValidation {
		return nil
	}

	if strings.IndexFunc(name[:1], invalidNameFirstChar) > -1 {
		return fmt.Errorf("invalid name %s", name)
	}
	if strings.IndexFunc(name[1:], invalidNameOtherChar) > -1 {
		return fmt.Errorf("invalid name %s", name)
	}

	return nil
}

func validateDefault(name string, schema Schema, def any) (any, error) {
	def, ok := isValidDefault(schema, def)
	if !ok {
		return nil, fmt.Errorf("avro: invalid default for field %s. %+v not a %s", name, def, schema.Type())
	}
	return def, nil
}

func isValidDefault(schema Schema, def any) (any, bool) {
	switch schema.Type() {
	case Ref:
		ref := schema.(*RefSchema)
		return isValidDefault(ref.Schema(), def)
	case Null:
		return nullDefault, def == nil
	case Enum:
		v, ok := def.(string)
		if !ok || len(v) == 0 {
			return def, false
		}

		var found bool
		for _, sym := range schema.(*EnumSchema).symbols {
			if def == sym {
				found = true
				break
			}
		}
		return def, found
	case String:
		if _, ok := def.(string); ok {
			return def, true
		}
	case Bytes, Fixed:
		// Spec: Default values for bytes and fixed fields are JSON strings,
		// where Unicode code points 0-255 are mapped to unsigned 8-bit byte values 0-255.
		if d, ok := def.(string); ok {
			if b, ok := isValidDefaultBytes(d); ok {
				if schema.Type() == Fixed {
					return byteSliceToArray(b, schema.(*FixedSchema).Size()), true
				}
				return b, true
			}
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
		arr, ok := def.([]any)
		if !ok {
			return nil, false
		}

		as := schema.(*ArraySchema)
		for i, v := range arr {
			v, ok := isValidDefault(as.Items(), v)
			if !ok {
				return nil, false
			}
			arr[i] = v
		}
		return arr, true
	case Map:
		m, ok := def.(map[string]any)
		if !ok {
			return nil, false
		}

		ms := schema.(*MapSchema)
		for k, v := range m {
			v, ok := isValidDefault(ms.Values(), v)
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
		m, ok := def.(map[string]any)
		if !ok {
			return nil, false
		}

		for _, field := range schema.(*RecordSchema).Fields() {
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

	sname := string(schema.Type())
	if lt := getLogicalType(schema); lt != "" {
		sname += "." + string(lt)
	}
	return sname
}

func isValidDefaultBytes(def string) ([]byte, bool) {
	runes := []rune(def)
	l := len(runes)
	b := make([]byte, l)
	for i := range l {
		if runes[i] < 0 || runes[i] > 255 {
			return nil, false
		}
		b[i] = byte(runes[i])
	}
	return b, true
}
