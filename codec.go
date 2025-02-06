package avro

import (
	"fmt"
	"math/big"
	"reflect"
	"time"
	"unsafe"

	"github.com/modern-go/reflect2"
)

var (
	timeType         = reflect.TypeOf(time.Time{})
	timeDurationType = reflect.TypeOf(time.Duration(0))
	ratType          = reflect.TypeOf(big.Rat{})
	durType          = reflect.TypeOf(LogicalDuration{})
)

type null struct{}

// ValDecoder represents an internal value decoder.
//
// You should never use ValDecoder directly.
type ValDecoder interface {
	Decode(ptr unsafe.Pointer, r *Reader)
}

// ValEncoder represents an internal value encoder.
//
// You should never use ValEncoder directly.
type ValEncoder interface {
	Encode(ptr unsafe.Pointer, w *Writer)
}

// ReadVal parses Avro value and stores the result in the value pointed to by obj.
func (r *Reader) ReadVal(schema Schema, obj any) {
	decoder := r.cfg.getDecoderFromCache(schema.CacheFingerprint(), reflect2.RTypeOf(obj))
	if decoder == nil {
		typ := reflect2.TypeOf(obj)
		if typ.Kind() != reflect.Ptr {
			r.ReportError("ReadVal", "can only unmarshal into pointer")
			return
		}
		decoder = r.cfg.DecoderOf(schema, typ)
	}

	ptr := reflect2.PtrOf(obj)
	if ptr == nil {
		r.ReportError("ReadVal", "can not read into nil pointer")
		return
	}

	decoder.Decode(ptr, r)
}

// WriteVal writes the Avro encoding of obj.
func (w *Writer) WriteVal(schema Schema, val any) {
	encoder := w.cfg.getEncoderFromCache(schema.Fingerprint(), reflect2.RTypeOf(val))
	if encoder == nil {
		typ := reflect2.TypeOf(val)
		encoder = w.cfg.EncoderOf(schema, typ)
	}
	encoder.Encode(reflect2.PtrOf(val), w)
}

func (c *frozenConfig) DecoderOf(schema Schema, typ reflect2.Type) ValDecoder {
	rtype := typ.RType()
	decoder := c.getDecoderFromCache(schema.CacheFingerprint(), rtype)
	if decoder != nil {
		return decoder
	}

	ptrType := typ.(*reflect2.UnsafePtrType)
	decoder = decoderOfType(newDecoderContext(c), schema, ptrType.Elem())
	c.addDecoderToCache(schema.CacheFingerprint(), rtype, decoder)
	return decoder
}

type deferDecoder struct {
	decoder ValDecoder
}

func (d *deferDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	d.decoder.Decode(ptr, r)
}

type deferEncoder struct {
	encoder ValEncoder
}

func (d *deferEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	d.encoder.Encode(ptr, w)
}

type decoderContext struct {
	cfg      *frozenConfig
	decoders map[cacheKey]ValDecoder
}

func newDecoderContext(cfg *frozenConfig) *decoderContext {
	return &decoderContext{
		cfg:      cfg,
		decoders: make(map[cacheKey]ValDecoder),
	}
}

type encoderContext struct {
	cfg      *frozenConfig
	encoders map[cacheKey]ValEncoder
}

func newEncoderContext(cfg *frozenConfig) *encoderContext {
	return &encoderContext{
		cfg:      cfg,
		encoders: make(map[cacheKey]ValEncoder),
	}
}

//nolint:dupl
func decoderOfType(d *decoderContext, schema Schema, typ reflect2.Type) ValDecoder {
	if dec := createDecoderOfMarshaler(schema, typ); dec != nil {
		return dec
	}

	// Handle eface (empty interface) case when it isn't a union
	if typ.Kind() == reflect.Interface && schema.Type() != Union {
		if _, ok := typ.(*reflect2.UnsafeIFaceType); !ok {
			return newEfaceDecoder(d, schema)
		}
	}

	switch schema.Type() {
	case Null:
		return &nullCodec{}
	case String, Bytes, Int, Long, Float, Double, Boolean:
		return createDecoderOfNative(schema.(*PrimitiveSchema), typ)
	case Record:
		key := cacheKey{fingerprint: schema.CacheFingerprint(), rtype: typ.RType()}
		defDec := &deferDecoder{}
		d.decoders[key] = defDec
		defDec.decoder = createDecoderOfRecord(d, schema.(*RecordSchema), typ)
		return defDec.decoder
	case Ref:
		key := cacheKey{fingerprint: schema.(*RefSchema).Schema().CacheFingerprint(), rtype: typ.RType()}
		if dec, f := d.decoders[key]; f {
			return dec
		}
		return decoderOfType(d, schema.(*RefSchema).Schema(), typ)
	case Enum:
		return createDecoderOfEnum(schema.(*EnumSchema), typ)
	case Array:
		return createDecoderOfArray(d, schema.(*ArraySchema), typ)
	case Map:
		return createDecoderOfMap(d, schema.(*MapSchema), typ)
	case Union:
		return createDecoderOfUnion(d, schema.(*UnionSchema), typ)
	case Fixed:
		return createDecoderOfFixed(schema.(*FixedSchema), typ)
	default:
		// It is impossible to get here with a valid schema
		return &errorDecoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

func (c *frozenConfig) EncoderOf(schema Schema, typ reflect2.Type) ValEncoder {
	if typ == nil {
		typ = reflect2.TypeOf((*null)(nil))
	}

	rtype := typ.RType()
	encoder := c.getEncoderFromCache(schema.Fingerprint(), rtype)
	if encoder != nil {
		return encoder
	}

	encoder = encoderOfType(newEncoderContext(c), schema, typ)
	if typ.LikePtr() {
		encoder = &onePtrEncoder{encoder}
	}
	c.addEncoderToCache(schema.Fingerprint(), rtype, encoder)
	return encoder
}

type onePtrEncoder struct {
	enc ValEncoder
}

func (e *onePtrEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	e.enc.Encode(noescape(unsafe.Pointer(&ptr)), w)
}

//nolint:dupl
func encoderOfType(e *encoderContext, schema Schema, typ reflect2.Type) ValEncoder {
	if enc := createEncoderOfMarshaler(schema, typ); enc != nil {
		return enc
	}

	if typ.Kind() == reflect.Interface {
		return &interfaceEncoder{schema: schema, typ: typ}
	}

	switch schema.Type() {
	case Null:
		return &nullCodec{}
	case String, Bytes, Int, Long, Float, Double, Boolean:
		return createEncoderOfNative(schema.(*PrimitiveSchema), typ)
	case Record:
		key := cacheKey{fingerprint: schema.Fingerprint(), rtype: typ.RType()}
		defEnc := &deferEncoder{}
		e.encoders[key] = defEnc
		defEnc.encoder = createEncoderOfRecord(e, schema.(*RecordSchema), typ)
		return defEnc.encoder
	case Ref:
		key := cacheKey{fingerprint: schema.(*RefSchema).Schema().Fingerprint(), rtype: typ.RType()}
		if enc, f := e.encoders[key]; f {
			return enc
		}
		return encoderOfType(e, schema.(*RefSchema).Schema(), typ)
	case Enum:
		return createEncoderOfEnum(schema.(*EnumSchema), typ)
	case Array:
		return createEncoderOfArray(e, schema.(*ArraySchema), typ)
	case Map:
		return createEncoderOfMap(e, schema.(*MapSchema), typ)
	case Union:
		return createEncoderOfUnion(e, schema.(*UnionSchema), typ)
	case Fixed:
		return createEncoderOfFixed(schema.(*FixedSchema), typ)
	default:
		// It is impossible to get here with a valid schema
		return &errorEncoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

type errorDecoder struct {
	err error
}

func (d *errorDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	if r.Error == nil {
		r.Error = d.err
	}
}

type errorEncoder struct {
	err error
}

func (e *errorEncoder) Encode(_ unsafe.Pointer, w *Writer) {
	if w.Error == nil {
		w.Error = e.err
	}
}
