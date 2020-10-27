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
	timeRType uintptr
	ratRType  uintptr
)

func init() {
	timeRType = reflect2.TypeOf(time.Time{}).RType()
	ratRType = reflect2.TypeOf(big.Rat{}).RType()
}

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
func (r *Reader) ReadVal(schema Schema, obj interface{}) {
	rtype := reflect2.RTypeOf(obj)
	decoder := r.cfg.getDecoderFromCache(schema.Fingerprint(), rtype)
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
func (w *Writer) WriteVal(schema Schema, val interface{}) {
	rtype := reflect2.RTypeOf(val)
	encoder := w.cfg.getEncoderFromCache(schema.Fingerprint(), rtype)
	if encoder == nil {
		typ := reflect2.TypeOf(val)

		encoder = w.cfg.EncoderOf(schema, typ)
	}

	encoder.Encode(reflect2.PtrOf(val), w)
}

func (c *frozenConfig) DecoderOf(schema Schema, typ reflect2.Type) ValDecoder {
	rtype := typ.RType()
	decoder := c.getDecoderFromCache(schema.Fingerprint(), rtype)
	if decoder != nil {
		return decoder
	}

	ptrType := typ.(*reflect2.UnsafePtrType)
	decoder = decoderOfType(c, schema, ptrType.Elem())
	c.addDecoderToCache(schema.Fingerprint(), rtype, decoder)
	return decoder
}

func decoderOfType(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	if dec := createDecoderOfMarshaler(cfg, schema, typ); dec != nil {
		return dec
	}

	// Handle eface case when it isnt a union
	if typ.Kind() == reflect.Interface && schema.Type() != Union {
		if _, ok := typ.(*reflect2.UnsafeIFaceType); !ok {
			return &efaceDecoder{schema: schema}
		}
	}

	switch schema.Type() {
	case String, Bytes, Int, Long, Float, Double, Boolean:
		return createDecoderOfNative(schema, typ)

	case Record:
		return createDecoderOfRecord(cfg, schema, typ)

	case Ref:
		return decoderOfType(cfg, schema.(*RefSchema).Schema(), typ)

	case Enum:
		return createDecoderOfEnum(schema, typ)

	case Array:
		return createDecoderOfArray(cfg, schema, typ)

	case Map:
		return createDecoderOfMap(cfg, schema, typ)

	case Union:
		return createDecoderOfUnion(cfg, schema, typ)

	case Fixed:
		return createDecoderOfFixed(schema, typ)

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

	encoder = encoderOfType(c, schema, typ)
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

func encoderOfType(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	if enc := createEncoderOfMarshaler(cfg, schema, typ); enc != nil {
		return enc
	}

	if typ.Kind() == reflect.Interface {
		return &interfaceEncoder{schema: schema, typ: typ}
	}

	switch schema.Type() {
	case String, Bytes, Int, Long, Float, Double, Boolean, Null:
		return createEncoderOfNative(schema, typ)

	case Record:
		return createEncoderOfRecord(cfg, schema, typ)

	case Ref:
		return encoderOfType(cfg, schema.(*RefSchema).Schema(), typ)

	case Enum:
		return createEncoderOfEnum(schema, typ)

	case Array:
		return createEncoderOfArray(cfg, schema, typ)

	case Map:
		return createEncoderOfMap(cfg, schema, typ)

	case Union:
		return createEncoderOfUnion(cfg, schema, typ)

	case Fixed:
		return createEncoderOfFixed(schema, typ)

	default:
		// It is impossible to get here with a valid schema
		return &errorEncoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

type errorDecoder struct {
	err error
}

func (d *errorDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	if r.Error == nil {
		r.Error = d.err
	}
}

type errorEncoder struct {
	err error
}

func (e *errorEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	if w.Error == nil {
		w.Error = e.err
	}
}
