package avro

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"math/big"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDefaultDecoder(cfg *frozenConfig, schema Schema, def any, typ reflect2.Type) ValDecoder {
	if typ.Kind() == reflect.Interface {
		if schema.Type() != Union && schema.Type() != Null {
			return &efaceDefaultDecoder{def: def, schema: schema}
		}
	}

	switch schema.Type() {
	case Null:
		return &nullDefaultDecoder{
			typ: typ,
		}
	case Boolean:
		return &boolDefaultDecoder{
			def: def.(bool),
		}
	case Int:
		return &intDefaultDecoder{
			def: def.(int),
			typ: typ,
		}
	case Long:
		return &longDefaultDecoder{
			def: def.(int64),
			typ: typ,
		}
	case Float:
		return &floatDefaultDecoder{
			def: def.(float32),
			typ: typ,
		}
	case Double:
		return &doubleDefaultDecoder{
			def: def.(float64),
			typ: typ,
		}
	case String:
		if typ.Implements(textUnmarshalerType) {
			return &textDefaultMarshalerCodec{typ, def.(string)}
		}
		ptrType := reflect2.PtrTo(typ)
		if ptrType.Implements(textUnmarshalerType) {
			return &referenceDecoder{
				&textDefaultMarshalerCodec{typ: ptrType, def: def.(string)},
			}
		}
		return &stringDefaultDecoder{
			def: def.(string),
		}
	case Bytes:
		return &bytesDefaultDecoder{
			def: def.([]byte),
			typ: typ,
		}
	case Fixed:
		return &fixedDefaultDecoder{
			fixed: schema.(*FixedSchema),
			def:   def.([]byte),
			typ:   typ,
		}
	case Enum:
		return &enumDefaultDecoder{typ: typ, def: def.(string)}
	case Ref:
		return createDefaultDecoder(cfg, schema.(*RefSchema).Schema(), def, typ)
	case Record:
		return defaultDecoderOfRecord(cfg, schema, def, typ)
	case Array:
		return defaultDecoderOfArray(cfg, schema, def, typ)
	case Map:
		return defaultDecoderOfMap(cfg, schema, def, typ)
	case Union:
		return defaultDecoderOfUnion(schema.(*UnionSchema), def, typ)
	default:
		return &errorDecoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

type textDefaultMarshalerCodec struct {
	typ reflect2.Type
	def string
}

func (d textDefaultMarshalerCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	obj := d.typ.UnsafeIndirect(ptr)
	if reflect2.IsNil(obj) {
		ptrType := d.typ.(*reflect2.UnsafePtrType)
		newPtr := ptrType.Elem().UnsafeNew()
		*((*unsafe.Pointer)(ptr)) = newPtr
		obj = d.typ.UnsafeIndirect(ptr)
	}
	unmarshaler := (obj).(encoding.TextUnmarshaler)

	b := []byte(d.def)

	err := unmarshaler.UnmarshalText(b)
	if err != nil {
		r.ReportError("decode default textMarshalerCodec", err.Error())
	}
}

type efaceDefaultDecoder struct {
	def    any
	schema Schema
}

func (d *efaceDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	rPtr, rTyp, err := dynamicReceiver(d.schema, r.cfg.resolver)
	if err != nil {
		r.ReportError("decode default", err.Error())
		return
	}

	createDefaultDecoder(r.cfg, d.schema, d.def, rTyp).Decode(rPtr, r)

	*(*any)(ptr) = rTyp.UnsafeIndirect(rPtr)
}

type nullDefaultDecoder struct {
	typ reflect2.Type
}

func (d *nullDefaultDecoder) Decode(ptr unsafe.Pointer, _ *Reader) {
	*((*unsafe.Pointer)(ptr)) = nil
}

type boolDefaultDecoder struct {
	def bool
}

func (d *boolDefaultDecoder) Decode(ptr unsafe.Pointer, _ *Reader) {
	*((*bool)(ptr)) = d.def
}

type intDefaultDecoder struct {
	def int
	typ reflect2.Type
}

func (d *intDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	switch d.typ.Kind() {
	case reflect.Int:
		*((*int)(ptr)) = d.def
	case reflect.Uint:
		*((*uint)(ptr)) = uint(d.def)
	case reflect.Int8:
		*((*int8)(ptr)) = int8(d.def)
	case reflect.Uint8:
		*((*uint8)(ptr)) = uint8(d.def)
	case reflect.Int16:
		*((*int16)(ptr)) = int16(d.def)
	case reflect.Uint16:
		*((*uint16)(ptr)) = uint16(d.def)
	case reflect.Int32:
		*((*int32)(ptr)) = int32(d.def)
	default:
		r.ReportError("decode default", "unsupported type")
	}
}

type longDefaultDecoder struct {
	def int64
	typ reflect2.Type
}

func (d *longDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	switch d.typ.Kind() {
	case reflect.Int32:
		*((*int32)(ptr)) = int32(d.def)
	case reflect.Uint32:
		*((*uint32)(ptr)) = uint32(d.def)
	case reflect.Int64:
		*((*int64)(ptr)) = d.def
	default:
		r.ReportError("decode default", "unsupported type")
	}
}

type floatDefaultDecoder struct {
	def float32
	typ reflect2.Type
}

func (d *floatDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	switch d.typ.Kind() {
	case reflect.Float32:
		*((*float32)(ptr)) = d.def
	case reflect.Float64:
		*((*float64)(ptr)) = float64(d.def)
	default:
		r.ReportError("decode default", "unsupported type")
	}
}

type doubleDefaultDecoder struct {
	def float64
	typ reflect2.Type
}

func (d *doubleDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	switch d.typ.Kind() {
	case reflect.Float64:
		*((*float64)(ptr)) = d.def
	case reflect.Float32:
		*((*float32)(ptr)) = float32(d.def)
	default:
		r.ReportError("decode default", "unsupported type")
	}
}

type stringDefaultDecoder struct {
	def string
}

func (d *stringDefaultDecoder) Decode(ptr unsafe.Pointer, _ *Reader) {
	*((*string)(ptr)) = d.def
}

type bytesDefaultDecoder struct {
	def []byte
	typ reflect2.Type
}

func (d *bytesDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	if d.typ.Kind() != reflect.Slice {
		r.ReportError("decode default", "inconvertible type")
		return
	}
	if d.typ.(reflect2.SliceType).Elem().Kind() != reflect.Uint8 {
		r.ReportError("decode default", "inconvertible type")
		return
	}

	d.typ.(*reflect2.UnsafeSliceType).UnsafeSet(ptr, reflect2.PtrOf(d.def))
}

func defaultDecoderOfRecord(cfg *frozenConfig, schema Schema, def any, typ reflect2.Type) ValDecoder {
	rec := schema.(*RecordSchema)
	mDef, ok := def.(map[string]any)
	if !ok {
		return &errorDecoder{err: fmt.Errorf("avro: invalid default for record field")}
	}

	fields := make([]*Field, len(rec.Fields()))
	for i, field := range rec.Fields() {
		f, err := NewField(field.Name(), field.Type(),
			WithDefault(mDef[field.Name()]), WithAliases(field.Aliases()), WithOrder(field.Order()),
		)
		if err != nil {
			return &errorDecoder{err: fmt.Errorf("avro: %w", err)}
		}
		f.action = FieldSetDefault
		fields[i] = f
	}

	r, err := NewRecordSchema(rec.Name(), rec.Namespace(), fields, WithAliases(rec.Aliases()))
	if err != nil {
		return &errorDecoder{err: fmt.Errorf("avro: %w", err)}
	}

	switch typ.Kind() {
	case reflect.Struct:
		return decoderOfStruct(cfg, r, typ)
	case reflect.Map:
		return decoderOfRecord(cfg, r, typ)
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

type enumDefaultDecoder struct {
	typ reflect2.Type
	def string
}

func (d *enumDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	unmarshal := func(def string, isPtr bool) {
		var obj any
		if isPtr {
			obj = d.typ.PackEFace(ptr)
		} else {
			obj = d.typ.UnsafeIndirect(ptr)
		}
		if reflect2.IsNil(obj) {
			ptrType := d.typ.(*reflect2.UnsafePtrType)
			newPtr := ptrType.Elem().UnsafeNew()
			*((*unsafe.Pointer)(ptr)) = newPtr
			obj = d.typ.UnsafeIndirect(ptr)
		}
		unmarshaler := (obj).(encoding.TextUnmarshaler)
		err := unmarshaler.UnmarshalText([]byte(def))
		if err != nil {
			r.ReportError("decode default textMarshalerCodec", err.Error())
		}
	}

	switch {
	case d.typ.Kind() == reflect.String:
		*((*string)(ptr)) = d.def
		return
	case reflect2.PtrTo(d.typ).Implements(textUnmarshalerType):
		unmarshal(d.def, true)
		return
	case d.typ.Implements(textUnmarshalerType):
		unmarshal(d.def, false)
		return
	default:
		r.ReportError("decode default", "unsupported type")
	}
}

func defaultDecoderOfArray(cfg *frozenConfig, schema Schema, def any, typ reflect2.Type) ValDecoder {
	if typ.Kind() != reflect.Slice {
		return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
	}

	return &sliceDefaultDecoder{
		def: def.([]any),
		typ: typ.(*reflect2.UnsafeSliceType),
		decoder: func(def any) ValDecoder {
			return createDefaultDecoder(cfg, schema.(*ArraySchema).Items(), def, typ.(*reflect2.UnsafeSliceType).Elem())
		},
	}
}

type sliceDefaultDecoder struct {
	def     []any
	typ     *reflect2.UnsafeSliceType
	decoder func(def any) ValDecoder
}

func (d *sliceDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	size := len(d.def)
	d.typ.UnsafeGrow(ptr, size)
	for i := 0; i < size; i++ {
		elemPtr := d.typ.UnsafeGetIndex(ptr, i)
		d.decoder(d.def[i]).Decode(elemPtr, r)
	}
}

func defaultDecoderOfMap(cfg *frozenConfig, schema Schema, def any, typ reflect2.Type) ValDecoder {
	if typ.Kind() != reflect.Map {
		return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
	}

	return &mapDefaultDecoder{
		typ: typ.(*reflect2.UnsafeMapType),
		def: def.(map[string]any),
		decoder: func(def any) ValDecoder {
			return createDefaultDecoder(cfg, schema.(*MapSchema).Values(), def, typ.(*reflect2.UnsafeMapType).Elem())
		},
	}
}

type mapDefaultDecoder struct {
	typ     *reflect2.UnsafeMapType
	decoder func(def any) ValDecoder
	def     map[string]any
}

func (d *mapDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	if d.typ.UnsafeIsNil(ptr) {
		d.typ.UnsafeSet(ptr, d.typ.UnsafeMakeMap(0))
	}
	for k, v := range d.def {
		key := k
		keyPtr := reflect2.PtrOf(&key)
		elemPtr := d.typ.UnsafeNew()
		d.decoder(v).Decode(elemPtr, r)
		d.typ.UnsafeSetIndex(ptr, keyPtr, elemPtr)
	}
}

type fixedDefaultDecoder struct {
	typ   reflect2.Type
	def   []byte
	fixed *FixedSchema
}

func (d *fixedDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	l := len(d.def)
	switch d.typ.Kind() {
	case reflect.Array:
		arrayType := d.typ.(reflect2.ArrayType)
		if arrayType.Elem().Kind() != reflect.Uint8 || arrayType.Len() != d.fixed.Size() {
			r.ReportError("decode default", "unsupported type")
			return
		}
		if arrayType.Len() != l {
			r.ReportError("decode default", "invalid default")
			return
		}
		for i := 0; i < arrayType.Len(); i++ {
			arrayType.UnsafeSetIndex(ptr, i, reflect2.PtrOf(d.def[i]))
		}

	case reflect.Uint64:
		if d.fixed.Size() != 8 {
			r.ReportError("decode default", "unsupported type")
			return
		}
		if l != 8 {
			r.ReportError("decode default", "invalid default")
			return
		}
		*(*uint64)(ptr) = binary.BigEndian.Uint64(d.def)

	case reflect.Struct:
		ls := d.fixed.Logical()
		if ls == nil {
			break
		}
		typ1 := d.typ.Type1()
		switch {
		case typ1.ConvertibleTo(durType) && ls.Type() == Duration:
			if l != 12 {
				r.ReportError("decode default", "invalid default")
				return
			}
			*((*LogicalDuration)(ptr)) = durationFromBytes(d.def)

		case typ1.ConvertibleTo(ratType) && ls.Type() == Decimal:
			dec := ls.(*DecimalLogicalSchema)
			if d.fixed.Size() != l {
				r.ReportError("decode default", "invalid default")
				return
			}
			*((*big.Rat)(ptr)) = *ratFromBytes(d.def, dec.Scale())
		default:
			r.ReportError("decode default", "unsupported type")
		}

	default:
		r.ReportError("decode default", "unsupported type")
	}
}

func durationFromBytes(b []byte) LogicalDuration {
	var duration LogicalDuration

	duration.Months = binary.LittleEndian.Uint32(b[0:4])
	duration.Days = binary.LittleEndian.Uint32(b[4:8])
	duration.Milliseconds = binary.LittleEndian.Uint32(b[8:12])

	return duration
}

func defaultDecoderOfUnion(schema *UnionSchema, def any, typ reflect2.Type) ValDecoder {
	return &unionDefaultDecoder{
		typ:   typ,
		def:   def,
		union: schema,
	}
}

type unionDefaultDecoder struct {
	typ   reflect2.Type
	def   any
	union *UnionSchema
}

func (d *unionDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	switch d.typ.Kind() {
	case reflect.Map:
		if d.typ.(reflect2.MapType).Key().Kind() != reflect.String ||
			d.typ.(reflect2.MapType).Elem().Kind() != reflect.Interface {
			break
		}
		schema := d.union.Types()[0]
		if schema.Type() == Null {
			return
		}

		mapType := d.typ.(*reflect2.UnsafeMapType)
		if mapType.UnsafeIsNil(ptr) {
			mapType.UnsafeSet(ptr, mapType.UnsafeMakeMap(0))
		}

		key := schemaTypeName(schema)
		keyPtr := reflect2.PtrOf(key)
		elemPtr := mapType.Elem().UnsafeNew()

		decoder := createDefaultDecoder(r.cfg, d.union.Types()[0], d.def, mapType.Elem())
		decoder.Decode(elemPtr, r)

		mapType.UnsafeSetIndex(ptr, keyPtr, elemPtr)

	case reflect.Ptr:
		if !d.union.Nullable() {
			break
		}
		if d.union.Types()[0].Type() == Null {
			*((*unsafe.Pointer)(ptr)) = nil
			return
		}

		decoder := createDefaultDecoder(r.cfg, d.union.Types()[0], d.def, d.typ.(*reflect2.UnsafePtrType).Elem())
		if *((*unsafe.Pointer)(ptr)) == nil {
			newPtr := d.typ.UnsafeNew()
			decoder.Decode(newPtr, r)
			*((*unsafe.Pointer)(ptr)) = newPtr
			return
		}
		decoder.Decode(*((*unsafe.Pointer)(ptr)), r)

	case reflect.Interface:
		decoder := createDefaultDecoder(r.cfg, d.union.Types()[0], d.def, d.typ)
		decoder.Decode(ptr, r)
	}
}
