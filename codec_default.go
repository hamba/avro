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
			return &efaceDefaultDecoder{def: def}
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
			def: def.(string),
			typ: typ,
		}

	case Fixed:
		return &fixedDefaultDecoder{
			fixed: schema.(*FixedSchema),
			def:   def.(string),
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
		return createDefaultDecoder(cfg, schema.(*UnionSchema).Types()[0], def, typ)

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
		r.ReportError("textMarshalerCodec", err.Error())
	}
}

type efaceDefaultDecoder struct {
	def any
}

func (d *efaceDefaultDecoder) Decode(ptr unsafe.Pointer, _ *Reader) {
	*(*any)(ptr) = d.def
}

type boolDefaultDecoder struct {
	def bool
}

func (d *boolDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*bool)(ptr)) = d.def
}

type nullDefaultDecoder struct {
	typ reflect2.Type
}

func (d *nullDefaultDecoder) Decode(ptr unsafe.Pointer, _ *Reader) {
	if d.typ.IsNullable() {
		d.typ.UnsafeSet(ptr, d.typ.UnsafeNew())
	}
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

func (d *stringDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*string)(ptr)) = d.def
}

type bytesDefaultDecoder struct {
	def string
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

	runes := []rune(d.def)
	l := len(runes)
	b := make([]byte, l)
	for i := 0; i < l; i++ {
		if runes[i] < 0 || runes[i] > 255 {
			r.ReportError("decode default", "invalid default")
			return
		}
		b[i] = uint8(runes[i])
	}
	d.typ.(*reflect2.UnsafeSliceType).UnsafeSet(ptr, reflect2.PtrOf(b))
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
			r.ReportError("textMarshalerCodec", err.Error())
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
		d.decoder(d.def[i]).Decode(elemPtr, nil)
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
		d.decoder(v).Decode(elemPtr, nil)
		d.typ.UnsafeSetIndex(ptr, keyPtr, elemPtr)
	}
}

type fixedDefaultDecoder struct {
	typ   reflect2.Type
	def   string
	fixed *FixedSchema
}

func (d *fixedDefaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	runes := []rune(d.def)
	l := len(runes)
	b := make([]byte, l)
	for i := 0; i < l; i++ {
		if runes[i] < 0 || runes[i] > 255 {
			r.ReportError("decode default", "invalid default")
			return
		}
		b[i] = uint8(runes[i])
	}

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
			arrayType.UnsafeSetIndex(ptr, i, reflect2.PtrOf(b[i]))
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
		*(*uint64)(ptr) = binary.BigEndian.Uint64(b)

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
			*((*LogicalDuration)(ptr)) = durationFromBytes(b)

		case typ1.ConvertibleTo(ratType) && ls.Type() == Decimal:
			dec := ls.(*DecimalLogicalSchema)
			if d.fixed.Size() != l {
				r.ReportError("decode default", "invalid default")
				return
			}
			*((*big.Rat)(ptr)) = *ratFromBytes(b, dec.Scale())
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
