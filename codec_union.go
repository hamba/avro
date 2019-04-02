package avro

import (
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

var unionType = reflect2.TypeOfPtr((*UnionType)(nil)).Elem()

func createDecoderOfUnion(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	switch typ.Kind() {
	case reflect.Map:
		if typ.(reflect2.MapType).Key().Kind() != reflect.String ||
			typ.(reflect2.MapType).Elem().Kind() != reflect.Interface {
			break
		}
		return decoderOfMapUnion(cfg, schema, typ)

	case reflect.Ptr:
		if typ.Implements(unionType) {
			return decoderOfTypedUnion(schema, typ)
		}
		if !schema.(*UnionSchema).Nullable() {
			break
		}
		return decoderOfPtrUnion(cfg, schema, typ)
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfUnion(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	switch typ.Kind() {
	case reflect.Map:
		if typ.(reflect2.MapType).Key().Kind() != reflect.String ||
			typ.(reflect2.MapType).Elem().Kind() != reflect.Interface {
			break
		}
		return encoderOfMapUnion(cfg, schema, typ)

	case reflect.Ptr:
		if typ.Implements(unionType) {
			return encoderOfTypedUnion(schema, typ)
		}
		if !schema.(*UnionSchema).Nullable() {
			break
		}
		return encoderOfPtrUnion(cfg, schema, typ)
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func decoderOfMapUnion(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	union := schema.(*UnionSchema)
	mapType := typ.(*reflect2.UnsafeMapType)

	return &mapUnionDecoder{
		cfg:      cfg,
		schema:   union,
		mapType:  mapType,
		elemType: mapType.Elem(),
	}
}

type mapUnionDecoder struct {
	cfg      *frozenConfig
	schema   *UnionSchema
	mapType  *reflect2.UnsafeMapType
	elemType reflect2.Type
}

func (d *mapUnionDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	resSchema := getUnionSchema(d.schema, r)
	if resSchema == nil {
		return
	}

	// In a null case, just return
	if resSchema.Type() == Null {
		return
	}

	if d.mapType.UnsafeIsNil(ptr) {
		d.mapType.UnsafeSet(ptr, d.mapType.UnsafeMakeMap(0))
	}

	key := string(resSchema.Type())
	if n, ok := resSchema.(NamedSchema); ok {
		key = n.Name()
	}
	keyPtr := reflect2.PtrOf(key)

	elemPtr := d.elemType.UnsafeNew()
	decoderOfType(d.cfg, resSchema, d.elemType).Decode(elemPtr, r)

	d.mapType.UnsafeSetIndex(ptr, keyPtr, elemPtr)
}

func encoderOfMapUnion(cfg *frozenConfig, schema Schema, _ reflect2.Type) ValEncoder {
	union := schema.(*UnionSchema)

	return &mapUnionEncoder{
		cfg:    cfg,
		schema: union,
	}
}

type mapUnionEncoder struct {
	cfg    *frozenConfig
	schema *UnionSchema
}

func (e *mapUnionEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	m := *((*map[string]interface{})(ptr))

	if len(m) > 1 {
		w.Error = errors.New("avro: cannot encode union map with multiple entries")
		return
	}

	name := "null"
	val := interface{}(nil)
	for k, v := range m {
		name = k
		val = v
		break
	}

	schema, pos := e.schema.Types().Get(name)
	if schema == nil {
		w.Error = fmt.Errorf("avro: unknown union type %s", name)
		return
	}

	w.WriteLong(int64(pos))

	if schema.Type() == Null && val == nil {
		return
	}

	elemType := reflect2.TypeOf(val)
	elemPtr := reflect2.PtrOf(val)
	encoderOfType(e.cfg, schema, elemType).Encode(elemPtr, w)
}

func decoderOfTypedUnion(schema Schema, typ reflect2.Type) ValDecoder {
	union := schema.(*UnionSchema)
	ptrType := typ.(*reflect2.UnsafePtrType)
	elemType := ptrType.Elem()

	return &unionTypedDecoder{
		schema: union,
		typ:    ptrType,
		elem:   elemType,
	}
}

type unionTypedDecoder struct {
	schema *UnionSchema
	typ    reflect2.Type
	elem   reflect2.Type
}

func (d *unionTypedDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	if *((*unsafe.Pointer)(ptr)) == nil {
		newPtr := d.elem.UnsafeNew()
		*((*unsafe.Pointer)(ptr)) = newPtr
	}
	union := d.typ.UnsafeIndirect(ptr).(UnionType)

	schema := getUnionSchema(d.schema, r)
	if schema == nil {
		return
	}

	key := string(schema.Type())
	if n, ok := schema.(NamedSchema); ok {
		key = n.Name()
	}

	if err := union.SetType(key); err != nil {
		r.Error = err
		return
	}

	// In a null case, just return
	if schema.Type() == Null {
		return
	}

	if *union.Value() == nil {
		r.ReportError("decode union type", "can not read into nil pointer")
		return
	}
	r.ReadVal(schema, union.Value())
}

func encoderOfTypedUnion(schema Schema, typ reflect2.Type) ValEncoder {
	union := schema.(*UnionSchema)

	return &unionTypedEncoder{
		schema: union,
		typ:    typ,
	}
}

type unionTypedEncoder struct {
	schema *UnionSchema
	typ    reflect2.Type
}

func (e *unionTypedEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	union := e.typ.UnsafeIndirect(ptr).(UnionType)
	name, err := union.GetType()
	if err != nil {
		w.Error = err
		return
	}

	schema, pos := e.schema.Types().Get(name)
	if schema == nil {
		w.Error = fmt.Errorf("avro: unknown union type %s", name)
		return
	}

	w.WriteLong(int64(pos))

	val := *union.Value()
	if schema.Type() == Null && val == nil {
		return
	}

	w.WriteVal(schema, val)
}

func decoderOfPtrUnion(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	union := schema.(*UnionSchema)
	ptrType := typ.(*reflect2.UnsafePtrType)
	decoder := decoderOfType(cfg, union.Types()[1], ptrType.Elem())

	return &unionPtrDecoder{
		schema:  union,
		typ:     ptrType,
		decoder: decoder,
	}
}

type unionPtrDecoder struct {
	schema  *UnionSchema
	typ     *reflect2.UnsafePtrType
	decoder ValDecoder
}

func (d *unionPtrDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	schema := getUnionSchema(d.schema, r)
	if schema == nil {
		return
	}

	if schema.Type() == Null {
		*((*unsafe.Pointer)(ptr)) = nil
		return
	}

	newPtr := d.typ.UnsafeNew()
	d.decoder.Decode(newPtr, r)
	*((*unsafe.Pointer)(ptr)) = newPtr
}

func encoderOfPtrUnion(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	union := schema.(*UnionSchema)
	ptrType := typ.(*reflect2.UnsafePtrType)
	encoder := encoderOfType(cfg, union.Types()[1], ptrType.Elem())

	return &unionPtrEncoder{
		schema:  union,
		typ:     ptrType,
		encoder: encoder,
	}
}

type unionPtrEncoder struct {
	schema  *UnionSchema
	typ     *reflect2.UnsafePtrType
	encoder ValEncoder
}

func (e *unionPtrEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	if *((*unsafe.Pointer)(ptr)) == nil {
		w.WriteLong(0)
		return
	}

	w.WriteLong(1)
	e.encoder.Encode(*((*unsafe.Pointer)(ptr)), w)
}

func getUnionSchema(schema *UnionSchema, r *Reader) Schema {
	types := schema.Types()

	idx := int(r.ReadLong())
	if idx < 0 || idx > len(types)-1 {
		r.ReportError("decode union type", "unknown union type")
		return nil
	}

	return types[idx]
}
