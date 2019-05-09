package avro

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfUnion(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	switch typ.Kind() {
	case reflect.Map:
		if typ.(reflect2.MapType).Key().Kind() != reflect.String ||
			typ.(reflect2.MapType).Elem().Kind() != reflect.Interface {
			break
		}
		return decoderOfMapUnion(cfg, schema, typ)

	case reflect.Ptr:
		if !schema.(*UnionSchema).Nullable() {
			break
		}
		return decoderOfPtrUnion(cfg, schema, typ)

	case reflect.Interface:
		if _, ok := typ.(*reflect2.UnsafeIFaceType); !ok {
			return decoderOfResolvedUnion(cfg, schema)
		}
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
		if !schema.(*UnionSchema).Nullable() {
			break
		}
		return encoderOfPtrUnion(cfg, schema, typ)
	}

	return encoderOfResolverUnion(cfg, schema, typ)
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

	key := schemaTypeName(resSchema)
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

func decoderOfResolvedUnion(cfg *frozenConfig, schema Schema) ValDecoder {
	union := schema.(*UnionSchema)

	return &unionResolvedDecoder{
		cfg:          cfg,
		schema:       union,
		efaceDecoder: &efaceDecoder{schema: schema},
	}
}

type unionResolvedDecoder struct {
	cfg          *frozenConfig
	schema       *UnionSchema
	efaceDecoder *efaceDecoder
}

func (d *unionResolvedDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	schema := getUnionSchema(d.schema, r)
	if schema == nil {
		return
	}

	name := schemaTypeName(schema)
	switch schema.Type() {
	case Map:
		name += ":"
		valSchema := schema.(*MapSchema).Values()
		valName := schemaTypeName(valSchema)

		name += valName

	case Array:
		name += ":"
		itemSchema := schema.(*ArraySchema).Items()
		itemName := schemaTypeName(itemSchema)

		name += itemName
	}

	pObj := (*interface{})(ptr)
	obj := *pObj

	if schema.Type() == Null {
		*pObj = nil
		return
	}

	typ, err := d.cfg.resolver.Type(name)
	if err != nil {
		// We cannot resolve this, set it to the map type
		obj := map[string]interface{}{}
		obj[name] = r.ReadNext(schema)

		*pObj = obj
		return
	}

	if typ.Kind() != reflect.Ptr {
		var newObj interface{}
		switch typ.Kind() {
		case reflect.Map:
			mapType := typ.(*reflect2.UnsafeMapType)
			newObj = mapType.MakeMap(1)

		case reflect.Slice:
			mapType := typ.(*reflect2.UnsafeSliceType)
			newObj = mapType.MakeSlice(1, 1)

		default:
			newObj = typ.New()
		}

		r.ReadVal(schema, newObj)

		*pObj = typ.Indirect(newObj)
		return
	}

	ptrType := typ.(*reflect2.UnsafePtrType)
	ptrElemType := ptrType.Elem()
	if reflect2.IsNil(obj) {
		obj := ptrElemType.New()
		r.ReadVal(schema, obj)
		*pObj = obj
		return
	}
	r.ReadVal(schema, obj)
}

func encoderOfResolverUnion(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	union := schema.(*UnionSchema)

	name, err := cfg.resolver.Name(typ)
	if err != nil {
		return &errorEncoder{err: err}
	}

	if idx := strings.Index(name, ":"); idx > 0 {
		name = name[:idx]
	}

	schema, pos := union.Types().Get(name)
	if schema == nil {
		return &errorEncoder{err: fmt.Errorf("avro: unknown union type %s", name)}
	}

	encoder := encoderOfType(cfg, schema, typ)

	return &unionResolverEncoder{
		pos:     pos,
		encoder: encoder,
	}
}

type unionResolverEncoder struct {
	pos     int
	encoder ValEncoder
}

func (e *unionResolverEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteLong(int64(e.pos))

	e.encoder.Encode(ptr, w)
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
