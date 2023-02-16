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
			dec, err := decoderOfResolvedUnion(cfg, schema)
			if err != nil {
				return &errorDecoder{err: fmt.Errorf("avro: problem resolving decoder for Avro %s: %w", schema.Type(), err)}
			}

			return dec
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
	_, resSchema := getUnionSchema(d.schema, r)
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
	m := *((*map[string]any)(ptr))

	if len(m) > 1 {
		w.Error = errors.New("avro: cannot encode union map with multiple entries")
		return
	}

	name := "null"
	val := any(nil)
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

	encoder := encoderOfType(e.cfg, schema, elemType)
	if elemType.LikePtr() {
		encoder = &onePtrEncoder{encoder}
	}
	encoder.Encode(elemPtr, w)
}

func decoderOfPtrUnion(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	union := schema.(*UnionSchema)
	_, typeIdx := union.Indices()
	ptrType := typ.(*reflect2.UnsafePtrType)
	elemType := ptrType.Elem()
	decoder := decoderOfType(cfg, union.Types()[typeIdx], elemType)

	return &unionPtrDecoder{
		schema:  union,
		typ:     elemType,
		decoder: decoder,
	}
}

type unionPtrDecoder struct {
	schema  *UnionSchema
	typ     reflect2.Type
	decoder ValDecoder
}

func (d *unionPtrDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	_, schema := getUnionSchema(d.schema, r)
	if schema == nil {
		return
	}

	if schema.Type() == Null {
		*((*unsafe.Pointer)(ptr)) = nil
		return
	}

	if *((*unsafe.Pointer)(ptr)) == nil {
		// Create new instance
		newPtr := d.typ.UnsafeNew()
		d.decoder.Decode(newPtr, r)
		*((*unsafe.Pointer)(ptr)) = newPtr
		return
	}

	// Reuse existing instance
	d.decoder.Decode(*((*unsafe.Pointer)(ptr)), r)
}

func encoderOfPtrUnion(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	union := schema.(*UnionSchema)
	nullIdx, typeIdx := union.Indices()
	ptrType := typ.(*reflect2.UnsafePtrType)
	encoder := encoderOfType(cfg, union.Types()[typeIdx], ptrType.Elem())

	return &unionPtrEncoder{
		schema:  union,
		encoder: encoder,
		nullIdx: int64(nullIdx),
		typeIdx: int64(typeIdx),
	}
}

type unionPtrEncoder struct {
	schema  *UnionSchema
	encoder ValEncoder
	nullIdx int64
	typeIdx int64
}

func (e *unionPtrEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	if *((*unsafe.Pointer)(ptr)) == nil {
		w.WriteLong(e.nullIdx)
		return
	}

	w.WriteLong(e.typeIdx)
	e.encoder.Encode(*((*unsafe.Pointer)(ptr)), w)
}

func decoderOfResolvedUnion(cfg *frozenConfig, schema Schema) (ValDecoder, error) {
	union := schema.(*UnionSchema)

	types := make([]reflect2.Type, len(union.Types()))
	decoders := make([]ValDecoder, len(union.Types()))
	for i, schema := range union.Types() {
		name := unionResolutionName(schema)

		typ, err := cfg.resolver.Type(name)
		if err != nil {
			if cfg.config.UnionResolutionError {
				return nil, err
			}

			if cfg.config.PartialUnionTypeResolution {
				decoders[i] = nil
				types[i] = nil
				continue
			}

			decoders = []ValDecoder{}
			types = []reflect2.Type{}
			break
		}

		decoder := decoderOfType(cfg, schema, typ)
		decoders[i] = decoder
		types[i] = typ
	}

	return &unionResolvedDecoder{
		cfg:      cfg,
		schema:   union,
		types:    types,
		decoders: decoders,
	}, nil
}

type unionResolvedDecoder struct {
	cfg      *frozenConfig
	schema   *UnionSchema
	types    []reflect2.Type
	decoders []ValDecoder
}

func (d *unionResolvedDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	i, schema := getUnionSchema(d.schema, r)
	if schema == nil {
		return
	}

	pObj := (*any)(ptr)

	if schema.Type() == Null {
		*pObj = nil
		return
	}

	if i >= len(d.decoders) || d.decoders[i] == nil {
		if d.cfg.config.UnionResolutionError {
			r.ReportError("decode union type", "unknown union type")
			return
		}

		// We cannot resolve this, set it to the map type
		name := schemaTypeName(schema)
		obj := map[string]any{}
		obj[name] = r.ReadNext(schema)

		*pObj = obj
		return
	}

	typ := d.types[i]
	var newPtr unsafe.Pointer
	switch typ.Kind() {
	case reflect.Map:
		mapType := typ.(*reflect2.UnsafeMapType)
		newPtr = mapType.UnsafeMakeMap(1)

	case reflect.Slice:
		mapType := typ.(*reflect2.UnsafeSliceType)
		newPtr = mapType.UnsafeMakeSlice(1, 1)

	case reflect.Ptr:
		elemType := typ.(*reflect2.UnsafePtrType).Elem()
		newPtr = elemType.UnsafeNew()

	default:
		newPtr = typ.UnsafeNew()
	}

	d.decoders[i].Decode(newPtr, r)
	*pObj = typ.UnsafeIndirect(newPtr)
}

func unionResolutionName(schema Schema) string {
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

	return name
}

func encoderOfResolverUnion(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	union := schema.(*UnionSchema)

	names, err := cfg.resolver.Name(typ)
	if err != nil {
		return &errorEncoder{err: err}
	}

	var pos int
	for _, name := range names {
		if idx := strings.Index(name, ":"); idx > 0 {
			name = name[:idx]
		}

		schema, pos = union.Types().Get(name)
		if schema != nil {
			break
		}
	}
	if schema == nil {
		return &errorEncoder{err: fmt.Errorf("avro: unknown union type %s", names[0])}
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

func getUnionSchema(schema *UnionSchema, r *Reader) (int, Schema) {
	types := schema.Types()

	idx := int(r.ReadLong())
	if idx < 0 || idx > len(types)-1 {
		r.ReportError("decode union type", "unknown union type")
		return 0, nil
	}

	return idx, types[idx]
}
