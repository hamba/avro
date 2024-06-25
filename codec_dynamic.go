package avro

import (
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

type efaceDecoder struct {
	schema Schema
	typ    reflect2.Type
	dec    ValDecoder
}

func newEfaceDecoder(d *decoderContext, schema Schema) *efaceDecoder {
	typ, _ := genericReceiver(schema)
	dec := decoderOfType(d, schema, typ)

	return &efaceDecoder{
		schema: schema,
		typ:    typ,
		dec:    dec,
	}
}

func (d *efaceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	pObj := (*any)(ptr)
	if *pObj == nil {
		*pObj = genericDecode(d.typ, d.dec, r)
		return
	}

	typ := reflect2.TypeOf(*pObj)
	if typ.Kind() != reflect.Ptr {
		*pObj = genericDecode(d.typ, d.dec, r)
		return
	}

	ptrType := typ.(*reflect2.UnsafePtrType)
	ptrElemType := ptrType.Elem()
	if reflect2.IsNil(*pObj) {
		obj := ptrElemType.New()
		r.ReadVal(d.schema, obj)
		*pObj = obj
		return
	}
	r.ReadVal(d.schema, *pObj)
}

type interfaceEncoder struct {
	schema Schema
	typ    reflect2.Type
}

func (e *interfaceEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := e.typ.UnsafeIndirect(ptr)
	w.WriteVal(e.schema, obj)
}
