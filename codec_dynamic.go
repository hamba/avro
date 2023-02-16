package avro

import (
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

type efaceDecoder struct {
	schema Schema
}

func (d *efaceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	pObj := (*any)(ptr)
	obj := *pObj
	if obj == nil {
		*pObj = r.ReadNext(d.schema)
		return
	}

	typ := reflect2.TypeOf(obj)
	if typ.Kind() != reflect.Ptr {
		*pObj = r.ReadNext(d.schema)
		return
	}

	ptrType := typ.(*reflect2.UnsafePtrType)
	ptrElemType := ptrType.Elem()
	if reflect2.IsNil(obj) {
		obj := ptrElemType.New()
		r.ReadVal(d.schema, obj)
		*pObj = obj
		return
	}
	r.ReadVal(d.schema, obj)
}

type interfaceEncoder struct {
	schema Schema
	typ    reflect2.Type
}

func (e *interfaceEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	obj := e.typ.UnsafeIndirect(ptr)
	w.WriteVal(e.schema, obj)
}
