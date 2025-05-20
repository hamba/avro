package avro

import (
	"errors"
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

	defer func() {
		obj, err := r.cfg.typeConverters.DecodeTypeConvert(*pObj, d.schema)
		if err != nil && !errors.Is(err, errNoTypeConverter) {
			r.Error = err
		}
		*pObj = obj
	}()

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

	obj, err := w.cfg.typeConverters.EncodeTypeConvert(obj, e.schema)
	if err != nil && !errors.Is(err, errNoTypeConverter) {
		w.Error = err
		return
	}

	w.WriteVal(e.schema, obj)
}
