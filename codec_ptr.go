package avro

import (
	"errors"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func decoderOfPtr(d *decoderContext, schema Schema, typ reflect2.Type) ValDecoder {
	ptrType := typ.(*reflect2.UnsafePtrType)
	elemType := ptrType.Elem()

	decoder := decoderOfType(d, schema, elemType)

	return &dereferenceDecoder{typ: elemType, decoder: decoder}
}

type dereferenceDecoder struct {
	typ     reflect2.Type
	decoder ValDecoder
}

func (d *dereferenceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
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

func encoderOfPtr(e *encoderContext, schema Schema, typ reflect2.Type) ValEncoder {
	ptrType := typ.(*reflect2.UnsafePtrType)
	elemType := ptrType.Elem()

	enc := encoderOfType(e, schema, elemType)

	return &dereferenceEncoder{typ: elemType, encoder: enc}
}

type dereferenceEncoder struct {
	typ     reflect2.Type
	encoder ValEncoder
}

func (d *dereferenceEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	if *((*unsafe.Pointer)(ptr)) == nil {
		w.Error = errors.New("avro: cannot encode nil pointer")
		return
	}

	d.encoder.Encode(*((*unsafe.Pointer)(ptr)), w)
}

type referenceDecoder struct {
	decoder ValDecoder
}

func (decoder *referenceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	decoder.decoder.Decode(unsafe.Pointer(&ptr), r)
}
