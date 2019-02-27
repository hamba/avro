package avro

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfFixed(schema Schema, typ reflect2.Type) ValDecoder {
	switch typ.Kind() {
	case reflect.Array:
		arrayType := typ.(reflect2.ArrayType)
		fixed := schema.(*FixedSchema)
		if arrayType.Elem().Kind() != reflect.Uint8 || arrayType.Len() != fixed.Size() {
			break
		}
		return &fixedCodec{arrayType: typ.(*reflect2.UnsafeArrayType)}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfFixed(schema Schema, typ reflect2.Type) ValEncoder {
	switch typ.Kind() {
	case reflect.Array:
		arrayType := typ.(reflect2.ArrayType)
		fixed := schema.(*FixedSchema)
		if arrayType.Elem().Kind() != reflect.Uint8 || arrayType.Len() != fixed.Size() {
			break
		}
		return &fixedCodec{arrayType: typ.(*reflect2.UnsafeArrayType)}
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

type fixedCodec struct {
	arrayType *reflect2.UnsafeArrayType
}

func (c *fixedCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	for i := 0; i < c.arrayType.Len(); i++ {
		c.arrayType.UnsafeSetIndex(ptr, i, reflect2.PtrOf(r.readByte()))
	}
}

func (c *fixedCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	for i := 0; i < c.arrayType.Len(); i++ {
		bytePtr := c.arrayType.UnsafeGetIndex(ptr, i)
		w.writeByte(*((*byte)(bytePtr)))
	}
}
