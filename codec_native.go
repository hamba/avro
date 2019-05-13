package avro

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfNative(schema Schema, typ reflect2.Type) ValDecoder {
	switch typ.Kind() {
	case reflect.Bool:
		if schema.Type() != Boolean {
			break
		}
		return &boolCodec{}

	case reflect.Int:
		if schema.Type() != Int {
			break
		}
		return &intCodec{}

	case reflect.Int8:
		if schema.Type() != Int {
			break
		}
		return &int8Codec{}

	case reflect.Int16:
		if schema.Type() != Int {
			break
		}
		return &int16Codec{}

	case reflect.Int32:
		if schema.Type() != Int {
			break
		}
		return &int32Codec{}

	case reflect.Int64:
		if schema.Type() != Long {
			break
		}
		return &int64Codec{}

	case reflect.Float32:
		if schema.Type() != Float {
			break
		}
		return &float32Codec{}

	case reflect.Float64:
		if schema.Type() != Double {
			break
		}
		return &float64Codec{}

	case reflect.String:
		if schema.Type() != String {
			break
		}
		return &stringCodec{}

	case reflect.Slice:
		if typ.(reflect2.SliceType).Elem().Kind() != reflect.Uint8 || schema.Type() != Bytes {
			break
		}
		return &bytesCodec{sliceType: typ.(*reflect2.UnsafeSliceType)}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfNative(schema Schema, typ reflect2.Type) ValEncoder {
	switch typ.Kind() {
	case reflect.Bool:
		if schema.Type() != Boolean {
			break
		}
		return &boolCodec{}

	case reflect.Int:
		if schema.Type() != Int {
			break
		}
		return &intCodec{}

	case reflect.Int8:
		if schema.Type() != Int {
			break
		}
		return &int8Codec{}

	case reflect.Int16:
		if schema.Type() != Int {
			break
		}
		return &int16Codec{}

	case reflect.Int32:
		if schema.Type() != Int {
			break
		}
		return &int32Codec{}

	case reflect.Int64:
		if schema.Type() != Long {
			break
		}
		return &int64Codec{}

	case reflect.Float32:
		if schema.Type() != Float {
			break
		}
		return &float32Codec{}

	case reflect.Float64:
		if schema.Type() != Double {
			break
		}
		return &float64Codec{}

	case reflect.String:
		if schema.Type() != String {
			break
		}
		return &stringCodec{}

	case reflect.Slice:
		if typ.(reflect2.SliceType).Elem().Kind() != reflect.Uint8 || schema.Type() != Bytes {
			break
		}
		return &bytesCodec{sliceType: typ.(*reflect2.UnsafeSliceType)}
	}

	if schema.Type() == Null {
		return &nullCodec{}
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

type nullCodec struct{}

func (*nullCodec) Encode(ptr unsafe.Pointer, w *Writer) {}

type boolCodec struct{}

func (*boolCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*bool)(ptr)) = r.ReadBool()
}

func (*boolCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteBool(*((*bool)(ptr)))
}

type intCodec struct{}

func (*intCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*int)(ptr)) = int(r.ReadInt())
}

func (*intCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteInt(int32(*((*int)(ptr))))
}

type int8Codec struct{}

func (*int8Codec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*int8)(ptr)) = int8(r.ReadInt())
}

func (*int8Codec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteInt(int32(*((*int8)(ptr))))
}

type int16Codec struct{}

func (*int16Codec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*int16)(ptr)) = int16(r.ReadInt())
}

func (*int16Codec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteInt(int32(*((*int16)(ptr))))
}

type int32Codec struct{}

func (*int32Codec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*int32)(ptr)) = r.ReadInt()
}

func (*int32Codec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteInt(*((*int32)(ptr)))
}

type int64Codec struct{}

func (*int64Codec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*int64)(ptr)) = r.ReadLong()
}

func (*int64Codec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteLong(*((*int64)(ptr)))
}

type float32Codec struct{}

func (*float32Codec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*float32)(ptr)) = r.ReadFloat()
}

func (*float32Codec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteFloat(*((*float32)(ptr)))
}

type float64Codec struct{}

func (*float64Codec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*float64)(ptr)) = r.ReadDouble()
}

func (*float64Codec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteDouble(*((*float64)(ptr)))
}

type stringCodec struct{}

func (*stringCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	*((*string)(ptr)) = r.ReadString()
}

func (*stringCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteString(*((*string)(ptr)))
}

type bytesCodec struct {
	sliceType *reflect2.UnsafeSliceType
}

func (c *bytesCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	b := r.ReadBytes()
	c.sliceType.UnsafeSet(ptr, reflect2.PtrOf(b))
}

func (c *bytesCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	w.WriteBytes(*((*[]byte)(ptr)))
}
