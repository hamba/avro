package avro

import (
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfEnum(schema *EnumSchema, typ reflect2.Type) ValDecoder {
	switch {
	case typ.Kind() == reflect.String:
		return &enumCodec{enum: schema}
	case typ.Implements(textUnmarshalerType):
		return &enumTextMarshalerCodec{typ: typ, enum: schema}
	case reflect2.PtrTo(typ).Implements(textUnmarshalerType):
		return &enumTextMarshalerCodec{typ: typ, enum: schema, ptr: true}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfEnum(schema *EnumSchema, typ reflect2.Type) ValEncoder {
	switch {
	case typ.Kind() == reflect.String:
		return &enumCodec{enum: schema}
	case typ.Implements(textMarshalerType):
		return &enumTextMarshalerCodec{typ: typ, enum: schema}
	case reflect2.PtrTo(typ).Implements(textMarshalerType):
		return &enumTextMarshalerCodec{typ: typ, enum: schema, ptr: true}
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

type enumCodec struct {
	enum *EnumSchema
}

func (c *enumCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	i := int(r.ReadInt())

	symbol, ok := c.enum.Symbol(i)
	if !ok {
		r.ReportError("decode enum symbol", "unknown enum symbol")
		return
	}

	*((*string)(ptr)) = symbol
}

func (c *enumCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	str := *((*string)(ptr))
	for i, sym := range c.enum.symbols {
		if str != sym {
			continue
		}

		w.WriteInt(int32(i))
		return
	}

	w.Error = fmt.Errorf("avro: unknown enum symbol: %s", str)
}

type enumTextMarshalerCodec struct {
	typ  reflect2.Type
	enum *EnumSchema
	ptr  bool
}

func (c *enumTextMarshalerCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	i := int(r.ReadInt())

	symbol, ok := c.enum.Symbol(i)
	if !ok {
		r.ReportError("decode enum symbol", "unknown enum symbol")
		return
	}

	var obj any
	if c.ptr {
		obj = c.typ.PackEFace(ptr)
	} else {
		obj = c.typ.UnsafeIndirect(ptr)
	}
	if reflect2.IsNil(obj) {
		ptrType := c.typ.(*reflect2.UnsafePtrType)
		newPtr := ptrType.Elem().UnsafeNew()
		*((*unsafe.Pointer)(ptr)) = newPtr
		obj = c.typ.UnsafeIndirect(ptr)
	}
	unmarshaler := (obj).(encoding.TextUnmarshaler)
	if err := unmarshaler.UnmarshalText([]byte(symbol)); err != nil {
		r.ReportError("decode enum text unmarshaler", err.Error())
	}
}

func (c *enumTextMarshalerCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	var obj any
	if c.ptr {
		obj = c.typ.PackEFace(ptr)
	} else {
		obj = c.typ.UnsafeIndirect(ptr)
	}
	if c.typ.IsNullable() && reflect2.IsNil(obj) {
		w.Error = errors.New("encoding nil enum text marshaler")
		return
	}
	marshaler := (obj).(encoding.TextMarshaler)
	b, err := marshaler.MarshalText()
	if err != nil {
		w.Error = err
		return
	}

	str := string(b)
	for i, sym := range c.enum.symbols {
		if str != sym {
			continue
		}

		w.WriteInt(int32(i))
		return
	}

	w.Error = fmt.Errorf("avro: unknown enum symbol: %s", str)
}
