package avro

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfEnum(schema Schema, typ reflect2.Type) ValDecoder {
	if typ.Kind() == reflect.String {
		return &enumCodec{symbols: schema.(*EnumSchema).Symbols()}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfEnum(schema Schema, typ reflect2.Type) ValEncoder {
	if typ.Kind() == reflect.String {
		return &enumCodec{symbols: schema.(*EnumSchema).Symbols()}
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

type enumCodec struct {
	symbols []string
}

func (c *enumCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	i := int(r.ReadInt())

	if i < 0 || i >= len(c.symbols) {
		r.ReportError("decode unknown enum symbol", "unknown enum symbol")
		return
	}

	*((*string)(ptr)) = c.symbols[i]
}

func (c *enumCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	str := *((*string)(ptr))
	for i, sym := range c.symbols {
		if str != sym {
			continue
		}

		w.WriteInt(int32(i))
		return
	}

	w.Error = fmt.Errorf("avro: unknown enum symbol: %s", str)
}
