package avro

import (
	"fmt"
	"math/big"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfFixed(schema Schema, typ reflect2.Type) ValDecoder {
	fixed := schema.(*FixedSchema)
	switch typ.Kind() {
	case reflect.Array:
		arrayType := typ.(reflect2.ArrayType)
		if arrayType.Elem().Kind() != reflect.Uint8 || arrayType.Len() != fixed.Size() {
			break
		}
		return &fixedCodec{arrayType: typ.(*reflect2.UnsafeArrayType)}

	case reflect.Struct:
		ls := fixed.Logical()
		if typ.RType() != ratRType || ls == nil || ls.Type() != Decimal {
			break
		}
		dec := ls.(*DecimalLogicalSchema)
		return &fixedDecimalCodec{prec: dec.Precision(), scale: dec.Scale(), size: fixed.Size()}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfFixed(schema Schema, typ reflect2.Type) ValEncoder {
	fixed := schema.(*FixedSchema)
	switch typ.Kind() {
	case reflect.Array:
		arrayType := typ.(reflect2.ArrayType)
		fixed := schema.(*FixedSchema)
		if arrayType.Elem().Kind() != reflect.Uint8 || arrayType.Len() != fixed.Size() {
			break
		}
		return &fixedCodec{arrayType: typ.(*reflect2.UnsafeArrayType)}

	case reflect.Ptr:
		ptrType := typ.(*reflect2.UnsafePtrType)
		elemType := ptrType.Elem()

		ls := fixed.Logical()
		if elemType.Kind() != reflect.Struct || elemType.RType() != ratRType || ls == nil || ls.Type() != Decimal {
			break
		}
		dec := ls.(*DecimalLogicalSchema)
		return &fixedDecimalCodec{prec: dec.Precision(), scale: dec.Scale(), size: fixed.Size()}
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

type fixedDecimalCodec struct {
	prec  int
	scale int
	size  int
}

func (c *fixedDecimalCodec) Decode(ptr unsafe.Pointer, r *Reader) {
	b := make([]byte, c.size)
	r.Read(b)
	*((*big.Rat)(ptr)) = *ratFromBytes(b, c.scale)
}

func (c *fixedDecimalCodec) Encode(ptr unsafe.Pointer, w *Writer) {
	r := *((**big.Rat)(ptr))
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(c.scale)), nil)
	i := (&big.Int{}).Mul(r.Num(), scale)
	i = i.Div(i, r.Denom())

	var b []byte
	switch i.Sign() {
	case 0:
		b = make([]byte, c.size)

	case 1:
		b = i.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		if len(b) < c.size {
			padded := make([]byte, c.size)
			copy(padded[c.size-len(b):], b)
			b = padded
		}

	case -1:
		b = i.Add(i, (&big.Int{}).Lsh(one, uint(c.size*8))).Bytes()
	}

	w.Write(b)
}
