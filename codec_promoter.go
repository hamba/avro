package avro

import (
	"reflect"

	"github.com/modern-go/reflect2"
)

func getCodecPromoter[T any](actual Type) *codecPromoter[T] {
	if actual == "" {
		return nil
	}

	return &codecPromoter[T]{actual: actual}
}

type codecPromoter[T any] struct {
	actual Type
}

func (p *codecPromoter[T]) promote(r *Reader) (t T) {
	tt := reflect2.TypeOf(t)

	convert := func(typ reflect2.Type, obj any) (t T) {
		if !reflect.TypeOf(obj).ConvertibleTo(typ.Type1()) {
			r.ReportError("decode promotable", "unsupported type")
			// return zero value
			return t
		}
		return reflect.ValueOf(obj).Convert(typ.Type1()).Interface().(T)
	}

	switch p.actual {
	case Int:
		var obj int32
		(&intCodec[int32]{}).Decode(reflect2.PtrOf(&obj), r)
		t = convert(tt, obj)

	case Long:
		var obj int64
		(&longCodec[int64]{}).Decode(reflect2.PtrOf(&obj), r)
		t = convert(tt, obj)

	case Float:
		var obj float32
		(&float32Codec{}).Decode(reflect2.PtrOf(&obj), r)
		t = convert(tt, obj)

	case String:
		var obj string
		(&stringCodec{}).Decode(reflect2.PtrOf(&obj), r)
		t = convert(tt, obj)

	case Bytes:
		var obj []byte
		(&bytesCodec{}).Decode(reflect2.PtrOf(&obj), r)
		t = convert(tt, obj)

	default:
		r.ReportError("decode promotable", "unsupported actual type")
	}

	return t
}
