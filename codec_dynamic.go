package avro

import (
	"fmt"
	"math/big"
	"reflect"
	"time"
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
		rPtr, rtyp, err := dynamicReceiver(d.schema, r.cfg.resolver)
		if err != nil {
			r.ReportError("Read", err.Error())
			return
		}
		decoderOfType(r.cfg, d.schema, rtyp).Decode(rPtr, r)
		*pObj = rtyp.UnsafeIndirect(rPtr)
		// *pObj = r.ReadNext(d.schema)
		return
	}

	typ := reflect2.TypeOf(obj)
	if typ.Kind() != reflect.Ptr {
		rPtr, rTyp, err := dynamicReceiver(d.schema, r.cfg.resolver)
		if err != nil {
			r.ReportError("Read", err.Error())
			return
		}
		decoderOfType(r.cfg, d.schema, rTyp).Decode(rPtr, r)
		*pObj = rTyp.UnsafeIndirect(rPtr)
		// *pObj = r.ReadNext(d.schema)
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

func dynamicReceiver(schema Schema, resolver *TypeResolver) (unsafe.Pointer, reflect2.Type, error) {
	var ls LogicalSchema
	lts, ok := schema.(LogicalTypeSchema)
	if ok {
		ls = lts.Logical()
	}

	name := string(schema.Type())
	if ls != nil {
		name += "." + string(ls.Type())
	}
	if resolver != nil {
		typ, err := resolver.Type(name)
		if err == nil {
			return typ.UnsafeNew(), typ, nil
		}
	}

	switch schema.Type() {
	case Boolean:
		var v bool
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Int:
		if ls != nil {
			switch ls.Type() {
			case Date:
				var v time.Time
				return unsafe.Pointer(&v), reflect2.TypeOf(v), nil

			case TimeMillis:
				var v time.Duration
				return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
			}
		}
		var v int
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Long:
		if ls != nil {
			switch ls.Type() {
			case TimeMicros:
				var v time.Duration
				return unsafe.Pointer(&v), reflect2.TypeOf(v), nil

			case TimestampMillis:
				var v time.Time
				return unsafe.Pointer(&v), reflect2.TypeOf(v), nil

			case TimestampMicros:
				var v time.Time
				return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
			}
		}
		var v int64
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Float:
		var v float32
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Double:
		var v float64
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case String:
		var v string
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Bytes:
		if ls != nil && ls.Type() == Decimal {
			var v *big.Rat
			return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
		}
		var v []byte
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Record:
		var v map[string]any
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Ref:
		return dynamicReceiver(schema.(*RefSchema).Schema(), resolver)
	case Enum:
		var v string
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Array:
		v := make([]any, 0)
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Map:
		var v map[string]any
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Union:
		var v map[string]any
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	case Fixed:
		fixed := schema.(*FixedSchema)
		ls := fixed.Logical()
		if ls != nil {
			switch ls.Type() {
			case Duration:
				var v LogicalDuration
				return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
			case Decimal:
				var v big.Rat
				return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
			}
		}
		// note that uint64 case is not supported, due to the lack of indicator at the schema-level (logical type)
		var v []byte
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	default:
		return nil, nil, fmt.Errorf("dynamic receiver not found for schema: %v", name)
	}
}
