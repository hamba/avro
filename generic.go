package avro

import (
	"fmt"
	"math/big"
	"time"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func genericDecode(schema Schema, r *Reader) any {
	rPtr, rTyp, err := genericReceiver(schema)
	if err != nil {
		r.ReportError("Read", err.Error())
		return nil
	}
	decoderOfType(r.cfg, schema, rTyp).Decode(rPtr, r)

	return rTyp.UnsafeIndirect(rPtr)
}

func genericReceiver(schema Schema) (unsafe.Pointer, reflect2.Type, error) {
	var ls LogicalSchema
	lts, ok := schema.(LogicalTypeSchema)
	if ok {
		ls = lts.Logical()
	}

	name := string(schema.Type())
	if ls != nil {
		name += "." + string(ls.Type())
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
		return genericReceiver(schema.(*RefSchema).Schema())
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
		v := byteSliceToArray(make([]byte, fixed.Size()), fixed.Size())
		return unsafe.Pointer(&v), reflect2.TypeOf(v), nil
	default:
		return nil, nil, fmt.Errorf("dynamic receiver not found for schema: %v", name)
	}
}
