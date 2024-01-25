package avro

import (
	"errors"
	"math/big"
	"time"

	"github.com/modern-go/reflect2"
)

func genericDecode(typ reflect2.Type, dec ValDecoder, r *Reader) any {
	ptr := typ.UnsafeNew()
	dec.Decode(ptr, r)
	if r.Error != nil {
		return nil
	}

	obj := typ.UnsafeIndirect(ptr)
	if reflect2.IsNil(obj) {
		return nil
	}

	// Generic reader returns a different result from the
	// codec in the case of a big.Rat. Handle this.
	if typ.Type1() == ratType {
		dec := obj.(big.Rat)
		return &dec
	}
	return obj
}

func genericReceiver(schema Schema) (reflect2.Type, error) {
	if schema.Type() == Ref {
		schema = schema.(*RefSchema).Schema()
	}

	var ls LogicalSchema
	lts, ok := schema.(LogicalTypeSchema)
	if ok {
		ls = lts.Logical()
	}

	schemaName := string(schema.Type())
	if ls != nil {
		schemaName += "." + string(ls.Type())
	}

	switch schema.Type() {
	case Boolean:
		var v bool
		return reflect2.TypeOf(v), nil
	case Int:
		if ls != nil {
			switch ls.Type() {
			case Date:
				var v time.Time
				return reflect2.TypeOf(v), nil

			case TimeMillis:
				var v time.Duration
				return reflect2.TypeOf(v), nil
			}
		}
		var v int
		return reflect2.TypeOf(v), nil
	case Long:
		if ls != nil {
			switch ls.Type() {
			case TimeMicros:
				var v time.Duration
				return reflect2.TypeOf(v), nil
			case TimestampMillis:
				var v time.Time
				return reflect2.TypeOf(v), nil
			case TimestampMicros:
				var v time.Time
				return reflect2.TypeOf(v), nil
			case LocalTimestampMillis:
				var v time.Time
				return reflect2.TypeOf(v), nil
			case LocalTimestampMicros:
				var v time.Time
				return reflect2.TypeOf(v), nil
			}
		}
		var v int64
		return reflect2.TypeOf(v), nil
	case Float:
		var v float32
		return reflect2.TypeOf(v), nil
	case Double:
		var v float64
		return reflect2.TypeOf(v), nil
	case String:
		var v string
		return reflect2.TypeOf(v), nil
	case Bytes:
		if ls != nil && ls.Type() == Decimal {
			var v *big.Rat
			return reflect2.TypeOf(v), nil
		}
		var v []byte
		return reflect2.TypeOf(v), nil
	case Record:
		var v map[string]any
		return reflect2.TypeOf(v), nil
	case Enum:
		var v string
		return reflect2.TypeOf(v), nil
	case Array:
		v := make([]any, 0)
		return reflect2.TypeOf(v), nil
	case Map:
		var v map[string]any
		return reflect2.TypeOf(v), nil
	case Union:
		var v map[string]any
		return reflect2.TypeOf(v), nil
	case Fixed:
		fixed := schema.(*FixedSchema)
		ls := fixed.Logical()
		if ls != nil {
			switch ls.Type() {
			case Duration:
				var v LogicalDuration
				return reflect2.TypeOf(v), nil
			case Decimal:
				var v big.Rat
				return reflect2.TypeOf(v), nil
			}
		}
		v := byteSliceToArray(make([]byte, fixed.Size()), fixed.Size())
		return reflect2.TypeOf(v), nil
	default:
		// This should not be possible.
		return nil, errors.New("dynamic receiver not found for schema " + schemaName)
	}
}
