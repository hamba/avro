package avro

import (
	"fmt"
	"reflect"
	"time"
)

// ReadNext reads the next Avro element as a generic interface.
func (r *Reader) ReadNext(schema Schema) any {
	var rp iReaderPromoter = r
	if sch, ok := schema.(*PrimitiveSchema); ok && sch.actual != "" {
		rp = newReaderPromoter(sch.actual, r)
	}

	var ls LogicalSchema
	lts, ok := schema.(LogicalTypeSchema)
	if ok {
		ls = lts.Logical()
	}

	switch schema.Type() {
	case Boolean:
		return r.ReadBool()
	case Int:
		if ls != nil {
			switch ls.Type() {
			case Date:
				i := r.ReadInt()
				sec := int64(i) * int64(24*time.Hour/time.Second)
				return time.Unix(sec, 0).UTC()

			case TimeMillis:
				return time.Duration(r.ReadInt()) * time.Millisecond
			}
		}
		return int(r.ReadInt())
	case Long:
		if ls != nil {
			switch ls.Type() {
			case TimeMicros:
				return time.Duration(rp.ReadLong()) * time.Microsecond

			case TimestampMillis:
				i := rp.ReadLong()
				sec := i / 1e3
				nsec := (i - sec*1e3) * 1e6
				return time.Unix(sec, nsec).UTC()

			case TimestampMicros:
				i := rp.ReadLong()
				sec := i / 1e6
				nsec := (i - sec*1e6) * 1e3
				return time.Unix(sec, nsec).UTC()
			}
		}
		return rp.ReadLong()
	case Float:
		return rp.ReadFloat()
	case Double:
		return rp.ReadDouble()
	case String:
		return rp.ReadString()
	case Bytes:
		if ls != nil && ls.Type() == Decimal {
			dec := ls.(*DecimalLogicalSchema)
			return ratFromBytes(rp.ReadBytes(), dec.Scale())
		}
		return rp.ReadBytes()
	case Record:
		fields := schema.(*RecordSchema).Fields()
		obj := make(map[string]any, len(fields))
		for _, field := range fields {
			obj[field.Name()] = r.ReadNext(field.Type())
		}
		return obj
	case Ref:
		return r.ReadNext(schema.(*RefSchema).Schema())
	case Enum:
		symbols := schema.(*EnumSchema).Symbols()
		idx := int(r.ReadInt())
		if idx < 0 || idx >= len(symbols) {
			r.ReportError("Read", "unknown enum symbol")
			return nil
		}
		return symbols[idx]
	case Array:
		arr := []any{}
		r.ReadArrayCB(func(r *Reader) bool {
			elem := r.ReadNext(schema.(*ArraySchema).Items())
			arr = append(arr, elem)
			return true
		})
		return arr
	case Map:
		obj := map[string]any{}
		r.ReadMapCB(func(r *Reader, field string) bool {
			elem := r.ReadNext(schema.(*MapSchema).Values())
			obj[field] = elem
			return true
		})
		return obj
	case Union:
		types := schema.(*UnionSchema).Types()
		idx := int(rp.ReadLong())
		if idx < 0 || idx > len(types)-1 {
			r.ReportError("Read", "unknown union type")
			return nil
		}
		schema = types[idx]
		if schema.Type() == Null {
			return nil
		}

		key := schemaTypeName(schema)
		obj := map[string]any{}
		obj[key] = r.ReadNext(types[idx])
		return obj
	case Fixed:
		size := schema.(*FixedSchema).Size()
		obj := make([]byte, size)
		r.Read(obj)
		if ls != nil && ls.Type() == Decimal {
			dec := ls.(*DecimalLogicalSchema)
			return ratFromBytes(obj, dec.Scale())
		}
		return byteSliceToArray(obj, size)
	default:
		r.ReportError("Read", fmt.Sprintf("unexpected schema type: %v", schema.Type()))
		return nil
	}
}

// ReadArrayCB reads an array with a callback per item.
func (r *Reader) ReadArrayCB(fn func(*Reader) bool) {
	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}
		for i := 0; i < int(l); i++ {
			fn(r)
		}
	}
}

// ReadMapCB reads an array with a callback per item.
func (r *Reader) ReadMapCB(fn func(*Reader, string) bool) {
	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		for i := 0; i < int(l); i++ {
			field := r.ReadString()
			fn(r, field)
		}
	}
}

var byteType = reflect.TypeOf((*byte)(nil)).Elem()

func byteSliceToArray(b []byte, size int) any {
	vArr := reflect.New(reflect.ArrayOf(size, byteType)).Elem()
	reflect.Copy(vArr, reflect.ValueOf(b))
	return vArr.Interface()
}
