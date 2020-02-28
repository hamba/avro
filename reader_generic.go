package avro

import (
	"fmt"
	"time"
)

// ReadNext reads the next Avro element as a generic interface.
func (r *Reader) ReadNext(schema Schema) interface{} {
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
				return time.Unix(0, int64(r.ReadInt())*int64(24*time.Hour)).UTC()

			case TimeMillis:
				return time.Duration(r.ReadInt()) * time.Millisecond
			}
		}
		return int(r.ReadInt())

	case Long:
		if ls != nil {
			switch ls.Type() {
			case TimeMicros:
				return time.Duration(r.ReadLong()) * time.Microsecond

			case TimestampMillis:
				return time.Unix(0, r.ReadLong()*int64(time.Millisecond)).UTC()

			case TimestampMicros:
				return time.Unix(0, r.ReadLong()*int64(time.Microsecond)).UTC()
			}
		}
		return r.ReadLong()

	case Float:
		return r.ReadFloat()

	case Double:
		return r.ReadDouble()

	case String:
		return r.ReadString()

	case Bytes:
		if ls != nil && ls.Type() == Decimal {
			dec := ls.(*DecimalLogicalSchema)
			return ratFromBytes(r.ReadBytes(), dec.Scale())
		}
		return r.ReadBytes()

	case Record:
		fields := schema.(*RecordSchema).Fields()
		obj := make(map[string]interface{}, len(fields))
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
		arr := []interface{}{}
		r.ReadArrayCB(func(r *Reader) bool {
			elem := r.ReadNext(schema.(*ArraySchema).Items())
			arr = append(arr, elem)
			return true
		})
		return arr

	case Map:
		obj := map[string]interface{}{}
		r.ReadMapCB(func(r *Reader, field string) bool {
			elem := r.ReadNext(schema.(*MapSchema).Values())
			obj[field] = elem
			return true
		})
		return obj

	case Union:
		types := schema.(*UnionSchema).Types()
		idx := int(r.ReadLong())
		if idx < 0 || idx > len(types)-1 {
			r.ReportError("Read", "unknown union type")
			return nil
		}
		schema := types[idx]
		if schema.Type() == Null {
			return nil
		}

		key := schemaTypeName(schema)
		obj := map[string]interface{}{}
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
		return obj

	default:
		r.ReportError("Read", fmt.Sprintf("unexpected schema type: %v", schema.Type()))
		return nil
	}
}

// ReadArrayCB reads an array with a callback per item.
func (r *Reader) ReadArrayCB(callback func(*Reader) bool) {
	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		for i := 0; i < int(l); i++ {
			callback(r)
		}
	}
}

// ReadMapCB reads an array with a callback per item.
func (r *Reader) ReadMapCB(callback func(*Reader, string) bool) {
	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		for i := 0; i < int(l); i++ {
			field := r.ReadString()
			callback(r, field)
		}
	}
}
