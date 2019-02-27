package avro

import (
	"fmt"
)

// ReadNext reads the next Avro element as a generic interface.
func (r *Reader) ReadNext(schema Schema) interface{} {
	switch schema.Type() {

	case Boolean:
		return r.ReadBool()

	case Int:
		return int(r.ReadInt())

	case Long:
		return r.ReadLong()

	case Float:
		return r.ReadFloat()

	case Double:
		return r.ReadDouble()

	case String:
		return r.ReadString()

	case Bytes:
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

		key := string(schema.Type())
		if n, ok := schema.(NamedSchema); ok {
			key = n.Name()
		}

		obj := map[string]interface{}{}
		obj[key] = r.ReadNext(types[idx])

		return obj

	case Fixed:
		obj := make([]byte, schema.(*FixedSchema).Size())
		r.Read(obj)
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
