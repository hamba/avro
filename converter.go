package avro

import (
	"fmt"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createLongConverter(typ Type) func(*Reader) int64 {
	switch typ {
	case Int:
		return func(r *Reader) int64 { return int64(r.ReadInt()) }
	default:
		return nil
	}
}

func createFloatConverter(typ Type) func(*Reader) float32 {
	switch typ {
	case Int:
		return func(r *Reader) float32 { return float32(r.ReadInt()) }
	case Long:
		return func(r *Reader) float32 { return float32(r.ReadLong()) }
	default:
		return nil
	}
}

func createDoubleConverter(typ Type) func(*Reader) float64 {
	switch typ {
	case Int:
		return func(r *Reader) float64 { return float64(r.ReadInt()) }
	case Long:
		return func(r *Reader) float64 { return float64(r.ReadLong()) }
	case Float:
		return func(r *Reader) float64 { return float64(r.ReadFloat()) }
	default:
		return nil
	}
}

func createLongEncoder[DataType float32 | float64, SchemaType smallInt | largeInt](dataType reflect2.Type, schemaType Type) func(ptr unsafe.Pointer) (int64, error) {
	return func(ptr unsafe.Pointer) (int64, error) {
		dataValue := *((*DataType)(ptr))
		avroValue := SchemaType(dataValue)
		if dataValue != DataType(avroValue) {
			return 0, fmt.Errorf("avro: %s with value %v is unsupported for Avro %s", dataType, dataValue, schemaType)
		}
		return int64(avroValue), nil
	}
}
