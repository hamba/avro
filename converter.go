package avro

import (
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createLongConverter(typ Type) func(*Reader) int64 {
	switch typ {
	case Int:
		return func(r *Reader) int64 { return int64(r.ReadInt()) }
	case Long:
		return func(r *Reader) int64 { return r.ReadLong() }
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
	case Float:
		return func(r *Reader) float32 { return r.ReadFloat() }
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
	case Double:
		return func(r *Reader) float64 { return r.ReadDouble() }
	default:
		return nil
	}
}

func createStringConverter(typ Type) func(*Reader) string {
	switch typ {
	case Bytes:
		return func(r *Reader) string {
			b := r.ReadBytes()
			if len(b) == 0 {
				return ""
			}
			return *(*string)(unsafe.Pointer(&b))
		}
	case String:
		return func(r *Reader) string { return r.ReadString() }
	default:
		return nil
	}
}

func createBytesConverter(typ Type) func(*Reader) []byte {
	switch typ {
	case String:
		return func(r *Reader) []byte {
			return reflect2.UnsafeCastString(r.ReadString())
		}
	case Bytes:
		return func(r *Reader) []byte { return r.ReadBytes() }
	default:
		return nil
	}
}
