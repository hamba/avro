package avro

import (
	"fmt"

	"github.com/modern-go/reflect2"
)

type converter struct {
	toLong   func(*Reader) int64
	toFloat  func(*Reader) float32
	toDouble func(*Reader) float64
	toString func(*Reader) string
	toBytes  func(*Reader) []byte
}

// resolveConverter returns a set of converter functions based on the actual type.
// Depending on the actual type value, some converter functions may be nil;
// thus, the downstream caller must first check the converter function value.
func resolveConverter(typ Type) converter {
	cv := converter{}
	cv.toLong, _ = createLongConverter(typ)
	cv.toFloat, _ = createFloatConverter(typ)
	cv.toDouble, _ = createDoubleConverter(typ)
	cv.toString, _ = createStringConverter(typ)
	cv.toBytes, _ = createBytesConverter(typ)
	return cv
}

func createLongConverter(typ Type) (func(*Reader) int64, error) {
	switch typ {
	case Int:
		return func(r *Reader) int64 { return int64(r.ReadInt()) }, nil
	case Long:
		return func(r *Reader) int64 { return r.ReadLong() }, nil
	default:
		return nil, fmt.Errorf("cannot promote from %q to %q", typ, Long)
	}
}

func createFloatConverter(typ Type) (func(*Reader) float32, error) {
	switch typ {
	case Int:
		return func(r *Reader) float32 { return float32(r.ReadInt()) }, nil
	case Long:
		return func(r *Reader) float32 { return float32(r.ReadLong()) }, nil
	case Float:
		return func(r *Reader) float32 { return r.ReadFloat() }, nil
	default:
		return nil, fmt.Errorf("cannot promote from %q to %q", typ, Long)
	}
}

func createDoubleConverter(typ Type) (func(*Reader) float64, error) {
	switch typ {
	case Int:
		return func(r *Reader) float64 { return float64(r.ReadInt()) }, nil
	case Long:
		return func(r *Reader) float64 { return float64(r.ReadLong()) }, nil
	case Float:
		return func(r *Reader) float64 { return float64(r.ReadFloat()) }, nil
	case Double:
		return func(r *Reader) float64 { return r.ReadDouble() }, nil
	default:
		return nil, fmt.Errorf("cannot promote from %q to %q", typ, Long)
	}
}

func createStringConverter(typ Type) (func(*Reader) string, error) {
	switch typ {
	case Bytes:
		return func(r *Reader) string {
			b := r.ReadBytes()
			// TBD: update go.mod version to go 1.20 minimum
			// runtime.KeepAlive(b)
			// return unsafe.String(unsafe.SliceData(b), len(b))
			return string(b)
		}, nil
	case String:
		return func(r *Reader) string { return r.ReadString() }, nil
	default:
		return nil, fmt.Errorf("cannot promote from %q to %q", typ, Long)
	}
}

func createBytesConverter(typ Type) (func(*Reader) []byte, error) {
	switch typ {
	case String:
		return func(r *Reader) []byte {
			return reflect2.UnsafeCastString(r.ReadString())
		}, nil
	case Bytes:
		return func(r *Reader) []byte { return r.ReadBytes() }, nil
	default:
		return nil, fmt.Errorf("cannot promote from %q to %q", typ, Long)
	}
}
