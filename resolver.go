package avro

import (
	"fmt"
	"math/big"
	"time"

	"github.com/modern-go/concurrent"
	"github.com/modern-go/reflect2"
)

// TypeResolver resolves types by name.
type TypeResolver struct {
	names *concurrent.Map // map[string]reflect2.Type
	types *concurrent.Map // map[int]string
}

// NewTypeResolver creates a new type resolver with all primitive types
// registered.
func NewTypeResolver() *TypeResolver {
	r := &TypeResolver{
		names: concurrent.NewMap(),
		types: concurrent.NewMap(),
	}

	// Register basic types
	r.Register(string(Null), &null{})
	r.Register(string(Int), int8(0))
	r.Register(string(Int), int16(0))
	r.Register(string(Int), int32(0))
	r.Register(string(Int), int(0))
	r.Register(string(Long), int64(0))
	r.Register(string(Float), float32(0))
	r.Register(string(Double), float64(0))
	r.Register(string(String), "")
	r.Register(string(Bytes), []byte{})
	r.Register(string(Boolean), bool(true))

	// Register logical types
	r.Register(string(Int)+"."+string(Date), time.Time{})
	r.Register(string(Int)+"."+string(TimeMillis), time.Duration(0))
	r.Register(string(Long)+"."+string(TimestampMillis), time.Time{})
	r.Register(string(Long)+"."+string(TimestampMicros), time.Time{})
	r.Register(string(Long)+"."+string(TimeMicros), time.Duration(0))
	r.Register(string(Bytes)+"."+string(Decimal), big.Rat{})

	// Register array type
	r.Register(string(Array), []interface{}{})

	return r
}

// Register registers names to their types for resolution.
func (r *TypeResolver) Register(name string, obj interface{}) {
	typ := reflect2.TypeOf(obj)
	rtype := typ.RType()

	r.names.Store(name, typ)
	r.types.Store(rtype, name)
}

// Name gets the name for a type, or an error.
func (r *TypeResolver) Name(typ reflect2.Type) (string, error) {
	rtype := typ.RType()

	name, ok := r.types.Load(rtype)
	if !ok {
		return "", fmt.Errorf("avro: unable to resolve type %s", typ.String())
	}

	return name.(string), nil
}

// Type gets the type for a name, or an error.
func (r *TypeResolver) Type(name string) (reflect2.Type, error) {
	typ, ok := r.names.Load(name)
	if !ok {
		return nil, fmt.Errorf("avro: unable to resolve type with name %s", name)
	}

	return typ.(reflect2.Type), nil
}

// Register registers names to their types for resolution. All primitive types are pre-registered.
func Register(name string, obj interface{}) {
	DefaultConfig.Register(name, obj)
}
