package avro

// UnionType is the interface implemented by types that can
// resolve schema types/names to and from objects.
type UnionType interface {
	Value() *interface{}
	SetType(string) error
	GetType() (string, error)
}
