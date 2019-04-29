package avro

import (
	"unsafe"
)

//go:linkname noescape runtime.noescape
func noescape(p unsafe.Pointer) unsafe.Pointer
