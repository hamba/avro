package avro

import (
	"unsafe"
)

// noescape hides a pointer from escape analysis.  noescape is
// the identity function but escape analysis doesn't think the
// output depends on the input.  noescape is inlined and currently
// compiles down to zero instructions.
// USE CAREFULLY!
//
// This function is taken from Go std lib:
// https://github.com/golang/go/blob/master/src/runtime/stubs.go#L178
//
//nolint:govet,staticcheck
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}
