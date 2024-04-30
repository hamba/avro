//go:build !386

package avro

// Max allocation size for an array due to the limit in number of bits in a heap address:
// https://github.com/golang/go/blob/7f76c00fc5678fa782708ba8fece63750cb89d03/src/runtime/malloc.go#L183
const maxAllocSize = int(1 << 48)
