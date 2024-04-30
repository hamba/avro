package avro

import "math"

// Max allocation size for an array due to the limit in number of bits in a heap address:
// https://github.com/golang/go/blob/7f76c00fc5678fa782708ba8fece63750cb89d03/src/runtime/malloc.go#L190
// 32-bit systems accept the full 32bit address space
const maxAllocSize = math.MaxInt
