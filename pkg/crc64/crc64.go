// Package crc64 implements the Avro CRC-64 checksum.
// See https://avro.apache.org/docs/current/spec.html#schema_fingerprints for information.
package crc64

import (
	"hash"
)

func init() {
	buildTable()
}

// Size is the of a CRC-64 checksum in bytes.
const Size = 8

// Empty is the empty checksum.
const Empty = 0xc15d213aa4d7a795

// Table is a 256-word table representing the polynomial for efficient processing.
type Table [256]uint64

func makeTable() *Table {
	t := new(Table)
	for i := 0; i < 256; i++ {
		fp := uint64(i)
		for j := 0; j < 8; j++ {
			fp = (fp >> 1) ^ (Empty & -(fp & 1))
		}
		t[i] = fp
	}
	return t
}

var crc64Table *Table

func buildTable() {
	crc64Table = makeTable()
}

type digest struct {
	crc uint64
	tab *Table
}

// New creates a new hash.Hash64 computing the Avro CRC-64 checksum.
// Its Sum method will lay the value out in big-endian byte order.
func New() hash.Hash64 {
	return &digest{
		crc: Empty,
		tab: crc64Table,
	}
}

// Size returns the bytes size of the checksum.
func (d *digest) Size() int {
	return Size
}

// BlockSize returns the block size of the checksum.
func (d *digest) BlockSize() int {
	return 1
}

// Reset resets the hash instance.
func (d *digest) Reset() {
	d.crc = Empty
}

// Write accumulatively adds the given data to the checksum.
func (d *digest) Write(p []byte) (n int, err error) {
	for i := 0; i < len(p); i++ {
		d.crc = (d.crc >> 8) ^ d.tab[(int)(byte(d.crc)^p[i])&0xff]
	}

	return len(p), nil
}

// Sum64 returns the checksum as a uint64.
func (d *digest) Sum64() uint64 {
	return d.crc
}

// Sum returns the checksum as a byte slice, using the given byte slice.
func (d *digest) Sum(in []byte) []byte {
	s := d.Sum64()
	return append(in, byte(s>>56), byte(s>>48), byte(s>>40), byte(s>>32), byte(s>>24), byte(s>>16), byte(s>>8), byte(s))
}

// Sum returns the MD5 checksum of the data.
func Sum(data []byte) [Size]byte {
	d := digest{crc: Empty, tab: crc64Table}
	d.Reset()
	_, _ = d.Write(data)
	s := d.Sum64()
	//nolint:lll
	return [Size]byte{byte(s >> 56), byte(s >> 48), byte(s >> 40), byte(s >> 32), byte(s >> 24), byte(s >> 16), byte(s >> 8), byte(s)}
}
