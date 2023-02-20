// Package bytesx implements bytes extensions.
package bytesx

import "io"

// ResetReader implements the io.Reader reading from a resettable byte slice.
type ResetReader struct {
	buf  []byte
	head int
	tail int
}

// NewResetReader returns a new ResetReader reading from b.
func NewResetReader(b []byte) *ResetReader {
	r := &ResetReader{}
	r.Reset(b)

	return r
}

// Read reads bytes into p.
func (r *ResetReader) Read(p []byte) (int, error) {
	if r.head == r.tail {
		return 0, io.EOF
	}

	n := copy(p, r.buf[r.head:])
	r.head += n

	return n, nil
}

// Reset resets the byte slice being read from.
func (r *ResetReader) Reset(b []byte) {
	r.buf = b
	r.head = 0
	r.tail = len(b)
}
