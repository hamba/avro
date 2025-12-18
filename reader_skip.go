package avro

import (
	"bytes"
	"fmt"
)

// SkipNBytes skips the given number of bytes in the reader.
func (r *Reader) SkipNBytes(n int) {
	read := 0
	for read < n {
		if r.head == r.tail {
			if !r.loadMore() {
				return
			}
		}

		if read+r.tail-r.head < n {
			read += r.tail - r.head
			r.head = r.tail
			continue
		}

		r.head += n - read
		read += n - read
	}
}

// SkipBool skips a Bool in the reader.
func (r *Reader) SkipBool() {
	_ = r.readByte()
}

// SkipInt skips an Int in the reader.
func (r *Reader) SkipInt() {
	var n int
	for r.Error == nil && n < maxIntBufSize {
		b := r.readByte()
		if b&0x80 == 0 {
			break
		}
		n++
	}
}

// SkipLong skips a Long in the reader.
func (r *Reader) SkipLong() {
	var n int
	for r.Error == nil && n < maxLongBufSize {
		b := r.readByte()
		if b&0x80 == 0 {
			break
		}
		n++
	}
}

// SkipFloat skips a Float in the reader.
func (r *Reader) SkipFloat() {
	r.SkipNBytes(4)
}

// SkipDouble skips a Double in the reader.
func (r *Reader) SkipDouble() {
	r.SkipNBytes(8)
}

// SkipString skips a String in the reader.
func (r *Reader) SkipString() {
	size := r.ReadLong()
	if size <= 0 {
		return
	}
	r.SkipNBytes(int(size))
}

// SkipBytes skips Bytes in the reader.
func (r *Reader) SkipBytes() {
	size := r.ReadLong()
	if size <= 0 {
		return
	}
	r.SkipNBytes(int(size))
}

// SkipTo skips to the given token in the reader.
func (r *Reader) SkipTo(token []byte) (int, error) {
	tokenLen := len(token)
	if tokenLen == 0 {
		return 0, nil
	}
	if tokenLen > len(r.buf) {
		return 0, fmt.Errorf("token length %d exceeds buffer size %d", tokenLen, len(r.buf))
	}

	var skipped int
	var stash []byte

	for {
		// Check boundary if we have stash from previous read
		if len(stash) > 0 {
			need := min(r.tail-r.head, tokenLen-1)

			// Construct boundary window: stash + beginning of new buffer
			boundary := make([]byte, len(stash)+need)
			copy(boundary, stash)
			copy(boundary[len(stash):], r.buf[r.head:r.head+need])

			if idx := bytes.Index(boundary, token); idx >= 0 {
				// Found in boundary
				bytesToEndOfToken := idx + tokenLen
				skipped += bytesToEndOfToken

				// Advance r.head by the number of bytes used from r.buf
				bufferBytesConsumed := bytesToEndOfToken - len(stash)
				r.head += bufferBytesConsumed
				return skipped, nil
			}

			// Not found in boundary, stash is definitely skipped
			skipped += len(stash)
			stash = nil
		}

		// Search in current buffer
		idx := bytes.Index(r.buf[r.head:r.tail], token)
		if idx >= 0 {
			advance := idx + tokenLen
			r.head += advance
			skipped += advance
			return skipped, nil
		}

		// Prepare stash for next iteration
		available := r.tail - r.head
		keep := min(tokenLen-1, available)

		// Bytes that are definitely skipped (not kept in stash)
		consumed := available - keep
		skipped += consumed

		if keep > 0 {
			stash = make([]byte, keep)
			copy(stash, r.buf[r.tail-keep:r.tail])
		}

		if !r.loadMore() {
			return skipped, r.Error
		}
	}
}
