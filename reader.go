package avro

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"unsafe"
)

const (
	maxIntBufSize  = 5
	maxLongBufSize = 10
)

// ReaderFunc is a function used to customize the Reader.
type ReaderFunc func(r *Reader)

// WithReaderConfig specifies the configuration to use with a reader.
func WithReaderConfig(cfg API) ReaderFunc {
	return func(r *Reader) {
		r.cfg = cfg.(*frozenConfig)
	}
}

// Reader is an Avro specific io.Reader.
type Reader struct {
	cfg    *frozenConfig
	reader io.Reader
	slab   []byte
	buf    []byte
	head   int
	tail   int
	Error  error
}

// NewReader creates a new Reader.
func NewReader(r io.Reader, bufSize int, opts ...ReaderFunc) *Reader {
	reader := &Reader{
		cfg:    DefaultConfig.(*frozenConfig),
		reader: r,
		buf:    make([]byte, bufSize),
		head:   0,
		tail:   0,
	}

	for _, opt := range opts {
		opt(reader)
	}

	return reader
}

// Reset resets a Reader with a new byte array attached.
func (r *Reader) Reset(b []byte) *Reader {
	r.reader = nil
	r.buf = b
	r.head = 0
	r.tail = len(b)
	return r
}

// ReportError record a error in iterator instance with current position.
func (r *Reader) ReportError(operation, msg string) {
	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		return
	}

	r.Error = fmt.Errorf("avro: %s: %s", operation, msg)
}

func (r *Reader) loadMore() bool {
	if r.reader == nil {
		if r.Error == nil {
			r.head = r.tail
			r.Error = io.EOF
		}
		return false
	}

	for {
		n, err := r.reader.Read(r.buf)
		if n == 0 {
			if err != nil {
				if r.Error == nil {
					r.Error = err
				}
				return false
			}
			continue
		}

		r.head = 0
		r.tail = n
		return true
	}
}

func (r *Reader) readByte() byte {
	if r.head == r.tail {
		if !r.loadMore() {
			return 0
		}
	}

	b := r.buf[r.head]
	r.head++

	return b
}

// Read reads data into the given bytes.
func (r *Reader) Read(b []byte) {
	size := len(b)
	read := 0

	for read < size {
		if r.head == r.tail {
			if !r.loadMore() {
				return
			}
		}

		n := copy(b[read:], r.buf[r.head:r.tail])
		r.head += n
		read += n
	}
}

// ReadBool reads a Bool from the Reader.
func (r *Reader) ReadBool() bool {
	b := r.readByte()

	if b != 0 && b != 1 {
		r.ReportError("ReadBool", "invalid bool")
	}
	return b == 1
}

// ReadInt reads an Int from the Reader.
func (r *Reader) ReadInt() int32 {
	var val uint32
	var offset int8

	for r.Error == nil {
		if offset == maxIntBufSize {
			r.ReportError("ReadInt", "int overflow")
			return 0
		}

		b := r.readByte()
		val |= uint32(b&0x7F) << uint(7*offset)
		if b&0x80 == 0 {
			break
		}
		offset++
	}

	return int32((val >> 1) ^ -(val & 1))
}

// ReadLong reads a Long from the Reader.
func (r *Reader) ReadLong() int64 {
	var val uint64
	var offset int8

	for r.Error == nil {
		if offset == maxLongBufSize {
			r.ReportError("ReadLong", "long overflow")
			return 0
		}

		b := r.readByte()
		val |= uint64(b&0x7F) << uint(7*offset)
		if b&0x80 == 0 {
			break
		}
		offset++
	}

	return int64((val >> 1) ^ -(val & 1))
}

// ReadFloat reads a Float from the Reader.
func (r *Reader) ReadFloat() float32 {
	var buf [4]byte
	r.Read(buf[:])

	float := *(*float32)(unsafe.Pointer(&buf[0]))
	return float
}

// ReadDouble reads a Double from the Reader.
func (r *Reader) ReadDouble() float64 {
	var buf [8]byte
	r.Read(buf[:])

	float := *(*float64)(unsafe.Pointer(&buf[0]))
	return float
}

// ReadBytes reads Bytes from the Reader.
func (r *Reader) ReadBytes() []byte {
	return r.readBytes("bytes")
}

// ReadString reads a String from the Reader.
func (r *Reader) ReadString() string {
	b := r.readBytes("string")
	if len(b) == 0 {
		return ""
	}

	return *(*string)(unsafe.Pointer(&b))
}

func (r *Reader) readBytes(op string) []byte {
	size := int(r.ReadLong())
	if size < 0 {
		fnName := "Read" + strings.ToTitle(op)
		r.ReportError(fnName, "invalid "+op+" length")
		return nil
	}
	if size == 0 {
		return []byte{}
	}
	if max := r.cfg.getMaxByteSliceSize(); max > 0 && size > max {
		fnName := "Read" + strings.ToTitle(op)
		r.ReportError(fnName, "size is greater than `Config.MaxByteSliceSize`")
		return nil
	}

	// The bytes are entirely in the buffer and of a reasonable size.
	// Use the byte slab.
	if r.head+size <= r.tail && size <= 1024 {
		if cap(r.slab) < size {
			r.slab = make([]byte, 1024)
		}
		dst := r.slab[:size]
		r.slab = r.slab[size:]
		copy(dst, r.buf[r.head:r.head+size])
		r.head += size
		return dst
	}

	buf := make([]byte, size)
	r.Read(buf)
	return buf
}

// ReadBlockHeader reads a Block Header from the Reader.
func (r *Reader) ReadBlockHeader() (int64, int64) {
	length := r.ReadLong()
	if length < 0 {
		size := r.ReadLong()

		return -length, size
	}

	return length, 0
}
