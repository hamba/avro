package avro

import (
	"encoding/binary"
	"io"
	"math"
)

// WriterFunc is a function used to customize the Writer.
type WriterFunc func(w *Writer)

// WithWriterConfig specifies the configuration to use with a writer.
func WithWriterConfig(cfg API) WriterFunc {
	return func(w *Writer) {
		w.cfg = cfg.(*frozenConfig)
	}
}

// Writer is an Avro specific io.Writer.
type Writer struct {
	cfg   *frozenConfig
	out   io.Writer
	buf   []byte
	Error error
}

// NewWriter creates a new Writer.
func NewWriter(out io.Writer, bufSize int, opts ...WriterFunc) *Writer {
	writer := &Writer{
		cfg:   DefaultConfig.(*frozenConfig),
		out:   out,
		buf:   make([]byte, 0, bufSize),
		Error: nil,
	}

	for _, opt := range opts {
		opt(writer)
	}

	return writer
}

// Reset resets the Writer with a new io.Writer attached.
func (w *Writer) Reset(out io.Writer) {
	w.out = out
	w.buf = w.buf[:0]
}

// Buffered returns the number of buffered bytes.
func (w *Writer) Buffered() int {
	return len(w.buf)
}

// Buffer gets the Writer buffer.
func (w *Writer) Buffer() []byte {
	return w.buf
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	if w.out == nil {
		return nil
	}
	if w.Error != nil {
		return w.Error
	}

	n, err := w.out.Write(w.buf)
	if n < len(w.buf) && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		if w.Error == nil {
			w.Error = err
		}
		return err
	}

	w.buf = w.buf[:0]

	return nil
}

func (w *Writer) writeByte(b byte) {
	w.buf = append(w.buf, b)
}

// Write writes raw bytes to the Writer.
func (w *Writer) Write(b []byte) (int, error) {
	w.buf = append(w.buf, b...)
	return len(b), nil
}

// WriteBool writes a Bool to the Writer.
func (w *Writer) WriteBool(b bool) {
	if b {
		w.writeByte(0x01)
		return
	}
	w.writeByte(0x00)
}

// WriteInt writes an Int to the Writer.
func (w *Writer) WriteInt(i int32) {
	e := uint64((uint32(i) << 1) ^ uint32(i>>31))
	w.encodeInt(e)
}

// WriteLong writes a Long to the Writer.
func (w *Writer) WriteLong(i int64) {
	e := (uint64(i) << 1) ^ uint64(i>>63)
	w.encodeInt(e)
}

func (w *Writer) encodeInt(i uint64) {
	if i == 0 {
		w.writeByte(0)
		return
	}

	for i > 0 {
		b := byte(i) & 0x7F
		i >>= 7

		if i != 0 {
			b |= 0x80
		}
		w.writeByte(b)
	}
}

// WriteFloat writes a Float to the Writer.
func (w *Writer) WriteFloat(f float32) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, math.Float32bits(f))

	w.buf = append(w.buf, b...)
}

// WriteDouble writes a Double to the Writer.
func (w *Writer) WriteDouble(f float64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(f))

	w.buf = append(w.buf, b...)
}

// WriteBytes writes Bytes to the Writer.
func (w *Writer) WriteBytes(b []byte) {
	w.WriteLong(int64(len(b)))
	w.buf = append(w.buf, b...)
}

// WriteString reads a String to the Writer.
func (w *Writer) WriteString(s string) {
	w.WriteLong(int64(len(s)))
	w.buf = append(w.buf, s...)
}

// WriteBlockHeader writes a Block Header to the Writer.
func (w *Writer) WriteBlockHeader(l, s int64) {
	if s > 0 && !w.cfg.config.DisableBlockSizeHeader {
		w.WriteLong(-l)
		w.WriteLong(s)
		return
	}
	w.WriteLong(l)
}

// WriteBlockCB writes a block using the callback.
func (w *Writer) WriteBlockCB(callback func(w *Writer) int64) int64 {
	var dummyHeader [18]byte
	headerStart := len(w.buf)

	// Write dummy header
	_, _ = w.Write(dummyHeader[:])

	// Write block data
	capturedAt := len(w.buf)
	length := callback(w)
	size := int64(len(w.buf) - capturedAt)

	// Take a reference to the block data
	captured := w.buf[capturedAt:len(w.buf)]

	// Rewrite the header
	w.buf = w.buf[:headerStart]
	w.WriteBlockHeader(length, size)

	// Copy the block data back to its position
	w.buf = append(w.buf, captured...)

	return length
}
