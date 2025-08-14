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

// WithDirectWrite enables direct writing to the underlying io.Writer without buffering.
func WithDirectWrite(direct bool) WriterFunc {
	return func(w *Writer) {
		w.directWrite = direct
	}
}

// Writer is an Avro specific io.Writer.
type Writer struct {
	cfg         *frozenConfig
	out         io.Writer
	buf         []byte
	directWrite bool
	Error       error
}

// NewWriter creates a new Writer.
func NewWriter(out io.Writer, bufSize int, opts ...WriterFunc) *Writer {
	writer := &Writer{
		cfg:         DefaultConfig.(*frozenConfig),
		out:         out,
		buf:         make([]byte, 0, bufSize),
		directWrite: false,
		Error:       nil,
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

// IsDirectWrite returns true if the writer is in direct write mode.
func (w *Writer) IsDirectWrite() bool {
	return w.directWrite
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	// If direct write is enabled, buffer is always empty, so nothing to flush
	if w.directWrite {
		return nil
	}

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
	if w.directWrite {
		if w.Error != nil {
			return
		}
		_, err := w.out.Write([]byte{b})
		if err != nil {
			w.Error = err
		}
		return
	}
	w.buf = append(w.buf, b)
}

// Write writes raw bytes to the Writer.
func (w *Writer) Write(b []byte) (int, error) {
	if w.directWrite {
		if w.Error != nil {
			return 0, w.Error
		}
		n, err := w.out.Write(b)
		if err != nil {
			w.Error = err
		}
		return n, err
	}
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

	if w.directWrite {
		if w.Error != nil {
			return
		}
		_, err := w.out.Write(b)
		if err != nil {
			w.Error = err
		}
		return
	}
	w.buf = append(w.buf, b...)
}

// WriteDouble writes a Double to the Writer.
func (w *Writer) WriteDouble(f float64) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(f))

	if w.directWrite {
		if w.Error != nil {
			return
		}
		_, err := w.out.Write(b)
		if err != nil {
			w.Error = err
		}
		return
	}
	w.buf = append(w.buf, b...)
}

// WriteBytes writes Bytes to the Writer.
func (w *Writer) WriteBytes(b []byte) {
	w.WriteLong(int64(len(b)))
	if w.directWrite {
		if w.Error != nil {
			return
		}
		_, err := w.out.Write(b)
		if err != nil {
			w.Error = err
		}
		return
	}
	w.buf = append(w.buf, b...)
}

// WriteString reads a String to the Writer.
func (w *Writer) WriteString(s string) {
	w.WriteLong(int64(len(s)))
	if w.directWrite {
		if w.Error != nil {
			return
		}
		_, err := w.out.Write([]byte(s))
		if err != nil {
			w.Error = err
		}
		return
	}
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
	// Note: WriteBlockCB requires buffering to work correctly, so we temporarily
	// disable direct write for this operation
	originalDirectWrite := w.directWrite
	w.directWrite = false

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

	// 关键修复：如果原来是直接写入模式，需要把缓冲区的数据写入到 out
	if originalDirectWrite {
		// 强制写入缓冲区数据到 out
		if w.out != nil && len(w.buf) > 0 {
			_, err := w.out.Write(w.buf)
			if err != nil {
				w.Error = err
			}
			// 清空缓冲区
			w.buf = w.buf[:0]
		}
	}

	// Restore original direct write setting
	w.directWrite = originalDirectWrite

	return length
}
