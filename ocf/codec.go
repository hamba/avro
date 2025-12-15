package ocf

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

type codecMode int

const (
	codecModeEncode codecMode = iota
	codecModeDecode
)

// CodecName represents a compression codec name.
type CodecName string

// Supported compression codecs.
const (
	Null      CodecName = "null"
	Deflate   CodecName = "deflate"
	Snappy    CodecName = "snappy"
	ZStandard CodecName = "zstandard"
)

type codecOptions struct {
	DeflateCompressionLevel int
	ZStandardOptions        zstdOptions
}

type zstdOptions struct {
	EOptions []zstd.EOption
	DOptions []zstd.DOption
	// Encoder and Decoder allow sharing pre-created instances across multiple codecs.
	// When set, EOptions/DOptions are ignored for that component.
	Encoder *zstd.Encoder
	Decoder *zstd.Decoder
}

func resolveCodec(name CodecName, codecOpts codecOptions, mode codecMode) (Codec, error) {
	switch name {
	case Null, "":
		return &NullCodec{}, nil

	case Deflate:
		return &DeflateCodec{compLvl: codecOpts.DeflateCompressionLevel}, nil

	case Snappy:
		return &SnappyCodec{}, nil

	case ZStandard:
		return newZStandardCodec(codecOpts.ZStandardOptions, mode)

	default:
		return nil, fmt.Errorf("unknown codec %s", name)
	}
}

// Codec represents a compression codec.
type Codec interface {
	// Decode decodes the given bytes.
	Decode([]byte) ([]byte, error)
	// Encode encodes the given bytes.
	Encode([]byte) []byte
}

// NullCodec is a no op codec.
type NullCodec struct{}

// Decode decodes the given bytes.
func (*NullCodec) Decode(b []byte) ([]byte, error) {
	return b, nil
}

// Encode encodes the given bytes.
func (*NullCodec) Encode(b []byte) []byte {
	return b
}

// DeflateCodec is a flate compression codec.
type DeflateCodec struct {
	compLvl int
}

// Decode decodes the given bytes.
func (c *DeflateCodec) Decode(b []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewBuffer(b))
	data, err := io.ReadAll(r)
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	_ = r.Close()

	return data, nil
}

// Encode encodes the given bytes.
func (c *DeflateCodec) Encode(b []byte) []byte {
	data := bytes.NewBuffer(make([]byte, 0, len(b)))

	w, _ := flate.NewWriter(data, c.compLvl)
	_, _ = w.Write(b)
	_ = w.Close()

	return data.Bytes()
}

// SnappyCodec is a snappy compression codec.
type SnappyCodec struct{}

// Decode decodes the given bytes.
func (*SnappyCodec) Decode(b []byte) ([]byte, error) {
	l := len(b)
	if l < 5 {
		return nil, errors.New("block does not contain snappy checksum")
	}

	dst, err := snappy.Decode(nil, b[:l-4])
	if err != nil {
		return nil, err
	}

	crc := binary.BigEndian.Uint32(b[l-4:])
	if crc32.ChecksumIEEE(dst) != crc {
		return nil, errors.New("snappy checksum mismatch")
	}

	return dst, nil
}

// Encode encodes the given bytes.
func (*SnappyCodec) Encode(b []byte) []byte {
	dst := snappy.Encode(nil, b)

	dst = append(dst, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(dst[len(dst)-4:], crc32.ChecksumIEEE(b))

	return dst
}

// ZStandardCodec is a zstandard compression codec.
// It creates only an encoder or decoder based on mode, not both.
type ZStandardCodec struct {
	decoder *zstd.Decoder
	encoder *zstd.Encoder
	// owns tracks whether we created the encoder/decoder and should close it.
	owns bool
}

func newZStandardCodec(opts zstdOptions, mode codecMode) (*ZStandardCodec, error) {
	c := &ZStandardCodec{}

	switch mode {
	case codecModeEncode:
		if opts.Encoder != nil {
			c.encoder = opts.Encoder
		} else {
			enc, err := zstd.NewWriter(nil, opts.EOptions...)
			if err != nil {
				return nil, err
			}
			c.encoder = enc
			c.owns = true
		}
	case codecModeDecode:
		if opts.Decoder != nil {
			c.decoder = opts.Decoder
		} else {
			dec, err := zstd.NewReader(nil, opts.DOptions...)
			if err != nil {
				return nil, err
			}
			c.decoder = dec
			c.owns = true
		}
	}

	return c, nil
}

// Decode decodes the given bytes.
func (c *ZStandardCodec) Decode(b []byte) ([]byte, error) {
	return c.decoder.DecodeAll(b, nil)
}

// Encode encodes the given bytes.
func (c *ZStandardCodec) Encode(b []byte) []byte {
	return c.encoder.EncodeAll(b, nil)
}

// Close closes the zstandard encoder or decoder, releasing resources.
// If the encoder/decoder was provided externally (shared), it is not closed.
func (c *ZStandardCodec) Close() error {
	if !c.owns {
		return nil
	}
	if c.decoder != nil {
		c.decoder.Close()
	}
	if c.encoder != nil {
		return c.encoder.Close()
	}
	return nil
}
