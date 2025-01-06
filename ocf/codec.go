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
}

func resolveCodec(name CodecName, codecOpts codecOptions) (Codec, error) {
	switch name {
	case Null, "":
		return &NullCodec{}, nil

	case Deflate:
		return &DeflateCodec{compLvl: codecOpts.DeflateCompressionLevel}, nil

	case Snappy:
		return &SnappyCodec{}, nil

	case ZStandard:
		return newZStandardCodec(codecOpts.ZStandardOptions), nil

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
type ZStandardCodec struct {
	decoder *zstd.Decoder
	encoder *zstd.Encoder
}

func newZStandardCodec(opts zstdOptions) *ZStandardCodec {
	decoder, _ := zstd.NewReader(nil, opts.DOptions...)
	encoder, _ := zstd.NewWriter(nil, opts.EOptions...)
	return &ZStandardCodec{
		decoder: decoder,
		encoder: encoder,
	}
}

// Decode decodes the given bytes.
func (zstdCodec *ZStandardCodec) Decode(b []byte) ([]byte, error) {
	defer func() { _ = zstdCodec.decoder.Reset(nil) }()
	return zstdCodec.decoder.DecodeAll(b, nil)
}

// Encode encodes the given bytes.
func (zstdCodec *ZStandardCodec) Encode(b []byte) []byte {
	defer zstdCodec.encoder.Reset(nil)
	return zstdCodec.encoder.EncodeAll(b, nil)
}
