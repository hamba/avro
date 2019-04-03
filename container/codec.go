package container

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/golang/snappy"
)

type CodecName string

const (
	Null    CodecName = "null"
	Deflate CodecName = "deflate"
	Snappy  CodecName = "snappy"
)

func resolveCodec(name CodecName) (Codec, error) {
	switch name {
	case Null, "":
		return &NullCodec{}, nil

	case Deflate:
		return &DeflateCodec{}, nil

	case Snappy:
		return &SnappyCodec{}, nil

	default:
		return nil, fmt.Errorf("unknown codec %s", name)
	}
}

type Codec interface {
	Decode([]byte) ([]byte, error)
	Encode([]byte) ([]byte, error)
}

type NullCodec struct{}

func (*NullCodec) Decode(b []byte) ([]byte, error) {
	return b, nil
}

func (*NullCodec) Encode(b []byte) ([]byte, error) {
	return b, nil
}

type DeflateCodec struct{}

func (*DeflateCodec) Decode(b []byte) ([]byte, error) {
	return b, nil
}

func (*DeflateCodec) Encode(b []byte) ([]byte, error) {
	return b, nil
}

type SnappyCodec struct{}

func (*SnappyCodec) Decode(b []byte) ([]byte, error) {
	l := len(b)
	if l < 5 {
		return nil, errors.New("block does not contain snappy checksum")
	}

	data := b[:l-4]

	dst, err := snappy.Decode(nil, data)
	if err != nil {
		return nil, err
	}

	crc := binary.BigEndian.Uint32(b[l-4:])
	if crc32.ChecksumIEEE(dst) != crc {
		return nil, errors.New("snappy checksum mismatch")
	}

	return b, nil
}

func (*SnappyCodec) Encode(b []byte) ([]byte, error) {
	dst := snappy.Encode(nil, b)

	dst = append(dst, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(dst[len(dst)-4:], crc32.ChecksumIEEE(b))

	return dst, nil
}
