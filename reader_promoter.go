package avro

import (
	"reflect"
)

type ReaderPromoter interface {
	ReadLong() int64
	ReadFloat() float32
	ReadDouble() float64
	ReadString() string
	ReadBytes() []byte
}

type readerPromoter struct {
	actual, current Type
	r               *Reader
}

var _ ReaderPromoter = &readerPromoter{}

var promotedInvalid = struct{}{}

func (p *readerPromoter) readActual() any {
	switch p.actual {
	case Int:
		return p.r.ReadInt()

	case Long:
		return p.r.ReadLong()

	case Float:
		return p.r.ReadFloat()

	case String:
		return p.r.ReadString()

	case Bytes:
		return p.r.ReadBytes()

	default:
		p.r.ReportError("decode promotable", "unsupported actual type")
		return promotedInvalid
	}
}

func (p *readerPromoter) ReadLong() int64 {
	if v := p.readActual(); v != promotedInvalid {
		return p.promote(v, p.current).(int64)
	}

	return 0
}

func (p *readerPromoter) ReadFloat() float32 {
	if v := p.readActual(); v != promotedInvalid {
		return p.promote(v, p.current).(float32)
	}

	return 0
}

func (p *readerPromoter) ReadDouble() float64 {
	if v := p.readActual(); v != promotedInvalid {
		return p.promote(v, p.current).(float64)
	}

	return 0
}

func (p *readerPromoter) ReadString() string {
	if v := p.readActual(); v != promotedInvalid {
		return p.promote(v, p.current).(string)
	}

	return ""
}

func (p *readerPromoter) ReadBytes() []byte {
	if v := p.readActual(); v != promotedInvalid {
		return p.promote(v, p.current).([]byte)
	}

	return nil
}

func (p *readerPromoter) promote(obj any, st Type) (t any) {
	switch st {
	case Long:
		return int64(reflect.ValueOf(obj).Int())
	case Float:
		return float32(reflect.ValueOf(obj).Int())
	case Double:
		return float64(reflect.ValueOf(obj).Float())
	case String:
		return string(reflect.ValueOf(obj).Bytes())
	case Bytes:
		return []byte(reflect.ValueOf(obj).String())
	}

	return obj
}
