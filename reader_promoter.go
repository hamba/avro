package avro

type iReaderPromoter interface {
	ReadLong() int64
	ReadFloat() float32
	ReadDouble() float64
	ReadString() string
	ReadBytes() []byte
}

type readerPromoter struct {
	actual Type
	r      *Reader
	converter
}

func newReaderPromoter(actual Type, r *Reader) *readerPromoter {
	rp := &readerPromoter{
		actual:    actual,
		r:         r,
		converter: resolveConverter(actual),
	}

	return rp
}

var _ iReaderPromoter = &readerPromoter{}

func (p *readerPromoter) ReadLong() int64 {
	if p.toLong != nil {
		return p.toLong(p.r)
	}

	return p.r.ReadLong()
}

func (p *readerPromoter) ReadFloat() float32 {
	if p.toFloat != nil {
		return p.toFloat(p.r)
	}

	return p.r.ReadFloat()
}

func (p *readerPromoter) ReadDouble() float64 {
	if p.toDouble != nil {
		v := p.toDouble(p.r)
		return v
	}

	return p.r.ReadDouble()
}

func (p *readerPromoter) ReadString() string {
	if p.toString != nil {
		return p.toString(p.r)
	}

	return p.r.ReadString()
}

func (p *readerPromoter) ReadBytes() []byte {
	if p.toBytes != nil {
		return p.toBytes(p.r)
	}

	return p.r.ReadBytes()
}
