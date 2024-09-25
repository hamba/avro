package avro

import (
	"fmt"
	"unsafe"
)

func createSkipDecoder(schema Schema) ValDecoder {
	switch schema.Type() {
	case Boolean:
		return &boolSkipDecoder{}

	case Int:
		return &intSkipDecoder{}

	case Long:
		return &longSkipDecoder{}

	case Float:
		return &floatSkipDecoder{}

	case Double:
		return &doubleSkipDecoder{}

	case String:
		return &stringSkipDecoder{}

	case Bytes:
		return &bytesSkipDecoder{}

	case Record:
		return skipDecoderOfRecord(schema)

	case Ref:
		return createSkipDecoder(schema.(*RefSchema).Schema())

	case Enum:
		return &enumSkipDecoder{symbols: schema.(*EnumSchema).Symbols()}

	case Array:
		return skipDecoderOfArray(schema)

	case Map:
		return skipDecoderOfMap(schema)

	case Union:
		return skipDecoderOfUnion(schema)

	case Fixed:
		return &fixedSkipDecoder{size: schema.(*FixedSchema).Size()}

	default:
		return &errorDecoder{err: fmt.Errorf("avro: schema type %s is unsupported", schema.Type())}
	}
}

type boolSkipDecoder struct{}

func (*boolSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipBool()
}

type intSkipDecoder struct{}

func (*intSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipInt()
}

type longSkipDecoder struct{}

func (*longSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipLong()
}

type floatSkipDecoder struct{}

func (*floatSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipFloat()
}

type doubleSkipDecoder struct{}

func (*doubleSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipDouble()
}

type stringSkipDecoder struct{}

func (*stringSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipString()
}

type bytesSkipDecoder struct{}

func (c *bytesSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipBytes()
}

func skipDecoderOfRecord(schema Schema) ValDecoder {
	rec := schema.(*RecordSchema)

	decoders := make([]ValDecoder, len(rec.Fields()))
	for i, field := range rec.Fields() {
		decoders[i] = createSkipDecoder(field.Type())
	}

	return &recordSkipDecoder{
		decoders: decoders,
	}
}

type recordSkipDecoder struct {
	decoders []ValDecoder
}

func (d *recordSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	for _, decoder := range d.decoders {
		decoder.Decode(nil, r)
	}
}

type enumSkipDecoder struct {
	symbols []string
}

func (c *enumSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipInt()
}

func skipDecoderOfArray(schema Schema) ValDecoder {
	arr := schema.(*ArraySchema)
	decoder := createSkipDecoder(arr.Items())

	return &sliceSkipDecoder{
		decoder: decoder,
	}
}

type sliceSkipDecoder struct {
	decoder ValDecoder
}

func (d *sliceSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	for {
		l, size := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		if size > 0 {
			r.SkipNBytes(int(size))
			continue
		}

		for range l {
			d.decoder.Decode(nil, r)
		}
	}
}

func skipDecoderOfMap(schema Schema) ValDecoder {
	m := schema.(*MapSchema)
	decoder := createSkipDecoder(m.Values())

	return &mapSkipDecoder{
		decoder: decoder,
	}
}

type mapSkipDecoder struct {
	decoder ValDecoder
}

func (d *mapSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	for {
		l, size := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		if size > 0 {
			r.SkipNBytes(int(size))
			continue
		}

		for range l {
			r.SkipString()
			d.decoder.Decode(nil, r)
		}
	}
}

func skipDecoderOfUnion(schema Schema) ValDecoder {
	union := schema.(*UnionSchema)

	return &unionSkipDecoder{
		schema: union,
	}
}

type unionSkipDecoder struct {
	schema *UnionSchema
}

func (d *unionSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	_, resSchema := getUnionSchema(d.schema, r)
	if resSchema == nil {
		return
	}

	// In a null case, just return
	if resSchema.Type() == Null {
		return
	}

	createSkipDecoder(resSchema).Decode(nil, r)
}

type fixedSkipDecoder struct {
	size int
}

func (d *fixedSkipDecoder) Decode(_ unsafe.Pointer, r *Reader) {
	r.SkipNBytes(d.size)
}
