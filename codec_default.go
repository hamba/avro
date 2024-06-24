package avro

import (
	"fmt"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDefaultDecoder(d *decoderContext, field *Field, typ reflect2.Type) ValDecoder {
	cfg := d.cfg
	fn := func(def any) ([]byte, error) {
		defaultType := reflect2.TypeOf(def)
		if defaultType == nil {
			defaultType = reflect2.TypeOf((*null)(nil))
		}
		defaultEncoder := encoderOfType(newEncoderContext(cfg), field.Type(), defaultType)
		if defaultType.LikePtr() {
			defaultEncoder = &onePtrEncoder{defaultEncoder}
		}
		w := cfg.borrowWriter()
		defer cfg.returnWriter(w)

		defaultEncoder.Encode(reflect2.PtrOf(def), w)
		if w.Error != nil {
			return nil, w.Error
		}
		b := w.Buffer()
		data := make([]byte, len(b))
		copy(data, b)

		return data, nil
	}

	b, err := field.encodeDefault(fn)
	if err != nil {
		return &errorDecoder{err: fmt.Errorf("decode default: %w", err)}
	}
	return &defaultDecoder{
		data:    b,
		decoder: decoderOfType(d, field.Type(), typ),
	}
}

type defaultDecoder struct {
	data    []byte
	decoder ValDecoder
}

// Decode implements ValDecoder.
func (d *defaultDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	rr := r.cfg.borrowReader(d.data)
	defer r.cfg.returnReader(rr)

	d.decoder.Decode(ptr, rr)
}

var _ ValDecoder = &defaultDecoder{}
