package avro

import (
	"fmt"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDefaultDecoder(cfg *frozenConfig, field *Field, typ reflect2.Type) ValDecoder {
	fn := func(def any) ([]byte, error) {
		defaultType := reflect2.TypeOf(def)
		var defaultEncoder ValEncoder
		// tmp workaround: codec_union failed to resolve name of struct{} typ
		if def == nullDefault {
			defaultEncoder = &nullCodec{}
		} else {
			defaultEncoder = encoderOfType(cfg, field.Type(), defaultType)
		}
		if defaultType.LikePtr() {
			defaultEncoder = &onePtrEncoder{defaultEncoder}
		}

		w := cfg.borrowWriter()
		defaultEncoder.Encode(reflect2.PtrOf(def), w)
		if w.Error != nil {
			return nil, w.Error
		}
		if err := w.Flush(); err != nil {
			return nil, err
		}

		return w.Buffer(), nil
	}

	b, err := field.encodeDefault(fn)
	if err != nil {
		return &errorDecoder{err: fmt.Errorf("decode default: %w", err)}
	}
	return &defaultDecoder{
		defaultReader: cfg.borrowReader(b),
		decoder:       decoderOfType(cfg, field.Type(), typ),
	}
}

type defaultDecoder struct {
	defaultReader *Reader
	decoder       ValDecoder
}

// Decode implements ValDecoder.
func (d *defaultDecoder) Decode(ptr unsafe.Pointer, _ *Reader) {
	d.decoder.Decode(ptr, d.defaultReader)
}

var _ ValDecoder = &defaultDecoder{}
