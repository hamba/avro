package avro

import (
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDefaultDecoder(
	cfg *frozenConfig,
	schema Schema,
	def any,
	typ reflect2.Type,
	w *Writer,
	r *Reader,
) ValDecoder {
	defaultType := reflect2.TypeOf(def)
	var defaultEncoder ValEncoder
	// tmp workaround: codec_union failed to resolve name of struct{} typ
	if def == nullDefault {
		defaultEncoder = &nullCodec{}
	} else {
		defaultEncoder = encoderOfType(cfg, schema, defaultType)
	}
	if defaultType.LikePtr() {
		defaultEncoder = &onePtrEncoder{defaultEncoder}
	}
	defaultEncoder.Encode(reflect2.PtrOf(def), w)

	return &defaultDecoder{
		defaultReader: r,
		decoder:       decoderOfType(cfg, schema, typ),
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
