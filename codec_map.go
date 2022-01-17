package avro

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfMap(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	if typ.Kind() == reflect.Map && typ.(reflect2.MapType).Key().Kind() == reflect.String {
		return decoderOfMap(cfg, schema, typ)
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfMap(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	if typ.Kind() == reflect.Map && typ.(reflect2.MapType).Key().Kind() == reflect.String {
		return encoderOfMap(cfg, schema, typ)
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func decoderOfMap(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	m := schema.(*MapSchema)
	mapType := typ.(*reflect2.UnsafeMapType)
	decoder := decoderOfType(cfg, m.Values(), mapType.Elem())

	return &mapDecoder{
		mapType:  mapType,
		elemType: mapType.Elem(),
		decoder:  decoder,
	}
}

type mapDecoder struct {
	mapType  *reflect2.UnsafeMapType
	elemType reflect2.Type
	decoder  ValDecoder
}

func (d *mapDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	if d.mapType.UnsafeIsNil(ptr) {
		d.mapType.UnsafeSet(ptr, d.mapType.UnsafeMakeMap(0))
	}

	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		for i := int64(0); i < l; i++ {
			keyPtr := reflect2.PtrOf(r.ReadString())
			elemPtr := d.elemType.UnsafeNew()
			d.decoder.Decode(elemPtr, r)

			d.mapType.UnsafeSetIndex(ptr, keyPtr, elemPtr)
		}
	}

	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		r.Error = fmt.Errorf("%v: %w", d.mapType, r.Error)
	}
}

func encoderOfMap(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	m := schema.(*MapSchema)
	mapType := typ.(*reflect2.UnsafeMapType)
	encoder := encoderOfType(cfg, m.Values(), mapType.Elem())

	return &mapEncoder{
		blockLength: cfg.getBlockLength(),
		mapType:     mapType,
		encoder:     encoder,
	}
}

type mapEncoder struct {
	blockLength int
	mapType     *reflect2.UnsafeMapType
	encoder     ValEncoder
}

func (e *mapEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	blockLength := e.blockLength

	iter := e.mapType.UnsafeIterate(ptr)

	for {
		wrote := w.WriteBlockCB(func(w *Writer) int64 {
			var i int
			for i = 0; iter.HasNext() && i < blockLength; i++ {
				keyPtr, elemPtr := iter.UnsafeNext()
				w.WriteString(*((*string)(keyPtr)))
				e.encoder.Encode(elemPtr, w)
			}

			return int64(i)
		})

		if wrote == 0 {
			break
		}
	}

	if w.Error != nil && !errors.Is(w.Error, io.EOF) {
		w.Error = fmt.Errorf("%v: %w", e.mapType, w.Error)
	}
}
