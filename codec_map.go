package avro

import (
	"encoding"
	"errors"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfMap(d *decoderContext, schema *MapSchema, typ reflect2.Type) ValDecoder {
	if typ.Kind() == reflect.Map {
		keyType := typ.(reflect2.MapType).Key()
		switch {
		case keyType.Kind() == reflect.String:
			return decoderOfMap(d, schema, typ)
		case keyType.Implements(textUnmarshalerType):
			return decoderOfMapUnmarshaler(d, schema, typ)
		}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func createEncoderOfMap(e *encoderContext, schema *MapSchema, typ reflect2.Type) ValEncoder {
	if typ.Kind() == reflect.Map {
		keyType := typ.(reflect2.MapType).Key()
		switch {
		case keyType.Kind() == reflect.String:
			return encoderOfMap(e, schema, typ)
		case keyType.Implements(textMarshalerType):
			return encoderOfMapMarshaler(e, schema, typ)
		}
	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for Avro %s", typ.String(), schema.Type())}
}

func decoderOfMap(d *decoderContext, m *MapSchema, typ reflect2.Type) ValDecoder {
	mapType := typ.(*reflect2.UnsafeMapType)
	decoder := decoderOfType(d, m.Values(), mapType.Elem())

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
			if r.Error != nil {
				r.Error = fmt.Errorf("reading map[string]%s: %w", d.elemType.String(), r.Error)
				return
			}

			d.mapType.UnsafeSetIndex(ptr, keyPtr, elemPtr)
		}
	}

	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		r.Error = fmt.Errorf("%v: %w", d.mapType, r.Error)
	}
}

func decoderOfMapUnmarshaler(d *decoderContext, m *MapSchema, typ reflect2.Type) ValDecoder {
	mapType := typ.(*reflect2.UnsafeMapType)
	decoder := decoderOfType(d, m.Values(), mapType.Elem())

	return &mapDecoderUnmarshaler{
		mapType:  mapType,
		keyType:  mapType.Key(),
		elemType: mapType.Elem(),
		decoder:  decoder,
	}
}

type mapDecoderUnmarshaler struct {
	mapType  *reflect2.UnsafeMapType
	keyType  reflect2.Type
	elemType reflect2.Type
	decoder  ValDecoder
}

func (d *mapDecoderUnmarshaler) Decode(ptr unsafe.Pointer, r *Reader) {
	if d.mapType.UnsafeIsNil(ptr) {
		d.mapType.UnsafeSet(ptr, d.mapType.UnsafeMakeMap(0))
	}

	for {
		l, _ := r.ReadBlockHeader()
		if l == 0 {
			break
		}

		for i := int64(0); i < l; i++ {
			keyPtr := d.keyType.UnsafeNew()
			keyObj := d.keyType.UnsafeIndirect(keyPtr)
			if reflect2.IsNil(keyObj) {
				ptrType := d.keyType.(*reflect2.UnsafePtrType)
				newPtr := ptrType.Elem().UnsafeNew()
				*((*unsafe.Pointer)(keyPtr)) = newPtr
				keyObj = d.keyType.UnsafeIndirect(keyPtr)
			}
			unmarshaler := keyObj.(encoding.TextUnmarshaler)
			err := unmarshaler.UnmarshalText([]byte(r.ReadString()))
			if err != nil {
				r.ReportError("mapDecoderUnmarshaler", err.Error())
				return
			}

			elemPtr := d.elemType.UnsafeNew()
			d.decoder.Decode(elemPtr, r)

			d.mapType.UnsafeSetIndex(ptr, keyPtr, elemPtr)
		}
	}

	if r.Error != nil && !errors.Is(r.Error, io.EOF) {
		r.Error = fmt.Errorf("%v: %w", d.mapType, r.Error)
	}
}

func encoderOfMap(e *encoderContext, m *MapSchema, typ reflect2.Type) ValEncoder {
	mapType := typ.(*reflect2.UnsafeMapType)
	encoder := encoderOfType(e, m.Values(), mapType.Elem())

	return &mapEncoder{
		blockLength: e.cfg.getBlockLength(),
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

func encoderOfMapMarshaler(e *encoderContext, m *MapSchema, typ reflect2.Type) ValEncoder {
	mapType := typ.(*reflect2.UnsafeMapType)
	encoder := encoderOfType(e, m.Values(), mapType.Elem())

	return &mapEncoderMarshaller{
		blockLength: e.cfg.getBlockLength(),
		mapType:     mapType,
		keyType:     mapType.Key(),
		encoder:     encoder,
	}
}

type mapEncoderMarshaller struct {
	blockLength int
	mapType     *reflect2.UnsafeMapType
	keyType     reflect2.Type
	encoder     ValEncoder
}

func (e *mapEncoderMarshaller) Encode(ptr unsafe.Pointer, w *Writer) {
	blockLength := e.blockLength

	iter := e.mapType.UnsafeIterate(ptr)

	for {
		wrote := w.WriteBlockCB(func(w *Writer) int64 {
			var i int
			for i = 0; iter.HasNext() && i < blockLength; i++ {
				keyPtr, elemPtr := iter.UnsafeNext()

				obj := e.keyType.UnsafeIndirect(keyPtr)
				if e.keyType.IsNullable() && reflect2.IsNil(obj) {
					w.Error = errors.New("avro: mapEncoderMarshaller: encoding nil TextMarshaller")
					return int64(0)
				}
				marshaler := (obj).(encoding.TextMarshaler)
				b, err := marshaler.MarshalText()
				if err != nil {
					w.Error = err
					return int64(0)
				}
				w.WriteString(string(b))

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
