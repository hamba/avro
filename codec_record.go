package avro

import (
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/modern-go/reflect2"
)

func createDecoderOfRecord(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	switch typ.Kind() {
	case reflect.Struct:
		return decoderOfStruct(cfg, schema, typ)

	case reflect.Map:
		if typ.(reflect2.MapType).Key().Kind() != reflect.String ||
			typ.(reflect2.MapType).Elem().Kind() != reflect.Interface {
			break
		}
		return decoderOfRecord(cfg, schema, typ)

	case reflect.Ptr:
		return decoderOfPtr(cfg, schema, typ)

	case reflect.Interface:
		if ifaceType, ok := typ.(*reflect2.UnsafeIFaceType); ok {
			return &recordIfaceDecoder{schema: schema, valType: ifaceType}
		}
	}

	return &errorDecoder{err: fmt.Errorf("avro: %s is unsupported for avro %s", typ.String(), schema.Type())}
}

func createEncoderOfRecord(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	switch typ.Kind() {
	case reflect.Struct:
		return encoderOfStruct(cfg, schema, typ)

	case reflect.Map:
		if typ.(reflect2.MapType).Key().Kind() != reflect.String ||
			typ.(reflect2.MapType).Elem().Kind() != reflect.Interface {
			break
		}
		return encoderOfRecord(cfg, schema, typ)

	case reflect.Ptr:
		return encoderOfPtr(cfg, schema, typ)

	}

	return &errorEncoder{err: fmt.Errorf("avro: %s is unsupported for avro %s", typ.String(), schema.Type())}
}

func decoderOfStruct(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	rec := schema.(*RecordSchema)
	structDesc := describeStruct(cfg.getTagKey(), typ)

	var fields []*structFieldDecoder
	for _, field := range rec.Fields() {
		sf := structDesc.Fields.Get(field.Name())

		// Skip field if it doesnt exist
		if sf == nil {
			fields = append(fields, &structFieldDecoder{
				decoder: createSkipDecoder(field.Type()),
			})
			continue
		}

		dec := decoderOfType(cfg, field.Type(), sf.Field.Type())
		fields = append(fields, &structFieldDecoder{
			field:   sf.Field,
			decoder: dec,
		})
	}

	return &structDecoder{typ: typ, fields: fields}
}

type structDecoder struct {
	typ    reflect2.Type
	fields []*structFieldDecoder
}

func (d *structDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	for _, field := range d.fields {
		field.Decode(ptr, r)
	}
}

type structFieldDecoder struct {
	field   reflect2.StructField
	decoder ValDecoder
}

func (d *structFieldDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	// Skip case
	if d.field == nil {
		d.decoder.Decode(nil, r)
		return
	}

	fieldPtr := d.field.UnsafeGet(ptr)
	d.decoder.Decode(fieldPtr, r)

	if r.Error != nil && r.Error != io.EOF {
		r.Error = fmt.Errorf("%s: %s", d.field.Name(), r.Error.Error())
	}
}

func encoderOfStruct(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	rec := schema.(*RecordSchema)
	structDesc := describeStruct(cfg.getTagKey(), typ)

	var fields []*structFieldEncoder
	for _, field := range rec.Fields() {
		sf := structDesc.Fields.Get(field.Name())

		if sf == nil {
			if !field.HasDefault() {
				// In all other cases, this is a required field
				return &errorEncoder{err: fmt.Errorf("avro: record %s is missing required field %s", rec.FullName(), field.Name())}
			}

			if field.Default() == nil {
				// We write nothing in a Null case, just skip it
				continue
			}

			defaultType := reflect2.TypeOf(field.Default())
			fields = append(fields, &structFieldEncoder{
				defaultPtr: reflect2.PtrOf(field.Default()),
				encoder:    encoderOfType(cfg, field.Type(), defaultType),
			})

			continue
		}

		fields = append(fields, &structFieldEncoder{
			field:   sf.Field,
			encoder: encoderOfType(cfg, field.Type(), sf.Field.Type()),
		})
	}

	return &structEncoder{typ: typ, fields: fields}
}

type structEncoder struct {
	typ    reflect2.Type
	fields []*structFieldEncoder
}

func (e *structEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	for _, field := range e.fields {
		field.Encode(ptr, w)
	}
}

type structFieldEncoder struct {
	field      reflect2.StructField
	defaultPtr unsafe.Pointer
	encoder    ValEncoder
}

func (e *structFieldEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	// Default case
	if e.field == nil {
		e.encoder.Encode(e.defaultPtr, w)
		return
	}

	fieldPtr := e.field.UnsafeGet(ptr)
	e.encoder.Encode(fieldPtr, w)

	if w.Error != nil && w.Error != io.EOF {
		w.Error = fmt.Errorf("%s: %s", e.field.Name(), w.Error.Error())
	}
}

func decoderOfRecord(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValDecoder {
	rec := schema.(*RecordSchema)
	mapType := typ.(*reflect2.UnsafeMapType)

	fields := make([]recordMapDecoderField, len(rec.Fields()))
	for i, field := range rec.Fields() {
		fields[i] = recordMapDecoderField{
			name:    field.Name(),
			decoder: decoderOfType(cfg, field.Type(), mapType.Elem()),
		}
	}

	return &recordMapDecoder{
		mapType:  mapType,
		elemType: mapType.Elem(),
		fields:   fields,
	}
}

type recordMapDecoderField struct {
	name    string
	decoder ValDecoder
}

type recordMapDecoder struct {
	mapType  *reflect2.UnsafeMapType
	elemType reflect2.Type
	fields   []recordMapDecoderField
}

func (d *recordMapDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	if d.mapType.UnsafeIsNil(ptr) {
		d.mapType.UnsafeSet(ptr, d.mapType.UnsafeMakeMap(0))
	}

	for _, field := range d.fields {
		elem := d.elemType.UnsafeNew()
		field.decoder.Decode(elem, r)

		d.mapType.UnsafeSetIndex(ptr, reflect2.PtrOf(field), elem)
	}

	if r.Error != nil && r.Error != io.EOF {
		r.Error = fmt.Errorf("%v: %s", d.mapType, r.Error.Error())
	}
}

func encoderOfRecord(cfg *frozenConfig, schema Schema, typ reflect2.Type) ValEncoder {
	rec := schema.(*RecordSchema)
	mapType := typ.(*reflect2.UnsafeMapType)

	fields := make([]mapEncoderField, len(rec.Fields()))
	for i, field := range rec.Fields() {
		fields[i] = mapEncoderField{
			name:    field.Name(),
			hasDef:  field.HasDefault(),
			def:     field.Default(),
			encoder: encoderOfType(cfg, field.Type(), mapType.Elem()),
		}

		if field.Default() != nil {
			defaultType := reflect2.TypeOf(field.Default())
			fields[i].defEncoder = encoderOfType(cfg, field.Type(), defaultType)
		}
	}

	return &recordMapEncoder{
		mapType: mapType,
		fields:  fields,
	}
}

type mapEncoderField struct {
	name       string
	hasDef     bool
	def        interface{}
	defEncoder ValEncoder
	encoder    ValEncoder
}

type recordMapEncoder struct {
	mapType *reflect2.UnsafeMapType
	fields  []mapEncoderField
}

func (e *recordMapEncoder) Encode(ptr unsafe.Pointer, w *Writer) {
	for _, field := range e.fields {
		valPtr := e.mapType.UnsafeGetIndex(ptr, reflect2.PtrOf(field))
		if valPtr == nil {
			// Missing required field
			if !field.hasDef {
				w.Error = fmt.Errorf("avro: missing required field %s", field.name)
				return
			}

			// Null default
			if field.def == nil {
				continue
			}

			defPtr := reflect2.PtrOf(field.def)
			field.defEncoder.Encode(defPtr, w)
			continue
		}

		field.encoder.Encode(valPtr, w)
	}
}

type recordIfaceDecoder struct {
	schema  Schema
	valType *reflect2.UnsafeIFaceType
}

func (d *recordIfaceDecoder) Decode(ptr unsafe.Pointer, r *Reader) {
	obj := d.valType.UnsafeIndirect(ptr)
	if reflect2.IsNil(obj) {
		r.ReportError("decode non empty interface", "can not unmarshal into nil")
		return
	}

	r.ReadVal(d.schema, obj)
}

type structDescriptor struct {
	Type   reflect2.Type
	Fields structFields
}

type structFields []*structField

func (sf structFields) Get(name string) *structField {
	for _, f := range sf {
		if f.Name == name {
			return f
		}
	}

	return nil
}

type structField struct {
	Field reflect2.StructField
	Name  string
}

func describeStruct(tagKey string, typ reflect2.Type) *structDescriptor {
	structType := typ.(*reflect2.UnsafeStructType)
	fields := structFields{}
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		fieldName := field.Name()
		if tag, ok := field.Tag().Lookup(tagKey); ok {
			fieldName = tag
		}

		fields = append(fields, &structField{
			Field: field,
			Name:  fieldName,
		})
	}

	return &structDescriptor{
		Type:   structType,
		Fields: fields,
	}
}
