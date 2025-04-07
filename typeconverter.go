package avro

import (
	"sync"
)

// TypeConverter represents a data type converter.
type TypeConverter interface {
	// Type returns the Avro data type of the type converter.
	Type() Type

	// LogicalType returns the Avro logical data type of the type converter.
	LogicalType() LogicalType

	// EncodeTypeConvert is the type conversion function called before encoding to Avro.
	EncodeTypeConvert(in any) (any, error)

	// DecodeTypeConvert is the type conversion function called after decoding from Avro.
	DecodeTypeConvert(in any) (any, error)
}

type specificType struct {
	typ Type
	lt  LogicalType
}

// TypeConverters holds the user-provided type conversion functions.
type TypeConverters struct {
	convs sync.Map // map[specificType]TypeConverter
}

// NewTypeConverters creates a new type converter.
func NewTypeConverters() *TypeConverters {
	return &TypeConverters{}
}

// RegisterTypeConverters registers type converters for converting the data types during encoding and decoding.
func (c *TypeConverters) RegisterTypeConverters(convs ...TypeConverter) {
	for _, conv := range convs {
		if typ := conv.Type(); len(typ) == 0 {
			continue
		}
		c.convs.Store(specificType{typ: conv.Type(), lt: conv.LogicalType()}, conv)
	}
}

// EncodeTypeConvert runs the encode type conversion function for the given value and schema.
func (c *TypeConverters) EncodeTypeConvert(in any, schema Schema) (any, error) {
	conv, ok := c.getTypeConverter(schema)
	if !ok {
		return in, nil
	}

	return conv.EncodeTypeConvert(in)
}

// DecodeTypeConvert runs the decode type conversion function for the given value and schema.
func (c *TypeConverters) DecodeTypeConvert(in any, schema Schema) (any, error) {
	conv, ok := c.getTypeConverter(schema)
	if !ok {
		return in, nil
	}

	return conv.DecodeTypeConvert(in)
}

func (c *TypeConverters) getTypeConverter(schema Schema) (TypeConverter, bool) {
	typ := schema.Type()
	lt := getLogicalType(schema)
	conv, ok := c.convs.Load(specificType{typ: typ, lt: lt})
	if !ok {
		return nil, false
	}
	return conv.(TypeConverter), ok
}

// RegisterTypeConverters registers type converters for encoding and decoding the data type specified in the converter.
func RegisterTypeConverters(convs ...TypeConverter) {
	DefaultConfig.RegisterTypeConverters(convs...)
}

// TypeConversionFuncs is a helper struct to reduce boilerplate in user code.
//
// Implements the TypeConverter interface.
type TypeConversionFuncs struct {
	AvroType              Type
	AvroLogicalType       LogicalType
	EncoderTypeConversion func(in any) (any, error)
	DecoderTypeConversion func(in any) (any, error)
}

// Type returns the Avro data type of the type converter.
func (c TypeConversionFuncs) Type() Type {
	return c.AvroType
}

// LogicalType returns the Avro data type of the type converter.
func (c TypeConversionFuncs) LogicalType() LogicalType {
	return c.AvroLogicalType
}

// EncodeTypeConvert runs the converter's encoder type conversion function if it's set.
func (c TypeConversionFuncs) EncodeTypeConvert(in any) (any, error) {
	if c.EncoderTypeConversion == nil {
		return in, nil
	}
	return c.EncoderTypeConversion(in)
}

// DecodeTypeConvert runs the converter's decoder type conversion function if it's set.
func (c TypeConversionFuncs) DecodeTypeConvert(in any) (any, error) {
	if c.DecoderTypeConversion == nil {
		return in, nil
	}
	return c.DecoderTypeConversion(in)
}
