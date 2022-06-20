package avro

import (
	"errors"
	"fmt"
	"sync"
)

type recursionError struct{}

func (e recursionError) Error() string {
	return ""
}

type compatKey struct {
	reader [32]byte
	writer [32]byte
}

// SchemaCompatibility determines the compatibility of schemas.
type SchemaCompatibility struct {
	cache sync.Map // map[compatKey]error
}

// NewSchemaCompatibility creates a new schema compatibility instance.
func NewSchemaCompatibility() *SchemaCompatibility {
	return &SchemaCompatibility{}
}

// Compatible determines the compatibility if the reader and writer schemas.
func (c *SchemaCompatibility) Compatible(reader, writer Schema) error {
	return c.compatible(reader, writer)
}

func (c *SchemaCompatibility) compatible(reader, writer Schema) error {
	key := compatKey{reader: reader.Fingerprint(), writer: writer.Fingerprint()}
	if err, ok := c.cache.Load(key); ok {
		if _, ok := err.(recursionError); ok {
			// Break the recursion here.
			return nil
		}

		if err == nil {
			return nil
		}

		return err.(error)
	}

	c.cache.Store(key, recursionError{})
	err := c.match(reader, writer)
	if err != nil {
		// We dont want to pay the cost of fmt.Errorf every time
		err = errors.New(err.Error())
	}
	c.cache.Store(key, err)
	return err
}

func (c *SchemaCompatibility) match(reader, writer Schema) error {
	// If the schema is a reference, get the actual schema
	if reader.Type() == Ref {
		reader = reader.(*RefSchema).Schema()
	}
	if writer.Type() == Ref {
		writer = writer.(*RefSchema).Schema()
	}

	if reader.Type() != writer.Type() {
		if writer.Type() == Union {
			// Reader must be compatible with all types in writer
			for _, schema := range writer.(*UnionSchema).Types() {
				if err := c.compatible(reader, schema); err != nil {
					return err
				}
			}

			return nil
		}

		if reader.Type() == Union {
			// Writer must be compatible with at least one reader schema
			var err error
			for _, schema := range reader.(*UnionSchema).Types() {
				err = c.compatible(schema, writer)
				if err == nil {
					return nil
				}
			}

			return fmt.Errorf("reader union lacking writer schema %s", writer.Type())
		}

		switch writer.Type() {
		case Int:
			if reader.Type() == Long || reader.Type() == Float || reader.Type() == Double {
				return nil
			}

		case Long:
			if reader.Type() == Float || reader.Type() == Double {
				return nil
			}

		case Float:
			if reader.Type() == Double {
				return nil
			}

		case String:
			if reader.Type() == Bytes {
				return nil
			}

		case Bytes:
			if reader.Type() == String {
				return nil
			}
		}

		return fmt.Errorf("reader schema %s not compatible with writer schema %s", reader.Type(), writer.Type())
	}

	switch reader.Type() {
	case Array:
		return c.compatible(reader.(*ArraySchema).Items(), writer.(*ArraySchema).Items())

	case Map:
		return c.compatible(reader.(*MapSchema).Values(), writer.(*MapSchema).Values())

	case Fixed:
		r := reader.(*FixedSchema)
		w := writer.(*FixedSchema)

		if err := c.checkSchemaName(r, w); err != nil {
			return err
		}

		if err := c.checkFixedSize(r, w); err != nil {
			return err
		}

	case Enum:
		r := reader.(*EnumSchema)
		w := writer.(*EnumSchema)

		if err := c.checkSchemaName(r, w); err != nil {
			return err
		}

		if err := c.checkEnumSymbols(r, w); err != nil {
			return err
		}

	case Record:
		r := reader.(*RecordSchema)
		w := writer.(*RecordSchema)

		if err := c.checkSchemaName(r, w); err != nil {
			return err
		}

		if err := c.checkRecordFields(r, w); err != nil {
			return err
		}

	case Union:
		for _, schema := range writer.(*UnionSchema).Types() {
			if err := c.compatible(reader, schema); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *SchemaCompatibility) checkSchemaName(reader, writer NamedSchema) error {
	if reader.FullName() != writer.FullName() {
		return fmt.Errorf("reader schema %s and writer schema %s  names do match", reader.FullName(), writer.FullName())
	}

	return nil
}

func (c *SchemaCompatibility) checkFixedSize(reader, writer *FixedSchema) error {
	if reader.Size() != writer.Size() {
		return fmt.Errorf("%s reader and writer fixed sizes do not match", reader.FullName())
	}

	return nil
}

func (c *SchemaCompatibility) checkEnumSymbols(reader, writer *EnumSchema) error {
	for _, symbol := range writer.Symbols() {
		if !c.contains(reader.Symbols(), symbol) {
			return fmt.Errorf("reader %s is missing symbol %s", reader.FullName(), symbol)
		}
	}

	return nil
}

func (c *SchemaCompatibility) checkRecordFields(reader, writer *RecordSchema) error {
	for _, field := range reader.Fields() {
		f, ok := c.getField(writer.Fields(), field)
		if !ok {
			if field.HasDefault() {
				continue
			}

			return fmt.Errorf("reader field %s is missing in writer schema and has no default", field.Name())
		}

		if err := c.compatible(field.Type(), f.Type()); err != nil {
			return err
		}
	}

	return nil
}

func (c *SchemaCompatibility) contains(a []string, s string) bool {
	for _, str := range a {
		if str == s {
			return true
		}
	}

	return false
}

func (c *SchemaCompatibility) getField(a []*Field, f *Field) (*Field, bool) {
	for _, field := range a {
		if field.Name() == f.Name() {
			return field, true
		}
	}

	return nil, false
}
