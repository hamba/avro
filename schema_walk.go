package avro

func walkSchema(schema Schema, fn func(Schema) Schema) Schema {
	schema = fn(schema)

	switch s := schema.(type) {
	case *RecordSchema:
		for _, f := range s.Fields() {
			f.typ = walkSchema(f.typ, fn)
		}
	case *ArraySchema:
		s.items = walkSchema(s.items, fn)
	case *MapSchema:
		s.values = walkSchema(s.values, fn)
	case *UnionSchema:
		for i, st := range s.types {
			s.types[i] = walkSchema(st, fn)
		}
	}
	return schema
}
