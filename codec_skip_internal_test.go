package avro

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateSkipDecoder_UnsupportedType(t *testing.T) {
	schema := NewPrimitiveSchema(Type("test"), nil, nil)

	dec := createSkipDecoder(schema)

	assert.IsType(t, &errorDecoder{}, dec)
}
