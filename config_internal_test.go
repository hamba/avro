package avro

import (
	"testing"

	"github.com/modern-go/reflect2"
	"github.com/stretchr/testify/assert"
)

func TestConfig_Freeze(t *testing.T) {
	api := Config{
		TagKey: "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	assert.Equal(t, "test", cfg.getTagKey())
	assert.Equal(t, 2, cfg.getBlockLength())
}

func TestConfig_ReusesDecoders(t *testing.T) {
	api := Config{
		TagKey: "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema := MustParse(`"long"`)
	var long int64
	typ := reflect2.TypeOfPtr(&long)

	dec1 := cfg.DecoderOf(schema, typ)
	dec2 := cfg.DecoderOf(schema, typ)

	assert.Equal(t, dec1, dec2)
}

func TestConfig_ReusesEncoders(t *testing.T) {
	api := Config{
		TagKey: "test",
		BlockLength: 2,
	}.Freeze()
	cfg := api.(*frozenConfig)

	schema := MustParse(`"long"`)
	var long int64
	typ := reflect2.TypeOfPtr(long)

	enc1 := cfg.EncoderOf(schema, typ)
	enc2 := cfg.EncoderOf(schema, typ)

	assert.Equal(t, enc1, enc2)
}

