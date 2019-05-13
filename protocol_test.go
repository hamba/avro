package avro_test

import (
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestParseProtocolFile(t *testing.T) {
	protocol, err := avro.ParseProtocolFile("testdata/echo.avpr")

	want := ``
	wantMD5 := ""
	assert.NoError(t, err)
	assert.Equal(t, want, protocol.String())
	assert.Equal(t, wantMD5, protocol.Hash())
}

func TestParseProtocolFile_InvalidPath(t *testing.T) {
	_, err := avro.ParseProtocolFile("test.avpr")

	assert.Error(t, err)
}