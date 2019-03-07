package bytesx_test

import (
	"io"
	"testing"

	"github.com/hamba/avro/internal/bytesx"
	"github.com/stretchr/testify/assert"
)

func TestNewResetReader(t *testing.T) {
	r := bytesx.NewResetReader([]byte{})

	assert.IsType(t, &bytesx.ResetReader{}, r)
	assert.Implements(t, (*io.Reader)(nil), r)
}

func TestResetReader_Read(t *testing.T) {
	r := bytesx.NewResetReader([]byte("test"))

	b := make([]byte, 4)
	n, err := r.Read(b)

	assert.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("test"), b)
}

func TestResetReader_ReadReturnsEOF(t *testing.T) {
	r := bytesx.NewResetReader([]byte{})

	b := make([]byte, 4)
	n, err := r.Read(b)

	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 0, n)
}
