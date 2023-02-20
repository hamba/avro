package registry

import (
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_WithAPI(t *testing.T) {
	client, err := NewClient("http://example.com")
	require.NoError(t, err)

	cfg := avro.Config{}.Freeze()

	dec := NewDecoder(client, WithAPI(cfg))

	assert.Equal(t, cfg, dec.api)
}

func TestExtractSchemaID(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantID  int
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "extracts id",
			data:    []byte{0x0, 0x0, 0x0, 0x0, 0x2a},
			wantID:  42,
			wantErr: require.NoError,
		},
		{
			name:    "handles short data",
			data:    []byte{0x0, 0x0, 0x0, 0x2a},
			wantID:  0,
			wantErr: require.Error,
		},
		{
			name:    "handles bad magic",
			data:    []byte{0x1, 0x0, 0x0, 0x0, 0x2a},
			wantID:  0,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := extractSchemaID(test.data)

			test.wantErr(t, err)
			assert.Equal(t, test.wantID, got)
		})
	}
}
