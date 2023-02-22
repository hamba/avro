package registry_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamba/avro/v2/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_Decode(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		schema  string
		want    int
		wantErr require.ErrorAssertionFunc
	}{
		{
			name:    "decodes data",
			data:    []byte{0x0, 0x0, 0x0, 0x0, 0x2a, 0x80, 0x2},
			schema:  `{"schema":"int"}`,
			want:    128,
			wantErr: require.NoError,
		},
		{
			name:    "handles short data",
			data:    []byte{0x0, 0x0, 0x0, 0x0, 0x2a},
			schema:  `{"schema":"int"}`,
			wantErr: require.Error,
		},
		{
			name:    "handles bad magic",
			data:    []byte{0x1, 0x0, 0x0, 0x0, 0x2a, 0x80, 0x2},
			schema:  `{"schema":"int"}`,
			wantErr: require.Error,
		},
		{
			name:    "handles bad schema",
			data:    []byte{0x0, 0x0, 0x0, 0x0, 0x2a, 0x80, 0x2},
			schema:  `{"schema":"nope"}`,
			wantErr: require.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			h := http.NewServeMux()
			h.Handle("/schemas/ids/42", http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				assert.Equal(t, "GET", req.Method)

				_, _ = rw.Write([]byte(test.schema))
			}))
			srv := httptest.NewServer(h)
			t.Cleanup(srv.Close)

			client, _ := registry.NewClient(srv.URL)
			decoder := registry.NewDecoder(client)

			var got int
			err := decoder.Decode(context.Background(), test.data, &got)

			test.wantErr(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}
