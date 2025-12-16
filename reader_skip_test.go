package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReader_SkipTo(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		bufSize     int
		token       []byte
		wantSkipped int
		wantErr     require.ErrorAssertionFunc
	}{
		// TokenInFirstBuffer
		{
			name:        "TokenInFirstBuffer",
			data:        []byte("abcdefgTOKENhij"),
			bufSize:     1024,
			token:       []byte("TOKEN"),
			wantSkipped: 12, // "abcdefgTOKEN" length
			wantErr:     require.NoError,
		},
		// TokenSplitAcrossBuffers
		{
			name:        "TokenSplitAcrossBuffers",
			data:        append(append(make([]byte, 10), []byte("TO")...), []byte("KEN")...),
			bufSize:     12, // 10 filler + "TO" = 12 bytes. Split happens exactly after "TO".
			token:       []byte("TOKEN"),
			wantSkipped: 15, // 10 filler + TOKEN
			wantErr:     require.NoError,
		},
		// FalsePositiveSplit: XXKEN should NOT match TOKEN
		{
			name:        "FalsePositiveSplit",
			data:        []byte("XXKEN"),
			bufSize:     2, // Split "XX", "KEN"
			token:       []byte("TOKEN"),
			wantSkipped: 0,
			wantErr:     require.Error, // Should fail to find TOKEN
		},
		// TokenNotFound
		{
			name:        "TokenNotFound",
			data:        []byte("abcdefg"),
			bufSize:     1024,
			token:       []byte("XYZ"),
			wantSkipped: 7,
			wantErr:     require.Error, // EOF causes error in SkipTo
		},
		{
			name:        "EmptyToken",
			data:        []byte("abc"),
			bufSize:     1024,
			token:       []byte{},
			wantSkipped: 0,
			wantErr:     require.NoError,
		},
		{
			name:        "PartialMatchAtEndButNotComplete",
			data:        []byte("abcTO"),
			bufSize:     10,
			token:       []byte("TOKEN"),
			wantSkipped: 5,
			wantErr:     require.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := avro.NewReader(bytes.NewReader(tt.data), tt.bufSize)
			skipped, err := r.SkipTo(tt.token)
			tt.wantErr(t, err)
			if err == nil {
				assert.Equal(t, tt.wantSkipped, skipped)
			}
		})
	}
}
