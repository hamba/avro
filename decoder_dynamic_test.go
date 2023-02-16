package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecoder_Interface(t *testing.T) {
	tests := []struct {
		name   string
		data   []byte
		schema string
		got    any
		want   any
	}{
		{
			name:   "Empty Interface",
			data:   []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema: `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			got:    nil,
			want:   map[string]any{"a": int64(27), "b": "foo"},
		},
		{
			name:   "Interface Non-Ptr",
			data:   []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema: `{"type": "record", "name": "test", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			got:    TestRecord{},
			want:   map[string]any{"a": int64(27), "b": "foo"},
		},
		{
			name:   "Interface Nil Ptr",
			data:   []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema: `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			got:    (*TestRecord)(nil),
			want:   &TestRecord{A: 27, B: "foo"},
		},
		{
			name:   "Interface Ptr",
			data:   []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema: `{"type": "record", "name": "test", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			got:    &TestRecord{},
			want:   &TestRecord{A: 27, B: "foo"},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			defer ConfigTeardown()

			dec, _ := avro.NewDecoder(test.schema, bytes.NewReader(test.data))

			err := dec.Decode(&test.got)

			require.NoError(t, err)
			assert.Equal(t, test.want, test.got)
		})
	}
}
