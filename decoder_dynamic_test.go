package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestDecoder_Interface(t *testing.T) {
	tests := []struct {
		name   string
		data   []byte
		schema string
		got    interface{}
		want   interface{}
	}{
		{
			name:   "Empty Interface",
			data:   []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema: `{"type": "record", "name": "test", "fields" : [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			got:    nil,
			want:   map[string]interface{}{"a": int64(27), "b": "foo"},
		},
		{
			name:   "Interface Non-Ptr",
			data:   []byte{0x36, 0x06, 0x66, 0x6f, 0x6f},
			schema: `{"type": "record", "name": "test", "fields": [{"name": "a", "type": "long"}, {"name": "b", "type": "string"}]}`,
			got:    TestRecord{},
			want:   map[string]interface{}{"a": int64(27), "b": "foo"},
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer ConfigTeardown()

			dec, _ := avro.NewDecoder(tt.schema, bytes.NewReader(tt.data))

			err := dec.Decode(&tt.got)

			assert.NoError(t, err)
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
