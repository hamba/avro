package avro_test

import (
	"bytes"
	"testing"

	"github.com/hamba/avro/v2"
)

func BenchmarkDecoder_Interface(b *testing.B) {
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
		b.Run(test.name, func(b *testing.B) {
			defer ConfigTeardown()
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				dec, _ := avro.NewDecoder(test.schema, bytes.NewReader(test.data))
				_ = dec.Decode(&test.got)
			}
		})
	}
}
