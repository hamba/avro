package ocf_test

import (
	"bytes"
	"compress/flate"
	"errors"
	"flag"
	"io"
	"os"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update the golden files")

var schema = `{
	"type":"record",
	"name":"FullRecord",
	"namespace":"org.hamba.avro",
	"fields":[
		{"name":"strings","type":{"type":"array","items":"string"}},
		{"name":"longs","type":{"type":"array","items":"long"}},
		{"name":"enum","type":{"type":"enum","name":"foo","symbols":["A","B","C","D"]}},
		{"name":"map","type":{"type":"map","values":"int"}},
		{"name":"nullable","type":["null","string"]},
		{"name":"fixed","type":{"type":"fixed","name":"md5","size":16}},
		{"name":"record","type":{
			"type":"record",
			"name":"TestRecord",
			"fields":[
				{"name":"long","type":"long"},
				{"name":"string","type":"string"},
				{"name":"int","type":"int"},
				{"name":"float","type":"float"},
				{"name":"double","type":"double"},
				{"name":"bool","type":"boolean"}
			]
		}}
	]
}`

type FullRecord struct {
	Strings  []string       `avro:"strings"`
	Longs    []int64        `avro:"longs"`
	Enum     string         `avro:"enum"`
	Map      map[string]int `avro:"map"`
	Nullable *string        `avro:"nullable"`
	Fixed    [16]byte       `avro:"fixed"`
	Record   *TestRecord    `avro:"record"`
}

type TestRecord struct {
	Long   int64   `avro:"long"`
	String string  `avro:"string"`
	Int    int32   `avro:"int"`
	Float  float32 `avro:"float"`
	Double float64 `avro:"double"`
	Bool   bool    `avro:"bool"`
}

func TestNewDecoder_InvalidHeader(t *testing.T) {
	data := []byte{'O', 'b', 'j'}

	_, err := ocf.NewDecoder(bytes.NewReader(data))

	assert.Error(t, err)
}

func TestNewDecoder_InvalidMagic(t *testing.T) {
	data := []byte{'f', 'o', 'o', 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

	_, err := ocf.NewDecoder(bytes.NewReader(data))

	assert.Error(t, err)
}

func TestNewDecoder_InvalidSchema(t *testing.T) {
	data := []byte{'O', 'b', 'j', 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

	_, err := ocf.NewDecoder(bytes.NewReader(data))

	assert.Error(t, err)
}

func TestNewDecoder_InvalidCodec(t *testing.T) {
	data := []byte{'O', 'b', 'j', 0x1, 0x3, 0x4c, 0x16, 'a', 'v', 'r', 'o', '.', 's', 'c', 'h', 'e', 'm', 'a', 0xc, 0x22, 'l', 'o', 'n', 'g',
		0x22, 0x14, 'a', 'v', 'r', 'o', 0x2e, 'c', 'o', 'd', 'e', 'c', 0xe, 'd', 'e', 'a', 'l', 'a', 't', 'e', 0x0,
		0x72, 0xce, 0x78, 0x7, 0x35, 0x81, 0xb0, 0x80, 0x77, 0x59, 0xa9, 0x83, 0xaf, 0x90, 0x3e, 0xaf,
	}

	_, err := ocf.NewDecoder(bytes.NewReader(data))

	assert.Error(t, err)
}

func TestDecoder(t *testing.T) {
	unionStr := "union value"
	want := FullRecord{
		Strings: []string{"string1", "string2", "string3", "string4", "string5"},
		Longs:   []int64{1, 2, 3, 4, 5},
		Enum:    "C",
		Map: map[string]int{
			"key1": 1,
			"key2": 2,
			"key3": 3,
			"key4": 4,
			"key5": 5,
		},
		Nullable: &unionStr,
		Fixed:    [16]byte{0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04},
		Record: &TestRecord{
			Long:   1925639126735,
			String: "I am a test record",
			Int:    666,
			Float:  7171.17,
			Double: 916734926348163.01973408746523,
			Bool:   true,
		},
	}

	f, err := os.Open("testdata/full.avro")
	if err != nil {
		t.Error(err)
		return
	}
	t.Cleanup(func() { _ = f.Close() })

	dec, err := ocf.NewDecoder(f)
	require.NoError(t, err)

	var count int
	for dec.HasNext() {
		count++
		var got FullRecord
		err = dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, want, got)
	}

	require.NoError(t, dec.Error())
	assert.Equal(t, 1, count)
}

func TestDecoder_WithDeflate(t *testing.T) {
	unionStr := "union value"
	want := FullRecord{
		Strings: []string{"string1", "string2", "string3", "string4", "string5"},
		Longs:   []int64{1, 2, 3, 4, 5},
		Enum:    "C",
		Map: map[string]int{
			"key1": 1,
			"key2": 2,
			"key3": 3,
			"key4": 4,
			"key5": 5,
		},
		Nullable: &unionStr,
		Fixed:    [16]byte{0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04},
		Record: &TestRecord{
			Long:   1925639126735,
			String: "I am a test record",
			Int:    666,
			Float:  7171.17,
			Double: 916734926348163.01973408746523,
			Bool:   true,
		},
	}

	f, err := os.Open("testdata/full-deflate.avro")
	if err != nil {
		t.Error(err)
		return
	}
	t.Cleanup(func() { _ = f.Close() })

	dec, err := ocf.NewDecoder(f)
	require.NoError(t, err)

	var count int
	for dec.HasNext() {
		count++
		var got FullRecord
		err = dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, want, got)
	}

	require.NoError(t, dec.Error())
	assert.Equal(t, 1, count)
}

func TestDecoder_WithDeflateHandlesInvalidData(t *testing.T) {
	f, err := os.Open("testdata/deflate-invalid-data.avro")
	if err != nil {
		t.Error(err)
		return
	}
	t.Cleanup(func() { _ = f.Close() })

	dec, err := ocf.NewDecoder(f)
	require.NoError(t, err)

	dec.HasNext()

	assert.Error(t, dec.Error())
}

func TestDecoder_WithSnappy(t *testing.T) {
	unionStr := "union value"
	want := FullRecord{
		Strings: []string{"string1", "string2", "string3", "string4", "string5"},
		Longs:   []int64{1, 2, 3, 4, 5},
		Enum:    "C",
		Map: map[string]int{
			"key1": 1,
			"key2": 2,
			"key3": 3,
			"key4": 4,
			"key5": 5,
		},
		Nullable: &unionStr,
		Fixed:    [16]byte{0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04},
		Record: &TestRecord{
			Long:   1925639126735,
			String: "I am a test record",
			Int:    666,
			Float:  7171.17,
			Double: 916734926348163.01973408746523,
			Bool:   true,
		},
	}

	f, err := os.Open("testdata/full-snappy.avro")
	if err != nil {
		t.Error(err)
		return
	}
	t.Cleanup(func() { _ = f.Close() })

	dec, err := ocf.NewDecoder(f)
	require.NoError(t, err)

	var count int
	for dec.HasNext() {
		count++
		var got FullRecord
		err = dec.Decode(&got)

		require.NoError(t, err)
		assert.Equal(t, want, got)
	}

	require.NoError(t, dec.Error())
	assert.Equal(t, 1, count)
}

func TestDecoder_WithSnappyHandlesInvalidData(t *testing.T) {
	f, err := os.Open("testdata/snappy-invalid-data.avro")
	if err != nil {
		t.Error(err)
		return
	}
	t.Cleanup(func() { _ = f.Close() })

	dec, err := ocf.NewDecoder(f)
	require.NoError(t, err)

	dec.HasNext()

	assert.Error(t, dec.Error())
}

func TestDecoder_WithSnappyHandlesShortCRC(t *testing.T) {
	f, err := os.Open("testdata/snappy-short-crc.avro")
	if err != nil {
		t.Error(err)
		return
	}
	t.Cleanup(func() { _ = f.Close() })

	dec, err := ocf.NewDecoder(f)
	require.NoError(t, err)

	dec.HasNext()

	assert.Error(t, dec.Error())
}

func TestDecoder_WithSnappyHandlesInvalidCRC(t *testing.T) {
	f, err := os.Open("testdata/snappy-invalid-crc.avro")
	if err != nil {
		t.Error(err)
		return
	}
	t.Cleanup(func() { _ = f.Close() })

	dec, err := ocf.NewDecoder(f)
	if err != nil {
		t.Error(err)
		return
	}

	dec.HasNext()

	assert.Error(t, dec.Error())
}

func TestDecoder_DecodeAvroError(t *testing.T) {
	data := []byte{'O', 'b', 'j', 0x01, 0x01, 0x26, 0x16, 'a', 'v', 'r', 'o', '.', 's', 'c', 'h', 'e', 'm', 'a',
		0x0c, '"', 'l', 'o', 'n', 'g', '"', 0x00, 0xfb, 0x2b, 0x0f, 0x1a, 0xdd, 0xfd, 0x90, 0x7d, 0x87, 0x12,
		0x15, 0x29, 0xd7, 0x1d, 0x1c, 0xdd, 0x02, 0x16, 0xe2, 0xa2, 0xf3, 0xad, 0xad, 0xad, 0xe2, 0xa2, 0xf3,
		0xad, 0xad, 0xfb, 0x2b, 0x0f, 0x1a, 0xdd, 0xfd, 0x90, 0x7d, 0x87, 0x12, 0x15, 0x29, 0xd7, 0x1d, 0x1c, 0xdd,
	}

	dec, _ := ocf.NewDecoder(bytes.NewReader(data))
	_ = dec.HasNext()

	var l int64
	err := dec.Decode(&l)

	assert.Error(t, err)
}

func TestDecoder_DecodeMustCallHasNext(t *testing.T) {
	data := []byte{'O', 'b', 'j', 0x01, 0x01, 0x26, 0x16, 'a', 'v', 'r', 'o', '.', 's', 'c', 'h', 'e', 'm', 'a',
		0x0c, '"', 'l', 'o', 'n', 'g', '"', 0x00, 0xfb, 0x2b, 0x0f, 0x1a, 0xdd, 0xfd, 0x90, 0x7d, 0x87, 0x12,
		0x15, 0x29, 0xd7, 0x1d, 0x1c, 0xdd, 0x02, 0x02, 0x02, 0xfb, 0x2b, 0x0f, 0x1a, 0xdd, 0xfd, 0x90, 0x7d,
		0x87, 0x12, 0x15, 0x29, 0xd7, 0x1d, 0x1c, 0xdd,
	}

	dec, _ := ocf.NewDecoder(bytes.NewReader(data))

	var l int64
	err := dec.Decode(&l)

	assert.Error(t, err)
}

func TestDecoder_InvalidBlock(t *testing.T) {
	data := []byte{'O', 'b', 'j', 0x01, 0x01, 0x26, 0x16, 'a', 'v', 'r', 'o', '.', 's', 'c', 'h', 'e', 'm', 'a',
		0x0c, '"', 'l', 'o', 'n', 'g', '"', 0x00, 0xfa, 0x2b, 0x0f, 0x1a, 0xdd, 0xfd, 0x90, 0x7d, 0x87, 0x12,
		0x15, 0x29, 0xd7, 0x1d, 0x1c, 0xdd, 0x02, 0x02, 0x02, 0xfb, 0x2b, 0x0f, 0x1a, 0xdd, 0xfd, 0x90, 0x7d,
		0x87, 0x12, 0x15, 0x29, 0xd7, 0x1d, 0x1c, 0xdd,
	}

	dec, _ := ocf.NewDecoder(bytes.NewReader(data))

	got := dec.HasNext()

	assert.False(t, got)
	assert.Error(t, dec.Error())
}

func TestNewEncoder_InvalidSchema(t *testing.T) {
	buf := &bytes.Buffer{}

	_, err := ocf.NewEncoder(``, buf)

	assert.Error(t, err)
}

func TestNewEncoder_InvalidCodec(t *testing.T) {
	buf := &bytes.Buffer{}

	_, err := ocf.NewEncoder(`"long"`, buf, ocf.WithCodec(ocf.CodecName("test")))

	assert.Error(t, err)
}

func TestEncoder(t *testing.T) {
	unionStr := "union value"
	record := FullRecord{
		Strings: []string{"string1", "string2", "string3", "string4", "string5"},
		Longs:   []int64{1, 2, 3, 4, 5},
		Enum:    "C",
		Map: map[string]int{
			"key1": 1,
			"key2": 2,
			"key3": 3,
			"key4": 4,
			"key5": 5,
		},
		Nullable: &unionStr,
		Fixed:    [16]byte{0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04},
		Record: &TestRecord{
			Long:   1925639126735,
			String: "I am a test record",
			Int:    666,
			Float:  7171.17,
			Double: 916734926348163.01973408746523,
			Bool:   true,
		},
	}

	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf)
	require.NoError(t, err)

	err = enc.Encode(record)
	require.NoError(t, err)

	err = enc.Close()
	assert.NoError(t, err)
}

func TestEncoder_WithEncodingConfig(t *testing.T) {
	arrSchema := `{"type": "array", "items": "long"}`
	syncMarker := [16]byte{0x1F, 0x1F, 0x1F, 0x1F, 0x2F, 0x2F, 0x2F, 0x2F, 0x3F, 0x3F, 0x3F, 0x3F, 0x4F, 0x4F, 0x4F, 0x4F}

	skipOcfHeader := func(encoded []byte) []byte {
		index := bytes.Index(encoded, syncMarker[:])
		require.False(t, index == -1)
		return encoded[index+len(syncMarker):] // +1 for the null byte
	}

	tests := []struct {
		name        string
		data        any
		encConfig   avro.API
		wantPayload []byte // without OCF header
	}{
		{
			name: "no encoding config",
			data: []int64{1, 2, 3, 4, 5},
			wantPayload: []byte{
				0x2, 0x10, // OCF block header: 1 elems, 8 bytes
				0x9, 0xA, // array block header: 5 elems, 5 bytes
				0x2, 0x4, 0x6, 0x8, 0xA, 0x0, // array block payload with terminator
				0x1F, 0x1F, 0x1F, 0x1F, 0x2F, 0x2F, 0x2F, 0x2F, 0x3F, 0x3F, 0x3F, 0x3F, 0x4F, 0x4F, 0x4F, 0x4F, // OCF trailing sync marker
			},
		},
		{
			name:      "no array bytes size",
			encConfig: avro.Config{DisableBlockSizeHeader: true}.Freeze(),
			data:      []int64{1, 2, 3, 4, 5},
			wantPayload: []byte{
				0x2, 0x0E, // OCF block header: 1 elem, 7 bytes
				0xA,                          // array block header: 5 elems
				0x2, 0x4, 0x6, 0x8, 0xA, 0x0, // array block payload with terminator
				0x1F, 0x1F, 0x1F, 0x1F, 0x2F, 0x2F, 0x2F, 0x2F, 0x3F, 0x3F, 0x3F, 0x3F, 0x4F, 0x4F, 0x4F, 0x4F, // OCF trailing sync marker
			},
		},
		{
			name:      "non-default array block length",
			encConfig: avro.Config{BlockLength: 5}.Freeze(),
			data:      []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
			wantPayload: []byte{
				0x2, 0x1c, // OCF block header: 1 elems, 15 bytes
				0x9, 0xA, // array block 1 header: 5 elems, 5 bytes
				0x2, 0x4, 0x6, 0x8, 0xA, // array block 1
				0x7, 0x8, // array block 2 header: 4 elems, 4 bytes
				0xC, 0xE, 0x10, 0x12, 0x0, // array block 2 with terminator
				0x1F, 0x1F, 0x1F, 0x1F, 0x2F, 0x2F, 0x2F, 0x2F, 0x3F, 0x3F, 0x3F, 0x3F, 0x4F, 0x4F, 0x4F, 0x4F, // OCF sync marker
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			opts := []ocf.EncoderFunc{ocf.WithSyncBlock(syncMarker)}
			if tt.encConfig != nil {
				opts = append(opts, ocf.WithEncodingConfig(tt.encConfig))
			}
			enc, err := ocf.NewEncoder(arrSchema, buf, opts...)
			require.NoError(t, err)

			err = enc.Encode(tt.data)
			require.NoError(t, err)

			err = enc.Close()
			assert.NoError(t, err)

			assert.Equal(t, tt.wantPayload, skipOcfHeader(buf.Bytes()))
		})
	}

}

func TestEncoder_ExistingOCF(t *testing.T) {
	record := FullRecord{
		Strings: []string{"another", "record"},
		Enum:    "A",
		Record:  &TestRecord{},
	}

	file := copyToTemp(t, "testdata/full.avro")
	t.Cleanup(func() {
		_ = file.Close()
		_ = os.Remove(file.Name())
	})

	enc, err := ocf.NewEncoder(schema, file)
	require.NoError(t, err)

	err = enc.Encode(record)
	require.NoError(t, err)

	err = enc.Close()
	assert.NoError(t, err)

	_, err = file.Seek(0, 0)
	require.NoError(t, err)
	got, err := io.ReadAll(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/full-appended.avro", got, 0o644)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/full-appended.avro")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestEncoder_NilWriter(t *testing.T) {
	_, err := ocf.NewEncoder(schema, nil)

	assert.Error(t, err)
}

func TestEncoder_Write(t *testing.T) {
	unionStr := "union value"
	record := FullRecord{
		Strings: []string{"string1", "string2", "string3", "string4", "string5"},
		Longs:   []int64{1, 2, 3, 4, 5},
		Enum:    "C",
		Map: map[string]int{
			"key1": 1,
			"key2": 2,
			"key3": 3,
			"key4": 4,
			"key5": 5,
		},
		Nullable: &unionStr,
		Fixed:    [16]byte{0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04},
		Record: &TestRecord{
			Long:   1925639126735,
			String: "I am a test record",
			Int:    666,
			Float:  7171.17,
			Double: 916734926348163.01973408746523,
			Bool:   true,
		},
	}

	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf)
	require.NoError(t, err)

	encodedBytes, err := avro.Marshal(avro.MustParse(schema), record)
	require.NoError(t, err)

	n, err := enc.Write(encodedBytes)
	require.NoError(t, err)

	err = enc.Close()
	require.NoError(t, err)

	require.Equal(t, n, len(encodedBytes))
	require.Equal(t, 957, buf.Len())
}

func TestEncoder_EncodeCompressesDeflate(t *testing.T) {
	unionStr := "union value"
	record := FullRecord{
		Strings: []string{"string1", "string2", "string3", "string4", "string5"},
		Longs:   []int64{1, 2, 3, 4, 5},
		Enum:    "C",
		Map: map[string]int{
			"key1": 1,
			"key2": 2,
			"key3": 3,
			"key4": 4,
			"key5": 5,
		},
		Nullable: &unionStr,
		Fixed:    [16]byte{0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04},
		Record: &TestRecord{
			Long:   1925639126735,
			String: "I am a test record",
			Int:    666,
			Float:  7171.17,
			Double: 916734926348163.01973408746523,
			Bool:   true,
		},
	}

	buf := &bytes.Buffer{}
	enc, _ := ocf.NewEncoder(schema, buf, ocf.WithCodec(ocf.Deflate))

	err := enc.Encode(record)
	assert.NoError(t, err)

	err = enc.Close()

	require.NoError(t, err)
	assert.Equal(t, 926, buf.Len())
}

func TestEncoder_EncodeCompressesDeflateWithLevel(t *testing.T) {
	unionStr := "union value"
	record := FullRecord{
		Strings: []string{"string1", "string2", "string3", "string4", "string5"},
		Longs:   []int64{1, 2, 3, 4, 5},
		Enum:    "C",
		Map: map[string]int{
			"key1": 1,
			"key2": 2,
			"key3": 3,
			"key4": 4,
			"key5": 5,
		},
		Nullable: &unionStr,
		Fixed:    [16]byte{0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04},
		Record: &TestRecord{
			Long:   1925639126735,
			String: "I am a test record",
			Int:    666,
			Float:  7171.17,
			Double: 916734926348163.01973408746523,
			Bool:   true,
		},
	}

	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf, ocf.WithCompressionLevel(flate.BestCompression))
	require.NoError(t, err)

	err = enc.Encode(record)
	require.NoError(t, err)

	err = enc.Close()

	require.NoError(t, err)
	assert.Equal(t, 926, buf.Len())
}

func TestEncoder_EncodeCompressesSnappy(t *testing.T) {
	unionStr := "union value"
	record := FullRecord{
		Strings: []string{"string1", "string2", "string3", "string4", "string5"},
		Longs:   []int64{1, 2, 3, 4, 5},
		Enum:    "C",
		Map: map[string]int{
			"key1": 1,
			"key2": 2,
			"key3": 3,
			"key4": 4,
			"key5": 5,
		},
		Nullable: &unionStr,
		Fixed:    [16]byte{0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04},
		Record: &TestRecord{
			Long:   1925639126735,
			String: "I am a test record",
			Int:    666,
			Float:  7171.17,
			Double: 916734926348163.01973408746523,
			Bool:   true,
		},
	}

	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf, ocf.WithBlockLength(1), ocf.WithCodec(ocf.Snappy))
	require.NoError(t, err)

	err = enc.Encode(record)
	require.NoError(t, err)

	err = enc.Close()

	require.NoError(t, err)
	assert.Equal(t, 938, buf.Len())
}

func TestEncoder_EncodeError(t *testing.T) {
	buf := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(`"long"`, buf)
	require.NoError(t, err)
	t.Cleanup(func() { _ = enc.Close() })

	err = enc.Encode("test")

	assert.Error(t, err)
}

func TestEncoder_EncodeWritesBlocks(t *testing.T) {
	buf := &bytes.Buffer{}
	enc, _ := ocf.NewEncoder(`"long"`, buf, ocf.WithBlockLength(1))
	t.Cleanup(func() { _ = enc.Close() })

	err := enc.Encode(int64(1))

	require.NoError(t, err)
	assert.Equal(t, 77, buf.Len())
}

func TestEncoder_EncodeHandlesWriteBlockError(t *testing.T) {
	w := &errorBlockWriter{}
	enc, _ := ocf.NewEncoder(`"long"`, w, ocf.WithBlockLength(1))
	t.Cleanup(func() { _ = enc.Close() })

	err := enc.Encode(int64(1))

	assert.Error(t, err)
}

func TestEncoder_CloseHandlesWriteBlockError(t *testing.T) {
	w := &errorBlockWriter{}
	enc, _ := ocf.NewEncoder(`"long"`, w)
	_ = enc.Encode(int64(1))

	err := enc.Close()

	assert.Error(t, err)
}

func TestEncodeDecodeMetadata(t *testing.T) {
	buf := &bytes.Buffer{}
	enc, _ := ocf.NewEncoder(`"long"`, buf, ocf.WithMetadata(map[string][]byte{
		"test": []byte("foo"),
	}))

	err := enc.Encode(int64(1))
	require.NoError(t, err)

	_ = enc.Close()

	dec, err := ocf.NewDecoder(buf)

	require.NoError(t, err)
	assert.Equal(t, []byte("foo"), dec.Metadata()["test"])
}

func TestEncode_WithSyncBlock(t *testing.T) {
	buf := &bytes.Buffer{}
	syncBlock := [16]byte{9, 9, 9, 9, 9, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, err := ocf.NewEncoder(`"long"`, buf, ocf.WithSyncBlock(syncBlock))
	require.NoError(t, err)

	reader := avro.NewReader(buf, 1024)

	var h ocf.Header
	reader.ReadVal(ocf.HeaderSchema, &h)
	require.NoError(t, reader.Error)
	assert.Equal(t, syncBlock, h.Sync)
}

func TestEncoder_NoBlocks(t *testing.T) {
	buf := &bytes.Buffer{}

	_, err := ocf.NewEncoder(`"long"`, buf)

	require.NoError(t, err)
	assert.Equal(t, 58, buf.Len())
}

func TestEncoder_WriteHeaderError(t *testing.T) {
	w := &errorHeaderWriter{}

	_, err := ocf.NewEncoder(`"long"`, w)

	assert.Error(t, err)
}

func copyToTemp(t *testing.T, src string) *os.File {
	t.Helper()

	file, err := os.CreateTemp(".", "temp-*.avro")
	require.NoError(t, err)

	b, err := os.ReadFile(src)
	require.NoError(t, err)

	_, err = io.Copy(file, bytes.NewReader(b))
	require.NoError(t, err)

	_, err = file.Seek(0, 0)
	require.NoError(t, err)

	return file
}

type errorBlockWriter struct {
	headerWritten bool
}

func (ew *errorBlockWriter) Write(p []byte) (n int, err error) {
	if !ew.headerWritten {
		ew.headerWritten = true
		return len(p), nil
	}
	return 0, errors.New("test")
}

type errorHeaderWriter struct{}

func (*errorHeaderWriter) Write(p []byte) (int, error) {
	return 0, errors.New("test")
}
