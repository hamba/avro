package ocf_test

import (
	"bytes"
	"compress/flate"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
	"github.com/klauspost/compress/zstd"
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
	data := []byte{
		'O', 'b', 'j', 0x1, 0x3, 0x4c, 0x16, 'a', 'v', 'r', 'o', '.', 's', 'c', 'h', 'e', 'm', 'a', 0xc, 0x22, 'l', 'o', 'n', 'g',
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

func TestDecoder_InvalidName(t *testing.T) {
	type record struct {
		Hello int    `avro:"hello"`
		What  string `avro:"what"`
	}
	want := record{
		What:  "yes",
		Hello: 1,
	}

	f, err := os.Open("testdata/invalid-name.avro")
	if err != nil {
		t.Error(err)
		return
	}
	t.Cleanup(func() { _ = f.Close() })

	avro.SkipNameValidation = true
	defer func() { avro.SkipNameValidation = false }()

	dec, err := ocf.NewDecoder(f)
	require.NoError(t, err)

	var count int
	for dec.HasNext() {
		count++
		var got record
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

func TestDecoder_WithZStandard(t *testing.T) {
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

	f, err := os.Open("testdata/full-zstd.avro")
	require.NoError(t, err)
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

func TestDecoder_WithZStandardHandlesInvalidData(t *testing.T) {
	f, err := os.Open("testdata/zstd-invalid-data.avro")
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	dec, err := ocf.NewDecoder(f)
	require.NoError(t, err)

	dec.HasNext()

	assert.Error(t, dec.Error())
}

func TestDecoder_WithZStandardOptions(t *testing.T) {
	unionStr := "union value"
	want := FullRecord{
		Strings: []string{"string1", "string2", "string3", "string4", "string5"},
		Longs:   []int64{1, 2, 3, 4, 5},
		Enum:    "C",
		Map: map[string]int{
			"ke\xa9\xb1": 1,
			"\x00\x00y2": 2,
			"key3":       3,
			"key4":       4,
			"key5":       5,
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

	f, err := os.Open("testdata/zstd-invalid-data.avro")
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	dec, err := ocf.NewDecoder(f, ocf.WithZStandardDecoderOptions(zstd.IgnoreChecksum(true)))
	require.NoError(t, err)

	dec.HasNext()

	var got FullRecord
	err = dec.Decode(&got)

	require.NoError(t, err, "should not cause an error because checksum is ignored")
	require.NoError(t, dec.Error(), "should not cause an error because checksum is ignored")
	assert.Equal(t, want, got, "should read corrupted data as valid because checksum is ignored")

	dec.HasNext()

	assert.ErrorContains(t, dec.Error(), "decoder: invalid block", "trailing byte in file should cause error before hitting zstd decoder")
}

func TestDecoder_DecodeAvroError(t *testing.T) {
	data := []byte{
		'O', 'b', 'j', 0x01, 0x01, 0x26, 0x16, 'a', 'v', 'r', 'o', '.', 's', 'c', 'h', 'e', 'm', 'a',
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
	data := []byte{
		'O', 'b', 'j', 0x01, 0x01, 0x26, 0x16, 'a', 'v', 'r', 'o', '.', 's', 'c', 'h', 'e', 'm', 'a',
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
	data := []byte{
		'O', 'b', 'j', 0x01, 0x01, 0x26, 0x16, 'a', 'v', 'r', 'o', '.', 's', 'c', 'h', 'e', 'm', 'a',
		0x0c, '"', 'l', 'o', 'n', 'g', '"', 0x00, 0xfa, 0x2b, 0x0f, 0x1a, 0xdd, 0xfd, 0x90, 0x7d, 0x87, 0x12,
		0x15, 0x29, 0xd7, 0x1d, 0x1c, 0xdd, 0x02, 0x02, 0x02, 0xfb, 0x2b, 0x0f, 0x1a, 0xdd, 0xfd, 0x90, 0x7d,
		0x87, 0x12, 0x15, 0x29, 0xd7, 0x1d, 0x1c, 0xdd,
	}

	dec, _ := ocf.NewDecoder(bytes.NewReader(data))

	got := dec.HasNext()

	assert.False(t, got)
	assert.Error(t, dec.Error())
}

func TestDecoder_WithConfig(t *testing.T) {
	const defaultMax = 1_048_576

	unionStr := "union value"
	longString := strings.Repeat("a", defaultMax+1)
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
			String: longString,
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
	require.NoError(t, err)

	t.Run("Default Fails", func(t *testing.T) {
		dec, err := ocf.NewDecoder(bytes.NewReader(buf.Bytes()))
		require.NoError(t, err)

		var got FullRecord
		require.True(t, dec.HasNext())
		require.ErrorContains(t, dec.Decode(&got), "size is greater than `Config.MaxByteSliceSize`")
	})

	t.Run("Custom Config Is Used", func(t *testing.T) {
		cfg := avro.Config{MaxByteSliceSize: defaultMax + 1}.Freeze()
		dec, err := ocf.NewDecoder(
			bytes.NewReader(buf.Bytes()),
			ocf.WithDecoderConfig(cfg),
		)
		require.NoError(t, err)

		var got FullRecord
		require.True(t, dec.HasNext())
		require.NoError(t, dec.Decode(&got))
		require.Equal(t, record, got)
	})
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

func TestEncoder_EncodeCompressesZStandard(t *testing.T) {
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
	enc, _ := ocf.NewEncoder(schema, buf, ocf.WithCodec(ocf.ZStandard))

	err := enc.Encode(record)
	assert.NoError(t, err)

	err = enc.Close()

	require.NoError(t, err)
	assert.Equal(t, 951, buf.Len())
}

func TestEncoder_EncodeCompressesZStandardWithLevel(t *testing.T) {
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
	enc, _ := ocf.NewEncoder(schema, buf, ocf.WithCodec(ocf.ZStandard), ocf.WithZStandardEncoderOptions(zstd.WithEncoderLevel(zstd.SpeedBestCompression)))

	err := enc.Encode(record)
	assert.NoError(t, err)

	err = enc.Close()

	require.NoError(t, err)
	assert.Equal(t, 942, buf.Len())
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

func TestEncoder_EncodeWritesBlocksSize(t *testing.T) {
	buf := &bytes.Buffer{}
	enc, _ := ocf.NewEncoder(`"long"`, buf, ocf.WithBlockSize(2))
	t.Cleanup(func() { _ = enc.Close() })

	// Initial header should be exactly 58 bytes
	assert.Equal(t, 58, buf.Len())

	// Encode a single int64 value (1), which zig-zag encodes to 1 byte
	err := enc.Encode(int64(1))
	require.NoError(t, err)

	// Block size threshold not reached yet; no data block should be written
	assert.Equal(t, 58, buf.Len())

	// Encode another int64 value (1), bringing buffer size to 2 bytes
	err = enc.Encode(int64(1))
	require.NoError(t, err)

	// Block size threshold reached; data block should now be flushed
	assert.Equal(t, 78, buf.Len())
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

func TestEncodeDecodeMetadataKeyVal(t *testing.T) {
	buf := &bytes.Buffer{}
	enc, _ := ocf.NewEncoder(`"long"`, buf,
		ocf.WithMetadataKeyVal("key1", []byte("val1")),
		ocf.WithMetadataKeyVal("key2", []byte("val2")),
	)

	err := enc.Encode(int64(1))
	require.NoError(t, err)

	_ = enc.Close()

	dec, err := ocf.NewDecoder(buf)

	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), dec.Metadata()["key1"])
	assert.Equal(t, []byte("val2"), dec.Metadata()["key2"])
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

func TestWithSchemaCache(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Foo",
		"namespace": "foo.bar.baz",
		"fields": [
			{
				"name": "name",
				"type": "string"
			},
			{
				"name": "id",
				"type": "long"
			},
			{
				"name": "meta",
				"type": {
					"type": "array",
					"items": {
						"type": "record",
						"name": "FooMetadataEntry",
						"namespace": "foo.bar.baz",
						"fields": [
							{
								"name": "key",
								"type": "string"
							},
							{
								"name": "values",
								"type": {
									"type": "array",
									"items": "string"
								}
							}
						]
					}
				}
			}
		]
	}`
	type metaEntry struct {
		Key    string   `avro:"key"`
		Values []string `avro:"values"`
	}
	type foo struct {
		Name string      `avro:"name"`
		ID   int64       `avro:"id"`
		Meta []metaEntry `avro:"meta"`
	}
	encoderCache := &avro.SchemaCache{}
	var buf bytes.Buffer
	enc, err := ocf.NewEncoder(schema, &buf, ocf.WithEncoderSchemaCache(encoderCache))
	require.NoError(t, err)
	val := foo{
		Name: "Bob Loblaw",
		ID:   42,
		Meta: []metaEntry{
			{
				Key:    "abc",
				Values: []string{"123", "456"},
			},
		},
	}
	require.NoError(t, enc.Encode(val))
	require.NoError(t, enc.Close())

	assert.NotNil(t, encoderCache.Get("foo.bar.baz.Foo"))
	assert.NotNil(t, encoderCache.Get("foo.bar.baz.FooMetadataEntry"))
	assert.Nil(t, avro.DefaultSchemaCache.Get("foo.bar.baz.Foo"))
	assert.Nil(t, avro.DefaultSchemaCache.Get("foo.bar.baz.FooMetadataEntry"))

	decoderCache := &avro.SchemaCache{}
	dec, err := ocf.NewDecoder(&buf, ocf.WithDecoderSchemaCache(decoderCache))
	require.NoError(t, err)
	require.True(t, dec.HasNext())
	var roundTripVal foo
	require.NoError(t, dec.Decode(&roundTripVal))
	require.False(t, dec.HasNext())
	require.Equal(t, val, roundTripVal)

	assert.NotNil(t, decoderCache.Get("foo.bar.baz.Foo"))
	assert.NotNil(t, decoderCache.Get("foo.bar.baz.FooMetadataEntry"))
	assert.Nil(t, avro.DefaultSchemaCache.Get("foo.bar.baz.Foo"))
	assert.Nil(t, avro.DefaultSchemaCache.Get("foo.bar.baz.FooMetadataEntry"))
}

func TestWithSchemaMarshaler(t *testing.T) {
	schema := `{
		"type": "record",
		"name": "Bar",
		"namespace": "foo.bar.baz",
		"fields": [
			{
				"name": "name",
				"type": "string",
				"field-id": 1
			},
			{
				"name": "id",
				"type": "long",
				"field-id": 2
			},
			{
				"name": "meta",
				"type": {
					"type": "array",
					"logicalType": "map",
					"items": {
						"type": "record",
						"name": "FooMetadataEntry",
						"namespace": "foo.bar.baz",
						"fields": [
							{
								"name": "key",
								"type": "string",
								"field-id": 4
							},
							{
								"name": "values",
								"type": {
									"type": "array",
									"items": "string",
									"element-id": 6
								},
								"field-id": 5
							}
						]
					}
				},
				"field-id": 3
			}
		]
	}`
	parsedSchema := avro.MustParse(schema)
	type metaEntry struct {
		Key    string   `avro:"key"`
		Values []string `avro:"values"`
	}
	type foo struct {
		Name string      `avro:"name"`
		ID   int64       `avro:"id"`
		Meta []metaEntry `avro:"meta"`
	}
	var buf bytes.Buffer
	enc, err := ocf.NewEncoderWithSchema(parsedSchema, &buf, ocf.WithSchemaMarshaler(ocf.FullSchemaMarshaler))
	require.NoError(t, err)
	val := foo{
		Name: "Bob Loblaw",
		ID:   42,
		Meta: []metaEntry{
			{
				Key:    "abc",
				Values: []string{"123", "456"},
			},
		},
	}
	require.NoError(t, enc.Encode(val))
	require.NoError(t, enc.Close())

	dec, err := ocf.NewDecoder(&buf)
	require.NoError(t, err)
	require.True(t, dec.HasNext())
	var roundTripVal foo
	require.NoError(t, dec.Decode(&roundTripVal))
	require.False(t, dec.HasNext())
	require.Equal(t, val, roundTripVal)

	got, err := json.MarshalIndent(dec.Schema(), "", "  ")
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/full-schema.json", got, 0o644)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/full-schema.json")
	require.NoError(t, err)
	assert.Equal(t, want, got)
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

// TestEncoder_Reset tests that Reset allows reusing encoder for multiple files.
func TestEncoder_Reset(t *testing.T) {
	record1 := FullRecord{
		Strings: []string{"first", "record"},
		Longs:   []int64{},
		Enum:    "A",
		Map:     map[string]int{},
		Record:  &TestRecord{Long: 1},
	}
	record2 := FullRecord{
		Strings: []string{"second", "record"},
		Longs:   []int64{},
		Enum:    "B",
		Map:     map[string]int{},
		Record:  &TestRecord{Long: 2},
	}

	// Create first file
	buf1 := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(schema, buf1)
	require.NoError(t, err)

	err = enc.Encode(record1)
	require.NoError(t, err)

	err = enc.Close()
	require.NoError(t, err)

	// Reset to write to second file
	buf2 := &bytes.Buffer{}
	err = enc.Reset(buf2)
	require.NoError(t, err)

	err = enc.Encode(record2)
	require.NoError(t, err)

	err = enc.Close()
	require.NoError(t, err)

	// Verify first file
	dec1, err := ocf.NewDecoder(buf1)
	require.NoError(t, err)

	require.True(t, dec1.HasNext())
	var got1 FullRecord
	err = dec1.Decode(&got1)
	require.NoError(t, err)
	assert.Equal(t, record1, got1)
	require.False(t, dec1.HasNext())

	// Verify second file
	dec2, err := ocf.NewDecoder(buf2)
	require.NoError(t, err)

	require.True(t, dec2.HasNext())
	var got2 FullRecord
	err = dec2.Decode(&got2)
	require.NoError(t, err)
	assert.Equal(t, record2, got2)
	require.False(t, dec2.HasNext())
}

// TestEncoder_ResetWithPendingData tests Reset flushes pending data.
func TestEncoder_ResetWithPendingData(t *testing.T) {
	buf1 := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(`"long"`, buf1, ocf.WithBlockLength(10))
	require.NoError(t, err)

	// Write data but don't close (pending data)
	err = enc.Encode(int64(42))
	require.NoError(t, err)

	// Reset should flush the pending data
	buf2 := &bytes.Buffer{}
	err = enc.Reset(buf2)
	require.NoError(t, err)

	// Verify first file has the data
	dec1, err := ocf.NewDecoder(buf1)
	require.NoError(t, err)

	require.True(t, dec1.HasNext())
	var got int64
	err = dec1.Decode(&got)
	require.NoError(t, err)
	assert.Equal(t, int64(42), got)
}

// TestEncoder_ResetGeneratesNewSyncMarker tests that each reset creates a new sync marker.
func TestEncoder_ResetGeneratesNewSyncMarker(t *testing.T) {
	buf1 := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(`"long"`, buf1)
	require.NoError(t, err)

	err = enc.Encode(int64(1))
	require.NoError(t, err)
	err = enc.Close()
	require.NoError(t, err)

	// Get sync marker from first file
	dec1, err := ocf.NewDecoder(bytes.NewReader(buf1.Bytes()))
	require.NoError(t, err)

	reader1 := avro.NewReader(bytes.NewReader(buf1.Bytes()), 1024)
	var h1 ocf.Header
	reader1.ReadVal(ocf.HeaderSchema, &h1)
	require.NoError(t, reader1.Error)
	sync1 := h1.Sync

	// Reset to second buffer
	buf2 := &bytes.Buffer{}
	err = enc.Reset(buf2)
	require.NoError(t, err)

	err = enc.Encode(int64(2))
	require.NoError(t, err)
	err = enc.Close()
	require.NoError(t, err)

	// Get sync marker from second file
	reader2 := avro.NewReader(bytes.NewReader(buf2.Bytes()), 1024)
	var h2 ocf.Header
	reader2.ReadVal(ocf.HeaderSchema, &h2)
	require.NoError(t, reader2.Error)
	sync2 := h2.Sync

	// Sync markers should be different
	assert.NotEqual(t, sync1, sync2, "each file should have a unique sync marker")

	// But both files should be readable
	_ = dec1
	dec2, err := ocf.NewDecoder(buf2)
	require.NoError(t, err)
	require.True(t, dec2.HasNext())
}

// TestEncoder_ResetMultipleTimes tests multiple sequential resets.
func TestEncoder_ResetMultipleTimes(t *testing.T) {
	buffers := make([]*bytes.Buffer, 5)
	for i := range buffers {
		buffers[i] = &bytes.Buffer{}
	}

	enc, err := ocf.NewEncoder(`"long"`, buffers[0])
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		if i > 0 {
			err = enc.Reset(buffers[i])
			require.NoError(t, err)
		}

		err = enc.Encode(int64(i * 10))
		require.NoError(t, err)

		err = enc.Close()
		require.NoError(t, err)
	}

	// Verify all files
	for i := 0; i < 5; i++ {
		dec, err := ocf.NewDecoder(buffers[i])
		require.NoError(t, err, "file %d", i)

		require.True(t, dec.HasNext(), "file %d", i)
		var got int64
		err = dec.Decode(&got)
		require.NoError(t, err, "file %d", i)
		assert.Equal(t, int64(i*10), got, "file %d", i)
	}
}

// TestEncoder_AppendToExistingFile tests appending records to an existing OCF file.
func TestEncoder_AppendToExistingFile(t *testing.T) {
	type SimpleRecord struct {
		Name string `avro:"name"`
		ID   int64  `avro:"id"`
	}
	simpleSchema := `{"type":"record","name":"SimpleRecord","fields":[{"name":"name","type":"string"},{"name":"id","type":"long"}]}`

	record1 := SimpleRecord{Name: "first", ID: 1}
	record2 := SimpleRecord{Name: "second", ID: 2}

	tmpFile, err := os.CreateTemp("", "append-test-*.avro")
	require.NoError(t, err)
	tmpName := tmpFile.Name()
	t.Cleanup(func() { _ = os.Remove(tmpName) })

	// Write first record
	enc, err := ocf.NewEncoder(simpleSchema, tmpFile)
	require.NoError(t, err)
	err = enc.Encode(record1)
	require.NoError(t, err)
	err = enc.Close()
	require.NoError(t, err)
	err = tmpFile.Close()
	require.NoError(t, err)

	// Reopen file and append second record
	file, err := os.OpenFile(tmpName, os.O_RDWR, 0o644)
	require.NoError(t, err)

	enc2, err := ocf.NewEncoder(simpleSchema, file)
	require.NoError(t, err)
	err = enc2.Encode(record2)
	require.NoError(t, err)
	err = enc2.Close()
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	// Read back and verify both records
	file, err = os.Open(tmpName)
	require.NoError(t, err)
	defer file.Close()

	dec, err := ocf.NewDecoder(file)
	require.NoError(t, err)

	var records []SimpleRecord
	for dec.HasNext() {
		var r SimpleRecord
		err = dec.Decode(&r)
		require.NoError(t, err)
		records = append(records, r)
	}
	require.NoError(t, dec.Error())

	require.Len(t, records, 2)
	assert.Equal(t, record1, records[0])
	assert.Equal(t, record2, records[1])
}

// TestEncoder_ResetPreservesCodec tests that codec is preserved across reset.
func TestEncoder_ResetPreservesCodec(t *testing.T) {
	buf1 := &bytes.Buffer{}
	enc, err := ocf.NewEncoder(`"long"`, buf1, ocf.WithCodec(ocf.Deflate))
	require.NoError(t, err)

	err = enc.Encode(int64(1))
	require.NoError(t, err)
	err = enc.Close()
	require.NoError(t, err)

	buf2 := &bytes.Buffer{}
	err = enc.Reset(buf2)
	require.NoError(t, err)

	err = enc.Encode(int64(2))
	require.NoError(t, err)
	err = enc.Close()
	require.NoError(t, err)

	// Both files should use deflate codec
	dec1, err := ocf.NewDecoder(buf1)
	require.NoError(t, err)
	assert.Equal(t, []byte("deflate"), dec1.Metadata()["avro.codec"])

	dec2, err := ocf.NewDecoder(buf2)
	require.NoError(t, err)
	assert.Equal(t, []byte("deflate"), dec2.Metadata()["avro.codec"])
}
