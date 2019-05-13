package avro_test

import (
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/assert"
)

func TestMustParseProtocol(t *testing.T) {
	proto := avro.MustParseProtocol(`{"protocol":"test", "namespace": "org.hamba.avro", "doc": "docs"}`)

	assert.IsType(t, &avro.Protocol{}, proto)
}

func TestMustParseProtocol_PanicsOnError(t *testing.T) {
	assert.Panics(t, func() {
		avro.MustParseProtocol("123")
	})
}

func TestNewProtocol_ValidatesName(t *testing.T) {
	_, err := avro.NewProtocol("0test", "", nil, nil)

	assert.Error(t, err)
}

func TestNewMessage(t *testing.T) {
	field, _ := avro.NewField("test", avro.NewPrimitiveSchema(avro.String), nil)
	fields := []*avro.Field{field}
	req, _ := avro.NewRecordSchema("test", "", fields)
	resp := avro.NewPrimitiveSchema(avro.String)
	types := []avro.Schema{avro.NewPrimitiveSchema(avro.String)}
	errs, _ := avro.NewUnionSchema(types)

	msg := avro.NewMessage(req, resp, errs, false)

	assert.Equal(t, req, msg.Request())
	assert.Equal(t, resp, msg.Response())
	assert.Equal(t, errs, msg.Errors())
	assert.False(t, msg.OneWay())
}

func TestParseProtocol(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		wantErr bool
	}{
		{
			name:    "Valid",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "doc": "docs"}`,
			wantErr: false,
		},
		{
			name:    "Invalid Json",
			schema:  `{`,
			wantErr: true,
		},
		{
			name:    "Invalid Name First Char",
			schema:  `{"protocol":"0test", "namespace": "org.hamba.avro"}`,
			wantErr: true,
		},
		{
			name:    "Invalid Name Other Char",
			schema:  `{"protocol":"test+", "namespace": "org.hamba.avro"}`,
			wantErr: true,
		},
		{
			name:    "Empty Name",
			schema:  `{"protocol":"", "namespace": "org.hamba.avro"}`,
			wantErr: true,
		},
		{
			name:    "No Name",
			schema:  `{"namespace": "org.hamba.avro"}`,
			wantErr: true,
		},
		{
			name:    "Invalid Namespace",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro+"}`,
			wantErr: true,
		},
		{
			name:    "Empty Namespace",
			schema:  `{"protocol":"test", "namespace": ""}`,
			wantErr: true,
		},
		{
			name:    "Invalid Type Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "types":["test"]}`,
			wantErr: true,
		},
		{
			name:    "Type Not Named Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "types":["string"]}`,
			wantErr: true,
		},
		{
			name:    "Message Not Object",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":["test"]}}`,
			wantErr: true,
		},
		{
			name:    "Message Request Invalid Request Json",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": "test"}}}`,
			wantErr: true,
		},
		{
			name:    "Message Request Invalid Field",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar"}]}}}`,
			wantErr: true,
		},
		{
			name:    "Message Response Invalid Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "response": "test"}}}`,
			wantErr: true,
		},
		{
			name:    "Message Errors Invalid Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": ["test"]}}}`,
			wantErr: true,
		},
		{
			name:    "Message Errors Record Not Error Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": [{"type":"record", "name":"test", "fields":[{"name": "field", "type": "int"}]}]}}}`,
			wantErr: true,
		},
		{
			name:    "Message Errors Duplicate Schema",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": ["string"]}}}`,
			wantErr: true,
		},
		{
			name:    "Message One Way Invalid",
			schema:  `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}], "errors": ["int"], "one-way": true}}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := avro.ParseProtocol(tt.schema)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestParseProtocol_DeterminesOneWayMessage(t *testing.T) {
	schema := `{"protocol":"test", "namespace": "org.hamba.avro", "messages":{"test":{"request": [{"name": "foobar", "type": "string"}]}}}`

	proto, err := avro.ParseProtocol(schema)

	assert.NoError(t, err)

	msg := proto.Message("test")
	assert.NotNil(t, msg)
	assert.True(t, msg.OneWay())
}

func TestParseProtocolFile(t *testing.T) {
	protocol, err := avro.ParseProtocolFile("testdata/echo.avpr")

	want := `{"protocol":"Echo","namespace":"org.hamba.avro","types":[{"name":"org.hamba.avro.Ping","type":"record","fields":[{"name":"timestamp","type":"long"},{"name":"text","type":"string"}]},{"name":"org.hamba.avro.Pong","type":"record","fields":[{"name":"timestamp","type":"long"},{"name":"ping","type":"org.hamba.avro.Ping"}]},{"name":"org.hamba.avro.PongError","type":"error","fields":[{"name":"timestamp","type":"long"},{"name":"reason","type":"string"}]}],"messages":{"ping":{"request":[{"name":"ping","type":"org.hamba.avro.Ping"}],"response":"org.hamba.avro.Pong","errors":["org.hamba.avro.PongError"]}}}`
	wantMD5 := "5bc594ae86fc8c209f553ce3bc4291a5"
	assert.NoError(t, err)
	assert.Equal(t, want, protocol.String())
	assert.Equal(t, wantMD5, protocol.Hash())
}

func TestParseProtocolFile_InvalidPath(t *testing.T) {
	_, err := avro.ParseProtocolFile("test.avpr")

	assert.Error(t, err)
}
