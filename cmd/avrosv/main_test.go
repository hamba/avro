package main

import (
	"bytes"
	"io"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
)

func TestAvroSv_RequiredFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{
			name:    "validates no schema is set",
			args:    []string{"avrosv"},
			wantErr: true,
		},
		{
			name:    "validates single schema is set",
			args:    []string{"avrosv", "some/file"},
			wantErr: true,
		},
		{
			name:    "validates multiple schemas are set",
			args:    []string{"avrosv", "some/file", "some/other"},
			wantErr: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := realMain(test.args, io.Discard, io.Discard)

			if !test.wantErr {
				assert.Equal(t, 0, got)
				return
			}

			assert.NotEqual(t, 0, got)
		})
	}
}

func TestAvroSv_ValidatesSchema(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{
			name:    "validates a simple schema",
			args:    []string{"avrosv", "testdata/schema.avsc"},
			wantErr: false,
		},
		{
			name:    "does not validate a bad schema",
			args:    []string{"avrosv", "testdata/bad-schema.avsc"},
			wantErr: true,
		},
		{
			name:    "does not validate a schema with a bad default",
			args:    []string{"avrosv", "testdata/bad-default-schema.avsc"},
			wantErr: true,
		},
		{
			name:    "does not validate a schema with a reference to a missing schema",
			args:    []string{"avrosv", "testdata/withref-schema.avsc"},
			wantErr: true,
		},
		{
			name:    "validates a schema with a reference to an existing schema",
			args:    []string{"avrosv", "testdata/schema.avsc", "testdata/withref-schema.avsc"},
			wantErr: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			avro.DefaultSchemaCache = &avro.SchemaCache{} // reset the schema cache
			got := realMain(test.args, io.Discard, io.Discard)

			if !test.wantErr {
				assert.Equal(t, 0, got)
				return
			}

			assert.NotEqual(t, 0, got)
		})
	}
}

func TestAvroSv_Verbose(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		out     string
		wantErr bool
	}{
		{
			name:    "dumps a simple schema",
			args:    []string{"avrosv", "-v", "testdata/schema.avsc"},
			out:     "{\"name\":\"test\",\"type\":\"record\",\"fields\":[{\"name\":\"someString\",\"type\":\"string\"}]}\n",
			wantErr: false,
		},
		{
			name:    "dumps a schema with a reference to an existing schema",
			args:    []string{"avrosv", "-v", "testdata/schema.avsc", "testdata/withref-schema.avsc"},
			out:     "{\"name\":\"testref\",\"type\":\"record\",\"fields\":[{\"name\":\"someref\",\"type\":\"test\"}]}\n",
			wantErr: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			avro.DefaultSchemaCache = &avro.SchemaCache{} // reset the schema cache
			w := &bytes.Buffer{}
			got := realMain(test.args, io.Discard, w)
			assert.Equal(t, 0, got)
			assert.Equal(t, test.out, w.String())
		})
	}
}
