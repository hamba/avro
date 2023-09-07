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
		name         string
		args         []string
		wantExitCode int
	}{
		{
			name:         "validates no schema is set",
			args:         []string{"avrosv"},
			wantExitCode: 1,
		},
		{
			name:         "validates single schema is set",
			args:         []string{"avrosv", "some/file"},
			wantExitCode: 2,
		},
		{
			name:         "validates multiple schemas are set",
			args:         []string{"avrosv", "some/file", "some/other"},
			wantExitCode: 2,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := realMain(test.args, io.Discard, io.Discard)

			assert.Equal(t, test.wantExitCode, got)
		})
	}
}

func TestAvroSv_ValidatesSchema(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantExitCode int
	}{
		{
			name:         "validates a simple schema",
			args:         []string{"avrosv", "testdata/schema.avsc"},
			wantExitCode: 0,
		},
		{
			name:         "does not validate a bad schema",
			args:         []string{"avrosv", "testdata/bad-schema.avsc"},
			wantExitCode: 2,
		},
		{
			name:         "does not validate a schema with a bad default",
			args:         []string{"avrosv", "testdata/bad-default-schema.avsc"},
			wantExitCode: 2,
		},
		{
			name:         "does not validate a schema with a reference to a missing schema",
			args:         []string{"avrosv", "testdata/withref-schema.avsc"},
			wantExitCode: 2,
		},
		{
			name:         "validates a schema with a reference to an existing schema",
			args:         []string{"avrosv", "testdata/schema.avsc", "testdata/withref-schema.avsc"},
			wantExitCode: 0,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			avro.DefaultSchemaCache = &avro.SchemaCache{} // reset the schema cache
			got := realMain(test.args, io.Discard, io.Discard)

			assert.Equal(t, test.wantExitCode, got)
		})
	}
}

func TestAvroSv_Verbose(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantStdout   string
		wantExitCode int
	}{
		{
			name:         "dumps a simple schema",
			args:         []string{"avrosv", "-v", "testdata/schema.avsc"},
			wantStdout:   "{\"name\":\"test\",\"type\":\"record\",\"fields\":[{\"name\":\"someString\",\"type\":\"string\"}]}\n",
			wantExitCode: 0,
		},
		{
			name:         "dumps a schema with a reference to an existing schema",
			args:         []string{"avrosv", "-v", "testdata/schema.avsc", "testdata/withref-schema.avsc"},
			wantStdout:   "{\"name\":\"testref\",\"type\":\"record\",\"fields\":[{\"name\":\"someref\",\"type\":{\"name\":\"test\",\"type\":\"record\",\"fields\":[{\"name\":\"someString\",\"type\":\"string\"}]}}]}\n",
			wantExitCode: 0,
		},
		{
			name:         "does not dump any schema when the schema file is invalid",
			args:         []string{"avrosv", "-v", "testdata/bad-schema.avsc"},
			wantStdout:   "",
			wantExitCode: 2,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer

			avro.DefaultSchemaCache = &avro.SchemaCache{} // reset the schema cache

			got := realMain(test.args, &buf, io.Discard)

			assert.Equal(t, test.wantStdout, buf.String())
			assert.Equal(t, test.wantExitCode, got)
		})
	}
}
