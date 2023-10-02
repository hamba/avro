package main

import (
	"bytes"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "Update golden files")

func TestAvroGen_RequiredFlags(t *testing.T) {
	tests := []struct {
		name         string
		args         []string
		wantExitCode int
	}{
		{
			name:         "validates schema is set",
			args:         []string{"avrogen", "-pkg", "test", "-o", "some/file"},
			wantExitCode: 1,
		},
		{
			name:         "validates schema exists",
			args:         []string{"avrogen", "-pkg", "test", "-o", "some/file", "some/schema"},
			wantExitCode: 2,
		},
		{
			name:         "validates package is set",
			args:         []string{"avrogen", "-o", "some/file", "schema.avsc"},
			wantExitCode: 1,
		},
		{
			name:         "validates tag format are valid",
			args:         []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", "snake", "schema.avsc"},
			wantExitCode: 1,
		},
		{
			name:         "validates tag key are valid",
			args:         []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", ":snake", "schema.avsc"},
			wantExitCode: 1,
		},
		{
			name:         "validates tag style are valid",
			args:         []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", "json:something", "schema.avsc"},
			wantExitCode: 1,
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

func TestAvroGen_GeneratesSchemaStdout(t *testing.T) {
	var buf bytes.Buffer

	args := []string{"avrogen", "-pkg", "testpkg", "testdata/schema.avsc"}
	gotCode := realMain(args, &buf, io.Discard)
	require.Equal(t, 0, gotCode)

	want, err := os.ReadFile("testdata/golden.go")
	require.NoError(t, err)
	assert.Equal(t, want, buf.Bytes())
}

func TestAvroGen_GeneratesSchema(t *testing.T) {
	path, err := os.MkdirTemp("./", "avrogen")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(path) })

	file := filepath.Join(path, "test.go")
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden.go", got, 0600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithFullname(t *testing.T) {
	path, err := os.MkdirTemp("./", "avrogen")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(path) })

	file := filepath.Join(path, "test.go")
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "-fullname", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_fullname.go", got, 0600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_fullname.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestAvroGen_GeneratesSchemaWithEncoders(t *testing.T) {
	path, err := os.MkdirTemp("./", "avrogen")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(path) })

	file := filepath.Join(path, "test.go")
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "-encoders", "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard, io.Discard)
	require.Equal(t, 0, gotCode)

	got, err := os.ReadFile(file)
	require.NoError(t, err)

	if *update {
		err = os.WriteFile("testdata/golden_encoders.go", got, 0600)
		require.NoError(t, err)
	}

	want, err := os.ReadFile("testdata/golden_encoders.go")
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestParseTags(t *testing.T) {
	tests := []struct {
		name string
		tags string
	}{
		{
			name: "snake case",
			tags: "json:snake",
		},
		{
			name: "camel case",
			tags: "json:camel",
		},
		{
			name: "upper camel case",
			tags: "json:upper-camel",
		},
		{
			name: "kebab case",
			tags: "json:kebab",
		},
		{
			name: "original case",
			tags: "json:original",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := parseTags(test.tags)

			require.NoError(t, err)
		})
	}
}

func TestParseInitialisms(t *testing.T) {
	tests := []struct {
		name        string
		initialisms string
		errFunc     assert.ErrorAssertionFunc
	}{
		{
			name:        "single initialism",
			initialisms: "ABC",
			errFunc:     assert.NoError,
		},
		{
			name:        "multiple initialisms",
			initialisms: "ABC,DEF",
			errFunc:     assert.NoError,
		},
		{
			name:        "wrong initialism",
			initialisms: "ABC,def,GHI",
			errFunc:     assert.Error,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := parseInitialisms(test.initialisms)

			test.errFunc(t, err)
		})
	}
}
