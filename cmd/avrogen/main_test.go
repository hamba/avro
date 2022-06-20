package main

import (
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
		name    string
		args    []string
		wantErr bool
	}{
		{
			name:    "validates schema is set",
			args:    []string{"avrogen", "-pkg", "test", "-o", "some/file"},
			wantErr: true,
		},
		{
			name:    "validates schema exists",
			args:    []string{"avrogen", "-pkg", "test", "-o", "some/file", "some/schema"},
			wantErr: true,
		},
		{
			name:    "validates package is set",
			args:    []string{"avrogen", "-o", "some/file", "schema.avsc"},
			wantErr: true,
		},
		{
			name:    "validates output file is set",
			args:    []string{"avrogen", "-pkg", "test", "schema.avsc"},
			wantErr: true,
		},
		{
			name:    "validates tag format are valid",
			args:    []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", "snake", "schema.avsc"},
			wantErr: true,
		},
		{
			name:    "validates tag key are valid",
			args:    []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", ":snake", "schema.avsc"},
			wantErr: true,
		},
		{
			name:    "validates tag style are valid",
			args:    []string{"avrogen", "-o", "some/file", "-pkg", "test", "-tags", "json:something", "schema.avsc"},
			wantErr: true,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := realMain(test.args, io.Discard)

			if !test.wantErr {
				assert.Equal(t, 0, got)
				return
			}

			assert.NotEqual(t, 0, got)
		})
	}
}

func TestAvroGen_GeneratesSchema(t *testing.T) {
	path, err := os.MkdirTemp("./", "avrogen")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(path) })

	file := filepath.Join(path, "test.go")
	args := []string{"avrogen", "-pkg", "testpkg", "-o", file, "testdata/schema.avsc"}
	gotCode := realMain(args, io.Discard)
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
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := parseTags(test.tags)

			require.NoError(t, err)
		})
	}
}
