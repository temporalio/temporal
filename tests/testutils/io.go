package testutils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// CreateTemp is a helper function which creates a temporary file,
// and it is automatically closed after test is completed
func CreateTemp(t testing.TB, dir, pattern string) *os.File {
	t.Helper()

	tempFile, err := os.CreateTemp(dir, pattern)
	require.NoError(t, err)
	require.NotNil(t, tempFile)

	t.Cleanup(func() { require.NoError(t, os.Remove(tempFile.Name())) })
	return tempFile
}

// MkdirTemp is a helper function which creates a temporary directory,
// and they are automatically removed after test is completed
func MkdirTemp(t testing.TB, dir, pattern string) string {
	t.Helper()

	tempDir, err := os.MkdirTemp(dir, pattern)
	require.NoError(t, err)
	require.NotNil(t, tempDir)

	t.Cleanup(func() { require.NoError(t, os.RemoveAll(tempDir)) })
	return tempDir
}
