package testrunner2

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogCapture_Path(t *testing.T) {
	t.Parallel()

	// No disk file
	lc1, _ := newLogCapture(logCaptureConfig{})
	require.Empty(t, lc1.Path())
	_ = lc1.Close()

	// With disk file
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	lc2, _ := newLogCapture(logCaptureConfig{LogPath: logPath})
	require.Equal(t, logPath, lc2.Path())
	_ = lc2.Close()
}

func TestBuildLogFilename(t *testing.T) {
	t.Parallel()

	logDir := "/tmp/testlogs"

	result := buildLogFilename(logDir, "TestFoo")
	require.True(t, strings.HasPrefix(result, "/tmp/testlogs/TestFoo_"))
	require.True(t, strings.HasSuffix(result, ".log"))

	// Test that multiple calls produce unique filenames
	result2 := buildLogFilename(logDir, "TestFoo")
	require.NotEqual(t, result, result2, "each call should produce a unique filename")

	// Empty name falls back to UUID-only
	result3 := buildLogFilename(logDir, "")
	require.True(t, strings.HasPrefix(result3, "/tmp/testlogs/"))
	require.True(t, strings.HasSuffix(result3, ".log"))
	require.False(t, strings.HasPrefix(result3, "/tmp/testlogs/_"))
}

func TestBuildCompileLogFilename(t *testing.T) {
	t.Parallel()

	logDir := "/tmp/testlogs"

	result := buildCompileLogFilename(logDir)
	require.True(t, strings.HasPrefix(result, "/tmp/testlogs/compile_"))
	require.True(t, strings.HasSuffix(result, ".log"))
}
