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

	result := buildLogFilename(logDir, "TestFoo", 1)
	require.True(t, strings.HasPrefix(result, "/tmp/testlogs/TestFoo_attempt-1_"))
	require.True(t, strings.HasSuffix(result, ".log"))

	// Test that multiple calls produce unique filenames
	result2 := buildLogFilename(logDir, "TestFoo", 1)
	require.NotEqual(t, result, result2, "each call should produce a unique filename")

	// Attempt number is included
	result3 := buildLogFilename(logDir, "TestFoo", 2)
	require.True(t, strings.HasPrefix(result3, "/tmp/testlogs/TestFoo_attempt-2_"))

	// Empty name falls back to attempt + UUID only
	result4 := buildLogFilename(logDir, "", 1)
	require.True(t, strings.HasPrefix(result4, "/tmp/testlogs/attempt-1_"))
	require.True(t, strings.HasSuffix(result4, ".log"))
}

func TestBuildCompileLogFilename(t *testing.T) {
	t.Parallel()

	logDir := "/tmp/testlogs"

	result := buildCompileLogFilename(logDir)
	require.True(t, strings.HasPrefix(result, "/tmp/testlogs/compile_"))
	require.True(t, strings.HasSuffix(result, ".log"))
}
