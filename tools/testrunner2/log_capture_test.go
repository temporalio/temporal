package testrunner2

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogCapture_WriteNoDisk(t *testing.T) {
	t.Parallel()

	lc, err := newLogCapture(logCaptureConfig{})
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	// Write some content
	_, err = lc.Write([]byte("line 1\n"))
	require.NoError(t, err)
	_, err = lc.Write([]byte("line 2\n"))
	require.NoError(t, err)

	// GetOutput should return the content from alert window when no disk
	output, err := lc.GetOutput()
	require.NoError(t, err)
	require.Contains(t, output, "line 1")
	require.Contains(t, output, "line 2")
}

func TestLogCapture_GetOutputExcludesHeader(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	lc, err := newLogCapture(logCaptureConfig{
		LogPath: logPath,
		Header: &logFileHeader{
			Package:   "./foo",
			TestFiles: []string{"foo_test.go"},
			Attempt:   1,
			Started:   time.Now(),
		},
	})
	require.NoError(t, err)

	_, err = lc.Write([]byte("=== RUN   TestFoo\n--- PASS: TestFoo (0.01s)\n"))
	require.NoError(t, err)

	// GetOutput should NOT include header metadata
	output, err := lc.GetOutput()
	require.NoError(t, err)
	require.NotContains(t, output, "TESTRUNNER LOG")
	require.NotContains(t, output, "Package:")
	require.Contains(t, output, "=== RUN   TestFoo")

	_ = lc.Close()
}

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
