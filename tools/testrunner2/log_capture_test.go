package testrunner2

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogCapture_WriteNoDisk(t *testing.T) {
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

func TestLogCapture_Alerts(t *testing.T) {
	lc, err := newLogCapture(logCaptureConfig{})
	require.NoError(t, err)
	defer func() { _ = lc.Close() }()

	// Write content with a panic
	_, err = lc.Write([]byte("some output\n"))
	require.NoError(t, err)
	_, err = lc.Write([]byte("panic: something went wrong\n"))
	require.NoError(t, err)
	_, err = lc.Write([]byte("goroutine 1 [running]:\n"))
	require.NoError(t, err)
	_, err = lc.Write([]byte("FAIL\n"))
	require.NoError(t, err)

	// Get alerts should return parsed alerts
	alerts := lc.GetAlerts()
	require.Len(t, alerts, 1)
	require.Equal(t, failureKindPanic, alerts[0].Kind)
	require.Equal(t, "something went wrong", alerts[0].Summary)
}

func TestLogCapture_DiskStreaming(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	lc, err := newLogCapture(logCaptureConfig{
		LogPath: logPath,
	})
	require.NoError(t, err)

	_, err = lc.Write([]byte("output line 1\n"))
	require.NoError(t, err)
	_, err = lc.Write([]byte("output line 2\n"))
	require.NoError(t, err)

	// Get complete output from disk
	output, err := lc.GetOutput()
	require.NoError(t, err)
	require.Contains(t, output, "output line 1")
	require.Contains(t, output, "output line 2")

	_ = lc.Close()

	// Verify file exists and contains content
	content, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(content), "output line 1")
	require.Contains(t, string(content), "output line 2")
}

func TestLogCapture_GetOutputCompleteAfterWindowTrim(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	lc, err := newLogCapture(logCaptureConfig{
		LogPath: logPath,
	})
	require.NoError(t, err)

	// Write enough data to trigger window trim (> alertWindowSize)
	largeData := strings.Repeat("x", alertWindowSize+1000) + "\n"
	_, err = lc.Write([]byte(largeData))
	require.NoError(t, err)

	// Alert window should have been trimmed
	require.LessOrEqual(t, lc.alertWindow.Len(), alertWindowSize)

	// But GetOutput should return ALL the data from disk
	output, err := lc.GetOutput()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(output), alertWindowSize+1000)

	_ = lc.Close()
}

func TestLogCapture_AlertsAcrossWindowBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")

	lc, err := newLogCapture(logCaptureConfig{
		LogPath: logPath,
	})
	require.NoError(t, err)

	// Write a panic alert
	_, err = lc.Write([]byte("panic: early panic\ngoroutine 1 [running]:\n"))
	require.NoError(t, err)

	// Write enough data to trigger trim
	largeData := strings.Repeat("x", alertWindowSize+1000) + "\n"
	_, err = lc.Write([]byte(largeData))
	require.NoError(t, err)

	// Write another panic alert
	_, err = lc.Write([]byte("panic: late panic\ngoroutine 2 [running]:\n"))
	require.NoError(t, err)

	// GetAlerts should find both alerts (early one from accumulated, late one from window)
	alerts := lc.GetAlerts()
	require.Len(t, alerts, 2)
	require.Equal(t, "early panic", alerts[0].Summary)
	require.Equal(t, "late panic", alerts[1].Summary)

	_ = lc.Close()
}

func TestLogCapture_Header(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "test.log")
	startTime := time.Date(2024, 1, 15, 10, 23, 45, 0, time.UTC)

	lc, err := newLogCapture(logCaptureConfig{
		LogPath: logPath,
		Header: &logFileHeader{
			Package:   "./common/persistence",
			TestFiles: []string{"persistence_test.go", "store_test.go"},
			Attempt:   1,
			Started:   startTime,
			Command:   "/tmp/bin/pkg.test -test.v",
		},
	})
	require.NoError(t, err)

	// Write some test output
	_, err = lc.Write([]byte("=== RUN   TestFoo\n"))
	require.NoError(t, err)
	_, err = lc.Write([]byte("--- PASS: TestFoo (0.01s)\n"))
	require.NoError(t, err)

	_ = lc.Close()

	// Read file and verify header is present
	content, err := os.ReadFile(logPath)
	require.NoError(t, err)

	contentStr := string(content)
	require.Contains(t, contentStr, "TESTRUNNER LOG")
	require.Contains(t, contentStr, "Package:     ./common/persistence")
	require.Contains(t, contentStr, "Test Files:  persistence_test.go, store_test.go")
	require.Contains(t, contentStr, "Attempt:     1")
	require.Contains(t, contentStr, "Started:     2024-01-15T10:23:45Z")
	require.Contains(t, contentStr, "Command:     /tmp/bin/pkg.test -test.v")
	require.Contains(t, contentStr, "=== RUN   TestFoo")
	require.Contains(t, contentStr, "--- PASS: TestFoo")
}

func TestLogCapture_GetOutputExcludesHeader(t *testing.T) {
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

func TestLogCapture_CreatesDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "nested", "deeply", "test.log")

	lc, err := newLogCapture(logCaptureConfig{
		LogPath: logPath,
	})
	require.NoError(t, err)

	_, err = lc.Write([]byte("test content\n"))
	require.NoError(t, err)
	_ = lc.Close()

	// Verify file exists
	content, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(content), "test content")
}

func TestSanitizePackageName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"./common/persistence", "common_persistence"},
		{"/common/persistence", "common_persistence"},
		{"common/persistence", "common_persistence"},
		{"./foo", "foo"},
		{"foo", "foo"},
		{"./a/b/c/d", "a_b_c_d"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := sanitizePackageName(tc.input)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildLogFilename(t *testing.T) {
	logDir := "/tmp/testlogs"
	pkg := "./common/persistence"
	files := []testFile{
		{path: "./common/persistence/persistence_test.go"},
		{path: "./common/persistence/store_test.go"},
	}

	result := buildLogFilename(logDir, pkg, files, 1)
	// Filename includes a UUID suffix for uniqueness, so check prefix and suffix
	require.True(t, strings.HasPrefix(result, "/tmp/testlogs/common_persistence/persistence_store_attempt1_"))
	require.True(t, strings.HasSuffix(result, ".log"))

	// Test with attempt 2
	result = buildLogFilename(logDir, pkg, files, 2)
	require.True(t, strings.HasPrefix(result, "/tmp/testlogs/common_persistence/persistence_store_attempt2_"))
	require.True(t, strings.HasSuffix(result, ".log"))

	// Test that multiple calls produce unique filenames
	result2 := buildLogFilename(logDir, pkg, files, 1)
	require.NotEqual(t, result, result2, "each call should produce a unique filename")
}

func TestBuildCompileLogFilename(t *testing.T) {
	logDir := "/tmp/testlogs"
	pkg := "./common/persistence"

	result := buildCompileLogFilename(logDir, pkg)
	expected := "/tmp/testlogs/common_persistence/compile.log"
	require.Equal(t, expected, result)
}
