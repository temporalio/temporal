package testrunner2

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// alertWindowSize is the size of the sliding window for alert detection.
	// This window is kept in memory to detect multi-line alerts (panics, data races)
	// that may span arbitrary boundaries.
	alertWindowSize = 8192 // 8KB

	// logHeaderSeparator is the separator line used in log file headers.
	logHeaderSeparator = "================================================================================"

	// filteredLogPrefix is a log line prefix that should be filtered out.
	filteredLogPrefix = "warning: no packages being tested depend on matches for pattern "
)

// logFileHeader contains metadata for a log file header.
type logFileHeader struct {
	Package   string
	TestFiles []string
	Attempt   int
	Started   time.Time
	Command   string
}

// logCapture captures test output, streaming to disk and keeping a sliding window
// for alert detection. The disk file is owned by logCapture and will be closed
// when Close() is called.
type logCapture struct {
	diskFile      *os.File     // Complete output (owned)
	diskPath      string       // Path to disk file
	alertWindow   bytes.Buffer // Sliding window for alert detection
	alerts        []alert      // Accumulated alerts from incremental parsing
	header        *logFileHeader
	headerWritten bool
	lineBuf       bytes.Buffer // Buffer for partial lines (for filtering)
}

// logCaptureConfig holds configuration for creating a logCapture.
type logCaptureConfig struct {
	LogPath string         // Disk file path (empty = no disk)
	Header  *logFileHeader // Optional header metadata
}

// newLogCapture creates a new log capture instance.
// If LogPath is provided, it creates the disk file and parent directories.
func newLogCapture(cfg logCaptureConfig) (*logCapture, error) {
	lc := &logCapture{
		diskPath: cfg.LogPath,
		header:   cfg.Header,
	}

	if cfg.LogPath != "" {
		dir := filepath.Dir(cfg.LogPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}

		f, err := os.Create(cfg.LogPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create log file: %w", err)
		}
		lc.diskFile = f
	}

	return lc, nil
}

// writeHeader writes the header to disk if configured and not yet written.
func (lc *logCapture) writeHeader() error {
	if lc.headerWritten || lc.header == nil || lc.diskFile == nil {
		return nil
	}

	var b strings.Builder
	b.WriteString(logHeaderSeparator)
	b.WriteString("\nTESTRUNNER LOG\n")
	b.WriteString(logHeaderSeparator)
	b.WriteByte('\n')

	fmt.Fprintf(&b, "Package:     %s\n", lc.header.Package)
	fmt.Fprintf(&b, "Test Files:  %s\n", strings.Join(lc.header.TestFiles, ", "))
	fmt.Fprintf(&b, "Attempt:     %d\n", lc.header.Attempt)
	fmt.Fprintf(&b, "Started:     %s\n", lc.header.Started.Format(time.RFC3339))
	if lc.header.Command != "" {
		fmt.Fprintf(&b, "Command:     %s\n", lc.header.Command)
	}

	b.WriteString(logHeaderSeparator)
	b.WriteString("\n\n")

	if _, err := io.WriteString(lc.diskFile, b.String()); err != nil {
		return err
	}
	lc.headerWritten = true
	return nil
}

// Write implements io.Writer. It writes data to disk immediately and maintains
// a sliding window for alert detection. Lines matching filteredLogPrefix are filtered out.
func (lc *logCapture) Write(p []byte) (n int, err error) {
	// Write header on first write
	if err := lc.writeHeader(); err != nil {
		return 0, err
	}

	// Filter out unwanted log lines
	filtered := lc.filterLines(p)

	// Write to disk immediately (filtered data)
	if lc.diskFile != nil && len(filtered) > 0 {
		if _, err := lc.diskFile.Write(filtered); err != nil {
			return 0, err
		}
	}

	// Write to alert window
	if len(filtered) > 0 {
		lc.alertWindow.Write(filtered)
	}

	// If window exceeds size, parse alerts and trim
	if lc.alertWindow.Len() > alertWindowSize {
		lc.trimAlertWindow()
	}

	return len(p), nil
}

// filterLines filters out lines that start with filteredLogPrefix.
// It handles partial lines by buffering them until a newline is received.
func (lc *logCapture) filterLines(p []byte) []byte {
	lc.lineBuf.Write(p)

	var result bytes.Buffer
	for {
		line, err := lc.lineBuf.ReadBytes('\n')
		if err != nil {
			// No complete line, put the partial line back
			lc.lineBuf.Write(line)
			break
		}
		// Filter out lines starting with the filtered prefix
		if !bytes.HasPrefix(line, []byte(filteredLogPrefix)) {
			result.Write(line)
		}
	}
	return result.Bytes()
}

// trimAlertWindow parses alerts from the current window and trims it to alertWindowSize.
func (lc *logCapture) trimAlertWindow() {
	// Parse alerts from current window before trimming
	newAlerts := parseAlerts(lc.alertWindow.String())
	lc.alerts = append(lc.alerts, newAlerts...)

	// Keep the last alertWindowSize bytes, trimming at newline boundary
	data := lc.alertWindow.Bytes()
	if len(data) > alertWindowSize {
		keepFrom := len(data) - alertWindowSize
		// Find a newline boundary to avoid splitting mid-line
		for i := keepFrom; i < len(data); i++ {
			if data[i] == '\n' {
				keepFrom = i + 1
				break
			}
		}
		lc.alertWindow.Reset()
		lc.alertWindow.Write(data[keepFrom:])
	}
}

// GetOutput returns the complete output from the disk file.
// This should be called after the test completes but before Close().
func (lc *logCapture) GetOutput() (string, error) {
	if lc.diskFile == nil {
		// No disk file, return what's in the alert window
		return lc.alertWindow.String(), nil
	}

	// Sync to ensure all data is written
	if err := lc.diskFile.Sync(); err != nil {
		return "", fmt.Errorf("failed to sync log file: %w", err)
	}

	// Read entire file from disk
	content, err := os.ReadFile(lc.diskPath)
	if err != nil {
		return "", fmt.Errorf("failed to read log file: %w", err)
	}

	// Skip header content for output parsing (find double newline after header)
	output := string(content)
	if lc.header != nil {
		// Header ends with separator + "\n\n"
		headerEnd := logHeaderSeparator + "\n\n"
		if idx := strings.LastIndex(output, headerEnd); idx >= 0 {
			output = output[idx+len(headerEnd):]
		}
	}

	return output, nil
}

// GetAlerts returns all alerts accumulated during writes plus any remaining
// in the current alert window. This should be called after the test completes.
func (lc *logCapture) GetAlerts() []alert {
	// Parse any alerts in the remaining window
	finalAlerts := parseAlerts(lc.alertWindow.String())

	// Combine with previously accumulated alerts and dedupe
	allAlerts := append(lc.alerts, finalAlerts...)
	return alerts(allAlerts).dedupe()
}

// Path returns the disk file path, or empty string if no disk file.
func (lc *logCapture) Path() string {
	return lc.diskPath
}

// Close flushes any remaining buffered data, syncs, and closes the disk file.
func (lc *logCapture) Close() error {
	// Flush any remaining partial line (if it doesn't match the filter)
	if lc.lineBuf.Len() > 0 {
		remaining := lc.lineBuf.Bytes()
		if !bytes.HasPrefix(remaining, []byte(filteredLogPrefix)) {
			if lc.diskFile != nil {
				_, _ = lc.diskFile.Write(remaining)
			}
			lc.alertWindow.Write(remaining)
		}
		lc.lineBuf.Reset()
	}

	if lc.diskFile == nil {
		return nil
	}

	if err := lc.diskFile.Sync(); err != nil {
		_ = lc.diskFile.Close()
		return fmt.Errorf("failed to sync log file: %w", err)
	}

	return lc.diskFile.Close()
}

// sanitizePackageName converts a package path to a safe directory name.
// For example, "./common/persistence" becomes "common_persistence".
func sanitizePackageName(pkg string) string {
	// Remove leading "./" or "/"
	name := strings.TrimPrefix(pkg, "./")
	name = strings.TrimPrefix(name, "/")

	// Replace path separators with underscores
	name = strings.ReplaceAll(name, "/", "_")

	return name
}

// buildLogFilename constructs the log file path for a test batch.
// A UUID is appended to ensure uniqueness when multiple tests from the same
// file run in parallel.
func buildLogFilename(logDir, pkg string, files []testFile, attempt int) string {
	pkgDir := filepath.Join(logDir, sanitizePackageName(pkg))

	// Build filename from test file names
	var names []string
	for _, tf := range files {
		name := filepath.Base(tf.path)
		name = strings.TrimSuffix(name, "_test.go")
		names = append(names, name)
	}

	filename := strings.Join(names, "_")
	id := uuid.New().String()[:8] // short UUID suffix for uniqueness
	return filepath.Join(pkgDir, fmt.Sprintf("%s_attempt%d_%s.log", filename, attempt, id))
}

// buildCompileLogFilename constructs the log file path for a compile operation.
func buildCompileLogFilename(logDir, pkg string) string {
	pkgDir := filepath.Join(logDir, sanitizePackageName(pkg))
	return filepath.Join(pkgDir, "compile.log")
}

// consoleWriter writes grouped output to stdout.
type consoleWriter struct {
	mu *sync.Mutex
}

// WriteGrouped writes output with a header line and indented body.
func (cw *consoleWriter) WriteGrouped(header, body string) {
	var out strings.Builder
	out.WriteString(header)
	out.WriteByte('\n')

	// Indent body lines
	for line := range strings.SplitSeq(body, "\n") {
		if line != "" {
			out.WriteString("    ")
			out.WriteString(line)
			out.WriteByte('\n')
		}
	}

	cw.mu.Lock()
	_, _ = os.Stdout.WriteString(out.String())
	cw.mu.Unlock()
}
