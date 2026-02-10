package testrunner2

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
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

// logCapture captures test output, streaming filtered lines to disk.
// The disk file is owned by logCapture and will be closed when Close() is called.
type logCapture struct {
	diskFile      *os.File // Complete output (owned, nil when no disk)
	diskPath      string   // Path to disk file
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

// Write implements io.Writer. It writes filtered data to disk.
// Lines matching filteredLogPrefix are filtered out.
func (lc *logCapture) Write(p []byte) (n int, err error) {
	// Write header on first write
	if err := lc.writeHeader(); err != nil {
		return 0, err
	}

	// Filter out unwanted log lines
	filtered := lc.filterLines(p)

	if len(filtered) > 0 && lc.diskFile != nil {
		if _, err := lc.diskFile.Write(filtered); err != nil {
			return 0, err
		}
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
		}
		lc.lineBuf.Reset()
	}

	if lc.diskFile == nil {
		return nil
	}

	syncErr := lc.diskFile.Sync()
	closeErr := lc.diskFile.Close()
	if syncErr != nil {
		return fmt.Errorf("failed to sync log file: %w", syncErr)
	}
	return closeErr
}

// buildLogFilename constructs a unique log file path for a test batch.
// The name is sanitized and included in the filename for easier identification.
func buildLogFilename(logDir, name string) string {
	safe := sanitizeFilename(name)
	if safe == "" {
		return filepath.Join(logDir, uuid.New().String()+".log")
	}
	return filepath.Join(logDir, safe+"_"+uuid.New().String()+".log")
}

// sanitizeFilename replaces characters that are unsafe in filenames.
func sanitizeFilename(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		case r == '/' || r == ' ':
			b.WriteByte('_')
		default:
			// drop
		}
	}
	return b.String()
}

// buildCompileLogFilename constructs a unique log file path for a compile operation.
func buildCompileLogFilename(logDir string) string {
	return filepath.Join(logDir, "compile_"+uuid.New().String()+".log")
}
