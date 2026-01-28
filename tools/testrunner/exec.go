package testrunner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

// batchDesc returns a human-readable description of a batch of test files.
// For small batches (< 5 files), it lists the file names; otherwise just the count.
func batchDesc(batch []testFile) string {
	if len(batch) < 5 {
		names := make([]string, len(batch))
		for i, tf := range batch {
			names[i] = filepath.Base(tf.path)
		}
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%d files", len(batch))
}

// batchResult holds the result of running a batch of test files.
type batchResult struct {
	junitReportPath  string
	coverProfilePath string
	exitCode         int
	duration         time.Duration
	attempt          int
	alerts           []alert
	failedTests      []string    // test names that failed, for retry
	capture          *logCapture // captured output, caller must Flush
}

// execTestBatch runs tests for a batch of test files in a single gotestsum invocation.
func execTestBatch(ctx context.Context, cfg runConfig, batch []testFile, baseArgs []string) batchResult {
	start := time.Now()
	attempt := batch[0].attempt // all files in batch have same attempt number
	result := batchResult{attempt: attempt}

	// Check if context is already cancelled
	if ctx.Err() != nil {
		result.exitCode = 1
		result.duration = time.Since(start)
		return result
	}

	// Apply timeout if set
	if cfg.runTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.runTimeout)
		defer cancel()
	}

	// Collect all test names and packages from the batch
	var allTestNames []string
	pkgSet := make(map[string]bool)
	for _, tf := range batch {
		allTestNames = append(allTestNames, tf.testNames...)
		pkgSet[tf.pkg] = true
	}

	// Create unique output files
	result.junitReportPath = filepath.Join(os.TempDir(), fmt.Sprintf("junit-%s.xml", uuid.NewString()))
	result.coverProfilePath = fmt.Sprintf(
		"%s_batch_%d%s",
		strings.TrimSuffix(cfg.coverProfilePath, codeCoverageExtension),
		attempt,
		codeCoverageExtension)

	// Build command args
	runPattern := testNamesToRunPattern(allTestNames)
	passthrough, postArgs := filterBaseArgs(baseArgs)

	args := []string{
		fmt.Sprintf("--junitfile=%s", result.junitReportPath),
		"--",
	}
	args = append(args, passthrough...)
	args = append(args, "-run", runPattern)
	if cfg.buildTags != "" {
		args = append(args, "-tags="+cfg.buildTags)
	}
	if cfg.runTimeout > 0 {
		args = append(args, fmt.Sprintf("-timeout=%s", cfg.runTimeout))
	}
	args = append(args, fmt.Sprintf("-coverprofile=%s", result.coverProfilePath))
	for pkg := range pkgSet {
		args = append(args, pkg)
	}
	args = append(args, postArgs...)

	cmd := exec.CommandContext(ctx, cfg.gotestsumPath, args...)

	// Capture output, filter warnings, and parse alerts
	result.capture = newLogCapture("warning: no packages being tested depend on matches for pattern")
	// Log the command at the start of the captured output
	result.capture.output.WriteString(cfg.gotestsumPath + " " + strings.Join(filterForLog(args), " ") + "\n")
	cmd.Stdout = result.capture.Writer(os.Stdout)
	cmd.Stderr = result.capture.Writer(os.Stderr)

	err := cmd.Run()

	if err == nil {
		result.exitCode = 0
	} else if exitErr, ok := err.(*exec.ExitError); ok {
		result.exitCode = exitErr.ExitCode()
	} else {
		result.exitCode = 1
	}

	result.alerts = parseAlerts(result.capture.output.String())
	if result.exitCode != 0 && fileExists(result.junitReportPath) {
		result.failedTests = extractFailedTestNames(result.junitReportPath)
	}
	result.duration = time.Since(start)
	return result
}

// filterBaseArgs separates baseArgs into passthrough args and post-args (after -args flag).
// It filters out flags that execTestFile manages directly.
func filterBaseArgs(baseArgs []string) (passthrough, postArgs []string) {
	var inPostArgs bool
	for _, arg := range baseArgs {
		if arg == "-args" {
			inPostArgs = true
			postArgs = append(postArgs, arg)
			continue
		}
		if inPostArgs {
			postArgs = append(postArgs, arg)
			continue
		}
		// Filter out flags we manage
		if strings.HasPrefix(arg, "-coverprofile") ||
			strings.HasPrefix(arg, "-timeout") ||
			strings.HasPrefix(arg, "--junitfile") ||
			arg == "--" {
			continue
		}
		passthrough = append(passthrough, arg)
	}
	return
}

// filterForLog removes verbose flags (like -coverpkg) from args for readable logging.
func filterForLog(args []string) []string {
	var filtered []string
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-coverpkg") {
			filtered = append(filtered, arg)
		}
	}
	return filtered
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// extractFailedTestNames reads a junit report and returns the raw names of failed tests.
func extractFailedTestNames(junitPath string) []string {
	jr := &junitReport{path: junitPath}
	if err := jr.read(); err != nil {
		log.Printf("warning: failed to read junit report for retry: %v", err)
		return nil
	}
	return jr.collectTestCaseFailures()
}

// testNamesToRunPattern converts a list of test names to a -run flag regex pattern.
func testNamesToRunPattern(names []string) string {
	var patterns []string
	for _, name := range names {
		patterns = append(patterns, goTestNameToRunFlagRegexp(name))
	}
	return "^(" + strings.Join(patterns, "|") + ")$"
}

// goTestNameToRunFlagRegexp converts a test name (possibly with subtests) to a -run flag regex.
// For example, "TestFoo/subtest" becomes "^TestFoo$/^subtest$".
func goTestNameToRunFlagRegexp(test string) string {
	parts := strings.Split(test, "/")
	var sb strings.Builder
	for i, p := range parts {
		if i > 0 {
			sb.WriteByte('/')
		}
		sb.WriteByte('^')
		sb.WriteString(regexp.QuoteMeta(p))
		sb.WriteByte('$')
	}
	return sb.String()
}

// logCapture captures test output from multiple streams (stdout/stderr) into a shared
// buffer, then writes all output at once when Flush is called (filtering unwanted lines).
type logCapture struct {
	filterPrefix []byte
	header       string
	output       bytes.Buffer
	streams      []*logStream
}

// logStream is an io.Writer for a single output stream (stdout or stderr).
type logStream struct {
	capture *logCapture
	dest    io.Writer
}

func newLogCapture(filterPrefix string) *logCapture {
	return &logCapture{filterPrefix: []byte(filterPrefix)}
}

func (c *logCapture) SetHeader(header string) {
	c.header = header
}

func (c *logCapture) Writer(dest io.Writer) io.Writer {
	s := &logStream{capture: c, dest: dest}
	c.streams = append(c.streams, s)
	return s
}

func (s *logStream) Write(p []byte) (n int, err error) {
	return s.capture.output.Write(p)
}

func (c *logCapture) Flush() error {
	if len(c.streams) == 0 {
		return nil
	}
	// Build complete output as a single string to avoid interleaving
	var result strings.Builder

	// Use GitHub Actions group markers for collapsible logs
	if c.header != "" {
		result.WriteString("::group::")
		result.WriteString(c.header)
		result.WriteByte('\n')
	}
	for _, line := range bytes.Split(c.output.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		if !bytes.HasPrefix(line, c.filterPrefix) {
			result.Write(line)
			result.WriteByte('\n')
		}
	}
	if c.header != "" {
		result.WriteString("::endgroup::\n")
	}

	// Write everything at once
	dest := c.streams[0].dest
	_, err := dest.Write([]byte(result.String()))
	return err
}
