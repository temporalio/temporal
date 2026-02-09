package testrunner2

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	codeCoverageExtension = ".cover.out"
	logPrefix             = "[runner] "

	testCommand       = "test"
	reportLogsCommand = "report-logs"
)

type config struct {
	log              func(format string, v ...any)
	exec             execFunc
	junitReportPath  string
	coverProfilePath string
	buildTags        string
	logDir           string        // directory for log files
	timeout          time.Duration // overall timeout for the test run
	runTimeout       time.Duration // timeout per test run file
	stuckTestTimeout time.Duration // abort test if individual test runs longer than this
	runFilter        string        // -run filter pattern (used to filter tests)
	maxAttempts      int
	parallelism      int
	totalShards      int       // for CI sharding
	shardIndex       int       // for CI sharding
	testBinaryArgs   []string  // args to pass to test binary (after -args)
	env              []string  // extra environment variables for child processes
	groupBy          GroupMode // how to group tests: test, none
}

// resultCollector aggregates results from work items in a thread-safe manner.
type resultCollector struct {
	mu           sync.Mutex
	junitReports []*junitReport
	alerts       []alert
	errors       []error
}

// progressTracker tracks completion progress across all test files.
// total is incremented dynamically as compile items discover tests.
type progressTracker struct {
	total     atomic.Int64
	completed atomic.Int64
}

func (p *progressTracker) addTotal(n int64) {
	p.total.Add(n)
}

func (p *progressTracker) complete(n int) (completed, total int) {
	c := p.completed.Add(int64(n))
	return int(c), int(p.total.Load())
}

func (c *resultCollector) addReport(r *junitReport) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.junitReports = append(c.junitReports, r)
}

func (c *resultCollector) addAlerts(a []alert) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alerts = append(c.alerts, a...)
}

func (c *resultCollector) addError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors = append(c.errors, err)
}

type runner struct {
	config
	console        *consoleWriter
	collector      *resultCollector
	progress       *progressTracker
	directRetrySeq atomic.Int64 // unique suffix for direct-mode retry file names
}

func newRunner() *runner {
	cfg := defaultConfig()
	cfg.log = log.Printf
	cfg.exec = defaultExec
	return &runner{
		config:  cfg,
		console: &consoleWriter{mu: &sync.Mutex{}, w: os.Stdout},
	}
}

// Main is the entry point for the testrunner tool.
//
//nolint:revive // deep-exit allowed in Main
func Main() {
	log.SetOutput(os.Stdout)
	log.SetPrefix(logPrefix)
	log.SetFlags(log.Ltime)
	ctx := context.Background()

	if len(os.Args) < 2 {
		log.Fatal("expected at least 2 arguments")
	}

	command := os.Args[1]

	r := newRunner()
	args, err := parseArgs(command, os.Args[2:], &r.config)
	if err != nil {
		log.Fatalf("failed to parse command line options: %v", err)
	}

	switch command {
	case testCommand:
		if err := r.runTests(ctx, args); err != nil {
			log.Fatalf(logPrefix+"failed:\n%v", err)
		}
	case reportLogsCommand:
		if err := r.reportLogs(); err != nil {
			log.Fatalf(logPrefix+"failed:\n%v", err)
		}
	default:
		log.Fatalf("unknown command %q", command)
	}
}

func (r *runner) reportLogs() error {
	// Read all package directories
	entries, err := os.ReadDir(r.logDir)
	if err != nil {
		return fmt.Errorf("failed to read log directory: %w", err)
	}

	if len(entries) == 0 {
		r.log("no log files found in %s", r.logDir)
		return nil
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		pkgName := entry.Name()
		pkgDir := filepath.Join(r.logDir, pkgName)

		// Read log files in package directory
		logFiles, err := os.ReadDir(pkgDir)
		if err != nil {
			r.log("warning: failed to read package directory %s: %v", pkgDir, err)
			continue
		}

		for _, logFile := range logFiles {
			if logFile.IsDir() || !strings.HasSuffix(logFile.Name(), ".log") {
				continue
			}

			logPath := filepath.Join(pkgDir, logFile.Name())
			if err := r.printLogFile(logPath, pkgName, logFile.Name()); err != nil {
				r.log("warning: failed to print log file %s: %v", logPath, err)
			}
		}
	}

	return nil
}

func (r *runner) printLogFile(path, pkgName, fileName string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	// Skip empty files
	if len(content) == 0 {
		return nil
	}

	fmt.Printf("\n=== %s/%s ===\n", pkgName, fileName)
	fmt.Print(string(content))

	// Ensure content ends with newline
	if len(content) > 0 && content[len(content)-1] != '\n' {
		fmt.Println()
	}

	return nil
}

func (r *runner) runTests(ctx context.Context, args []string) error {
	// Apply overall timeout if set
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	// Parse args to extract test directories and base args
	testDirs, baseArgs, testBinaryArgs := parseTestArgs(args)
	if len(testDirs) == 0 {
		return errors.New("no test directories specified")
	}
	r.testBinaryArgs = testBinaryArgs

	// "none" mode runs go test directly without precompilation
	if r.groupBy == GroupByNone {
		return r.runDirectMode(ctx, testDirs, baseArgs)
	}

	// Discover packages that contain test files
	pkgs, err := findTestPackages(testDirs)
	if err != nil {
		return err
	}
	if len(pkgs) == 0 {
		return fmt.Errorf("no test files found in directories: %v", testDirs)
	}

	if r.totalShards > 1 {
		r.log("shard %d/%d (group-by=%s)", r.shardIndex+1, r.totalShards, r.groupBy)
	}

	r.log("test packages: %v", pkgs)

	// Create log directory
	if err := os.MkdirAll(r.logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	r.log("log directory: %s", r.logDir)

	// Create temp directory for binaries
	binDir, err := os.MkdirTemp("", "testrunner-bin-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(binDir) }()

	// Create result collector and progress tracker
	r.collector = &resultCollector{}
	r.progress = &progressTracker{}

	// Create compile items for each package
	items := r.createCompileItems(pkgs, binDir, baseArgs)

	// Run via scheduler
	r.log("starting scheduler with parallelism=%d", r.parallelism)
	sched := newScheduler(r.parallelism)
	sched.run(ctx, items)

	// Convert alerts to a junit report
	if len(r.collector.alerts) > 0 {
		alertsReport := &junitReport{}
		alertsReport.appendAlerts(r.collector.alerts)
		r.collector.junitReports = append(r.collector.junitReports, alertsReport)
	}

	// Finalize report
	if err := r.finalizeReport(r.collector.junitReports); err != nil {
		return err
	}

	// Print summary
	if len(r.collector.errors) > 0 {
		return errors.Join(r.collector.errors...)
	}
	r.log("test run completed")
	return nil
}

// --- execConfig and newExecItem: unified test execution ---

// execConfig configures a single test execution item.
type execConfig struct {
	// startProcess starts the test process, writing output to the writer.
	startProcess func(ctx context.Context, output io.Writer) commandResult

	// Display
	label   string
	attempt int

	// Log paths
	logPath   string
	junitPath string
	logHeader *logFileHeader // optional, compiled mode only

	// Whether to emit retries mid-stream (direct mode = true, compiled mode = false).
	streamRetries bool

	// Retry callbacks for failures, crashes, and unknown exits.
	retry retryHandler
}

func (r *runner) newExecItem(cfg execConfig) *queueItem {
	return &queueItem{
		run: func(ctx context.Context, emit func(...*queueItem)) {
			start := time.Now()

			// 1. Set up log capture
			lc, err := newLogCapture(logCaptureConfig{
				LogPath: cfg.logPath,
				Header:  cfg.logHeader,
			})
			if err != nil {
				if cfg.logHeader != nil {
					r.log("warning: failed to create log file: %v", err)
					lc, _ = newLogCapture(logCaptureConfig{})
				} else {
					r.collector.addError(fmt.Errorf("failed to create log file: %w", err))
					return
				}
			}

			// 2. Set up event stream with mid-stream retry handler
			testCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			emittedRetries := make(map[string]bool)
			children := make(map[string]bool)

			handler := r.midStreamRetryHandler(cfg, emittedRetries, children, emit)
			stream := newTestEventStream(testEventStreamConfig{
				Writer:            lc,
				Handler:           handler,
				StuckThreshold:    r.stuckTestTimeout,
				AllStuckThreshold: r.runTimeout,
				StuckCancel:       cancel,
				Log:               r.log,
			})
			defer stream.Close()

			// 3. Start process and collect results
			result := cfg.startProcess(testCtx, stream)
			outputStr, err := lc.GetOutput()
			if err != nil {
				r.log("warning: failed to get test output: %v", err)
			}
			_ = lc.Close()

			results := newJUnitReport(outputStr, cfg.junitPath)
			detectedAlerts := r.collectAlerts(outputStr, stream)

			// 4. Read JUnit report and classify outcome
			numTests, numFailedTests, failureKind := r.collectJUnitResult(cfg, result, detectedAlerts)

			// 5. Console output
			writeConsoleResult(r, cfg, result, numTests, numFailedTests,
				failureKind, detectedAlerts, results, start)

			failed := result.exitCode != 0 || numFailedTests > 0 || numTests == 0
			if !failed {
				_ = os.Remove(cfg.logPath)
			}

			// 6. Post-exit retry logic
			r.emitPostExitRetries(cfg, failed, numTests, failureKind,
				results, detectedAlerts, emittedRetries, emit)
		},
	}
}

// midStreamRetryHandler returns a testEvent handler that emits retries for
// leaf test failures as they happen (direct mode only).
func (r *runner) midStreamRetryHandler(cfg execConfig, emittedRetries, children map[string]bool, emit func(...*queueItem)) func(testEvent) {
	return func(ev testEvent) {
		if strings.Contains(ev.Test, "/") {
			children[parentTestName(ev.Test)] = true
		}
		if !cfg.streamRetries || ev.Action != actionFail {
			return
		}
		if children[ev.Test] || emittedRetries[ev.Test] || cfg.attempt >= r.maxAttempts {
			return
		}
		emittedRetries[ev.Test] = true
		if items := cfg.retry.forFailures([]string{ev.Test}, cfg.attempt); len(items) > 0 {
			emit(items...)
		}
	}
}

// collectAlerts parses alerts from test output and appends stuck test alerts.
func (r *runner) collectAlerts(outputStr string, stream *testEventStream) alerts {
	detectedAlerts := parseAlerts(outputStr)
	if stuckNames, stuckDur := stream.StuckTests(); len(stuckNames) > 0 {
		detectedAlerts = append(detectedAlerts, alert{
			Kind:    failureKindTimeout,
			Summary: fmt.Sprintf("test stuck (no progress for %v)", stuckDur.Round(time.Second)),
			Tests:   stuckNames,
		})
	}
	r.collector.addAlerts(detectedAlerts)
	return detectedAlerts
}

// collectJUnitResult reads the JUnit report file and classifies the test outcome.
func (r *runner) collectJUnitResult(cfg execConfig, result commandResult, detectedAlerts alerts) (numTests, numFailed int, failureKind string) {
	failureKind = classifyAlerts(detectedAlerts)

	jr := &junitReport{path: cfg.junitPath, attempt: cfg.attempt}
	if err := jr.read(); err == nil {
		r.collector.addReport(jr)
		numTests = jr.Tests
		numFailed = jr.Failures
		if numTests == 0 && failureKind == "" {
			failureKind = "no tests"
			r.collector.addAlerts([]alert{{
				Kind:    failureKindCrash,
				Summary: "No tests were executed (possible parsing error or test filter mismatch)",
			}})
		}
	} else {
		failureKind = "crash"
		r.collector.addAlerts([]alert{{
			Kind:    failureKindCrash,
			Summary: fmt.Sprintf("Process exited without junit report (exit code: %d)", result.exitCode),
		}})
	}
	return
}

// emitPostExitRetries decides which retry strategy to use after a test process exits.
func (r *runner) emitPostExitRetries(cfg execConfig, failed bool, numTests int,
	failureKind string, results testResults, detectedAlerts alerts,
	emittedRetries map[string]bool, emit func(...*queueItem)) {

	if !failed && numTests > 0 {
		if cfg.streamRetries {
			r.log("all tests passed on attempt %d", cfg.attempt)
		}
		return
	}
	if cfg.attempt >= r.maxAttempts {
		r.collector.addError(fmt.Errorf("%s failed on attempt %d", cfg.label, cfg.attempt))
		return
	}

	switch {
	case failureKind == "timeout" || failureKind == "crash":
		quarantined := quarantinedTestNames(detectedAlerts)
		if items := cfg.retry.forCrash(results.passes, quarantined, cfg.attempt); len(items) > 0 {
			emit(items...)
		}
	case len(filterEmitted(results.failures, emittedRetries)) > 0:
		unemitted := filterEmitted(results.failures, emittedRetries)
		failures := filterParentFailures(unemitted)
		var failedNames []string
		for _, f := range failures {
			failedNames = append(failedNames, f.Name)
		}
		if items := cfg.retry.forFailures(failedNames, cfg.attempt); len(items) > 0 {
			emit(items...)
		}
	case len(emittedRetries) == 0:
		if items := cfg.retry.forUnknown(results.passes, cfg.attempt); len(items) > 0 {
			emit(items...)
		}
	}
}

// effectiveTimeout returns the overall timeout if set, falling back to runTimeout.
// The overall timeout is used for Go's -test.timeout to give test suites enough
// total execution time. The per-test --run-timeout is used for stuck detection.
func (r *runner) effectiveTimeout() time.Duration {
	if r.timeout > 0 {
		return r.timeout
	}
	return r.runTimeout
}

func (r *runner) finalizeReport(reports []*junitReport) error {
	mergedReport, err := mergeReports(reports, quarantinedTestNames(r.collector.alerts))
	if err != nil {
		return err
	}
	mergedReport.path = r.junitReportPath
	if err := mergedReport.write(); err != nil {
		return err
	}

	// Print test count summary for CI visibility. Comparing these numbers
	// across runs helps detect if tests are being accidentally skipped.
	r.log("test counts: total=%d passed=%d failed=%d errors=%d skipped=%d",
		mergedReport.Tests,
		mergedReport.Tests-mergedReport.Failures-mergedReport.Errors,
		mergedReport.Failures,
		mergedReport.Errors,
		mergedReport.Skipped)

	return errors.Join(mergedReport.reportingErrs...)
}

// --- console output ---

// consoleWriter writes grouped output to a writer.
type consoleWriter struct {
	mu *sync.Mutex
	w  io.Writer
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
	_, _ = io.WriteString(cw.w, out.String())
	cw.mu.Unlock()
}

// writeConsoleResult formats and prints the test result to the console.
func writeConsoleResult(r *runner, cfg execConfig, result commandResult,
	numTests, numFailed int, failureKind string, detectedAlerts alerts,
	results testResults, start time.Time) {

	failed := result.exitCode != 0 || numFailed > 0 || numTests == 0
	status := "❌️"
	if !failed {
		if r.progress != nil {
			completed, total := r.progress.complete(1)
			status = fmt.Sprintf("✅ [%d/%d]", completed, total)
		} else {
			status = "✅"
		}
	}
	passedTests := numTests - numFailed
	failureInfo := ""
	if failed {
		failureInfo = fmt.Sprintf(", failure=%s", cmp.Or(failureKind, "failed"))
	}

	var header string
	if failureKind != "" {
		header = fmt.Sprintf("%s%s %s %s (attempt=%d, passed=%d/?%s, runtime=%v)",
			logPrefix, time.Now().Format("15:04:05"), status, cfg.label, cfg.attempt, passedTests, failureInfo, time.Since(start).Round(time.Second))
	} else {
		header = fmt.Sprintf("%s%s %s %s (attempt=%d, passed=%d/%d%s, runtime=%v)",
			logPrefix, time.Now().Format("15:04:05"), status, cfg.label, cfg.attempt, passedTests, numTests, failureInfo, time.Since(start).Round(time.Second))
	}

	var body strings.Builder

	// Append alerts if test failed
	if failed && len(detectedAlerts) > 0 {
		for _, a := range detectedAlerts.dedupe() {
			if testName := primaryTestName(a.Tests); testName != "" {
				fmt.Fprintf(&body, "--- %s: %s — in %s\n", strings.ToUpper(string(a.Kind)), a.Summary, testName)
			} else {
				fmt.Fprintf(&body, "--- %s: %s\n", strings.ToUpper(string(a.Kind)), a.Summary)
			}
		}
	}

	// Append test failure details
	if failed && len(results.failures) > 0 {
		for _, f := range results.failures {
			fmt.Fprintf(&body, "\n--- %s\n", f.Name)
			if f.ErrorTrace != "" {
				for line := range strings.SplitSeq(f.ErrorTrace, "\n") {
					fmt.Fprintf(&body, "%s\n", line)
				}
			}
		}
	}

	r.console.WriteGrouped(header, body.String())
}
