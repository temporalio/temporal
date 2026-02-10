package testrunner2

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	codeCoverageExtension = ".cover.out"
	logPrefix             = "[runner] "
)

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
	if command != "test" {
		log.Fatalf("unknown command %q", command)
	}

	cfg := defaultConfig()
	cfg.log = log.Printf
	args, err := parseArgs(os.Args[2:], &cfg)
	if err != nil {
		log.Fatalf("failed to parse command line options: %v", err)
	}

	if cfg.junitReportPath == "" {
		log.Fatalf("missing required argument %q", junitReportFlag)
	}
	if cfg.coverProfilePath == "" {
		log.Fatalf("missing required argument %q", coverProfileFlag)
	}
	if cfg.logDir == "" {
		log.Fatalf("missing required argument %q", logDirFlag)
	}
	if cfg.groupBy == "" {
		log.Fatalf("missing required argument %q: use 'test' for compiled per-test execution, 'none' for direct go test", groupByFlag)
	}

	r := newRunner(cfg)
	if err := r.runTests(ctx, args); err != nil {
		log.Fatalf(logPrefix+"failed:\n%v", err)
	}
}

// --- runner ---

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

type runner struct {
	config
	console        *consoleWriter
	directRetrySeq atomic.Int64 // unique suffix for direct-mode retry file names

	// Result collection (thread-safe via mu)
	mu           sync.Mutex
	junitReports []*junitReport
	alerts       []alert
	errors       []error

	// Progress tracking
	progressTotal     atomic.Int64
	progressCompleted atomic.Int64
}

func (r *runner) addReport(jr *junitReport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.junitReports = append(r.junitReports, jr)
}

func (r *runner) addAlerts(a []alert) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.alerts = append(r.alerts, a...)
}

func (r *runner) addError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, err)
}

func (r *runner) addProgressTotal(n int64) {
	r.progressTotal.Add(n)
}

func (r *runner) completeProgress(n int) (completed, total int) {
	c := r.progressCompleted.Add(int64(n))
	return int(c), int(r.progressTotal.Load())
}

func newRunner(cfg config) *runner {
	cfg.exec = defaultExec
	return &runner{
		config:  cfg,
		console: &consoleWriter{mu: &sync.Mutex{}, w: os.Stdout},
	}
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

	// Create compile items for each package
	items := r.createCompileItems(pkgs, binDir, baseArgs)

	r.log("starting scheduler with parallelism=%d", r.parallelism)
	return r.runWithScheduler(ctx, r.parallelism, items, 0)
}

// runWithScheduler runs queue items through the scheduler and finalizes the report.
// initialTotal pre-seeds the progress tracker (direct mode passes 1; compiled mode
// passes 0 because compile items dynamically add totals).
func (r *runner) runWithScheduler(ctx context.Context, parallelism int, items []*queueItem, initialTotal int64) error {
	r.junitReports = nil
	r.alerts = nil
	r.errors = nil
	r.progressTotal.Store(0)
	r.progressCompleted.Store(0)
	if initialTotal > 0 {
		r.addProgressTotal(initialTotal)
	}

	sched := newScheduler(parallelism)
	sched.run(ctx, items)

	// Convert alerts to a junit report
	if len(r.alerts) > 0 {
		alertsReport := &junitReport{}
		alertsReport.appendAlerts(r.alerts)
		r.junitReports = append(r.junitReports, alertsReport)
	}

	if err := r.finalizeReport(r.junitReports); err != nil {
		return err
	}
	if len(r.errors) > 0 {
		return errors.Join(r.errors...)
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
			if err != nil && cfg.logHeader == nil {
				r.addError(fmt.Errorf("failed to create log file: %w", err))
				return
			}
			if err != nil {
				r.log("warning: failed to create log file: %v", err)
				lc, _ = newLogCapture(logCaptureConfig{})
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
	r.addAlerts(detectedAlerts)
	return detectedAlerts
}

// collectJUnitResult reads the JUnit report file and classifies the test outcome.
func (r *runner) collectJUnitResult(cfg execConfig, result commandResult, detectedAlerts alerts) (numTests, numFailed int, failureKind string) {
	failureKind = classifyAlerts(detectedAlerts)

	jr := &junitReport{path: cfg.junitPath, attempt: cfg.attempt}
	if err := jr.read(); err == nil {
		r.addReport(jr)
		numTests = jr.Tests
		numFailed = jr.Failures
		if numTests == 0 && failureKind == "" {
			failureKind = "no tests"
			r.addAlerts([]alert{{
				Kind:    failureKindCrash,
				Summary: "No tests were executed (possible parsing error or test filter mismatch)",
			}})
		}
	} else {
		failureKind = "crash"
		r.addAlerts([]alert{{
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
		r.addError(fmt.Errorf("%s failed on attempt %d", cfg.label, cfg.attempt))
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
	default:
		// All failures were already retried mid-stream; nothing to do.
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
	mergedReport, err := mergeReports(reports, quarantinedTestNames(r.alerts))
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

// writeConsoleResult formats and prints the test result to the console.
func writeConsoleResult(r *runner, cfg execConfig, result commandResult,
	numTests, numFailed int, failureKind string, detectedAlerts alerts,
	results testResults, start time.Time) {

	failed := result.exitCode != 0 || numFailed > 0 || numTests == 0
	status := "❌️"
	if !failed {
		if r.progressTotal.Load() > 0 {
			completed, total := r.completeProgress(1)
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

	totalStr := fmt.Sprintf("%d", numTests)
	if failureKind != "" {
		totalStr = "?"
	}
	header := fmt.Sprintf("%s%s %s %s (attempt=%d, passed=%d/%s%s, runtime=%v)",
		logPrefix, time.Now().Format("15:04:05"), status, cfg.label, cfg.attempt,
		passedTests, totalStr, failureInfo, time.Since(start).Round(time.Second))

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
