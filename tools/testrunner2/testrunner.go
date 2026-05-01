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

	if len(os.Args) > 1 && os.Args[1] == summaryCommand {
		if err := printSummary(os.Args[2:], os.Stdout); err != nil {
			log.Fatalf("failed to print summary: %v", err)
		}
		return
	}

	cfg := defaultConfig()
	cfg.log = log.Printf
	args, err := parseArgs(os.Args[1:], &cfg)
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
	retryOrdinalMu sync.Mutex
	retryOrdinals  map[int]int
	tracker        workTracker

	mu           sync.Mutex
	junitReports []*junitReport
	alerts       []alert
	errors       []error
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

func (r *runner) resetRetryOrdinals() {
	r.retryOrdinalMu.Lock()
	defer r.retryOrdinalMu.Unlock()
	r.retryOrdinals = nil
}

func (r *runner) nextRetryOrdinal(attempt int) int {
	r.retryOrdinalMu.Lock()
	defer r.retryOrdinalMu.Unlock()
	if r.retryOrdinals == nil {
		r.retryOrdinals = make(map[int]int)
	}
	r.retryOrdinals[attempt]++
	return r.retryOrdinals[attempt]
}

func newRunner(cfg config) *runner {
	cfg.exec = defaultExec
	return &runner{
		config:  cfg,
		console: &consoleWriter{mu: &sync.Mutex{}, w: os.Stdout},
	}
}

func (r *runner) runTests(ctx context.Context, args []string) error {
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	testDirs, baseArgs, testBinaryArgs := parseTestArgs(args)
	if len(testDirs) == 0 {
		return errors.New("no test directories specified")
	}
	r.testBinaryArgs = testBinaryArgs

	if r.groupBy == GroupByNone {
		return r.runDirectMode(ctx, testDirs, baseArgs)
	}

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

	if err := os.MkdirAll(r.logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	r.log("log directory: %s", r.logDir)

	binDir, err := os.MkdirTemp("", "testrunner-bin-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(binDir) }()

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
	r.tracker.reset()
	r.resetRetryOrdinals()
	if initialTotal > 0 {
		r.tracker.addRoots(int(initialTotal))
	}

	sched := newScheduler(parallelism)
	sched.run(ctx, items)

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

type retryMode int

const (
	retryCompiled retryMode = iota
	retryDirect
)

type execConfig struct {
	startProcess func(ctx context.Context, output io.Writer) int

	unit         workUnit
	attempt      int
	retryOrdinal int

	logPath   string
	junitPath string
	logHeader *logFileHeader

	retryMode          retryMode
	compiledBinaryPath string
	directPkgs         []string
	directRace         bool
	directExtraArgs    []string
}

func (cfg execConfig) displayAttempt() string {
	return displayAttempt(cfg.attempt, cfg.retryOrdinal)
}

func displayAttempt(attempt, retryOrdinal int) string {
	if retryOrdinal > 0 {
		return fmt.Sprintf("%d.%d", attempt, retryOrdinal)
	}
	return fmt.Sprintf("%d", attempt)
}

func (r *runner) newExecItem(cfg execConfig) *queueItem {
	return &queueItem{
		onEnqueue: func() {
			r.tracker.beginAttempt(cfg.unit.rootName)
		},
		run: func(ctx context.Context, emit func(...*queueItem)) {
			start := time.Now()

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

			testCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			emittedRetries := make(map[string]bool)
			children := make(map[string]bool)

			handler := r.streamRetryHandler(cfg, emittedRetries, children, emit)
			stream := newTestEventStream(testEventStreamConfig{
				Writer:         lc,
				Handler:        handler,
				StuckThreshold: r.stuckTestTimeout,
				StuckCancel:    cancel,
				Log:            r.log,
			})
			defer stream.Close()

			exitCode := cfg.startProcess(testCtx, stream)
			_ = lc.Close()

			results := newJUnitReport(cfg.logPath, cfg.junitPath)
			detectedAlerts := r.collectAlertsFromFile(cfg.logPath, stream)
			numTests, numFailedTests, failureKind := r.collectJUnitResult(cfg, exitCode, detectedAlerts)

			failed := exitCode != 0 || numFailedTests > 0 || numTests == 0
			if failed {
				writeConsoleResult(r, cfg, exitCode, numTests, numFailedTests,
					failureKind, detectedAlerts, results, start, 0, 0, false)
			} else {
				progress := r.tracker.finishAttempt(cfg.unit.rootName, true)
				writeConsoleResult(r, cfg, exitCode, numTests, numFailedTests,
					failureKind, detectedAlerts, results, start, progress.completed, progress.total, progress.done)
				_ = os.Remove(cfg.logPath)
			}

			r.emitCrashRecoveryRetries(cfg, failed, numTests, emittedRetries, stream, emit)
			if failed {
				r.tracker.finishAttempt(cfg.unit.rootName, false)
			}
		},
	}
}

// streamRetryHandler returns a testEvent handler that emits retries for
// leaf test failures and stuck tests as they happen (both modes).
func (r *runner) streamRetryHandler(cfg execConfig, emittedRetries, children map[string]bool, emit func(...*queueItem)) func(testEvent) {
	passed := make(map[string]bool)

	return func(ev testEvent) {
		if strings.Contains(ev.Test, "/") {
			children[parentTestName(ev.Test)] = true
		}
		if ev.Action == actionPass {
			passed[ev.Test] = true
			return
		}
		if ev.Action != actionFail && ev.Action != actionStuck {
			return
		}
		// Parent failures are retried through their failing leaf children. Stuck
		// events are already emitted only for leaf tests, or for a parent whose
		// children completed before teardown got stuck.
		if ev.Action == actionFail && children[ev.Test] {
			return
		}
		if emittedRetries[ev.Test] || cfg.attempt >= r.maxAttempts {
			return
		}
		emittedRetries[ev.Test] = true
		skipNames := passedSiblings(ev.Test, passed)
		r.scheduleRetry(cfg, []string{ev.Test}, skipNames, emit)
	}
}

func (r *runner) scheduleRetry(cfg execConfig, failedNames, skipNames []string, emit func(...*queueItem)) {
	nextAttempt := cfg.attempt + 1
	retryOrdinal := r.nextRetryOrdinal(nextAttempt)
	r.log("🔄 scheduling retry: %s (attempt %s)", buildTestFilterPattern(failedNames), displayAttempt(nextAttempt, retryOrdinal))

	switch cfg.retryMode {
	case retryCompiled:
		unit := workUnit{
			pkg:         cfg.unit.pkg,
			rootName:    cfg.unit.rootName,
			displayName: cfg.unit.displayName,
			runTests:    failedNames,
			skipTests:   skipNames,
		}
		emit(r.newExecItem(r.compiledExecConfig(unit, cfg.compiledBinaryPath, nextAttempt, retryOrdinal)))
	case retryDirect:
		emit(r.newExecItem(r.directExecConfig(
			cfg.directPkgs,
			cfg.directRace,
			cfg.directExtraArgs,
			nextAttempt,
			retryOrdinal,
			buildTestFilterPattern(failedNames),
			buildTestFilterPattern(skipNames),
		)))
	default:
	}
}

// passedSiblings returns passed tests that can be skipped when retrying testName.
// For subtests (e.g., TestSuite/FailChild), it returns passed siblings under the
// same parent. For top-level tests (e.g., TestSuite), it returns passed children.
func passedSiblings(testName string, passed map[string]bool) []string {
	parent := parentTestName(testName)
	var prefix string
	if parent == "" {
		prefix = testName + "/" // top-level: skip passed children
	} else {
		prefix = parent + "/" // subtest: skip passed siblings
	}
	var result []string
	for name := range passed {
		if strings.HasPrefix(name, prefix) {
			result = append(result, name)
		}
	}
	return result
}

// collectAlertsFromFile parses alerts from a log file on disk and appends stuck test alerts.
func (r *runner) collectAlertsFromFile(logPath string, stream *testEventStream) alerts {
	detectedAlerts := alerts(parseAlertsFromFile(logPath))
	detectedAlerts = append(detectedAlerts, stream.StuckAlerts()...)
	r.addAlerts(detectedAlerts)
	return detectedAlerts
}

// collectJUnitResult reads the JUnit report file and classifies the test outcome.
func (r *runner) collectJUnitResult(cfg execConfig, exitCode int, detectedAlerts alerts) (numTests, numFailed int, failureKind string) {
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
	} else if failureKind == "" {
		failureKind = "crash"
		r.addAlerts([]alert{{
			Kind:    failureKindCrash,
			Summary: fmt.Sprintf("Process exited without junit report (exit code: %d)", exitCode),
		}})
	}
	return
}

// emitCrashRecoveryRetries retries any tests that were still running when the process
// exited (e.g., a panic killed the process before `--- FAIL` was emitted).
func (r *runner) emitCrashRecoveryRetries(cfg execConfig, failed bool, numTests int,
	emittedRetries map[string]bool, stream *testEventStream, emit func(...*queueItem)) {

	if !failed && numTests > 0 {
		return
	}
	if cfg.attempt >= r.maxAttempts {
		if len(emittedRetries) == 0 {
			r.addError(fmt.Errorf("%s failed on attempt %d", cfg.unit.displayName, cfg.attempt))
		}
		return
	}

	stillRunning := stream.RunningTests()
	var unretried []string
	for _, name := range stillRunning {
		if !emittedRetries[name] {
			unretried = append(unretried, name)
		}
	}
	if len(unretried) > 0 {
		r.scheduleRetry(cfg, unretried, nil, emit)
		return
	}

	if len(emittedRetries) == 0 {
		r.addError(fmt.Errorf("%s failed on attempt %d", cfg.unit.displayName, cfg.attempt))
	}
}

func (r *runner) finalizeReport(reports []*junitReport) error {
	mergedReport, err := mergeReports(reports)
	if err != nil {
		return err
	}
	mergedReport.path = r.junitReportPath
	if err := mergedReport.write(); err != nil {
		return err
	}

	r.log("attempt counts: total=%d passed=%d failed=%d errors=%d skipped=%d",
		mergedReport.Tests,
		mergedReport.Tests-mergedReport.Failures-mergedReport.Errors,
		mergedReport.Failures,
		mergedReport.Errors,
		mergedReport.Skipped)

	return errors.Join(mergedReport.reportingErrs...)
}

func writeConsoleResult(r *runner, cfg execConfig, exitCode int,
	numTests, numFailed int, failureKind string, detectedAlerts alerts,
	results testResults, start time.Time, progressCompleted, progressTotal int, progressDone bool) {

	failed := exitCode != 0 || numFailed > 0 || numTests == 0
	status := "❌️"
	if !failed {
		if progressDone && progressTotal > 0 {
			status = fmt.Sprintf("✅ [%d/%d]", progressCompleted, progressTotal)
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
	header := fmt.Sprintf("%s%s %s %s (attempt=%s, passed=%d/%s%s, runtime=%v)",
		logPrefix, time.Now().Format("15:04:05"), status, cfg.unit.displayName, cfg.displayAttempt(),
		passedTests, totalStr, failureInfo, time.Since(start).Round(time.Second))

	var body strings.Builder

	if failed && len(detectedAlerts) > 0 {
		for _, a := range detectedAlerts.dedupe() {
			if testName := primaryTestName(a.Tests); testName != "" {
				fmt.Fprintf(&body, "--- %s: %s — in %s\n", strings.ToUpper(string(a.Kind)), a.Summary, testName)
			} else {
				fmt.Fprintf(&body, "--- %s: %s\n", strings.ToUpper(string(a.Kind)), a.Summary)
			}
		}
	}

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
