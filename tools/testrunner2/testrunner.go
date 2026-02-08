package testrunner2

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
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
type progressTracker struct {
	total     int64
	completed atomic.Int64
}

func (p *progressTracker) complete(n int) (completed, total int) {
	c := p.completed.Add(int64(n))
	return int(c), int(p.total)
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
	args, err := parseConfig(command, os.Args[2:], &r.config)
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

	// Discover test files (filtered by shard if sharding is enabled)
	tp, err := newTestPackage(testPackageConfig{
		log:         r.log,
		buildTags:   r.buildTags,
		totalShards: r.totalShards,
		shardIndex:  r.shardIndex,
		groupBy:     r.groupBy,
		runFilter:   r.runFilter,
	}, testDirs)
	if err != nil {
		return fmt.Errorf("failed to discover test files: %w", err)
	}

	if len(tp.files) == 0 {
		return fmt.Errorf("no test files found in directories: %v", testDirs)
	}

	// Get work units based on grouping mode
	units := tp.groupByMode(r.groupBy)

	if r.totalShards > 1 {
		r.log("shard %d/%d: running %d work units (group-by=%s)", r.shardIndex+1, r.totalShards, len(units), r.groupBy)
	}

	if len(units) == 0 {
		return fmt.Errorf("no work units after grouping (mode=%s) in directories: %v", r.groupBy, testDirs)
	}

	// Print test files (from work units to reflect sharding)
	r.log("test files: %s", formatWorkUnits(units, r.groupBy))
	r.log("work units: %d (group-by=%s)", len(units), r.groupBy)

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
	r.progress = &progressTracker{total: int64(len(units))}

	// Create compile items for each package
	items := r.createCompileItems(tp, units, binDir, baseArgs)

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

// runDirectMode runs all tests via `go test` without precompilation, using the
// scheduler for retry lifecycle. This is the GroupByNone path.
func (r *runner) runDirectMode(ctx context.Context, testDirs []string, baseArgs []string) error {
	r.log("running in 'none' mode - executing go test directly without precompilation")

	// Create log directory
	if err := os.MkdirAll(r.logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	r.log("log directory: %s", r.logDir)

	// Parse base args: extract flags the runner manages and collect go test flags.
	var race bool
	var extraArgs []string
	for _, arg := range baseArgs {
		switch {
		case arg == "-race":
			race = true
		case arg == "--":
			// stop processing; everything after is already in testBinaryArgs
		case strings.HasPrefix(arg, "-timeout="),
			strings.HasPrefix(arg, "-tags="),
			strings.HasPrefix(arg, "-coverprofile="),
			strings.HasPrefix(arg, "--junitfile="),
			strings.HasPrefix(arg, "--log-dir="),
			strings.HasPrefix(arg, "--max-attempts="),
			strings.HasPrefix(arg, "--run-timeout="),
			strings.HasPrefix(arg, "--stuck-test-timeout="),
			strings.HasPrefix(arg, "--parallelism="),
			strings.HasPrefix(arg, "--group-by="),
			strings.HasPrefix(arg, "-run="):
			// Already managed by the runner; skip.
		case strings.HasPrefix(arg, "-"):
			// Pass through other go test flags (e.g., -shuffle, -count).
			extraArgs = append(extraArgs, arg)
		default:
			// Pass through non-flag values that follow flags (e.g., "on" in "-shuffle on").
			extraArgs = append(extraArgs, arg)
		}
	}

	// Normalize package paths (ensure ./ prefix)
	pkgs := make([]string, len(testDirs))
	for i, dir := range testDirs {
		if !strings.HasPrefix(dir, "./") && !strings.HasPrefix(dir, "/") {
			pkgs[i] = "./" + dir
		} else {
			pkgs[i] = dir
		}
	}
	r.log("test packages: %v", pkgs)

	// Create result collector and progress tracker
	r.collector = &resultCollector{}
	r.progress = &progressTracker{total: 1}

	// Run via scheduler (at least 2 workers for mid-stream retries)
	sched := newScheduler(max(2, r.parallelism))
	sched.run(ctx, []*queueItem{r.newExecItem(r.directExecConfig(pkgs, race, extraArgs, 1, "", ""))})

	// Finalize report
	if err := r.finalizeReport(r.collector.junitReports); err != nil {
		return err
	}

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

	// Factory functions for building retry queueItems.
	retryForFailures func(failedNames []string, attempt int) []*queueItem
	retryForCrash    func(passedNames, quarantinedNames []string, attempt int) []*queueItem
	retryForUnknown  func(passedNames []string, attempt int) []*queueItem
}

//nolint:revive // cyclomatic
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

			// 2. Set up event stream
			testCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			// Track mid-stream retries to avoid duplicates in post-exit logic
			emittedRetries := make(map[string]bool)
			children := make(map[string]bool) // tracks which tests have subtests

			handler := func(ev testEvent) {
				// Track parent/child relationships
				if strings.Contains(ev.Test, "/") {
					children[parentTestName(ev.Test)] = true
				}

				if !cfg.streamRetries || ev.Action != "fail" {
					return
				}
				// Mid-stream retry: only for leaf, top-level or childless tests
				if children[ev.Test] {
					return // parent test, children handle retries
				}
				if emittedRetries[ev.Test] {
					return // already emitted
				}
				if cfg.attempt >= r.maxAttempts {
					return // out of retries
				}
				emittedRetries[ev.Test] = true
				if items := cfg.retryForFailures([]string{ev.Test}, cfg.attempt); len(items) > 0 {
					emit(items...)
				}
			}

			stream := newTestEventStream(testEventStreamConfig{
				Writer:            lc,
				Handler:           handler,
				StuckThreshold:    r.stuckTestTimeout,
				AllStuckThreshold: r.runTimeout,
				StuckCancel:       cancel,
				Log:               r.log,
			})
			defer stream.Close()

			// 3. Start process
			result := cfg.startProcess(testCtx, stream)

			// 4. Get output, generate JUnit, parse alerts
			outputStr, err := lc.GetOutput()
			if err != nil {
				r.log("warning: failed to get test output: %v", err)
			}
			_ = lc.Close()

			results := newJUnitReport(outputStr, cfg.junitPath)
			passedTestNames := results.passes

			detectedAlerts := parseAlerts(outputStr)

			// Check stuck tests (per-test or all-stuck detection)
			if stuckNames, stuckDur := stream.StuckTests(); len(stuckNames) > 0 {
				detectedAlerts = append(detectedAlerts, alert{
					Kind:    failureKindTimeout,
					Summary: fmt.Sprintf("test stuck (no progress for %v)", stuckDur.Round(time.Second)),
					Tests:   stuckNames,
				})
			}

			r.collector.addAlerts(detectedAlerts)
			failureKind := classifyAlerts(detectedAlerts)

			// 5. JUnit report
			var numTests, numFailedTests int
			jr := &junitReport{path: cfg.junitPath, attempt: cfg.attempt}
			if err := jr.read(); err == nil {
				r.collector.addReport(jr)
				numTests = jr.Tests
				numFailedTests = jr.Failures
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

			// 6. Console output
			writeConsoleResult(r, cfg, result, numTests, numFailedTests,
				failureKind, detectedAlerts, results, start)

			failed := result.exitCode != 0 || numFailedTests > 0 || numTests == 0

			// Delete log file if test passed
			if !failed {
				_ = os.Remove(cfg.logPath)
			}

			// 7. Post-exit retry logic
			if !failed && numTests > 0 {
				if cfg.streamRetries {
					r.log("all tests passed on attempt %d", cfg.attempt)
				}
				return // success
			}
			if cfg.attempt >= r.maxAttempts {
				r.collector.addError(fmt.Errorf("%s failed on attempt %d", cfg.label, cfg.attempt))
				return
			}

			if failureKind == "timeout" || failureKind == "crash" {
				quarantined := quarantinedTestNames(detectedAlerts)
				if items := cfg.retryForCrash(passedTestNames, quarantined, cfg.attempt); len(items) > 0 {
					emit(items...)
				}
			} else if unemitted := filterEmitted(results.failures, emittedRetries); len(unemitted) > 0 {
				failures := filterParentFailures(unemitted)
				var failedNames []string
				for _, f := range failures {
					failedNames = append(failedNames, f.Name)
				}
				if items := cfg.retryForFailures(failedNames, cfg.attempt); len(items) > 0 {
					emit(items...)
				}
			} else if len(emittedRetries) == 0 {
				if items := cfg.retryForUnknown(passedTestNames, cfg.attempt); len(items) > 0 {
					emit(items...)
				}
			}
		},
	}
}

// classifyAlerts determines the failure kind from a set of alerts.
func classifyAlerts(detectedAlerts alerts) string {
	for _, a := range detectedAlerts {
		switch a.Kind {
		case failureKindTimeout:
			return "timeout"
		case failureKindCrash, failureKindPanic, failureKindFatal, failureKindDataRace:
			return "crash"
		default:
		}
	}
	return ""
}

// filterEmitted returns failures not already emitted as mid-stream retries.
// It also filters out parent tests whose children were already emitted, since
// retrying the parent would duplicate the child retry.
func filterEmitted(failures []testFailure, emitted map[string]bool) []testFailure {
	var out []testFailure
	for _, f := range failures {
		if emitted[f.Name] {
			continue
		}
		// Also filter parent tests if any child was already emitted.
		isParentOfEmitted := false
		for name := range emitted {
			if strings.HasPrefix(name, f.Name+"/") {
				isParentOfEmitted = true
				break
			}
		}
		if isParentOfEmitted {
			continue
		}
		out = append(out, f)
	}
	return out
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

// --- retry hooks ---

// retryItemFunc converts a retryPlan into a queueItem for a given attempt.
type retryItemFunc func(plan retryPlan, attempt int) *queueItem

// buildRetryHooks wires up the three retry callbacks using a shared retryItemFunc.
func (r *runner) buildRetryHooks(makeItem retryItemFunc) (
	retryForFailures func([]string, int) []*queueItem,
	retryForCrash func([]string, []string, int) []*queueItem,
	retryForUnknown func([]string, int) []*queueItem,
) {
	retryForFailures = func(failedNames []string, attempt int) []*queueItem {
		plan := retryPlan{tests: failedNames}
		r.log("🔄 scheduling retry: -run %s", buildTestFilterPattern(failedNames))
		if item := makeItem(plan, attempt+1); item != nil {
			return []*queueItem{item}
		}
		return nil
	}

	retryForCrash = func(passedNames, quarantinedNames []string, attempt int) []*queueItem {
		plans := buildRetryPlans(passedNames, quarantinedNames)
		var items []*queueItem
		for _, p := range plans {
			switch {
			case p.tests != nil && p.skipTests != nil:
				r.log("🔄 scheduling retry: -run %q -skip %q",
					buildTestFilterPattern(p.tests),
					buildTestFilterPattern(p.skipTests))
			case p.skipTests != nil:
				r.log("🔄 scheduling retry: -skip %q",
					buildTestFilterPattern(p.skipTests))
			default:
				r.log("🔄 scheduling retry...")
			}
			if item := makeItem(p, attempt+1); item != nil {
				items = append(items, item)
			}
		}
		return items
	}

	retryForUnknown = func(passedNames []string, attempt int) []*queueItem {
		return retryForCrash(passedNames, nil, attempt)
	}

	return
}

// --- compiled mode execConfig ---

func (r *runner) compiledExecConfig(unit workUnit, binaryPath string, attempt int) execConfig {
	// The Go binary timeout (-test.timeout) limits TOTAL execution time for
	// all subtests. Large suites (e.g., 400 leaf tests) may need much more
	// time than --run-timeout allows. Use the overall timeout (if set) as
	// the binary timeout so suites have enough total time. The all-stuck
	// monitor (threshold = --run-timeout) handles detecting when individual
	// tests are stuck and cancels the run early.
	timeout := r.timeout
	if timeout == 0 {
		timeout = r.runTimeout
	}

	desc := describeUnit(unit, r.groupBy)
	coverProfile := fmt.Sprintf("%s_run_%d%s",
		strings.TrimSuffix(r.coverProfilePath, codeCoverageExtension),
		attempt,
		codeCoverageExtension)
	coverProfile, _ = filepath.Abs(coverProfile)

	junitFilename := fmt.Sprintf("junit_%s.xml", uuid.New().String())
	junitPath := filepath.Join(r.logDir, junitFilename)
	logPath, _ := filepath.Abs(buildLogFilename(r.logDir, desc))

	fileNames := make([]string, len(unit.files))
	for i, tf := range unit.files {
		fileNames[i] = filepath.Base(tf.path)
	}

	retryForFailures, retryForCrash, retryForUnknown := r.buildRetryHooks(
		func(plan retryPlan, attempt int) *queueItem {
			var wu workUnit
			if plan.tests != nil && plan.skipTests == nil {
				// Simple failure retry: build a retry unit from the failed test names
				var failedTests []testCase
				for _, name := range plan.tests {
					failedTests = append(failedTests, testCase{name: name, attempts: attempt})
				}
				retryUnit := buildRetryUnit(unit, failedTests)
				if retryUnit == nil {
					return nil
				}
				wu = *retryUnit
			} else {
				// Crash/quarantine retry: use the original unit with run/skip lists.
				// Merge plan's skip list with the current unit's to accumulate
				// skips across attempts (so subtests that passed in earlier
				// attempts don't re-run).
				wu = workUnit{
					pkg:       unit.pkg,
					files:     unit.files,
					label:     unit.label,
					skipTests: mergeUnique(unit.skipTests, plan.skipTests),
				}
				if plan.tests != nil {
					wu.tests = make([]testCase, len(plan.tests))
					for i, name := range plan.tests {
						wu.tests[i] = testCase{name: name, attempts: attempt - 1}
					}
				} else {
					wu.tests = incrementAttempts(unit.tests, attempt-1)
				}
			}
			// Don't emit if skip would cover every test (vacuous plan)
			if wouldSkipAll(wu.tests, wu.skipTests) {
				return nil
			}
			return r.newExecItem(r.compiledExecConfig(wu, binaryPath, attempt))
		},
	)

	return execConfig{
		startProcess: func(ctx context.Context, output io.Writer) commandResult {
			return executeTest(ctx, r.exec, executeTestInput{
				binary:       binaryPath,
				pkgDir:       unit.pkg,
				tests:        unit.tests,
				skipPattern:  buildTestFilterPattern(unit.skipTests),
				timeout:      timeout,
				coverProfile: coverProfile,
				extraArgs:    r.testBinaryArgs,
				env:          append(r.env, fmt.Sprintf("TEMPORAL_TEST_ATTEMPT=%d", attempt)),
				output:       output,
			}, func(command string) {
				r.console.WriteGrouped(
					fmt.Sprintf("%s%s 🚀 %s (attempt %d)", logPrefix, time.Now().Format("15:04:05"), desc, attempt),
					"$ "+command+"\n",
				)
			})
		},
		label:     desc,
		attempt:   attempt,
		logPath:   logPath,
		junitPath: junitPath,
		logHeader: &logFileHeader{
			Package:   unit.pkg,
			TestFiles: fileNames,
			Attempt:   attempt,
			Started:   time.Now(),
			Command:   binaryPath,
		},
		streamRetries:    false, // one test per process
		retryForFailures: retryForFailures,
		retryForCrash:    retryForCrash,
		retryForUnknown:  retryForUnknown,
	}
}

// --- direct mode execConfig ---

func (r *runner) directExecConfig(pkgs []string, race bool, extraArgs []string, attempt int, runFilter, skipFilter string) execConfig {
	desc := "all"

	// When there's a run filter, this is a retry for specific tests. Multiple
	// retries at the same attempt number (from stream retries) need unique file
	// names to avoid overwriting each other's log and JUnit files.
	// When there's a run filter, this is a retry for specific tests. Multiple
	// retries at the same attempt number (from stream retries) need unique file
	// names to avoid overwriting each other's log and JUnit files.
	fileSuffix := ""
	if runFilter != "" || skipFilter != "" {
		fileSuffix = fmt.Sprintf("_%d", r.directRetrySeq.Add(1))
	}

	retryForFailures, retryForCrash, retryForUnknown := r.buildRetryHooks(
		func(plan retryPlan, attempt int) *queueItem {
			runF := buildTestFilterPattern(plan.tests)
			skipF := buildTestFilterPattern(plan.skipTests)
			return r.newExecItem(r.directExecConfig(pkgs, race, extraArgs, attempt, runF, skipF))
		},
	)

	// In direct mode, use the overall timeout for go test's -timeout flag
	// since the entire test suite runs as a single invocation. The per-test
	// --run-timeout is designed for compiled mode where each test binary
	// runs separately.
	timeout := r.timeout
	if timeout == 0 {
		timeout = r.runTimeout
	}

	return execConfig{
		startProcess: func(ctx context.Context, output io.Writer) commandResult {
			return runDirectGoTest(ctx, r.exec, runDirectGoTestInput{
				pkgs:         pkgs,
				buildTags:    r.buildTags,
				race:         race,
				coverProfile: r.coverProfilePath,
				timeout:      timeout,
				env:          append(r.env, fmt.Sprintf("TEMPORAL_TEST_ATTEMPT=%d", attempt)),
				output:       output,
				runFilter:    runFilter,
				skipFilter:   skipFilter,
				extraArgs:    extraArgs,
			}, func(command string) {
				r.console.WriteGrouped(
					fmt.Sprintf("%s%s 🚀 %s (attempt %d)", logPrefix, time.Now().Format("15:04:05"), desc, attempt),
					"$ "+command+"\n",
				)
			})
		},
		label:            desc,
		attempt:          attempt,
		logPath:          filepath.Join(r.logDir, fmt.Sprintf("all_mode_attempt_%d%s.log", attempt, fileSuffix)),
		junitPath:        filepath.Join(r.logDir, fmt.Sprintf("junit_all_attempt_%d%s.xml", attempt, fileSuffix)),
		streamRetries:    true,
		retryForFailures: retryForFailures,
		retryForCrash:    retryForCrash,
		retryForUnknown:  retryForUnknown,
	}
}

// --- compile items ---

func (r *runner) createCompileItems(tp *testPackage, units []workUnit, binDir string, baseArgs []string) []*queueItem {
	// Group units by package for compilation
	unitsByPkg := make(map[string][]workUnit)
	for _, u := range units {
		unitsByPkg[u.pkg] = append(unitsByPkg[u.pkg], u)
	}

	// Sort units by file size (largest first) for better load balancing
	for _, pkgUnits := range unitsByPkg {
		slices.SortFunc(pkgUnits, func(a, b workUnit) int {
			return cmp.Compare(unitFileSize(b), unitFileSize(a)) // descending
		})
	}

	pkgs := slices.Sorted(maps.Keys(unitsByPkg))

	items := make([]*queueItem, 0, len(pkgs))
	for _, pkg := range pkgs {
		binName := strings.ReplaceAll(strings.TrimPrefix(pkg, "./"), "/", "_") + ".test"
		binaryPath := filepath.Join(binDir, binName)

		items = append(items, r.newCompileItem(pkg, unitsByPkg[pkg], binaryPath, baseArgs))
	}
	return items
}

func (r *runner) newCompileItem(pkg string, units []workUnit, binaryPath string, baseArgs []string) *queueItem {
	var lc *logCapture
	var compileErr error

	return &queueItem{
		run: func(ctx context.Context, emit func(...*queueItem)) {
			// Create log capture for compile output
			logPath := buildCompileLogFilename(r.logDir)
			var err error
			lc, err = newLogCapture(logCaptureConfig{
				LogPath: logPath,
			})
			if err != nil {
				compileErr = fmt.Errorf("failed to create compile log file %s: %w", logPath, err)
				r.collector.addError(compileErr)
				return
			}

			result := compileTest(ctx, r.exec, compileTestInput{
				pkg:        pkg,
				binaryPath: binaryPath,
				buildTags:  r.buildTags,
				baseArgs:   baseArgs,
				env:        r.env,
				output:     lc,
			}, func(command string) {
				r.console.WriteGrouped(fmt.Sprintf("%s%s 🚀 compiling %s", logPrefix, time.Now().Format("15:04:05"), pkg), "$ "+command+"\n")
			})

			if result.exitCode != 0 {
				compileErr = fmt.Errorf("failed to compile %s (exit code %d)", pkg, result.exitCode)
				r.collector.addError(compileErr)
			}

			// Get output for display
			outputStr, err := lc.GetOutput()
			if err != nil {
				r.log("warning: failed to get compile output: %v", err)
			}

			header := fmt.Sprintf("%s%s 🔨 compiled %s", logPrefix, time.Now().Format("15:04:05"), pkg)
			r.console.WriteGrouped(header, outputStr)

			_ = lc.Close()

			// Emit test items if compilation succeeded
			if compileErr != nil {
				return
			}

			r.log("starting test run for %s: units=%d", pkg, len(units))

			var items []*queueItem
			for _, unit := range units {
				items = append(items, r.newExecItem(r.compiledExecConfig(unit, binaryPath, 1)))
			}
			emit(items...)
		},
	}
}

// --- helper functions ---

// formatWorkUnits builds a human-readable summary of work units for logging.
func formatWorkUnits(units []workUnit, groupBy GroupMode) string {
	var sb strings.Builder
	if groupBy == GroupByTest {
		formatWorkUnitsByTest(&sb, units)
	} else {
		formatWorkUnitsByFile(&sb, units)
	}
	return sb.String()
}

func formatWorkUnitsByTest(sb *strings.Builder, units []workUnit) {
	testsByFile := make(map[string][]string)
	fileOrder := make([]string, 0)
	for _, u := range units {
		for _, tf := range u.files {
			if _, seen := testsByFile[tf.path]; !seen {
				fileOrder = append(fileOrder, tf.path)
				testsByFile[tf.path] = nil
			}
			for _, tc := range tf.tests {
				testsByFile[tf.path] = append(testsByFile[tf.path], tc.name)
			}
		}
	}
	for _, path := range fileOrder {
		sb.WriteString("\n  ")
		sb.WriteString(path)
		if tests := testsByFile[path]; len(tests) > 0 {
			sb.WriteString(" (")
			sb.WriteString(strings.Join(tests, ", "))
			sb.WriteString(")")
		}
	}
}

func formatWorkUnitsByFile(sb *strings.Builder, units []workUnit) {
	seen := make(map[string]bool)
	for _, u := range units {
		for _, tf := range u.files {
			if !seen[tf.path] {
				seen[tf.path] = true
				sb.WriteString("\n  ")
				sb.WriteString(tf.path)
			}
		}
	}
}

func describeUnit(unit workUnit, mode GroupMode) string {
	return unit.label
}

func buildRetryUnit(unit workUnit, failedTests []testCase) *workUnit {
	retryFiles := buildRetryFiles(unit.files, failedTests)
	if len(retryFiles) == 0 {
		return nil
	}

	return &workUnit{
		pkg:   unit.pkg,
		files: retryFiles,
		tests: failedTests,
		label: unit.label,
	}
}

// retryPlan describes a single retry invocation in terms of -run/-skip test names.
type retryPlan struct {
	tests     []string // tests to -run (nil = all from original set)
	skipTests []string // tests to -skip
}

// buildRetryPlans computes retry plans with quarantine logic.
//
// When quarantinedTests is non-empty and there are passed sibling tests under
// the same parent, the quarantined test is isolated into a separate plan. This
// avoids mixed-depth skip patterns (e.g., "TestA" at depth 1 and "TestB/Var1"
// at depth 2) that can't be expressed cleanly in a single -test.skip pattern.
func buildRetryPlans(passedTests, quarantinedTests []string) []retryPlan {
	if len(passedTests) == 0 || len(quarantinedTests) == 0 {
		// Simple case: no quarantine needed.
		// Pass through passedTests directly; buildTestFilterPattern handles the
		// per-level regex. Some shallower names may be dropped from the pattern
		// (causing those passed tests to re-run) but never over-skipped.
		return []retryPlan{{skipTests: passedTests}}
	}

	// Build quarantine plans for each quarantined test.
	var quarantinePlans []retryPlan
	quarantinedParents := make(map[string]bool)
	quarantinedTopLevel := make(map[string]bool)
	for _, qt := range quarantinedTests {
		parent := parentTestName(qt)
		if parent == "" {
			// Top-level test (e.g., panicking TestFoo): retry in isolation,
			// skipping subtests that already passed.
			if !quarantinedTopLevel[qt] {
				quarantinedTopLevel[qt] = true
				quarantinePlans = append(quarantinePlans, retryPlan{
					tests:     []string{qt},
					skipTests: filterByPrefix(passedTests, qt),
				})
			}
			continue
		}
		if quarantinedParents[parent] {
			continue // already created a quarantine plan for this parent
		}
		siblings := filterByPrefix(passedTests, parent)
		if len(siblings) == 0 {
			continue // no passed siblings to skip, no need to quarantine
		}
		quarantinedParents[parent] = true
		quarantinePlans = append(quarantinePlans, retryPlan{
			tests:     []string{parent},
			skipTests: siblings,
		})
	}

	if len(quarantinePlans) == 0 {
		// No quarantine was possible, fall back to simple case
		return []retryPlan{{skipTests: passedTests}}
	}

	// Build the regular retry plan: skip all passed tests + quarantined tests.
	// We include the quarantined tests themselves (at their full depth) rather
	// than adding parent names at a shallower depth. Mixed depths would cause
	// buildTestFilterPattern's per-level pattern to drop the shallower entries,
	// silently un-skipping the quarantined parent.
	regularSkip := append(slices.Clone(passedTests), quarantinedTests...)

	return append(quarantinePlans, retryPlan{skipTests: regularSkip})
}

// buildRetryUnitExcluding builds retry units that use -test.skip to skip tests that already passed.
// Used for crash/timeout retries where we want to skip tests that completed successfully.
func buildRetryUnitExcluding(unit workUnit, passedTests []string, quarantinedTests []string, attempt int) []*workUnit {
	plans := buildRetryPlans(passedTests, quarantinedTests)
	units := make([]*workUnit, len(plans))
	for i, p := range plans {
		wu := &workUnit{
			pkg:       unit.pkg,
			files:     unit.files,
			label:     unit.label,
			skipTests: p.skipTests,
		}
		if p.tests != nil {
			wu.tests = make([]testCase, len(p.tests))
			for j, name := range p.tests {
				wu.tests[j] = testCase{name: name, attempts: attempt}
			}
		} else {
			wu.tests = incrementAttempts(unit.tests, attempt)
		}
		units[i] = wu
	}
	return units
}

// quarantinedTestNames extracts test names from alerts that crash the process.
// These tests are quarantined (retried in isolation) so they don't take down
// the bulk retry of remaining tests.
func quarantinedTestNames(as alerts) []string {
	var names []string
	for _, a := range as {
		switch a.Kind {
		case failureKindTimeout, failureKindPanic, failureKindFatal, failureKindDataRace:
			for _, t := range a.Tests {
				// Panic/fatal/race alerts use fully-qualified names
				// (e.g., "pkg.TestFoo.func1.3") while the rest of the
				// retry system uses plain names ("TestFoo"). Clean them.
				_, name := splitTestName(t)
				names = append(names, name)
			}
		default:
		}
	}
	return names
}

// parentTestName returns the parent of a test name by truncating the last "/" segment.
// Returns "" if the name has no parent (top-level test).
func parentTestName(name string) string {
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		return name[:idx]
	}
	return ""
}

// filterByPrefix returns names that start with prefix + "/".
func filterByPrefix(names []string, prefix string) []string {
	p := prefix + "/"
	var out []string
	for _, n := range names {
		if strings.HasPrefix(n, p) {
			out = append(out, n)
		}
	}
	return out
}

// mergeUnique merges two string slices, deduplicating entries. Used to accumulate
// skip test lists across retry attempts so that subtests that passed in earlier
// attempts remain skipped in later retries.
func mergeUnique(a, b []string) []string {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]bool, len(a)+len(b))
	result := make([]string, 0, len(a)+len(b))
	for _, s := range a {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	for _, s := range b {
		if !seen[s] {
			seen[s] = true
			result = append(result, s)
		}
	}
	return result
}

func buildRetryFiles(files []testFile, failedTests []testCase) []testFile {
	// Build a map from top-level test name to all its failed subtests
	failedByTopLevel := make(map[string][]testCase)
	for _, tc := range failedTests {
		topLevel := tc.name
		if idx := strings.Index(tc.name, "/"); idx > 0 {
			topLevel = tc.name[:idx]
		}
		failedByTopLevel[topLevel] = append(failedByTopLevel[topLevel], tc)
	}

	// For each file, check if any of its tests failed
	var retryFiles []testFile
	for _, tf := range files {
		var fileFailedTests []testCase
		for _, tc := range tf.tests {
			if failed, ok := failedByTopLevel[tc.name]; ok {
				fileFailedTests = append(fileFailedTests, failed...)
			}
		}
		if len(fileFailedTests) > 0 {
			retryFiles = append(retryFiles, testFile{
				path:  tf.path,
				pkg:   tf.pkg,
				tests: fileFailedTests,
			})
		}
	}
	return retryFiles
}

// filterParentFailures removes parent test names from the failure list when
// subtests of that parent are also present. Go's test framework marks parent
// tests as failed whenever a child fails, but these parent entries are redundant
// for retry purposes and cause mixed-depth names that break buildTestFilterPattern.
func filterParentFailures(failures []testFailure) []testFailure {
	names := make(map[string]bool, len(failures))
	for _, f := range failures {
		names[f.Name] = true
	}
	var filtered []testFailure
	for _, f := range failures {
		isParent := false
		for other := range names {
			if other != f.Name && strings.HasPrefix(other, f.Name+"/") {
				isParent = true
				break
			}
		}
		if !isParent {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

// wouldSkipAll returns true if every test in tests has a matching entry in skipTests.
// Used to detect vacuous retry plans where run and skip patterns cancel out.
func wouldSkipAll(tests []testCase, skipTests []string) bool {
	if len(skipTests) == 0 || len(tests) == 0 {
		return false
	}
	skipSet := make(map[string]bool, len(skipTests))
	for _, s := range skipTests {
		skipSet[s] = true
	}
	for _, tc := range tests {
		if !skipSet[tc.name] {
			return false
		}
	}
	return true
}

// incrementAttempts returns a copy of tests with attempts set to the given value.
func incrementAttempts(tests []testCase, attempt int) []testCase {
	result := make([]testCase, len(tests))
	for i, tc := range tests {
		result[i] = testCase{name: tc.name, attempts: attempt}
	}
	return result
}

// unitFileSize returns the total size of all files in the work unit.
func unitFileSize(u workUnit) int64 {
	var total int64
	for _, f := range u.files {
		if info, err := os.Stat(f.path); err == nil {
			total += info.Size()
		}
	}
	return total
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
	return errors.Join(mergedReport.reportingErrs...)
}
