package testrunner2

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
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
	maxAttempts      int
	parallelism      int
	filesPerWorker   int
	totalShards      int       // for CI sharding
	shardIndex       int       // for CI sharding
	testBinaryArgs   []string  // args to pass to test binary (after -args)
	groupBy          GroupMode // how to group tests: package, file, suite
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
	console   *consoleWriter
	collector *resultCollector
	progress  *progressTracker
}

func newRunner() *runner {
	cfg := defaultConfig()
	cfg.log = log.Printf
	cfg.exec = defaultExec
	return &runner{config: cfg}
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
			log.Fatalf("failed: %v", err)
		}
	case reportLogsCommand:
		if err := r.reportLogs(); err != nil {
			log.Fatalf("failed: %v", err)
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
		return r.runAllMode(ctx, testDirs, baseArgs)
	}

	// Discover test files (filtered by shard if sharding is enabled)
	tp, err := newTestPackage(testPackageConfig{
		log:         r.log,
		buildTags:   r.buildTags,
		totalShards: r.totalShards,
		shardIndex:  r.shardIndex,
		groupBy:     r.groupBy,
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
	var sb strings.Builder
	if r.groupBy == GroupBySuite {
		// For suite mode, group suites by file
		suitesByFile := make(map[string][]string)
		fileOrder := make([]string, 0)
		for _, u := range units {
			for _, tf := range u.files {
				if _, seen := suitesByFile[tf.path]; !seen {
					fileOrder = append(fileOrder, tf.path)
					suitesByFile[tf.path] = nil
				}
				for _, tc := range tf.tests {
					suitesByFile[tf.path] = append(suitesByFile[tf.path], tc.name)
				}
			}
		}
		for _, path := range fileOrder {
			sb.WriteString("\n  ")
			sb.WriteString(path)
			if suites := suitesByFile[path]; len(suites) > 0 {
				sb.WriteString(" (")
				sb.WriteString(strings.Join(suites, ", "))
				sb.WriteString(")")
			}
		}
	} else {
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
	r.log("test files:%s", sb.String())
	r.log("work units: %d (group-by=%s)", len(units), r.groupBy)

	r.console = &consoleWriter{mu: &sync.Mutex{}}

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

// runAllMode runs all tests via a single `go test` command without precompilation.
// This is optimized for unit tests that don't need per-package isolation.
func (r *runner) runAllMode(ctx context.Context, testDirs []string, baseArgs []string) error {
	r.log("running in 'none' mode - executing go test directly without precompilation")

	// Create log directory
	if err := os.MkdirAll(r.logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	r.log("log directory: %s", r.logDir)

	// Parse base args for flags we need
	var race bool
	for _, arg := range baseArgs {
		if arg == "-race" {
			race = true
		}
	}

	// Convert test directories to package paths if needed (ensure ./prefix)
	pkgs := make([]string, len(testDirs))
	for i, dir := range testDirs {
		if !strings.HasPrefix(dir, "./") && !strings.HasPrefix(dir, "/") {
			pkgs[i] = "./" + dir
		} else {
			pkgs[i] = dir
		}
	}
	r.log("test packages: %v", pkgs)

	var lastReport *junitReport
	var lastExitCode int

	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		r.log("attempt %d/%d", attempt, r.maxAttempts)

		// Create log capture for test output
		logPath := filepath.Join(r.logDir, fmt.Sprintf("all_mode_attempt_%d.log", attempt))
		lc, err := newLogCapture(logCaptureConfig{
			LogPath: logPath,
		})
		if err != nil {
			return fmt.Errorf("failed to create log file: %w", err)
		}

		// Run go test directly
		result := runDirectGoTest(ctx, r.exec, runDirectGoTestInput{
			pkgs:         pkgs,
			buildTags:    r.buildTags,
			race:         race,
			coverProfile: r.coverProfilePath,
			timeout:      r.runTimeout,
			output:       lc,
		}, func(command string) {
			r.log("executing: %s", command)
		})

		lastExitCode = result.exitCode

		// Get output for JUnit generation
		outputStr, err := lc.GetOutput()
		if err != nil {
			r.log("warning: failed to get test output: %v", err)
		}
		_ = lc.Close()

		// Generate JUnit report
		junitPath := filepath.Join(r.logDir, fmt.Sprintf("junit_all_attempt_%d.xml", attempt))
		_ = newJUnitReport(outputStr, junitPath)

		// Read the JUnit report
		jr := &junitReport{path: junitPath, attempt: attempt}
		if err := jr.read(); err != nil {
			r.log("warning: failed to read junit report: %v", err)
		}
		lastReport = jr

		// Check for success
		if result.exitCode == 0 && jr.Failures == 0 {
			r.log("all tests passed on attempt %d", attempt)
			// Delete log file on success
			_ = os.Remove(logPath)
			break
		}

		r.log("attempt %d failed: exit_code=%d, failures=%d", attempt, result.exitCode, jr.Failures)

		if attempt < r.maxAttempts {
			r.log("retrying entire test run...")
		}
	}

	// Finalize report
	if lastReport != nil {
		lastReport.path = r.junitReportPath
		if err := lastReport.write(); err != nil {
			return fmt.Errorf("failed to write junit report: %w", err)
		}
	}

	if lastExitCode != 0 {
		return fmt.Errorf("tests failed after %d attempt(s)", r.maxAttempts)
	}

	r.log("test run completed")
	return nil
}

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

	var items []*queueItem
	for pkg, pkgUnits := range unitsByPkg {
		binName := strings.ReplaceAll(strings.TrimPrefix(pkg, "./"), "/", "_") + ".test"
		binaryPath := filepath.Join(binDir, binName)

		items = append(items, r.newCompileItem(pkg, pkgUnits, binaryPath, baseArgs))
	}
	return items
}

func (r *runner) newCompileItem(pkg string, units []workUnit, binaryPath string, baseArgs []string) *queueItem {
	var lc *logCapture
	var compileErr error

	return &queueItem{
		run: func(ctx context.Context) {
			// Create log capture for compile output
			logPath := buildCompileLogFilename(r.logDir, pkg)
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
				output:     lc,
			}, func(command string) {
				r.console.WriteGrouped(fmt.Sprintf("%s%s 🚀 compiling %s", logPrefix, time.Now().Format("15:04:05"), pkg), command+"\n")
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
		},
		next: func() []*queueItem {
			if compileErr != nil {
				return nil
			}

			r.log("starting test run for %s: units=%d", pkg, len(units))

			// Create test items for each work unit
			var items []*queueItem
			for _, unit := range units {
				items = append(items, r.newTestItem(unit, binaryPath, 1))
			}
			return items
		},
	}
}

//nolint:revive // cyclomatic
func (r *runner) newTestItem(unit workUnit, binary string, attempt int) *queueItem {
	var result executeTestOutput
	var results testResults
	var passedTestNames []string
	var lc *logCapture
	var alerts alerts
	var failureKind string
	var numFailedTests int
	var numTests int

	// Store JUnit reports in the log directory for debugging
	junitFilename := fmt.Sprintf("junit_%s_attempt%d.xml", sanitizeFilename(describeUnit(unit, r.groupBy)), attempt)
	junitPath := filepath.Join(r.logDir, junitFilename)

	coverProfile := fmt.Sprintf("%s_run_%d%s",
		strings.TrimSuffix(r.coverProfilePath, codeCoverageExtension),
		attempt,
		codeCoverageExtension)
	coverProfile, _ = filepath.Abs(coverProfile) // absolute since test is run from package dir

	desc := describeUnit(unit, r.groupBy)
	pkg := unit.pkg
	files := unit.files

	return &queueItem{
		run: func(ctx context.Context) {
			start := time.Now()

			// Build header for log file
			fileNames := make([]string, len(files))
			for i, tf := range files {
				fileNames[i] = filepath.Base(tf.path)
			}

			// Create log capture for test output
			// Use absolute path since test runs from package dir
			logPath, _ := filepath.Abs(buildLogFilename(r.logDir, pkg, files, attempt))
			var err error
			lc, err = newLogCapture(logCaptureConfig{
				LogPath: logPath,
				Header: &logFileHeader{
					Package:   pkg,
					TestFiles: fileNames,
					Attempt:   attempt,
					Started:   start,
					Command:   binary,
				},
			})
			if err != nil {
				r.log("warning: failed to create log file: %v", err)
				// Create a log capture without disk file
				lc, _ = newLogCapture(logCaptureConfig{})
			}

			result = executeTest(ctx, r.exec, executeTestInput{
				binary:       binary,
				pkgDir:       pkg,
				tests:        unit.tests,
				skipPattern:  testNamesToSkipPattern(unit.skipTests),
				timeout:      r.runTimeout,
				coverProfile: coverProfile,
				extraArgs:    r.testBinaryArgs,
				output:       lc,
			}, func(command string) {
				r.console.WriteGrouped(fmt.Sprintf("%s%s 🚀 %s (attempt %d)", logPrefix, time.Now().Format("15:04:05"), desc, attempt), command+"\n")
			})

			// Get complete output from disk for JUnit report
			outputStr, err := lc.GetOutput()
			if err != nil {
				r.log("warning: failed to get test output: %v", err)
			}

			// Parse output and generate JUnit report
			results = newJUnitReport(outputStr, junitPath)
			// Use passes from the parsed output directly, as the JUnit XML may have
			// errors for tests that didn't complete (e.g., during timeout)
			passedTestNames = results.passes

			// Get alerts from incremental parsing
			alerts = lc.GetAlerts()
			r.collector.addAlerts(alerts)
			for _, a := range alerts {
				switch a.Kind {
				case failureKindTimeout:
					failureKind = "timeout"
				case failureKindCrash, failureKindPanic, failureKindFatal, failureKindDataRace:
					failureKind = "crash"
				default:
					// Other alert kinds don't set failureKind
				}
				if failureKind != "" {
					break
				}
			}

			// Read junit report for test counts and merge tracking
			jr := &junitReport{path: junitPath, attempt: attempt}
			if err := jr.read(); err == nil {
				r.collector.addReport(jr)
				numTests = jr.Tests
				numFailedTests = jr.Failures
				// Note: passedTestNames is already set from results.passes above,
				// which is more reliable than jr.collectTestCasePasses() for timeouts
				if numTests == 0 {
					failureKind = "no tests"
					r.collector.addAlerts([]alert{{
						Kind:    failureKindCrash,
						Summary: "No tests were executed (possible parsing error or test filter mismatch)",
					}})
				}
			} else {
				// JUnit report missing - emit crash alert
				failureKind = "crash"
				r.collector.addAlerts([]alert{{
					Kind:    failureKindCrash,
					Summary: fmt.Sprintf("Process exited without junit report (exit code: %d)", result.exitCode),
				}})
			}

			// Build complete stdout output as one string
			failed := result.exitCode != 0 || numFailedTests > 0 || numTests == 0
			status := "❌️"
			if !failed {
				completed, total := r.progress.complete(1) // one unit completed
				status = fmt.Sprintf("✅ [%d/%d]", completed, total)
			}
			passedTests := numTests - numFailedTests
			failureInfo := ""
			if failed {
				failureInfo = fmt.Sprintf(", failure=%s", cmp.Or(failureKind, "failed"))
			}

			// For crash/timeout, show passed=X/? since we don't know the total
			var header string
			if failureKind != "" {
				header = fmt.Sprintf("%s%s %s %s (attempt=%d, passed=%d/?%s, runtime=%v)",
					logPrefix, time.Now().Format("15:04:05"), status, desc, attempt, passedTests, failureInfo, time.Since(start).Round(time.Second))
			} else {
				header = fmt.Sprintf("%s%s %s %s (attempt=%d, passed=%d/%d%s, runtime=%v)",
					logPrefix, time.Now().Format("15:04:05"), status, desc, attempt, passedTests, numTests, failureInfo, time.Since(start).Round(time.Second))
			}

			var body strings.Builder

			// Append alerts if test failed (summary only, details are in log file)
			if failed && len(alerts) > 0 {
				for _, a := range alerts.dedupe() {
					if testName := primaryTestName(a.Tests); testName != "" {
						fmt.Fprintf(&body, "    %s: %s — in %s\n", strings.ToUpper(string(a.Kind)), a.Summary, testName)
					} else {
						fmt.Fprintf(&body, "    %s: %s\n", strings.ToUpper(string(a.Kind)), a.Summary)
					}
				}
			}

			// Append test failure details
			if failed && len(results.failures) > 0 {
				for _, f := range results.failures {
					if f.ErrorTrace != "" {
						fmt.Fprintf(&body, "\n")
						for line := range strings.SplitSeq(f.ErrorTrace, "\n") {
							fmt.Fprintf(&body, "%s\n", line)
						}
						fmt.Fprintf(&body, "\n") // blank line between failures
					}
				}
			}

			r.console.WriteGrouped(header, body.String())

			// Close log file
			_ = lc.Close()

			// Delete log file if test passed (no need to keep successful logs)
			if !failed {
				_ = os.Remove(lc.Path())
			}
		},
		next: func() []*queueItem {
			// Success - no more work
			if result.exitCode == 0 && numFailedTests == 0 && numTests > 0 {
				return nil
			}

			// Out of attempts - record error
			if attempt >= r.maxAttempts {
				r.collector.addError(fmt.Errorf("%s failed on attempt %d", desc, attempt))
				return nil
			}

			// Build retry item
			var retryUnit *workUnit
			if failureKind == "timeout" || failureKind == "crash" {
				// Timeout/crash - exclude tests that already passed, re-run everything else
				retryUnit = buildRetryUnitExcluding(unit, passedTestNames, attempt)
			} else if len(results.failures) > 0 {
				// Specific test failures - retry just those tests
				var failedTests []testCase
				for _, f := range results.failures {
					failedTests = append(failedTests, testCase{name: f.Name, attempts: attempt})
				}
				retryUnit = buildRetryUnit(unit, failedTests)
			} else {
				// Unknown failure - exclude tests that already passed
				retryUnit = buildRetryUnitExcluding(unit, passedTestNames, attempt)
			}
			if retryUnit == nil {
				return nil
			}

			return []*queueItem{r.newTestItem(*retryUnit, binary, attempt+1)}
		},
	}
}

func describeUnit(unit workUnit, mode GroupMode) string {
	switch mode {
	case GroupByPackage:
		return unit.pkg
	case GroupBySuite:
		return unit.label
	default: // file
		if len(unit.files) == 1 {
			f := unit.files[0]
			base := filepath.Base(f.path)
			if len(f.tests) <= 5 {
				names := make([]string, len(f.tests))
				for i, tc := range f.tests {
					names[i] = tc.name
				}
				return fmt.Sprintf("%s (%s)", base, strings.Join(names, ", "))
			}
			return fmt.Sprintf("%s (%d tests)", base, len(f.tests))
		}
		return describeFiles(unit.files)
	}
}

func describeFiles(files []testFile) string {
	if len(files) < 5 {
		names := make([]string, len(files))
		for i, tf := range files {
			names[i] = filepath.Base(tf.path)
		}
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%d files", len(files))
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

// buildRetryUnitExcluding builds a retry unit that uses -test.skip to skip tests that already passed.
// Used for crash/timeout retries where we want to skip tests that completed successfully.
func buildRetryUnitExcluding(unit workUnit, passedTests []string, attempt int) *workUnit {
	if len(passedTests) == 0 {
		// No tests passed, retry all tests without skip
		return &workUnit{
			pkg:   unit.pkg,
			files: unit.files,
			tests: incrementAttempts(unit.tests, attempt),
			label: unit.label,
		}
	}

	// Use -test.skip to skip tests that already passed.
	// This is more reliable than trying to filter work units, especially for subtests
	// where the work unit only contains the parent test name.
	return &workUnit{
		pkg:       unit.pkg,
		files:     unit.files,
		tests:     incrementAttempts(unit.tests, attempt),
		label:     unit.label,
		skipTests: passedTests,
	}
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

// sanitizeFilename replaces characters that are invalid in filenames.
func sanitizeFilename(s string) string {
	replacer := strings.NewReplacer(
		"/", "_",
		"\\", "_",
		":", "_",
		" ", "_",
		"(", "",
		")", "",
		",", "_",
	)
	return replacer.Replace(s)
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
