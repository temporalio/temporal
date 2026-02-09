package testrunner2

import (
	"context"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// execFunc executes a command and returns the exit code.
type execFunc func(ctx context.Context, dir, name string, args, env []string, output io.Writer) int

// defaultExec runs a command and returns its exit code.
func defaultExec(ctx context.Context, dir, name string, args, env []string, output io.Writer) int {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdout = output
	cmd.Stderr = output
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return cmd.Process.Signal(syscall.SIGTERM)
		}
		return nil
	}
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return exitErr.ExitCode()
		}
		return 1
	}
	return 0
}

// compileTestInput holds the input for compileTest.
type compileTestInput struct {
	pkg        string
	binaryPath string
	buildTags  string
	baseArgs   []string
	env        []string
	output     io.Writer
}

// commandResult holds the result of running a command.
type commandResult struct {
	exitCode int
}

// compileTest compiles a test package into a binary.
// If onCommand is non-nil, it is called with the command string before execution.
func compileTest(ctx context.Context, execFn execFunc, req compileTestInput, onCommand func(string)) commandResult {
	// Build compile args for `go test -c`
	var compileFlags []string
	hasCover := false
	for _, arg := range req.baseArgs {
		if arg == "-race" || strings.HasPrefix(arg, "-tags=") || strings.HasPrefix(arg, "-coverpkg=") {
			compileFlags = append(compileFlags, arg)
		}
		if arg == "-cover" || strings.HasPrefix(arg, "-coverprofile=") || strings.HasPrefix(arg, "-coverpkg=") {
			hasCover = true
		}
	}
	if hasCover {
		compileFlags = append(compileFlags, "-cover")
	}

	args := []string{"test", "-c"}
	args = append(args, compileFlags...)
	if req.buildTags != "" {
		args = append(args, "-tags="+req.buildTags)
	}
	args = append(args, "-o", req.binaryPath, req.pkg)

	command := fmt.Sprintf("go %s", strings.Join(args, " "))
	if onCommand != nil {
		onCommand(command)
	}

	exitCode := execFn(ctx, "", "go", args, req.env, req.output)

	return commandResult{exitCode: exitCode}
}

// executeTestInput holds the input for executeTest.
type executeTestInput struct {
	binary       string
	pkgDir       string
	tests        []testCase
	skipPattern  string // regex pattern for -test.skip (to skip passed tests on retry)
	timeout      time.Duration
	coverProfile string
	extraArgs    []string // args to pass after -args (e.g., -persistenceType=xxx)
	env          []string
	output       io.Writer
}

// runDirectGoTestInput holds the input for runDirectGoTest.
type runDirectGoTestInput struct {
	pkgs         []string      // package paths to test
	buildTags    string        // build tags
	race         bool          // enable race detector
	coverProfile string        // coverage profile path
	timeout      time.Duration // test timeout
	env          []string      // extra environment variables
	output       io.Writer     // where to write output
	runFilter    string        // -run pattern (to target specific tests on retry)
	skipFilter   string        // -skip pattern (to exclude passed tests on retry)
	extraArgs    []string      // extra args to pass through to go test (e.g., -shuffle)
}

// runDirectGoTest runs `go test` directly on packages without precompilation.
// If onCommand is non-nil, it is called with the command string before execution.
func runDirectGoTest(ctx context.Context, execFn execFunc, req runDirectGoTestInput, onCommand func(string)) commandResult {
	// Apply timeout buffer so Go's -timeout fires first with stacktrace
	if req.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.timeout+30*time.Second)
		defer cancel()
	}

	args := []string{"test", "-v"}
	if req.race {
		args = append(args, "-race")
	}
	if req.buildTags != "" {
		args = append(args, "-tags="+req.buildTags)
	}
	if req.timeout > 0 {
		args = append(args, fmt.Sprintf("-timeout=%s", req.timeout))
	}
	if req.coverProfile != "" {
		args = append(args, fmt.Sprintf("-coverprofile=%s", req.coverProfile))
	}
	if req.runFilter != "" {
		args = append(args, "-run", req.runFilter)
	}
	if req.skipFilter != "" {
		args = append(args, "-skip", req.skipFilter)
	}
	args = append(args, req.extraArgs...)
	args = append(args, req.pkgs...)

	command := fmt.Sprintf("go %s", strings.Join(args, " "))
	if onCommand != nil {
		onCommand(command)
	}

	exitCode := execFn(ctx, "", "go", args, req.env, req.output)

	return commandResult{exitCode: exitCode}
}

// executeTest runs a compiled test binary.
// If onCommand is non-nil, it is called with the command string before execution.
func executeTest(ctx context.Context, execFn execFunc, req executeTestInput, onCommand func(string)) commandResult {
	// Apply timeout buffer so Go's -timeout fires first with stacktrace
	if req.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.timeout+30*time.Second)
		defer cancel()
	}

	args := []string{
		"-test.v=test2json", // Use test2json mode to get streaming subtest pass/fail output
		"-test.run", testCasesToRunPattern(req.tests),
	}
	if req.skipPattern != "" {
		args = append(args, "-test.skip", req.skipPattern)
	}
	if req.timeout > 0 {
		args = append(args, fmt.Sprintf("-test.timeout=%s", req.timeout))
	}
	if req.coverProfile != "" {
		args = append(args, fmt.Sprintf("-test.coverprofile=%s", req.coverProfile))
	}
	// Append extra args (e.g., -persistenceType=xxx -persistenceDriver=xxx)
	args = append(args, req.extraArgs...)

	// Build command string for logging
	command := fmt.Sprintf("%s %s", req.binary, strings.Join(args, " "))

	if onCommand != nil {
		onCommand(command)
	}

	exitCode := execFn(ctx, req.pkgDir, req.binary, args, req.env, req.output)

	return commandResult{exitCode: exitCode}
}

// execLogger returns an onCommand callback that logs the command to the console.
func (r *runner) execLogger(desc string, attempt int) func(string) {
	return func(command string) {
		r.console.WriteGrouped(
			fmt.Sprintf("%s%s 🚀 %s (attempt %d)", logPrefix, time.Now().Format("15:04:05"), desc, attempt),
			"$ "+command+"\n",
		)
	}
}

// --- compiled mode (compile + run per test) ---

// compiledExecConfig creates an execConfig for running a precompiled test binary.
func (r *runner) compiledExecConfig(unit workUnit, binaryPath string, attempt int) execConfig {
	timeout := r.effectiveTimeout()

	desc := unit.label
	coverProfile := fmt.Sprintf("%s_run_%d%s",
		strings.TrimSuffix(r.coverProfilePath, codeCoverageExtension),
		attempt,
		codeCoverageExtension)
	coverProfile, _ = filepath.Abs(coverProfile)

	junitFilename := fmt.Sprintf("junit_%s.xml", uuid.New().String())
	junitPath := filepath.Join(r.logDir, junitFilename)
	logPath, _ := filepath.Abs(buildLogFilename(r.logDir, desc))

	retry := r.buildRetryHandler(
		func(plan retryPlan, attempt int) *queueItem {
			var wu workUnit
			if plan.tests != nil && plan.skipTests == nil {
				// Simple failure retry: build a retry unit from the failed test names
				var failedTests []testCase
				for _, name := range plan.tests {
					failedTests = append(failedTests, testCase{name: name, attempts: attempt})
				}
				wu = workUnit{
					pkg:   unit.pkg,
					tests: failedTests,
					label: unit.label,
				}
			} else {
				// Crash/quarantine retry: use the original unit with run/skip lists.
				// Merge plan's skip list with the current unit's to accumulate
				// skips across attempts (so subtests that passed in earlier
				// attempts don't re-run).
				wu = workUnit{
					pkg:       unit.pkg,
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
			}, r.execLogger(desc, attempt))
		},
		label:     desc,
		attempt:   attempt,
		logPath:   logPath,
		junitPath: junitPath,
		logHeader: &logFileHeader{
			Package: unit.pkg,
			Attempt: attempt,
			Started: time.Now(),
			Command: binaryPath,
		},
		streamRetries: false, // one test per process
		retry:         retry,
	}
}

// createCompileItems creates compile queue items for each package.
func (r *runner) createCompileItems(pkgs []string, binDir string, baseArgs []string) []*queueItem {
	// Dedupe and sort packages
	pkgSet := make(map[string]bool, len(pkgs))
	for _, pkg := range pkgs {
		pkgSet[pkg] = true
	}
	sortedPkgs := slices.Sorted(maps.Keys(pkgSet))

	items := make([]*queueItem, 0, len(sortedPkgs))
	for _, pkg := range sortedPkgs {
		binName := strings.ReplaceAll(strings.TrimPrefix(pkg, "./"), "/", "_") + ".test"
		binaryPath := filepath.Join(binDir, binName)

		items = append(items, r.newCompileItem(pkg, binaryPath, baseArgs))
	}
	return items
}

func (r *runner) newCompileItem(pkg string, binaryPath string, baseArgs []string) *queueItem {
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

			// Discover tests from the compiled binary
			testNames, err := listTestsFromBinary(ctx, r.exec, binaryPath)
			if err != nil {
				compileErr = fmt.Errorf("failed to list tests from %s: %w", binaryPath, err)
				r.collector.addError(compileErr)
				return
			}

			// Apply run filter and sharding
			units := buildWorkUnits(pkg, testNames, r.runFilter, r.totalShards, r.shardIndex)

			if len(units) == 0 {
				r.log("no tests matched filter/shard for %s", pkg)
				return
			}

			r.log("discovered %d tests for %s", len(units), pkg)

			// Update progress tracker total
			r.progress.addTotal(int64(len(units)))

			// Log discovered tests
			var testList strings.Builder
			for _, u := range units {
				testList.WriteString("\n  ")
				testList.WriteString(u.label)
			}
			r.log("tests for %s:%s", pkg, testList.String())

			var items []*queueItem
			for _, unit := range units {
				items = append(items, r.newExecItem(r.compiledExecConfig(unit, binaryPath, 1)))
			}
			emit(items...)
		},
	}
}

// --- direct mode (go test without precompilation) ---

// runDirectMode runs all tests via `go test` without precompilation, using the
// scheduler for retry lifecycle. This is the GroupByNone path.
func (r *runner) runDirectMode(ctx context.Context, testDirs []string, baseArgs []string) error {
	r.log("running in 'none' mode - executing go test directly without precompilation")

	if err := os.MkdirAll(r.logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}
	r.log("log directory: %s", r.logDir)

	race, extraArgs := filterBaseArgs(baseArgs)
	pkgs := normalizePkgPaths(testDirs)
	r.log("test packages: %v", pkgs)

	items := []*queueItem{r.newExecItem(r.directExecConfig(pkgs, race, extraArgs, 1, r.runFilter, ""))}
	return r.runWithScheduler(ctx, max(2, r.parallelism), items, 1)
}

// normalizePkgPaths ensures each directory has a "./" prefix.
func normalizePkgPaths(dirs []string) []string {
	pkgs := make([]string, len(dirs))
	for i, dir := range dirs {
		if !strings.HasPrefix(dir, "./") && !strings.HasPrefix(dir, "/") {
			pkgs[i] = "./" + dir
		} else {
			pkgs[i] = dir
		}
	}
	return pkgs
}

// directExecConfig creates an execConfig for running go test directly.
func (r *runner) directExecConfig(pkgs []string, race bool, extraArgs []string, attempt int, runFilter, skipFilter string) execConfig {
	desc := "all"

	// When there's a run filter, this is a retry for specific tests. Multiple
	// retries at the same attempt number (from stream retries) need unique file
	// names to avoid overwriting each other's log and JUnit files.
	fileSuffix := ""
	if runFilter != "" || skipFilter != "" {
		fileSuffix = fmt.Sprintf("_%d", r.directRetrySeq.Add(1))
	}

	retry := r.buildRetryHandler(func(plan retryPlan, attempt int) *queueItem {
		runF := buildTestFilterPattern(plan.tests)
		skipF := buildTestFilterPattern(plan.skipTests)
		return r.newExecItem(r.directExecConfig(pkgs, race, extraArgs, attempt, runF, skipF))
	})

	timeout := r.effectiveTimeout()

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
			}, r.execLogger(desc, attempt))
		},
		label:         desc,
		attempt:       attempt,
		logPath:       filepath.Join(r.logDir, fmt.Sprintf("all_mode_attempt_%d%s.log", attempt, fileSuffix)),
		junitPath:     filepath.Join(r.logDir, fmt.Sprintf("junit_all_attempt_%d%s.xml", attempt, fileSuffix)),
		streamRetries: true,
		retry:         retry,
	}
}
