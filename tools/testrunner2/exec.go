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

// compileTest compiles a test package into a binary.
func compileTest(ctx context.Context, execFn execFunc, req compileTestInput, onCommand func(string)) int {
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

	return execFn(ctx, "", "go", args, req.env, req.output)
}

// executeTestInput holds the input for executeTest.
type executeTestInput struct {
	binary       string
	pkgDir       string
	tests        []string
	skipPattern  string // -test.skip pattern to skip already-passed tests on retry
	coverProfile string
	extraArgs    []string // args to pass after -args (e.g., -persistenceType=xxx)
	env          []string
	output       io.Writer
}

// runDirectGoTestInput holds the input for runDirectGoTest.
type runDirectGoTestInput struct {
	pkgs         []string // package paths to test
	buildTags    string   // build tags
	race         bool     // enable race detector
	coverProfile string   // coverage profile path
	env          []string // extra environment variables
	output       io.Writer
	runFilter    string   // -run pattern (to target specific tests on retry)
	skipFilter   string   // -skip pattern (to skip passed tests on retry)
	extraArgs    []string // extra args to pass through to go test (e.g., -shuffle)
}

func runDirectGoTest(ctx context.Context, execFn execFunc, req runDirectGoTestInput, onCommand func(string)) int {
	args := []string{"test", "-v"}
	if req.race {
		args = append(args, "-race")
	}
	if req.buildTags != "" {
		args = append(args, "-tags="+req.buildTags)
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

	return execFn(ctx, "", "go", args, req.env, req.output)
}

func executeTest(ctx context.Context, execFn execFunc, req executeTestInput, onCommand func(string)) int {
	args := []string{
		"-test.v=test2json",
		"-test.run", buildTestFilterPattern(req.tests),
	}
	if req.skipPattern != "" {
		args = append(args, "-test.skip", req.skipPattern)
	}
	if req.coverProfile != "" {
		args = append(args, fmt.Sprintf("-test.coverprofile=%s", req.coverProfile))
	}
	args = append(args, req.extraArgs...)

	command := fmt.Sprintf("%s %s", req.binary, strings.Join(args, " "))

	if onCommand != nil {
		onCommand(command)
	}

	return execFn(ctx, req.pkgDir, req.binary, args, req.env, req.output)
}

// execLogger returns an onCommand callback that logs the command to the console.
func (r *runner) execLogger(desc, attempt string) func(string) {
	return func(command string) {
		r.console.WriteGrouped(
			fmt.Sprintf("%s%s 🚀 %s (attempt %s)", logPrefix, time.Now().Format("15:04:05"), desc, attempt),
			"$ "+command+"\n",
		)
	}
}

// --- compiled mode (compile + run per test) ---

func (r *runner) compiledExecConfig(unit workUnit, binaryPath string, attempt, retryOrdinal int) execConfig {
	coverProfile := fmt.Sprintf("%s_run_%d%s",
		strings.TrimSuffix(r.coverProfilePath, codeCoverageExtension),
		attempt,
		codeCoverageExtension)
	coverProfile, _ = filepath.Abs(coverProfile)

	junitFilename := fmt.Sprintf("junit_%s.xml", uuid.New().String())
	junitPath := filepath.Join(r.logDir, junitFilename)
	logPath, _ := filepath.Abs(buildLogFilename(r.logDir, unit.displayName, attempt))

	return execConfig{
		startProcess: func(ctx context.Context, output io.Writer) int {
			return executeTest(ctx, r.exec, executeTestInput{
				binary:       binaryPath,
				pkgDir:       unit.pkg,
				tests:        unit.runTests,
				skipPattern:  unit.skipPattern(),
				coverProfile: coverProfile,
				extraArgs:    r.testBinaryArgs,
				env:          append(r.env, fmt.Sprintf("TEMPORAL_TEST_ATTEMPT=%d", attempt)),
				output:       output,
			}, r.execLogger(unit.displayName, displayAttempt(attempt, retryOrdinal)))
		},
		unit:         unit,
		attempt:      attempt,
		retryOrdinal: retryOrdinal,
		logPath:      logPath,
		junitPath:    junitPath,
		logHeader: &logFileHeader{
			Package: unit.pkg,
			Attempt: displayAttempt(attempt, retryOrdinal),
			Started: time.Now(),
			Command: binaryPath,
		},
		retryMode:          retryCompiled,
		compiledBinaryPath: binaryPath,
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
	return &queueItem{
		run: func(ctx context.Context, emit func(...*queueItem)) {
			logPath := buildCompileLogFilename(r.logDir)
			lc, err := newLogCapture(logCaptureConfig{
				LogPath: logPath,
			})
			if err != nil {
				r.addError(fmt.Errorf("failed to create compile log file %s: %w", logPath, err))
				return
			}

			exitCode := compileTest(ctx, r.exec, compileTestInput{
				pkg:        pkg,
				binaryPath: binaryPath,
				buildTags:  r.buildTags,
				baseArgs:   baseArgs,
				env:        r.env,
				output:     lc,
			}, func(command string) {
				r.console.WriteGrouped(fmt.Sprintf("%s%s 🚀 compiling %s", logPrefix, time.Now().Format("15:04:05"), pkg), "$ "+command+"\n")
			})

			_ = lc.Close()
			outputBytes, readErr := os.ReadFile(logPath)
			outputStr := ""
			if readErr == nil {
				outputStr = string(outputBytes)
			}

			header := fmt.Sprintf("%s%s 🔨 compiled %s", logPrefix, time.Now().Format("15:04:05"), pkg)
			r.console.WriteGrouped(header, outputStr)

			if exitCode != 0 {
				r.addError(fmt.Errorf("failed to compile %s (exit code %d)", pkg, exitCode))
				return
			}

			testNames, err := listTestsFromBinary(ctx, r.exec, binaryPath)
			if err != nil {
				r.addError(fmt.Errorf("failed to list tests from %s: %w", binaryPath, err))
				return
			}

			units := buildWorkUnits(pkg, testNames, r.runFilter, r.totalShards, r.shardIndex)

			if len(units) == 0 {
				r.log("no tests matched filter/shard for %s", pkg)
				return
			}

			r.log("discovered %d tests for %s", len(units), pkg)

			r.tracker.addRoots(len(units))

			var testList strings.Builder
			for _, u := range units {
				testList.WriteString("\n  ")
				testList.WriteString(u.displayName)
			}
			r.log("tests for %s:%s", pkg, testList.String())

			var items []*queueItem
			for _, unit := range units {
				items = append(items, r.newExecItem(r.compiledExecConfig(unit, binaryPath, 1, 0)))
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

	items := []*queueItem{r.newExecItem(r.directExecConfig(pkgs, race, extraArgs, 1, 0, r.runFilter, ""))}
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

func (r *runner) directExecConfig(pkgs []string, race bool, extraArgs []string, attempt, retryOrdinal int, runFilter, skipFilter string) execConfig {
	fileSuffix := ""
	if retryOrdinal > 0 {
		fileSuffix = fmt.Sprintf("_%d", retryOrdinal)
	}

	unit := workUnit{rootName: "all", displayName: "all"}

	return execConfig{
		startProcess: func(ctx context.Context, output io.Writer) int {
			return runDirectGoTest(ctx, r.exec, runDirectGoTestInput{
				pkgs:         pkgs,
				buildTags:    r.buildTags,
				race:         race,
				coverProfile: r.coverProfilePath,
				env:          append(r.env, fmt.Sprintf("TEMPORAL_TEST_ATTEMPT=%d", attempt)),
				output:       output,
				runFilter:    runFilter,
				skipFilter:   skipFilter,
				extraArgs:    extraArgs,
			}, r.execLogger(unit.displayName, displayAttempt(attempt, retryOrdinal)))
		},
		unit:            unit,
		attempt:         attempt,
		retryOrdinal:    retryOrdinal,
		logPath:         filepath.Join(r.logDir, fmt.Sprintf("all_mode_attempt_%d%s.log", attempt, fileSuffix)),
		junitPath:       filepath.Join(r.logDir, fmt.Sprintf("junit_all_attempt_%d%s.xml", attempt, fileSuffix)),
		retryMode:       retryDirect,
		directPkgs:      pkgs,
		directRace:      race,
		directExtraArgs: extraArgs,
	}
}
