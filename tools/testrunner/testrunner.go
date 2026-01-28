package testrunner

import (
	"bufio"
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	codeCoverageExtension = ".cover.out"
	maxAttemptsFlag       = "--max-attempts="
	coverProfileFlag      = "-coverprofile="
	junitReportFlag       = "--junitfile="
	crashReportNameFlag   = "--crashreportname="
	gotestsumPathFlag     = "--gotestsum-path="
	runTimeoutFlag        = "--run-timeout="
	parallelismFlag       = "--parallelism="
	parallelFlag          = "--parallel"
	timeoutFlag           = "-timeout="
	tagsFlag              = "-tags="
)

const (
	testCommand        = "test"
	crashReportCommand = "report-crash"
)

// testFile represents a test file and its test functions.
// It also serves as a work item in the test queue.
type testFile struct {
	path      string
	pkg       string
	testNames []string // tests to run (all tests on first attempt, only failed tests on retry)
	attempt   int      // current attempt number (1-based)
}

// testFuncRegex matches test functions like "func TestFoo(t *testing.T)"
var testFuncRegex = regexp.MustCompile(`^func\s+(Test\w+)\s*\(\s*t\s+\*testing\.T\s*\)`)

type runConfig struct {
	gotestsumPath    string
	junitOutputPath  string
	coverProfilePath string
	buildTags        string
	crashName        string
	timeout          time.Duration // overall timeout for the test run
	runTimeout       time.Duration // timeout per test file
	maxAttempts      int
	parallelism      int
	filesPerWorker   int
	totalShards      int  // for CI sharding
	shardIndex       int  // for CI sharding
	parallel         bool // run tests in parallel (one process per test file)
}

type runner struct {
	runConfig
	alerts []alert
}

func newRunner() *runner {
	r := &runner{
		runConfig: runConfig{
			maxAttempts:    1,
			parallelism:    runtime.NumCPU(),
			filesPerWorker: 1,
			totalShards:    1,
			shardIndex:     0,
		},
	}

	// Read sharding config from environment (used by CI)
	if v := os.Getenv("TEST_RUNNER_SHARDS_TOTAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			r.totalShards = n
		}
	}
	if v := os.Getenv("TEST_RUNNER_SHARD_INDEX"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			r.shardIndex = n
		}
	}
	// Read parallelism config from environment
	if v := os.Getenv("TEST_RUNNER_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			r.parallelism = n
		}
	}
	if v := os.Getenv("TEST_RUNNER_FILES_PER_WORKER"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			r.filesPerWorker = n
		}
	}

	return r
}

// nolint:revive,cognitive-complexity
func (r *runner) sanitizeAndParseArgs(command string, args []string) ([]string, error) {
	var sanitizedArgs []string
	for _, arg := range args {
		if after, ok := strings.CutPrefix(arg, maxAttemptsFlag); ok {
			n, err := strconv.Atoi(after)
			if err != nil {
				return nil, fmt.Errorf("invalid argument %s: %w", maxAttemptsFlag, err)
			}
			if n == 0 {
				return nil, fmt.Errorf("invalid argument %s: must be greater than zero", maxAttemptsFlag)
			}
			r.maxAttempts = n
			log.Printf("max attempts set to %d", r.maxAttempts)
			continue // this is a `testrunner` only arg and not passed through
		}

		if after, ok := strings.CutPrefix(arg, gotestsumPathFlag); ok {
			r.gotestsumPath = after
			continue // this is a `testrunner` only arg and not passed through
		}

		if after, ok := strings.CutPrefix(arg, crashReportNameFlag); ok {
			if after == "" {
				return nil, fmt.Errorf("invalid argument %s: must not be empty", crashReportNameFlag)
			}
			if command != crashReportCommand {
				return nil, fmt.Errorf("argument %s is only valid for command %q", crashReportNameFlag, crashReportCommand)
			}
			r.crashName = after
			continue // this is a `testrunner` only arg and not passed through
		}

		if after, ok := strings.CutPrefix(arg, parallelismFlag); ok {
			n, err := strconv.Atoi(after)
			if err != nil {
				return nil, fmt.Errorf("invalid argument %s: %w", parallelismFlag, err)
			}
			if n <= 0 {
				return nil, fmt.Errorf("invalid argument %s: must be greater than zero", parallelismFlag)
			}
			r.parallelism = n
			log.Printf("parallelism set to %d", r.parallelism)
			continue // this is a `testrunner` only arg and not passed through
		}

		if after, ok := strings.CutPrefix(arg, runTimeoutFlag); ok {
			d, err := time.ParseDuration(after)
			if err != nil {
				return nil, fmt.Errorf("invalid argument %s: %w", runTimeoutFlag, err)
			}
			r.runTimeout = d
			log.Printf("run timeout set to %v", r.runTimeout)
			continue // this is a `testrunner` only arg and not passed through
		}

		if arg == parallelFlag {
			r.parallel = true
			log.Printf("parallel mode enabled")
			continue // this is a `testrunner` only arg and not passed through
		}

		if after, ok := strings.CutPrefix(arg, timeoutFlag); ok {
			d, err := time.ParseDuration(after)
			if err != nil {
				return nil, fmt.Errorf("invalid argument %s: %w", timeoutFlag, err)
			}
			r.timeout = d
			log.Printf("total timeout set to %v", r.timeout)
			// let it pass through to `go test`
		}

		if after, ok := strings.CutPrefix(arg, tagsFlag); ok {
			r.buildTags = after
			continue // testrunner manages this flag explicitly
		}

		if after, ok := strings.CutPrefix(arg, coverProfileFlag); ok {
			r.coverProfilePath = after
		} else if after, ok := strings.CutPrefix(arg, junitReportFlag); ok {
			r.junitOutputPath = after
		}

		sanitizedArgs = append(sanitizedArgs, arg)
	}

	if r.junitOutputPath == "" {
		return nil, fmt.Errorf("missing required argument %q", junitReportFlag)
	}

	// When --parallel is not set, run all files together in a single batch
	if !r.parallel {
		r.parallelism = 1
		r.filesPerWorker = math.MaxInt
	}

	switch command {
	case testCommand:
		if r.coverProfilePath == "" {
			return nil, fmt.Errorf("missing required argument %q", coverProfileFlag)
		}
		if r.junitOutputPath == "" {
			return nil, fmt.Errorf("missing required argument %q", junitReportFlag)
		}
		if r.gotestsumPath == "" {
			return nil, fmt.Errorf("missing required argument %q", gotestsumPathFlag)
		}
	case crashReportCommand:
		if r.crashName == "" {
			return nil, fmt.Errorf("missing required argument %q", crashReportNameFlag)
		}
	default:
		return nil, fmt.Errorf("unknown command %q", command)
	}

	return sanitizedArgs, nil
}

// Main is the entry point for the testrunner tool.
// nolint:revive,deep-exit
func Main() {
	log.SetPrefix("[runner] ")
	log.SetFlags(log.Ltime)
	ctx := context.Background()

	if len(os.Args) < 2 {
		log.Fatalf("expected at least 2 arguments")
	}

	command := os.Args[1]

	r := newRunner()
	args, err := r.sanitizeAndParseArgs(command, os.Args[2:])
	if err != nil {
		log.Fatalf("failed to parse command line options: %v", err)
	}

	switch command {
	case testCommand:
		r.runTests(ctx, args)
	case crashReportCommand:
		r.reportCrash()
	default:
		log.Fatalf("unknown command %q", command)
	}
}

// nolint:revive,deep-exit
func (r *runner) reportCrash() {
	jr := generateStatic([]string{r.crashName}, "crash", "Crash")
	jr.path = r.junitOutputPath
	if err := jr.write(); err != nil {
		log.Fatal(err)
	}
}

// nolint:revive,cognitive-complexity,deep-exit
func (r *runner) runTests(ctx context.Context, args []string) {
	// Apply overall timeout if set
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	// Parse args to extract test directories and base args
	testDirs, baseArgs := r.parseTestArgs(args)
	if len(testDirs) == 0 {
		log.Fatalf("no test directories specified")
	}

	// Find all test files
	var testFiles []testFile
	for _, dir := range testDirs {
		files, err := findTestFiles(dir)
		if err != nil {
			log.Fatalf("failed to find test files in %s: %v", dir, err)
		}
		testFiles = append(testFiles, files...)
	}

	if len(testFiles) == 0 {
		log.Fatalf("no test files found in directories: %v", testDirs)
	}

	// Filter by shard if sharding is enabled
	if r.totalShards > 1 {
		testFiles = r.filterByShard(testFiles)
		log.Printf("shard %d/%d: running %d test files", r.shardIndex+1, r.totalShards, len(testFiles))
	}

	// Shuffle test files to distribute load and catch ordering-dependent bugs
	rand.Shuffle(len(testFiles), func(i, j int) {
		testFiles[i], testFiles[j] = testFiles[j], testFiles[i]
	})

	// Print test files
	var sb strings.Builder
	for _, tf := range testFiles {
		sb.WriteString("\n  ")
		sb.WriteString(tf.path)
	}
	log.Printf("test files:%s", sb.String())

	// Compile tests first (only in parallel mode - sequential mode compiles on demand)
	if r.parallel {
		if err := r.compileTests(ctx, testFiles, baseArgs); err != nil {
			log.Fatalf("failed to compile tests: %v", err)
		}
	}

	// Run tests
	result := newWorkerPool(ctx, r.runConfig, testFiles, baseArgs).run()

	// Finalize report
	r.alerts = result.alerts
	r.finalizeReport(result.junitReports)

	// Print summary
	if result.failed > 0 {
		log.Printf("test run completed: %d/%d passed",
			result.total-result.failed, result.total)
		os.Exit(1)
	}
	log.Printf("test run completed: %d/%d passed", result.total, result.total)
}

// finalizeReport merges junit reports, appends alerts, and writes the final report.
// nolint:revive,deep-exit
func (r *runner) finalizeReport(junitReports []*junitReport) {
	mergedReport, err := mergeReports(junitReports)
	if err != nil {
		log.Fatal(err)
	}
	if len(r.alerts) > 0 {
		mergedReport.appendAlertsSuite(r.alerts)
	}
	mergedReport.path = r.junitOutputPath
	if err = mergedReport.write(); err != nil {
		log.Fatal(err)
	}
}

// parseTestArgs separates test directories from other args.
func (r *runner) parseTestArgs(args []string) (testDirs []string, baseArgs []string) {
	var inTestArgs bool
	for _, arg := range args {
		if arg == "-args" {
			inTestArgs = true
			baseArgs = append(baseArgs, arg)
			continue
		}
		if inTestArgs {
			baseArgs = append(baseArgs, arg)
			continue
		}
		// Test directories start with ./ or /
		if strings.HasPrefix(arg, "./") || strings.HasPrefix(arg, "/") {
			testDirs = append(testDirs, arg)
		} else {
			baseArgs = append(baseArgs, arg)
		}
	}
	return
}

// filterByShard filters test files to only include those belonging to the current shard.
func (r *runner) filterByShard(files []testFile) []testFile {
	var filtered []testFile
	for _, f := range files {
		if getShardForFile(f.path, r.totalShards) == r.shardIndex {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

// compileTests compiles all test packages upfront.
func (r *runner) compileTests(ctx context.Context, testFiles []testFile, baseArgs []string) error {
	// Get unique packages to compile
	pkgSet := make(map[string]bool)
	for _, tf := range testFiles {
		pkgSet[tf.pkg] = true
	}

	// Extract compile-relevant flags from baseArgs (e.g., -race, -tags, -cover)
	var compileFlags []string
	hasCover := false
	for _, arg := range baseArgs {
		if arg == "-race" || strings.HasPrefix(arg, "-tags=") {
			compileFlags = append(compileFlags, arg)
		} else if arg == "-cover" || strings.HasPrefix(arg, "-coverprofile=") {
			hasCover = true
		}
	}
	// Coverage instrumentation is done at compile time
	if hasCover {
		compileFlags = append(compileFlags, "-cover")
	}

	// Compile each package
	for pkg := range pkgSet {
		args := []string{"test", "-c", "-o", "/dev/null"}
		args = append(args, compileFlags...)
		if r.buildTags != "" {
			args = append(args, "-tags="+r.buildTags)
		}
		args = append(args, pkg)
		log.Printf("🔨 go %s", strings.Join(args, " "))

		cmd := exec.CommandContext(ctx, "go", args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to compile %s: %w", pkg, err)
		}
	}
	return nil
}

// findTestFiles finds all test files in a directory (non-recursive) and extracts their test function names.
func findTestFiles(dir string) ([]testFile, error) {
	var files []testFile

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}

		path := filepath.Join(dir, entry.Name())

		// Extract test function names from the file
		testNames, err := extractTestFuncNames(path)
		if err != nil {
			log.Printf("warning: failed to extract test names from %s: %v", path, err)
			continue
		}
		if len(testNames) == 0 {
			// No test functions found in this file, skip it
			continue
		}

		// Get the package path
		pkg := dir
		if !strings.HasPrefix(pkg, "./") && !strings.HasPrefix(pkg, "/") {
			pkg = "./" + pkg
		}

		files = append(files, testFile{
			path:      path,
			pkg:       pkg,
			testNames: testNames,
		})
	}
	return files, nil
}

// extractTestFuncNames extracts all test function names from a test file.
func extractTestFuncNames(path string) (names []string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		matches := testFuncRegex.FindStringSubmatch(line)
		if len(matches) >= 2 {
			names = append(names, matches[1])
		}
	}
	return names, scanner.Err()
}

// getShardForFile returns the shard index for a given file path using consistent hashing.
func getShardForFile(path string, totalShards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(path))
	return int(h.Sum32() % uint32(totalShards))
}

func stripRunFromArgs(args []string) (argsNoRun []string) {
	var skipNext bool
	for _, arg := range args {
		if skipNext {
			skipNext = false
			continue
		} else if arg == "-run" {
			skipNext = true
			continue
		} else if strings.HasPrefix(arg, "-run=") {
			continue
		}
		argsNoRun = append(argsNoRun, arg)
	}
	return
}
