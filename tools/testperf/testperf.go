// Package testperf provides a CLI tool for running tests with performance analysis.
// It uses gotestsum to run tests, parses the JSON output to generate performance
// reports, and re-runs slow packages with execution tracing.
package testperf

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	defaultOutputDir = ".testoutput"
	defaultTopK      = 3
	defaultTopN      = 50
)

// Config holds the CLI configuration
type Config struct {
	Packages    string
	TopK        int
	TopN        int
	OutputDir   string
	GoTestArgs  []string
	SkipTracing bool
}

// TestEvent represents a single event from go test -json output
type TestEvent struct {
	Time    time.Time `json:"Time"`
	Action  string    `json:"Action"`
	Package string    `json:"Package"`
	Test    string    `json:"Test"`
	Elapsed float64   `json:"Elapsed"`
	Output  string    `json:"Output"`
}

// TestResult holds aggregated results for a single test
type TestResult struct {
	Package string
	Test    string
	Elapsed float64
	Passed  bool
}

// PackageResult holds aggregated results for a package
type PackageResult struct {
	Package   string
	Elapsed   float64
	Passed    bool
	TestCount int
	StartTime time.Time
	EndTime   time.Time
}

// ParallelismMetrics holds concurrency analysis data
type ParallelismMetrics struct {
	MaxConcurrent   int
	AvgConcurrency  float64
	SingleThreadPct float64
	TotalWallTime   time.Duration
}

// concurrencyPoint represents a change in test concurrency at a point in time
type concurrencyPoint struct {
	time  time.Time
	delta int // +1 for start, -1 for end
}

// Report holds all analysis results
type Report struct {
	TotalWallTime    time.Duration
	TotalTests       int
	PassedTests      int
	FailedTests      int
	SkippedTests     int
	SlowestPackages  []PackageResult
	SlowestTests     []TestResult
	Parallelism      ParallelismMetrics
	TraceFiles       []string
	TestsPassed      bool
}

func Main() {
	cfg := parseFlags()

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.Packages, "pkg", "./...", "Package pattern to test")
	flag.IntVar(&cfg.TopK, "topk", defaultTopK, "Number of slowest packages to trace")
	flag.IntVar(&cfg.TopN, "topn", defaultTopN, "Number of slowest tests/packages to show in report")
	flag.StringVar(&cfg.OutputDir, "output", defaultOutputDir, "Output directory for artifacts")
	flag.BoolVar(&cfg.SkipTracing, "skip-trace", false, "Skip tracing phase")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [-- go test args...]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Run tests with performance analysis and generate reports.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  go run ./cmd/tools/test-perf -- ./...\n")
		fmt.Fprintf(os.Stderr, "  go run ./cmd/tools/test-perf --pkg ./service/... --topk 5 --topn 100\n")
		fmt.Fprintf(os.Stderr, "  go run ./cmd/tools/test-perf --pkg ./... -- -race -timeout 30m\n")
	}

	// Find -- separator
	var args []string
	for i, arg := range os.Args[1:] {
		if arg == "--" {
			args = os.Args[1 : i+1]
			cfg.GoTestArgs = os.Args[i+2:]
			break
		}
	}
	if args == nil {
		args = os.Args[1:]
	}

	// Parse our flags from the args before --
	flagSet := flag.NewFlagSet("test-perf", flag.ExitOnError)
	flagSet.StringVar(&cfg.Packages, "pkg", "./...", "Package pattern to test")
	flagSet.IntVar(&cfg.TopK, "topk", defaultTopK, "Number of slowest packages to trace")
	flagSet.IntVar(&cfg.TopN, "topn", defaultTopN, "Number of slowest tests/packages to show in report")
	flagSet.StringVar(&cfg.OutputDir, "output", defaultOutputDir, "Output directory for artifacts")
	flagSet.BoolVar(&cfg.SkipTracing, "skip-trace", false, "Skip tracing phase")
	flagSet.Usage = flag.Usage

	if err := flagSet.Parse(args); err != nil {
		os.Exit(1)
	}

	// If positional args remain (after flags, before --), treat as package pattern
	if flagSet.NArg() > 0 {
		cfg.Packages = flagSet.Arg(0)
	}

	return cfg
}

func run(cfg Config) error {
	// Ensure output directory exists
	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	tracesDir := filepath.Join(cfg.OutputDir, "traces")
	if err := os.MkdirAll(tracesDir, 0755); err != nil {
		return fmt.Errorf("creating traces directory: %w", err)
	}

	// Check for gotestsum
	if _, err := exec.LookPath("gotestsum"); err != nil {
		return fmt.Errorf("gotestsum not found in PATH. Install with: go install gotest.tools/gotestsum@latest")
	}

	fmt.Println("=== Running tests with gotestsum ===")

	junitPath := filepath.Join(cfg.OutputDir, "junit.xml")
	jsonPath := filepath.Join(cfg.OutputDir, "test2json.json")

	// Build gotestsum command
	gotestsumArgs := []string{
		"--junitfile", junitPath,
		"--jsonfile", jsonPath,
		"--format", "standard-verbose",
		"--",
		cfg.Packages,
	}

	// Add -count=1 unless user specified their own
	hasCount := false
	for _, arg := range cfg.GoTestArgs {
		if strings.HasPrefix(arg, "-count") {
			hasCount = true
			break
		}
	}
	if !hasCount {
		gotestsumArgs = append(gotestsumArgs, "-count=1")
	}

	gotestsumArgs = append(gotestsumArgs, cfg.GoTestArgs...)

	cmd := exec.Command("gotestsum", gotestsumArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	startTime := time.Now()
	testErr := cmd.Run()
	wallTime := time.Since(startTime)

	testsPassed := testErr == nil

	// Parse results even if tests failed
	fmt.Println("\n=== Analyzing test results ===")

	report, err := analyzeResults(jsonPath, cfg.TopN, wallTime)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to analyze results: %v\n", err)
	} else {
		report.TestsPassed = testsPassed

		// Run traces for slowest packages
		if !cfg.SkipTracing && len(report.SlowestPackages) > 0 {
			fmt.Println("\n=== Generating traces for slowest packages ===")
			report.TraceFiles = runTraces(report.SlowestPackages, cfg.TopK, tracesDir, cfg.GoTestArgs)
		}

		// Generate and print report
		reportPath := filepath.Join(cfg.OutputDir, "report.md")
		if err := writeReport(report, reportPath); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to write report: %v\n", err)
		}

		printSummary(report, reportPath, cfg.OutputDir)
	}

	if testErr != nil {
		return fmt.Errorf("tests failed: %w", testErr)
	}

	return nil
}

func analyzeResults(jsonPath string, topN int, wallTime time.Duration) (*Report, error) {
	f, err := os.Open(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("opening json file: %w", err)
	}
	defer f.Close()

	var events []TestEvent
	decoder := json.NewDecoder(bufio.NewReader(f))
	for {
		var ev TestEvent
		if err := decoder.Decode(&ev); err == io.EOF {
			break
		} else if err != nil {
			// Skip malformed lines
			continue
		}
		events = append(events, ev)
	}

	if len(events) == 0 {
		return nil, fmt.Errorf("no test events found")
	}

	// Analyze events
	report := &Report{
		TotalWallTime: wallTime,
	}

	// Track tests and packages
	testResults := make(map[string]*TestResult) // key: pkg/test
	pkgResults := make(map[string]*PackageResult)

	// Track in-flight tests for parallelism analysis
	var concurrencyPoints []concurrencyPoint

	for _, ev := range events {
		// Package-level events (no Test field)
		if ev.Test == "" {
			pkg := ev.Package
			if pkg == "" {
				continue
			}

			pr, ok := pkgResults[pkg]
			if !ok {
				pr = &PackageResult{
					Package:   pkg,
					StartTime: ev.Time,
				}
				pkgResults[pkg] = pr
			}

			// Update end time
			if ev.Time.After(pr.EndTime) {
				pr.EndTime = ev.Time
			}

			switch ev.Action {
			case "pass":
				pr.Passed = true
				if ev.Elapsed > 0 {
					pr.Elapsed = ev.Elapsed
				}
			case "fail":
				pr.Passed = false
				if ev.Elapsed > 0 {
					pr.Elapsed = ev.Elapsed
				}
			}
			continue
		}

		// Test-level events
		key := ev.Package + "/" + ev.Test
		tr, ok := testResults[key]
		if !ok {
			tr = &TestResult{
				Package: ev.Package,
				Test:    ev.Test,
			}
			testResults[key] = tr
		}

		switch ev.Action {
		case "run":
			concurrencyPoints = append(concurrencyPoints, concurrencyPoint{ev.Time, 1})
		case "pass":
			tr.Passed = true
			tr.Elapsed = ev.Elapsed
			report.PassedTests++
			report.TotalTests++
			concurrencyPoints = append(concurrencyPoints, concurrencyPoint{ev.Time, -1})
		case "fail":
			tr.Passed = false
			tr.Elapsed = ev.Elapsed
			report.FailedTests++
			report.TotalTests++
			concurrencyPoints = append(concurrencyPoints, concurrencyPoint{ev.Time, -1})
		case "skip":
			report.SkippedTests++
			report.TotalTests++
			// No concurrency change for skip - wasn't really running
		}

		// Update package test count
		if pr, ok := pkgResults[ev.Package]; ok {
			if ev.Action == "pass" || ev.Action == "fail" || ev.Action == "skip" {
				pr.TestCount++
			}
		}
	}

	// Calculate parallelism metrics
	report.Parallelism = calculateParallelism(concurrencyPoints, wallTime)

	// Collect and sort packages by elapsed time
	var pkgList []PackageResult
	for _, pr := range pkgResults {
		// Calculate elapsed from timestamps if not set
		if pr.Elapsed == 0 && !pr.EndTime.IsZero() && !pr.StartTime.IsZero() {
			pr.Elapsed = pr.EndTime.Sub(pr.StartTime).Seconds()
		}
		pkgList = append(pkgList, *pr)
	}
	sort.Slice(pkgList, func(i, j int) bool {
		return pkgList[i].Elapsed > pkgList[j].Elapsed
	})
	if len(pkgList) > topN {
		pkgList = pkgList[:topN]
	}
	report.SlowestPackages = pkgList

	// Collect and sort tests by elapsed time
	var testList []TestResult
	for _, tr := range testResults {
		testList = append(testList, *tr)
	}
	sort.Slice(testList, func(i, j int) bool {
		return testList[i].Elapsed > testList[j].Elapsed
	})
	if len(testList) > topN {
		testList = testList[:topN]
	}
	report.SlowestTests = testList

	return report, nil
}

func calculateParallelism(points []concurrencyPoint, wallTime time.Duration) ParallelismMetrics {
	if len(points) == 0 {
		return ParallelismMetrics{TotalWallTime: wallTime}
	}

	// Sort by time
	sort.Slice(points, func(i, j int) bool {
		return points[i].time.Before(points[j].time)
	})

	metrics := ParallelismMetrics{
		TotalWallTime: wallTime,
	}

	current := 0
	var lastTime time.Time
	var totalWeightedConcurrency float64
	var singleThreadTime time.Duration
	var totalTime time.Duration

	for i, pt := range points {
		if i > 0 {
			duration := pt.time.Sub(lastTime)
			totalTime += duration
			totalWeightedConcurrency += float64(current) * float64(duration)
			if current == 1 {
				singleThreadTime += duration
			}
		}

		current += pt.delta
		if current > metrics.MaxConcurrent {
			metrics.MaxConcurrent = current
		}
		lastTime = pt.time
	}

	if totalTime > 0 {
		metrics.AvgConcurrency = totalWeightedConcurrency / float64(totalTime)
		metrics.SingleThreadPct = float64(singleThreadTime) / float64(totalTime) * 100
	}

	return metrics
}

func runTraces(packages []PackageResult, topK int, tracesDir string, extraArgs []string) []string {
	if topK > len(packages) {
		topK = len(packages)
	}

	var traceFiles []string

	for i := 0; i < topK; i++ {
		pkg := packages[i]

		// Sanitize package name for filename
		sanitized := strings.ReplaceAll(pkg.Package, "/", "_")
		sanitized = strings.ReplaceAll(sanitized, ".", "_")
		traceFile := filepath.Join(tracesDir, sanitized+".trace")

		fmt.Printf("Tracing package %s (%.2fs elapsed)...\n", pkg.Package, pkg.Elapsed)

		args := []string{"test", pkg.Package, "-count=1", "-trace", traceFile}
		args = append(args, extraArgs...)

		cmd := exec.Command("go", args...)
		cmd.Stdout = io.Discard
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: tracing %s failed: %v\n", pkg.Package, err)
			continue
		}

		traceFiles = append(traceFiles, traceFile)
	}

	return traceFiles
}

func writeReport(report *Report, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)

	fmt.Fprintf(w, "# Test Performance Report\n\n")
	fmt.Fprintf(w, "Generated: %s\n\n", time.Now().Format(time.RFC3339))

	// Summary
	fmt.Fprintf(w, "## Summary\n\n")
	fmt.Fprintf(w, "| Metric | Value |\n")
	fmt.Fprintf(w, "|--------|-------|\n")
	fmt.Fprintf(w, "| Total Wall Time | %s |\n", report.TotalWallTime.Round(time.Millisecond))
	fmt.Fprintf(w, "| Total Tests | %d |\n", report.TotalTests)
	fmt.Fprintf(w, "| Passed | %d |\n", report.PassedTests)
	fmt.Fprintf(w, "| Failed | %d |\n", report.FailedTests)
	fmt.Fprintf(w, "| Skipped | %d |\n", report.SkippedTests)
	status := "PASS"
	if !report.TestsPassed {
		status = "FAIL"
	}
	fmt.Fprintf(w, "| Status | %s |\n", status)
	fmt.Fprintf(w, "\n")

	// Parallelism
	fmt.Fprintf(w, "## Parallelism Metrics\n\n")
	fmt.Fprintf(w, "| Metric | Value |\n")
	fmt.Fprintf(w, "|--------|-------|\n")
	fmt.Fprintf(w, "| Max Concurrent Tests | %d |\n", report.Parallelism.MaxConcurrent)
	fmt.Fprintf(w, "| Avg Concurrency (time-weighted) | %.2f |\n", report.Parallelism.AvgConcurrency)
	fmt.Fprintf(w, "| Time at Concurrency=1 | %.1f%% |\n", report.Parallelism.SingleThreadPct)
	fmt.Fprintf(w, "\n")

	// Slowest packages
	fmt.Fprintf(w, "## Slowest Packages\n\n")
	fmt.Fprintf(w, "| # | Package | Elapsed | Tests | Status |\n")
	fmt.Fprintf(w, "|---|---------|---------|-------|--------|\n")
	for i, pkg := range report.SlowestPackages {
		status := "PASS"
		if !pkg.Passed {
			status = "FAIL"
		}
		fmt.Fprintf(w, "| %d | `%s` | %.2fs | %d | %s |\n",
			i+1, pkg.Package, pkg.Elapsed, pkg.TestCount, status)
	}
	fmt.Fprintf(w, "\n")

	// Slowest tests
	fmt.Fprintf(w, "## Slowest Tests\n\n")
	fmt.Fprintf(w, "| # | Package | Test | Elapsed | Status |\n")
	fmt.Fprintf(w, "|---|---------|------|---------|--------|\n")
	for i, t := range report.SlowestTests {
		status := "PASS"
		if !t.Passed {
			status = "FAIL"
		}
		fmt.Fprintf(w, "| %d | `%s` | `%s` | %.2fs | %s |\n",
			i+1, t.Package, t.Test, t.Elapsed, status)
	}
	fmt.Fprintf(w, "\n")

	// Trace files
	if len(report.TraceFiles) > 0 {
		fmt.Fprintf(w, "## Trace Files\n\n")
		fmt.Fprintf(w, "View traces with:\n\n")
		fmt.Fprintf(w, "```bash\n")
		for _, tf := range report.TraceFiles {
			fmt.Fprintf(w, "go tool trace %s\n", tf)
		}
		fmt.Fprintf(w, "```\n")
	}

	return w.Flush()
}

func printSummary(report *Report, reportPath, outputDir string) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("TEST PERFORMANCE SUMMARY")
	fmt.Println(strings.Repeat("=", 60))

	status := "PASS"
	if !report.TestsPassed {
		status = "FAIL"
	}

	fmt.Printf("\nStatus: %s\n", status)
	fmt.Printf("Wall Time: %s\n", report.TotalWallTime.Round(time.Millisecond))
	fmt.Printf("Tests: %d passed, %d failed, %d skipped (total: %d)\n",
		report.PassedTests, report.FailedTests, report.SkippedTests, report.TotalTests)

	fmt.Printf("\nParallelism:\n")
	fmt.Printf("  Max concurrent: %d\n", report.Parallelism.MaxConcurrent)
	fmt.Printf("  Avg concurrency: %.2f\n", report.Parallelism.AvgConcurrency)
	fmt.Printf("  Single-threaded: %.1f%%\n", report.Parallelism.SingleThreadPct)

	fmt.Printf("\nTop 5 Slowest Packages:\n")
	limit := 5
	if len(report.SlowestPackages) < limit {
		limit = len(report.SlowestPackages)
	}
	for i := 0; i < limit; i++ {
		pkg := report.SlowestPackages[i]
		fmt.Printf("  %d. %s (%.2fs)\n", i+1, pkg.Package, pkg.Elapsed)
	}

	fmt.Printf("\nTop 5 Slowest Tests:\n")
	limit = 5
	if len(report.SlowestTests) < limit {
		limit = len(report.SlowestTests)
	}
	for i := 0; i < limit; i++ {
		t := report.SlowestTests[i]
		fmt.Printf("  %d. %s/%s (%.2fs)\n", i+1, t.Package, t.Test, t.Elapsed)
	}

	fmt.Printf("\nArtifacts:\n")
	fmt.Printf("  Report: %s\n", reportPath)
	fmt.Printf("  JUnit XML: %s/junit.xml\n", outputDir)
	fmt.Printf("  Raw JSON: %s/test2json.json\n", outputDir)

	if len(report.TraceFiles) > 0 {
		fmt.Printf("\nTrace Files:\n")
		for _, tf := range report.TraceFiles {
			fmt.Printf("  go tool trace %s\n", tf)
		}
	}

	fmt.Println(strings.Repeat("=", 60))
}
