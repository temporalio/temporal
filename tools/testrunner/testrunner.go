package testrunner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	codeCoverageExtension = ".cover.out"
	maxAttemptsFlag       = "--max-attempts="
	gotestsumPathFlag     = "--gotestsum-path="
	coverProfileFlag      = "-coverprofile="
	junitReportFlag       = "--junitfile="
	junitGlobFlag         = "--junit-glob="
	summaryOutputDirFlag  = "--summary-output-dir="
	crashReportNameFlag   = "--crashreportname="
	totalTimeoutFlag      = "--total-timeout="

	// fullRerunThreshold is the number of test failures above which we do a full
	// rerun instead of retrying only the failed tests.
	fullRerunThreshold = 20
)

const (
	testCommand        = "test"
	crashReportCommand = "report-crash"
	summaryCommand     = "generate-summary"
)

type attempt struct {
	runner           *runner
	number           int
	exitErr          *exec.ExitError
	junitReport      *junitReport
	coverProfilePath string
}

func (a *attempt) run(ctx context.Context, args []string) (string, error) {
	args = a.goTestArgs(args)
	log.Printf("starting test attempt #%d: %v %v",
		a.number, "go test", strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, "go", append([]string{"test"}, args...)...)
	output := newGoTestJSONOutput()
	var stderr strings.Builder
	cmd.Stdout = output
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	cmd.Stdin = os.Stdin

	err := cmd.Run()
	stdout := output.String() + stderr.String()
	reportErr, writeErr := a.finishReport(output)
	if reportErr != nil {
		err = reportErr
	} else if writeErr != nil && err == nil {
		err = writeErr
	}
	return stdout, err
}

func (a *attempt) goTestArgs(args []string) []string {
	args = slices.Clone(args)
	hasJSON := false
	for i, arg := range args {
		switch {
		case arg == "-json":
			hasJSON = true
		case strings.HasPrefix(arg, coverProfileFlag):
			// Each attempt writes a separate coverage profile for later merging.
			args[i] = coverProfileFlag + a.coverProfilePath
		}
	}
	if !hasJSON {
		args = append([]string{"-json"}, args...)
	}
	return slices.DeleteFunc(args, func(arg string) bool {
		// --junitfile is consumed by the runner; go test does not understand it.
		return strings.HasPrefix(arg, junitReportFlag)
	})
}

func (a *attempt) finishReport(output *goTestJSONOutput) (parseErr error, writeErr error) {
	reportPath := a.junitReport.path
	a.junitReport, parseErr = output.junitReport()
	a.junitReport.path = reportPath
	if parseErr != nil {
		return parseErr, nil
	}
	return nil, a.junitReport.write()
}

type runner struct {
	junitOutputPath  string
	coverProfilePath string
	attempts         []*attempt
	maxAttempts      int
	crashName        string
	junitGlob        string
	summaryOutputDir string
	alerts           []alert
	totalTimeout     time.Duration
}

func newRunner() *runner {
	return &runner{
		attempts:    make([]*attempt, 0),
		maxAttempts: 1,
	}
}

// nolint:revive,cognitive-complexity
func (r *runner) sanitizeAndParseArgs(command string, args []string) ([]string, error) {
	var sanitizedArgs []string
	for _, arg := range args {
		if arg == "--" || strings.HasPrefix(arg, gotestsumPathFlag) {
			continue
		}

		if strings.HasPrefix(arg, maxAttemptsFlag) {
			var err error
			r.maxAttempts, err = strconv.Atoi(strings.Split(arg, "=")[1])
			if err != nil {
				return nil, fmt.Errorf("invalid argument %q: %w", maxAttemptsFlag, err)
			}
			if r.maxAttempts == 0 {
				return nil, fmt.Errorf("invalid argument %q: must be greater than zero", maxAttemptsFlag)
			}
			continue // this is a `testrunner` only arg and not passed through
		}

		if strings.HasPrefix(arg, totalTimeoutFlag) {
			var err error
			r.totalTimeout, err = time.ParseDuration(strings.TrimPrefix(arg, totalTimeoutFlag))
			if err != nil {
				return nil, fmt.Errorf("invalid argument %q: %w", totalTimeoutFlag, err)
			}
			if r.totalTimeout == 0 {
				return nil, fmt.Errorf("invalid argument %q: must be greater than zero", totalTimeoutFlag)
			}
			continue
		}

		if strings.HasPrefix(arg, crashReportNameFlag) {
			r.crashName = strings.Split(arg, "=")[1]
			if r.crashName == "" {
				return nil, fmt.Errorf("invalid argument %q: must not be empty", crashReportNameFlag)
			}
			if command != crashReportCommand {
				return nil, fmt.Errorf("argument %q is only valid for command %q", crashReportNameFlag, crashReportCommand)
			}
			continue // this is a `testrunner` only arg and not passed through
		}

		if strings.HasPrefix(arg, junitGlobFlag) {
			r.junitGlob = strings.Split(arg, "=")[1]
			continue
		}
		if strings.HasPrefix(arg, summaryOutputDirFlag) {
			r.summaryOutputDir = strings.Split(arg, "=")[1]
			if command != summaryCommand {
				return nil, fmt.Errorf("argument %q is only valid for command %q", summaryOutputDirFlag, summaryCommand)
			}
			continue
		}
		if strings.HasPrefix(arg, coverProfileFlag) {
			r.coverProfilePath = strings.Split(arg, "=")[1]
		} else if strings.HasPrefix(arg, junitReportFlag) {
			r.junitOutputPath = strings.Split(arg, "=")[1]
		}

		sanitizedArgs = append(sanitizedArgs, arg)
	}

	switch command {
	case testCommand:
		if r.coverProfilePath == "" {
			return nil, fmt.Errorf("missing required argument %q", coverProfileFlag)
		}
		if r.junitOutputPath == "" {
			return nil, fmt.Errorf("missing required argument %q", junitReportFlag)
		}
	case crashReportCommand:
		if r.junitOutputPath == "" {
			return nil, fmt.Errorf("missing required argument %q", junitReportFlag)
		}
		if r.crashName == "" {
			return nil, fmt.Errorf("missing required argument %q", crashReportNameFlag)
		}
	case summaryCommand:
		if r.junitGlob == "" {
			return nil, fmt.Errorf("missing required argument %q", junitGlobFlag)
		}
		if r.summaryOutputDir == "" {
			return nil, fmt.Errorf("missing required argument %q", summaryOutputDirFlag)
		}
	default:
		return nil, fmt.Errorf("unknown command %q", command)
	}

	return sanitizedArgs, nil
}

func (r *runner) newAttempt() *attempt {
	a := &attempt{
		runner: r,
		number: len(r.attempts) + 1,
		coverProfilePath: fmt.Sprintf(
			"%v_%v%v",
			strings.TrimSuffix(r.coverProfilePath, codeCoverageExtension),
			len(r.attempts),
			codeCoverageExtension),
		junitReport: &junitReport{
			path: filepath.Join(os.TempDir(), fmt.Sprintf("temporalio-temporal-%s-junit.xml", uuid.NewString())),
		},
	}
	r.attempts = append(r.attempts, a)
	return a
}

func (r *runner) allReports() []*junitReport {
	var reports []*junitReport
	for _, a := range r.attempts {
		reports = append(reports, a.junitReport)
	}
	return reports
}

// Main is the entry point for the testrunner tool.
// nolint:revive,deep-exit
func Main() {
	log.SetPrefix("[testrunner] ")
	ctx := context.Background()

	if len(os.Args) < 2 {
		log.Fatalf("expected at least 2 arguments")
	}
	r := newRunner()

	command := os.Args[1]
	args, err := r.sanitizeAndParseArgs(command, os.Args[2:])
	if err != nil {
		log.Fatalf("failed to parse command line options: %v", err)
	}

	if r.totalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.totalTimeout)
		defer cancel()
	}

	switch command {
	case testCommand:
		r.runTests(ctx, args)
	case crashReportCommand:
		r.reportCrash()
	case summaryCommand:
		if err := r.generateSummary(); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown command %q", command)
	}
}

// nolint:revive,deep-exit
func (r *runner) reportCrash() {
	jr := generateReport([]string{r.crashName}, "crash", failureTypeCrash)
	jr.path = r.junitOutputPath
	if err := jr.write(); err != nil {
		log.Fatal(err)
	}
}

func (r *runner) generateSummary() error {
	paths, err := filepath.Glob(r.junitGlob)
	if err != nil {
		return fmt.Errorf("failed to expand junit glob %q: %w", r.junitGlob, err)
	}
	slices.Sort(paths)

	reports := make([]*junitReport, 0, len(paths))
	for _, path := range paths {
		report := &junitReport{path: path}
		if err := report.read(); err != nil {
			return fmt.Errorf("failed to read junit report %q: %w", path, err)
		}
		reports = append(reports, report)
	}

	summary := newSummaryFromReports(reports)
	if len(summary.Rows) == 0 {
		fmt.Println("no failed tests found in junit reports; skipping test summary")
		return nil
	}

	markdown := summary.Markdown()
	if err := os.MkdirAll(r.summaryOutputDir, 0o755); err != nil {
		return fmt.Errorf("failed to create summary output directory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(r.summaryOutputDir, "test-summary.md"), []byte(markdown), 0o644); err != nil {
		return fmt.Errorf("failed to write summary markdown: %w", err)
	}

	content, err := summary.JSON()
	if err != nil {
		return fmt.Errorf("failed to render summary json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(r.summaryOutputDir, "test-summary.json"), append(content, '\n'), 0o644); err != nil {
		return fmt.Errorf("failed to write summary json: %w", err)
	}
	return nil
}

// writeCurrentReport writes the merged report from all completed attempts to the
// final output path. It is called after each attempt so that partial results
// survive if the process is killed externally between attempts.
// Reporting errors (e.g. unexpected missing reruns) are intentionally ignored
// here; they are only checked for the final write at the end of runTests.
func (r *runner) writeCurrentReport() {
	reports := r.allReports()
	if len(reports) == 0 {
		return
	}
	merged, err := mergeReports(reports)
	if err != nil {
		log.Printf("warning: failed to merge reports for intermediate write: %v", err)
		return
	}
	if len(r.alerts) > 0 {
		merged.appendAlertsSuite(r.alerts)
	}
	merged.path = r.junitOutputPath
	if err := merged.write(); err != nil {
		log.Printf("warning: failed to write intermediate report: %v", err)
	}
}

// nolint:revive,deep-exit
func (r *runner) runTests(ctx context.Context, args []string) {
	var currentAttempt *attempt
	var totalTimeoutFired bool
	for a := 1; a <= r.maxAttempts; a++ {
		currentAttempt = r.newAttempt()

		// Run tests.
		stdout, err := currentAttempt.run(ctx, args)
		// Extract prominent alerts from this attempt's output.
		r.alerts = append(r.alerts, parseAlerts(stdout)...)

		// Check whether our total timeout fired (context deadline exceeded).
		// This happens when the go test binary hangs and never produces its own
		// "test timed out" panic. We collect whatever results are available from
		// completed attempts and from the partially-executed current attempt, then
		// flush the XML before the external kill arrives.
		if ctx.Err() != nil {
			log.Printf("total timeout reached, collecting partial results from %d completed attempt(s)", a-1)
			totalTimeoutFired = true
			// Without this, a mid-run timeout leaves an empty JUnit and CI shows green.
			currentAttempt.junitReport.appendSyntheticFailure(
				"testrunner.TotalTimeout",
				failureTypeTimeout,
				fmt.Sprintf("test-runner total timeout (%s) reached before all tests completed", r.totalTimeout),
			)
			break
		}

		if err != nil && !errors.As(err, &currentAttempt.exitErr) {
			log.Fatalf("test run failed with an unexpected error: %v", err)
		}

		stacktrace, timedoutTests := parseTestTimeouts(stdout)
		if len(timedoutTests) > 0 {
			// Run timed out and was aborted.
			// Update JUnit XML output for timed out tests since none will have been generated.
			currentAttempt.junitReport = generateReport(timedoutTests, "timed out", failureTypeTimeout)
			log.Print(stacktrace)

			// Don't retry.
			break
		}

		// Write intermediate results so they survive if we are killed externally
		// between attempts (e.g. a GitHub Actions job timeout fires after this
		// attempt but before the next one completes).
		r.writeCurrentReport()

		// If the run completely successfull, no need to retry.
		if currentAttempt.exitErr == nil {
			break
		}

		// Sanity check: make sure failures are reported when the run failed.
		failures := currentAttempt.junitReport.collectTestCaseFailures()
		if len(failures) == 0 {
			log.Fatalf("tests failed but no failures have been detected, not rerunning tests")
		}

		// Rerun all tests from previous attempt if there are too many failures in a single suite.
		if len(failures) > fullRerunThreshold && a < r.maxAttempts {
			log.Printf(
				"number of failures exceeds configured threshold (%d/%d) for narrowing down tests to retry, retrying with previous attempt's args",
				len(failures), fullRerunThreshold)
			continue
		}
		args = stripRunFromArgs(args)
		for i, failure := range failures {
			failures[i] = goTestNameToRunFlagRegexp(failure)
		}
		failureArg := strings.Join(failures, "|")
		// -args has special semantics in Go.
		argsIdx := slices.Index(args, "-args")
		if argsIdx == -1 {
			args = append(args, "-run", failureArg)
		} else {
			args = slices.Insert(args, argsIdx, "-run", failureArg)
		}
	}

	// Merge reports from all attempts and write the final JUnit report.
	mergedReport, err := mergeReports(r.allReports())
	if err != nil {
		log.Fatal(err)
	}
	// Append ALERTS suite to the merged JUnit if any were found.
	if len(r.alerts) > 0 {
		mergedReport.appendAlertsSuite(r.alerts)
	}
	mergedReport.path = r.junitOutputPath
	if err = mergedReport.write(); err != nil {
		log.Fatal(err)
	}

	// Skip the strict rerun-coverage check when the total timeout fired: the
	// in-progress attempt was killed before it could execute all expected tests.

	if len(mergedReport.reportingErrs) > 0 && ctx.Err() == nil {
		log.Fatal(mergedReport.reportingErrs)
	}

	// Exit with the exit code of the last attempt.
	if currentAttempt.exitErr != nil {
		log.Printf("exiting with failure after running %d attempt(s)", len(r.attempts))
		os.Exit(currentAttempt.exitErr.ExitCode())
	}
	// Without a non-zero exit, a total-timeout makes CI silently green.
	if totalTimeoutFired {
		log.Printf("exiting with failure: total timeout (%s) reached", r.totalTimeout)
		os.Exit(1)
	}
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
