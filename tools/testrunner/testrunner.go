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
	coverProfileFlag      = "-coverprofile="
	junitReportFlag       = "--junitfile="
	crashReportNameFlag   = "--crashreportname="
	gotestsumPathFlag     = "--gotestsum-path="

	// goTestTimeoutFlag is the go test flag whose value is also used as the
	// testrunner's total-run deadline (so results are flushed before an external
	// kill such as a GitHub Actions timeout).
	goTestTimeoutFlagEq = "-timeout="

	// fullRerunThreshold is the number of test failures above which we do a full
	// rerun instead of retrying only the failed tests.
	fullRerunThreshold = 20
)

const (
	testCommand        = "test"
	crashReportCommand = "report-crash"
)

type attempt struct {
	runner           *runner
	number           int
	exitErr          *exec.ExitError
	junitReport      *junitReport
	coverProfilePath string
}

func (a *attempt) run(ctx context.Context, args []string) (string, error) {
	for i, arg := range args {
		if strings.HasPrefix(arg, coverProfileFlag) {
			args[i] = coverProfileFlag + a.coverProfilePath
		} else if strings.HasPrefix(arg, junitReportFlag) {
			args[i] = junitReportFlag + a.junitReport.path
		}
	}
	log.Printf("starting test attempt #%d: %v %v",
		a.number, a.runner.gotestsumPath, strings.Join(args, " "))
	cmd := exec.CommandContext(ctx, a.runner.gotestsumPath, args...)
	var output strings.Builder
	cmd.Stdout = io.MultiWriter(os.Stdout, &output)
	cmd.Stderr = io.MultiWriter(os.Stderr, &output)
	cmd.Stdin = os.Stdin
	err := cmd.Run()
	return output.String(), err
}

type runner struct {
	gotestsumPath    string
	junitOutputPath  string
	coverProfilePath string
	attempts         []*attempt
	maxAttempts      int
	crashName        string
	alerts           []alert
	totalTimeout     time.Duration // derived from the -timeout go test flag
}

func newRunner() *runner {
	return &runner{
		attempts:    make([]*attempt, 0),
		maxAttempts: 1,
	}
}

// nolint:revive,cognitive-complexity
func (r *runner) sanitizeAndParseArgs(command string, args []string) ([]string, error) {
	// Pre-pass: read the go test -timeout value and use it as the testrunner's
	// total deadline so results are flushed before an external kill (e.g. GitHub
	// Actions timeout). The flag is NOT consumed — it still passes through to gotestsum.
	for _, arg := range args {
		if strings.HasPrefix(arg, goTestTimeoutFlagEq) {
			if d, err := time.ParseDuration(strings.TrimPrefix(arg, goTestTimeoutFlagEq)); err == nil {
				r.totalTimeout = d
			}
		}
	}

	var sanitizedArgs []string
	for _, arg := range args {
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

		if strings.HasPrefix(arg, gotestsumPathFlag) {
			r.gotestsumPath = strings.Split(arg, "=")[1]
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

		if strings.HasPrefix(arg, coverProfileFlag) {
			r.coverProfilePath = strings.Split(arg, "=")[1]
		} else if strings.HasPrefix(arg, junitReportFlag) {
			// --junitfile is used by gotestsum
			r.junitOutputPath = strings.Split(arg, "=")[1]
		}

		sanitizedArgs = append(sanitizedArgs, arg)
	}

	if r.junitOutputPath == "" {
		return nil, fmt.Errorf("missing required argument %q", junitReportFlag)
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

func (r *runner) runTests(ctx context.Context, args []string) {
	var currentAttempt *attempt
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
			// Try to read whatever gotestsum managed to write before it was killed.
			if readErr := currentAttempt.junitReport.read(); readErr != nil {
				// gotestsum didn't finish writing a JUnit XML. Fall back to parsing
				// stdout for any "--- FAIL:" lines that completed before the kill.
				if failedTests := parseFailedTestsFromOutput(stdout); len(failedTests) > 0 {
					currentAttempt.junitReport = generateStatic(failedTests, "total timeout", "Timeout")
				}
				// If no failed tests are found either, the current attempt's report
				// remains empty and mergeReports will include only prior attempts.
			}
			break
		}

		if err != nil && !errors.As(err, &currentAttempt.exitErr) {
			log.Fatalf("test run failed with an unexpected error: %v", err)
		}

		stacktrace, timedoutTests := parseTestTimeouts(stdout)
		if len(timedoutTests) > 0 {
			// Run timed out and was aborted.
			// Update JUnit XML output for timed out tests since none will have been generated.
			currentAttempt.junitReport = generateStatic(timedoutTests, "timed out", "Timeout")
			log.Print(stacktrace)

			// Don't retry.
			break
		}

		// All tests were run, parse JUnit XML output.
		if err = currentAttempt.junitReport.read(); err != nil {
			log.Fatal(err)
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
