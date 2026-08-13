package testrunner

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/server/tools/common/junit"
)

const (
	codeCoverageExtension = ".cover.out"
	maxAttemptsFlag       = "--max-attempts="
	coverProfileFlag      = "-coverprofile="
	junitReportFlag       = "--junitfile="
	junitGlobFlag         = "--junit-glob="
	summaryOutputDirFlag  = "--summary-output-dir="
	crashReportNameFlag   = "--crashreportname="
	gotestsumPathFlag     = "--gotestsum-path="
	totalTimeoutFlag      = "--total-timeout="

	// targetedRetryThreshold is the number of test failures above which we
	// repeat the current scope instead of retrying only the failed tests.
	targetedRetryThreshold = 20
)

const (
	testCommand        = "test"
	crashReportCommand = "report-crash"
	summaryCommand     = "generate-summary"
)

type attempt struct {
	number           int
	coverProfilePath string
	result           attemptResult
}

type runner struct {
	junitOutputPath  string
	coverProfilePath string
	attempts         []*attempt
	maxAttempts      int
	crashName        string
	junitGlob        string
	summaryOutputDir string
	totalTimeout     time.Duration
	executeAttempt   func(context.Context, attemptSpec) attemptResult
}

func newRunner() *runner {
	return &runner{
		attempts:       make([]*attempt, 0),
		maxAttempts:    1,
		executeAttempt: runAttempt,
	}
}

// nolint:revive,cognitive-complexity
func (r *runner) sanitizeAndParseArgs(command string, args []string) ([]string, error) {
	var sanitizedArgs []string
	forwarded := false
	for _, arg := range args {
		if forwarded {
			if strings.HasPrefix(arg, coverProfileFlag) {
				r.coverProfilePath = strings.Split(arg, "=")[1]
			}
			sanitizedArgs = append(sanitizedArgs, arg)
			continue
		}
		// The `--` separator ends testrunner's own flags; everything after it goes to `go test`.
		if arg == "--" {
			forwarded = true
			continue
		}

		if strings.HasPrefix(arg, maxAttemptsFlag) {
			var err error
			r.maxAttempts, err = strconv.Atoi(strings.Split(arg, "=")[1])
			if err != nil {
				return nil, fmt.Errorf("invalid argument %q: %w", maxAttemptsFlag, err)
			}
			if r.maxAttempts < 1 {
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

		// TODO: Remove gotestsumPathFlag once downstream services no longer pass it.
		if strings.HasPrefix(arg, gotestsumPathFlag) {
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
			continue
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
		number: len(r.attempts) + 1,
		coverProfilePath: fmt.Sprintf(
			"%v_%v%v",
			strings.TrimSuffix(r.coverProfilePath, codeCoverageExtension),
			len(r.attempts),
			codeCoverageExtension),
	}
	r.attempts = append(r.attempts, a)
	return a
}

func (r *runner) attemptResults() []attemptResult {
	results := make([]attemptResult, 0, len(r.attempts))
	for _, attempt := range r.attempts {
		results = append(results, attempt.result)
	}
	return results
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
	report := renderCrashJUnit(r.crashName)
	if err := r.writeReport(&report); err != nil {
		log.Fatal(err)
	}
}

func (r *runner) generateSummary() error {
	paths, err := filepath.Glob(r.junitGlob)
	if err != nil {
		return fmt.Errorf("failed to expand junit glob %q: %w", r.junitGlob, err)
	}
	slices.Sort(paths)

	reports := make([]*junit.Testsuites, 0, len(paths))
	for _, path := range paths {
		report, err := junit.Read(path)
		if err != nil {
			return err
		}
		reports = append(reports, report)
	}

	summary := newSummaryFromReports(reports...)
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

func (r *runner) writeReport(report *junit.Testsuites) error {
	if err := junit.ValidateCounters(report); err != nil {
		return fmt.Errorf("invalid JUnit report: %w", err)
	}
	if err := junit.Write(r.junitOutputPath, report); err != nil {
		return err
	}
	log.Printf("wrote junit report to %s", r.junitOutputPath)
	return nil
}

// writeCurrentReport writes the report from all completed attempts to the
// final output path. It is called after each attempt so that partial results
// survive if the process is killed externally between attempts.
func (r *runner) writeCurrentReport() {
	results := r.attemptResults()
	if len(results) == 0 {
		return
	}
	report := renderJUnit(results)
	if err := r.writeReport(&report); err != nil {
		log.Printf("warning: failed to write intermediate report: %v", err)
	}
}

// nolint:revive,deep-exit
func (r *runner) runTests(ctx context.Context, args []string) {
	policy := retryPolicy{targetedThreshold: targetedRetryThreshold}
	currentArgs := slices.Clone(args)
	var pending *retryPlan
	var validationErr error
	var currentAttempt *attempt
	for a := 1; a <= r.maxAttempts; a++ {
		currentAttempt = r.newAttempt()

		// Run tests.
		result := r.executeAttempt(ctx, attemptSpec{
			number:           currentAttempt.number,
			args:             currentArgs,
			coverProfilePath: currentAttempt.coverProfilePath,
		})
		currentAttempt.result = result

		// Check whether our total timeout fired (context deadline exceeded).
		// This happens when the go test binary hangs and never produces its own
		// "test timed out" panic. We collect whatever results are available from
		// completed attempts and from the partially-executed current attempt, then
		// flush the XML before the external kill arrives.
		if result.process.state == processDeadlineExceeded {
			log.Printf("total timeout reached, collecting partial results from %d completed attempt(s)", a-1)
			currentAttempt.result.process.details = r.totalTimeoutDetails(result)
		}

		// Write intermediate results so they survive if we are killed externally
		// between attempts (e.g. a GitHub Actions job timeout fires after this
		// attempt but before the next one completes).
		r.writeCurrentReport()
		if pending != nil {
			if err := pending.validate(currentAttempt.result); err != nil {
				validationErr = err
				break
			}
		}

		for _, pkg := range currentAttempt.result.abortedPackages() {
			details := packageAbortDetails(currentAttempt.result, pkg)
			log.Printf("%s: %s", failureTypeAborted, packageAbortLogSummary(details))
		}

		plan := policy.plan(currentAttempt.result)
		if plan.mode == retryStop || a == r.maxAttempts {
			break
		}
		log.Print(plan.reason)
		currentArgs = plan.apply(currentArgs)
		pending = &plan
	}

	// Render results from all attempts and write the final JUnit report.
	report := renderJUnit(r.attemptResults())
	if err := r.writeReport(&report); err != nil {
		log.Fatal(err)
	}

	if validationErr != nil {
		log.Fatal(validationErr)
	}

	if !currentAttempt.result.successful() {
		log.Printf("exiting with failure after running %d attempt(s)", len(r.attempts))
		exitCode := currentAttempt.result.process.exitCode
		if exitCode <= 0 {
			exitCode = 1
		}
		os.Exit(exitCode)
	}
}

func (r *runner) totalTimeoutDetails(result attemptResult) string {
	var incomplete []string
	for _, pkg := range result.packages {
		for _, execution := range pkg.meaningfulIncompleteExecutions() {
			incomplete = append(incomplete, execution.id.packageName+"."+execution.id.testName)
		}
	}
	slices.Sort(incomplete)
	details := fmt.Sprintf("test-runner total timeout (%s) reached before all tests completed", r.totalTimeout)
	if len(incomplete) > 0 {
		details += "\n\nTests without final results:\n- " + strings.Join(incomplete, "\n- ")
	}
	return details
}
