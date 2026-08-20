package testrunner

import (
	"context"
	"errors"
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

type runner struct {
	junitOutputPath  string
	coverProfilePath string
	results          []attemptResult
	maxAttempts      int
	crashName        string
	junitGlob        string
	summaryOutputDir string
	totalTimeout     time.Duration
	executeAttempt   func(context.Context, attemptSpec) attemptResult
}

func newRunner() *runner {
	return &runner{
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
		exitCode, err := r.runTests(ctx, args)
		if err != nil {
			log.Printf("test run failed: %v", err)
		}
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	case crashReportCommand:
		if err := r.reportCrash(); err != nil {
			log.Fatal(err)
		}
	case summaryCommand:
		if err := r.generateSummary(); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown command %q", command)
	}
}

func (r *runner) reportCrash() error {
	report := renderCrashJUnit(r.crashName)
	return r.writeReport(&report)
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
func (r *runner) writeCurrentReport() error {
	if len(r.results) == 0 {
		return nil
	}
	report := renderJUnit(r.results)
	return r.writeReport(&report)
}

func (r *runner) runTests(ctx context.Context, args []string) (int, error) {
	policy := retryPolicy{targetedThreshold: targetedRetryThreshold}
	currentArgs := slices.Clone(args)
	var pending *retryPlan
	var validationErr error
	var writeErr error

	for attemptNumber := 1; attemptNumber <= r.maxAttempts; attemptNumber++ {
		// Run tests.
		result := r.executeAttempt(ctx, attemptSpec{
			number:           attemptNumber,
			args:             currentArgs,
			coverProfilePath: r.attemptCoveragePath(attemptNumber),
		})

		// Check whether our total timeout fired (context deadline exceeded).
		// This happens when the go test binary hangs and never produces its own
		// "test timed out" panic. We collect whatever results are available from
		// completed attempts and from the partially-executed current attempt, then
		// flush the XML before the external kill arrives.
		if result.process.state == processDeadlineExceeded {
			log.Printf("total timeout reached, collecting partial results from %d completed attempt(s)", attemptNumber-1)
			result.process.details = r.totalTimeoutDetails(result)
		}
		r.results = append(r.results, result)

		// Write intermediate results so they survive if we are killed externally
		// between attempts (e.g. a GitHub Actions job timeout fires after this
		// attempt but before the next one completes).
		if writeErr = r.writeCurrentReport(); writeErr != nil {
			log.Printf("warning: failed to write intermediate report: %v", writeErr)
		}

		if pending != nil {
			if err := pending.validate(result); err != nil {
				validationErr = err
				break
			}
		}

		for _, pkg := range result.abortedPackages() {
			details := packageAbortDetails(result, pkg)
			log.Printf("%s: %s", failureTypeAborted, packageAbortLogSummary(details))
		}

		plan := policy.plan(result)
		if plan.mode == retryStop || attemptNumber == r.maxAttempts {
			break
		}
		log.Print(plan.reason)
		currentArgs = plan.apply(currentArgs)
		pending = &plan
	}

	// Every completed attempt was persisted above. Promote the last write failure
	// to the caller instead of leaving only a warning from an intermediate write.
	if writeErr != nil {
		return 1, fmt.Errorf("failed to write JUnit report: %w", errors.Join(writeErr, validationErr))
	}
	if validationErr != nil {
		return 1, validationErr
	}
	last := r.results[len(r.results)-1]
	if last.successful() {
		return 0, nil
	}
	log.Printf("exiting with failure after running %d attempt(s)", len(r.results))
	if last.process.exitCode > 0 {
		return last.process.exitCode, nil
	}
	return 1, nil
}

func (r *runner) attemptCoveragePath(attemptNumber int) string {
	return fmt.Sprintf(
		"%v_%v%v",
		strings.TrimSuffix(r.coverProfilePath, codeCoverageExtension),
		attemptNumber-1,
		codeCoverageExtension,
	)
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
