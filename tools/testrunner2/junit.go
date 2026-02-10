package testrunner2

import (
	"encoding/xml"
	"errors"
	"fmt"
	"iter"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/jstemmer/go-junit-report/v2/gtr"
	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/jstemmer/go-junit-report/v2/parser/gotest"
)

type junitReport struct {
	junit.Testsuites
	path          string
	attempt       int // 1-based attempt number (1 = first run, 2+ = retry)
	reportingErrs []error
}

func (j *junitReport) read() error {
	f, err := os.Open(j.path)
	if err != nil {
		return fmt.Errorf("failed to open junit report file: %w", err)
	}
	defer func() { _ = f.Close() }()

	decoder := xml.NewDecoder(f)
	if err = decoder.Decode(&j.Testsuites); err != nil {
		return fmt.Errorf("failed to read junit report file: %w", err)
	}
	return nil
}

func (j *junitReport) write() error {
	f, err := os.Create(j.path)
	if err != nil {
		return fmt.Errorf("failed to open junit report file: %w", err)
	}
	defer func() { _ = f.Close() }()

	encoder := xml.NewEncoder(f)
	encoder.Indent("", "    ")
	if err = encoder.Encode(j.Testsuites); err != nil {
		return fmt.Errorf("failed to write junit report file: %w", err)
	}
	return nil
}

// appendAlerts adds a synthetic JUnit suite for all alerts.
func (j *junitReport) appendAlerts(alerts alerts) {
	if len(alerts) == 0 {
		return
	}

	var cases []junit.Testcase
	for _, alert := range alerts.dedupe() {
		// Use the test name as the case name so it's properly tracked for retries.
		// splitTestName extracts a clean test name and package classname from
		// fully-qualified goroutine trace names.
		fullName := primaryTestName(alert.Tests)
		var classname, name string
		if fullName == "" {
			name = fmt.Sprintf("%s: %s", alert.Kind, alert.Summary)
		} else {
			classname, name = splitTestName(fullName)
		}
		// Include alert details in the failure message
		message := fmt.Sprintf("%s: %s", alert.Kind, alert.Summary)
		var details string
		if len(alert.Tests) > 0 {
			details = fmt.Sprintf("Detected in tests:\n\t%s", strings.Join(alert.Tests, "\n\t"))
		}
		r := &junit.Result{Message: message, Data: details}
		cases = append(cases, junit.Testcase{
			Classname: classname,
			Name:      name,
			Failure:   r,
		})
	}

	suite := junit.Testsuite{
		Name:      "ALERTS",
		Failures:  len(cases),
		Tests:     len(cases),
		Testcases: cases,
	}
	j.Suites = append(j.Suites, suite)
	j.Failures += suite.Failures
	j.Tests += suite.Tests
}

// testcases iterates over all test cases across all suites.
func (j *junitReport) testcases() iter.Seq[junit.Testcase] {
	return func(yield func(junit.Testcase) bool) {
		for _, suite := range j.Suites {
			for _, tc := range suite.Testcases {
				if !yield(tc) {
					return
				}
			}
		}
	}
}

func (j *junitReport) collectTestCases() map[string]struct{} {
	cases := make(map[string]struct{})
	for tc := range j.testcases() {
		cases[tc.Name] = struct{}{}
	}
	return cases
}

func (j *junitReport) collectTestCaseFailures() []string {
	var failures []string
	for tc := range j.testcases() {
		if tc.Failure != nil {
			failures = append(failures, tc.Name)
		}
	}
	slices.Sort(failures)
	return filterParentNames(failures)
}

func mergeReports(reports []*junitReport) (*junitReport, error) {
	if len(reports) == 0 {
		return nil, errors.New("no reports to merge")
	}

	var combined junit.Testsuites
	combined.XMLName = reports[0].XMLName
	combined.Name = reports[0].Name

	for _, report := range reports {
		combined.Tests += report.Tests
		combined.Errors += report.Errors
		combined.Failures += report.Failures
		combined.Skipped += report.Skipped
		combined.Disabled += report.Disabled
		combined.Time += report.Time

		// Add retry suffix only for actual retries (attempt > 1)
		var suffix string
		if report.attempt > 1 {
			suffix = fmt.Sprintf(" (retry %d)", report.attempt-1)
		}

		for _, suite := range report.Suites {
			if len(suite.Testcases) == 0 {
				continue
			}

			newSuite := suite // shallow copy

			// Strip logs from suite to reduce report size
			newSuite.SystemOut = nil
			newSuite.SystemErr = nil

			newSuite.Name += suffix
			newSuite.Testcases = make([]junit.Testcase, 0, len(suite.Testcases))

			// Sort test cases by name for parent filtering.
			slices.SortFunc(suite.Testcases, func(a, b junit.Testcase) int {
				return strings.Compare(a.Name, b.Name)
			})

			// Build set of leaf test names (filtering out parent tests).
			names := make([]string, len(suite.Testcases))
			for j, tc := range suite.Testcases {
				names[j] = tc.Name
			}
			leafSet := make(map[string]struct{}, len(names))
			for _, n := range filterParentNames(names) {
				leafSet[n] = struct{}{}
			}

			// Collect leaf test cases.
			for _, testCase := range suite.Testcases {
				if _, isLeaf := leafSet[testCase.Name]; !isLeaf {
					continue
				}

				// Strip logs from test case to reduce report size
				testCase.SystemOut = nil
				testCase.SystemErr = nil

				testCase.Name += suffix
				newSuite.Testcases = append(newSuite.Testcases, testCase)
			}
			combined.Suites = append(combined.Suites, newSuite)
		}
	}

	return &junitReport{
		Testsuites:    combined,
		reportingErrs: validateRetries(reports),
	}, nil
}

// validateRetries checks that all failures from attempt N were retried in attempt N+1.
func validateRetries(reports []*junitReport) []error {
	byAttempt := make(map[int][]*junitReport)
	for _, r := range reports {
		byAttempt[r.attempt] = append(byAttempt[r.attempt], r)
	}
	var attempts []int
	for a := range byAttempt {
		if a > 0 {
			attempts = append(attempts, a)
		}
	}
	slices.Sort(attempts)

	var errs []error
	for i := 0; i < len(attempts)-1; i++ {
		currAttempt, nextAttempt := attempts[i], attempts[i+1]
		prevFailures := make(map[string]bool)
		for _, r := range byAttempt[currAttempt] {
			for _, f := range r.collectTestCaseFailures() {
				prevFailures[f] = true
			}
		}
		nextCases := make(map[string]bool)
		for _, r := range byAttempt[nextAttempt] {
			for k := range r.collectTestCases() {
				nextCases[k] = true
			}
		}
		var missing []string
		for f := range prevFailures {
			if !nextCases[f] {
				missing = append(missing, f)
			}
		}
		if len(missing) > 0 {
			errs = append(errs, fmt.Errorf(
				"expected rerun of all failures from attempt %d, missing in attempt %d: %v",
				currAttempt, nextAttempt, missing))
		}
	}
	return errs
}

// testFailure holds information about a failed test.
type testFailure struct {
	Name       string
	ErrorTrace string // extracted error trace/details
}

// testResults holds failures and passes extracted from test output.
type testResults struct {
	failures []testFailure
	passes   []string
}

// newJUnitReport parses go test -v output from a log file and writes a JUnit XML report.
// Returns the test results including failures and passes.
func newJUnitReport(logPath string, junitPath string) testResults {
	data, err := os.ReadFile(logPath)
	if err != nil {
		log.Printf("[runner] warning: failed to open log file for JUnit report: %v", err)
		return testResults{}
	}
	output := stripLogHeader(string(data))

	// Use ExcludeParents to filter out parent test entries (e.g., TestSuite when
	// we only care about TestSuite/TestMethod). This works correctly with
	// -test.v=test2json which emits --- PASS/FAIL lines as subtests complete.
	parser := gotest.NewParser(gotest.SetSubtestMode(gotest.ExcludeParents))
	report, err := parser.Parse(strings.NewReader(output))
	if err != nil {
		log.Printf("[runner] warning: failed to parse test output for JUnit report: %v", err)
		return testResults{}
	}

	// Extract failed and passed tests from a clean parse (without test2json markers).
	results := extractResultsClean(output)

	// Convert to JUnit and write
	hostname, _ := os.Hostname()
	testsuites := junit.CreateFromReport(report, hostname)

	// Filter failure/error data to just error traces
	for i := range testsuites.Suites {
		for j := range testsuites.Suites[i].Testcases {
			tc := &testsuites.Suites[i].Testcases[j]
			if tc.Failure != nil && tc.Failure.Data != "" {
				tc.Failure.Data = extractErrorTrace(strings.Split(tc.Failure.Data, "\n"))
			}
			if tc.Error != nil && tc.Error.Data != "" {
				tc.Error.Data = extractErrorTrace(strings.Split(tc.Error.Data, "\n"))
			}
		}
	}

	jf, err := os.Create(junitPath)
	if err != nil {
		return results
	}
	defer func() { _ = jf.Close() }()

	if _, err := jf.WriteString(xml.Header); err != nil {
		return results
	}
	encoder := xml.NewEncoder(jf)
	encoder.Indent("", "    ")
	if err := encoder.Encode(testsuites); err != nil {
		return results
	}

	return results
}

// extractResults returns failed and passed tests from a gtr.Report.
// This extracts directly from the parsed report, which is more reliable than
// reading back the JUnit XML (which may mark tests as errors if they didn't complete).
func extractResults(report gtr.Report) testResults {
	var results testResults
	for _, pkg := range report.Packages {
		for _, test := range pkg.Tests {
			switch test.Result {
			case gtr.Fail:
				trace := extractErrorTrace(test.Output)
				if trace == "" {
					trace = lastLines(test.Output, 20)
				}
				results.failures = append(results.failures, testFailure{
					Name:       test.Name,
					ErrorTrace: trace,
				})
			case gtr.Pass:
				results.passes = append(results.passes, test.Name)
			default:
				// skip other results (e.g., skip, unknown)
			}
		}
	}
	return results
}

// extractResultsClean re-parses the output with test2json markers stripped so
// that test output is correctly attributed to individual tests (needed for
// extracting error traces for console display).
func extractResultsClean(output string) testResults {
	clean := strings.ReplaceAll(output, "\x16", "")
	parser := gotest.NewParser(gotest.SetSubtestMode(gotest.ExcludeParents))
	report, err := parser.Parse(strings.NewReader(clean))
	if err != nil {
		return testResults{}
	}
	return extractResults(report)
}

// stripLogHeader removes the TESTRUNNER LOG header from log file content.
func stripLogHeader(content string) string {
	headerEnd := logHeaderSeparator + "\n\n"
	if idx := strings.Index(content, headerEnd); idx >= 0 {
		// Find the last occurrence of the separator (the header has 3 of them)
		rest := content[idx+len(headerEnd):]
		return rest
	}
	return content
}

// parseAlertsFromFile reads a log file from disk and parses alerts from it.
func parseAlertsFromFile(logPath string) []alert {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil
	}
	output := stripLogHeader(string(data))
	return parseAlerts(output)
}

// lastLines returns up to n non-empty lines from the end of output.
func lastLines(output []string, n int) string {
	var lines []string
	for i := len(output) - 1; i >= 0 && len(lines) < n; i-- {
		if s := strings.TrimSpace(output[i]); s != "" {
			lines = append(lines, output[i])
		}
	}
	slices.Reverse(lines)
	return strings.Join(lines, "\n")
}

