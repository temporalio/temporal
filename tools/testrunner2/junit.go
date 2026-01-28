package testrunner2

import (
	"encoding/xml"
	"errors"
	"fmt"
	"iter"
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

func generateStatic(names []string, suffix string, message string) *junitReport {
	var testcases []junit.Testcase
	for _, name := range names {
		testcases = append(testcases, junit.Testcase{
			Name:    fmt.Sprintf("%s (%s)", name, suffix),
			Failure: &junit.Result{Message: message},
		})
	}
	return &junitReport{
		Testsuites: junit.Testsuites{
			Tests:    len(names),
			Failures: len(names),
			Suites: []junit.Testsuite{
				{
					Name:      "suite",
					Tests:     len(names),
					Failures:  len(names),
					Testcases: testcases,
				},
			},
		},
	}
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
		// Use the test name as the case name so it's properly tracked for retries
		name := primaryTestName(alert.Tests)
		if name == "" {
			name = fmt.Sprintf("%s: %s", alert.Kind, alert.Summary)
		}
		// Include alert details in the failure message
		message := fmt.Sprintf("%s: %s", alert.Kind, alert.Summary)
		var details string
		if len(alert.Tests) > 0 {
			details = fmt.Sprintf("Detected in tests:\n\t%s", strings.Join(alert.Tests, "\n\t"))
		}
		r := &junit.Result{Message: message, Data: details}
		cases = append(cases, junit.Testcase{
			Name:    name,
			Failure: r,
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

func (j *junitReport) collectTestCases() map[string]struct{} {
	cases := make(map[string]struct{})
	for _, suite := range j.Suites {
		for _, tc := range suite.Testcases {
			cases[tc.Name] = struct{}{}
		}
	}
	return cases
}

func (j *junitReport) collectTestCasePasses() []string {
	var passes []string
	for _, suite := range j.Suites {
		for _, tc := range suite.Testcases {
			// A test is considered passed if it has no failure AND no error.
			// Tests with errors (e.g., from timeout) should not be counted as passed.
			if tc.Failure == nil && tc.Error == nil {
				passes = append(passes, tc.Name)
			}
		}
	}
	return passes
}

func (j *junitReport) collectTestCaseFailures() []string {
	var failures []string
	for _, suite := range j.Suites {
		if suite.Failures == 0 {
			continue
		}
		for _, tc := range suite.Testcases {
			if tc.Failure != nil {
				failures = append(failures, tc.Name)
			}
		}
	}

	// Sort lexicographically
	slices.Sort(failures)

	// Find leaf failures using the simplified algorithm
	var leafFailures []string
	for i := 0; i < len(failures)-1; i++ {
		if !strings.HasPrefix(failures[i+1], failures[i]+"/") {
			leafFailures = append(leafFailures, failures[i])
		}
	}
	if len(failures) > 0 {
		leafFailures = append(leafFailures, failures[len(failures)-1])
	}

	return leafFailures
}

//nolint:revive // cognitive-complexity: merging reports requires nested loops
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

			// Collect test cases, filtering out parent tests.
			for j := range len(suite.Testcases) {
				testCase := suite.Testcases[j]
				// Check if this is a parent test case (ie prefix of next test).
				if j != len(suite.Testcases)-1 && strings.HasPrefix(suite.Testcases[j+1].Name, testCase.Name+"/") {
					// Discard test case parents since they provide no value.
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

	// Validate that all failures from attempt N were retried in attempt N+1
	var reportingErrs []error
	byAttempt := make(map[int][]*junitReport)
	for _, r := range reports {
		byAttempt[r.attempt] = append(byAttempt[r.attempt], r)
	}
	var attempts []int
	for a := range byAttempt {
		attempts = append(attempts, a)
	}
	slices.Sort(attempts)
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
			reportingErrs = append(reportingErrs, fmt.Errorf(
				"expected rerun of all failures from attempt %d, missing in attempt %d: %v",
				currAttempt, nextAttempt, missing))
		}
	}

	return &junitReport{
		Testsuites:    combined,
		reportingErrs: reportingErrs,
	}, nil
}

type testCaseNode struct {
	children map[string]testCaseNode
}

func (n testCaseNode) visitor(path ...string) func(yield func(string, testCaseNode) bool) {
	return func(yield func(string, testCaseNode) bool) {
		for name, child := range n.children {
			path := append(path, name)
			if !yield(strings.Join(path, "/"), child) {
				return
			}
			child.visitor(path...)(yield)
		}
	}
}

func (n testCaseNode) walk() iter.Seq2[string, testCaseNode] {
	return n.visitor()
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

// newJUnitReport parses go test -v output and writes a JUnit XML report.
// Returns the test results including failures and passes.
func newJUnitReport(output string, junitPath string) testResults {
	// Use ExcludeParents to filter out parent test entries (e.g., TestSuite when
	// we only care about TestSuite/TestMethod). This works correctly with
	// -test.v=test2json which emits --- PASS/FAIL lines as subtests complete.
	parser := gotest.NewParser(gotest.SetSubtestMode(gotest.ExcludeParents))
	report, err := parser.Parse(strings.NewReader(output))
	if err != nil {
		return testResults{}
	}

	// Extract failed and passed tests from the report
	results := extractResults(report)

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

	f, err := os.Create(junitPath)
	if err != nil {
		return results
	}
	defer func() { _ = f.Close() }()

	if _, err := f.WriteString(xml.Header); err != nil {
		return results
	}
	encoder := xml.NewEncoder(f)
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
				results.failures = append(results.failures, testFailure{
					Name:       test.Name,
					ErrorTrace: extractErrorTrace(test.Output),
				})
			case gtr.Pass:
				results.passes = append(results.passes, test.Name)
			}
		}
	}
	return results
}
