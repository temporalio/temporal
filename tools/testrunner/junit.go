package testrunner

import (
	"encoding/xml"
	"errors"
	"fmt"
	"iter"
	"log"
	"os"
	"slices"
	"strings"

	"github.com/jstemmer/go-junit-report/v2/junit"
)

// alertsSuiteName is the JUnit suite name used for structural alerts (data
// races, panics, fatal errors).
const alertsSuiteName = "ALERTS"

const junitAlertDetailsMaxBytes = 64 * 1024

type failureType string

const (
	// failureTypeFailed marks a failed assertion.
	failureTypeFailed   failureType = "Failed"
	failureTypeTimeout  failureType = "TIMEOUT"
	failureTypeCrash    failureType = "CRASH"
	failureTypeDataRace failureType = "DATA RACE"
	failureTypePanic    failureType = "PANIC"
	failureTypeFatal    failureType = "FATAL"
)

type junitReport struct {
	junit.Testsuites
	path          string
	reportingErrs []error
}

func (j *junitReport) read() error {
	f, err := os.Open(j.path)
	if err != nil {
		return fmt.Errorf("failed to open junit report file: %w", err)
	}
	defer f.Close()

	decoder := xml.NewDecoder(f)
	if err = decoder.Decode(&j.Testsuites); err != nil {
		return fmt.Errorf("failed to read junit report file: %w", err)
	}
	return nil
}

// generateReport builds a JUnit report for failures that the runner
// derives itself, such as timeouts and crashes. Failure.Type stores the
// canonical failure type (for example TIMEOUT or CRASH), and Failure.Data is
// intentionally left empty.
func generateReport(names []string, suffix string, kind failureType) *junitReport {
	var testcases []junit.Testcase
	for _, name := range names {
		testcases = append(testcases, junit.Testcase{
			Name:    fmt.Sprintf("%s (%s)", name, suffix),
			Failure: generateFailure(kind, ""),
		})
	}
	return &junitReport{
		Testsuites: junit.Testsuites{
			Suites: []junit.Testsuite{
				{
					Name:      "suite",
					Testcases: testcases,
				},
			},
		},
	}
}

func generateFailure(kind failureType, data string) *junit.Result {
	return &junit.Result{
		Message: string(kind),
		Type:    string(kind),
		Data:    data,
	}
}

func (j *junitReport) write() error {
	f, err := os.Create(j.path)
	if err != nil {
		return fmt.Errorf("failed to open junit report file: %w", err)
	}
	defer f.Close()

	encoder := xml.NewEncoder(f)
	encoder.Indent("", "    ")
	if err = encoder.Encode(j.Testsuites); err != nil {
		return fmt.Errorf("failed to write junit report file: %w", err)
	}
	log.Printf("wrote junit report to %s", j.path)
	return nil
}

// appendAlertsSuite adds a synthetic JUnit suite summarizing high-priority alerts
// (data races, panics, fatals) so that CI surfaces them prominently.
func (j *junitReport) appendAlertsSuite(alerts []alert) {
	// Deduplicate by type+details to avoid noisy repeats across retries.
	alerts = dedupeAlerts(alerts)
	if len(alerts) == 0 {
		return
	}

	// Convert alerts to JUnit test cases.
	var cases []junit.Testcase
	for _, a := range alerts {
		name := fmt.Sprintf("%s: %s", a.Type, a.Summary)
		if p := primaryTestName(a.Tests); p != "" {
			name = fmt.Sprintf("%s — in %s", name, p)
		}
		var sb strings.Builder
		if a.Details != "" {
			sb.WriteString(truncateAlertDetails(sanitizeXML(a.Details)))
			sb.WriteByte('\n')
		}
		if len(a.Tests) > 0 {
			fmt.Fprintf(&sb, "Detected in tests:\n\t%s", strings.Join(a.Tests, "\n\t"))
		}
		f := generateFailure(a.Type, strings.TrimRight(sb.String(), "\n"))
		cases = append(cases, junit.Testcase{
			Name:    name,
			Failure: f,
		})
	}

	// Append the alerts suite to the report.
	suite := junit.Testsuite{
		Name:      alertsSuiteName,
		Failures:  len(cases),
		Tests:     len(cases),
		Testcases: cases,
	}
	j.Suites = append(j.Suites, suite)
	j.Failures += suite.Failures
	j.Tests += suite.Tests
}

// sanitizeXML removes characters that are invalid in XML 1.0. Go's xml.Encoder
// escapes <, >, & etc., but control characters other than \t, \n, \r are not
// legal XML and cause parsers to reject the document.
func sanitizeXML(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\t' || r == '\n' || r == '\r' {
			return r
		}
		if r < 0x20 || r == 0xFFFE || r == 0xFFFF {
			return -1
		}
		return r
	}, s)
}

// truncateAlertDetails keeps alert payloads from bloating the JUnit artifact.
func truncateAlertDetails(s string) string {
	if len(s) <= junitAlertDetailsMaxBytes {
		return s
	}
	const marker = "\n... (truncated) ...\n"
	return s[:junitAlertDetailsMaxBytes-len(marker)] + marker
}

// dedupeAlerts removes duplicate alerts (e.g., repeated across retries) based
// on type and details while preserving the first-seen order.
func dedupeAlerts(alerts []alert) []alert {
	seen := make(map[string]struct{}, len(alerts))
	var out []alert
	for _, a := range alerts {
		key := string(a.Type) + "\n" + a.Details
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, a)
	}
	return out
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

func mergeReports(reports []*junitReport) (*junitReport, error) {
	if len(reports) == 0 {
		return nil, errors.New("no reports to merge")
	}

	var reportingErrs []error
	var combined junit.Testsuites
	combined.XMLName = reports[0].Testsuites.XMLName
	combined.Name = reports[0].Testsuites.Name

	for i, report := range reports {
		combined.Tests += report.Testsuites.Tests
		combined.Errors += report.Testsuites.Errors
		combined.Failures += report.Testsuites.Failures
		combined.Skipped += report.Testsuites.Skipped
		combined.Disabled += report.Testsuites.Disabled
		combined.Time += report.Testsuites.Time

		// If the report is for a retry ...
		var suffix string
		if i > 0 {
			suffix = fmt.Sprintf(" (retry %d)", i)
			if i == len(reports)-1 {
				suffix += " (final)"
			}
			prevFailures := reports[i-1].collectTestCaseFailures()
			currCases := report.collectTestCases()

			var missing []string
			for _, f := range prevFailures {
				if _, ok := currCases[f]; !ok {
					missing = append(missing, f)
				}
			}
			if len(missing) > 0 {
				reportingErrs = append(reportingErrs, fmt.Errorf(
					"expected rerun of all failures from previous attempt, missing: %v", missing))
			}
		}

		for _, suite := range report.Testsuites.Suites {
			if len(suite.Testcases) == 0 {
				continue
			}

			newSuite := suite // shallow copy
			newSuite.Name += suffix
			newSuite.Testcases = make([]junit.Testcase, 0, len(suite.Testcases))

			// Sort test cases by name.
			slices.SortFunc(suite.Testcases, func(a, b junit.Testcase) int {
				return strings.Compare(a.Name, b.Name)
			})

			// Collect test cases.
			for j := range len(suite.Testcases) {
				testCase := suite.Testcases[j]
				// Check if this is a parent test case (ie prefix of next subtest).
				// Use testCase.Name+"/" to avoid matching iteration suffixes like #01.
				if j != len(suite.Testcases)-1 && strings.HasPrefix(suite.Testcases[j+1].Name, testCase.Name+"/") {
					// Discard test case parents since they provide no value.
					continue
				}

				// Parse failure details from Failure.Data, if present.
				if testCase.Failure != nil && testCase.Failure.Data != "" {
					if details := parseFailureDetails(testCase.Failure.Data); details != noFailureDetails {
						testCase.Failure.Data = details
					}
				}

				// Failure.Type carries the canonical kind in merged JUnit.
				if testCase.Failure != nil {
					if suite.Name == alertsSuiteName {
						if testCase.Failure.Type == "" {
							testCase.Failure.Type = testCase.Failure.Message
						}
					} else {
						testCase.Failure.Type = string(failureTypeFailed)
					}
				}
				testCase.Name += suffix
				newSuite.Testcases = append(newSuite.Testcases, testCase)
			}
			combined.Suites = append(combined.Suites, newSuite)
		}
	}

	return &junitReport{
		Testsuites:    combined,
		reportingErrs: reportingErrs,
	}, nil
}

type node struct {
	children map[string]node
}

func (n node) visitor(path ...string) func(yield func(string, node) bool) {
	return func(yield func(string, node) bool) {
		for name, child := range n.children {
			path := append(path, name)
			if !yield(strings.Join(path, "/"), child) {
				return
			}
			child.visitor(path...)(yield)
		}
	}
}

func (n node) walk() iter.Seq2[string, node] {
	return n.visitor()
}
