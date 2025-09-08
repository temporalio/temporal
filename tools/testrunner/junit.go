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

type junitReport struct {
	path string
	junit.Testsuites
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
			Suites: []junit.Testsuite{
				{
					Name:      "suite",
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
	defer f.Close()

	encoder := xml.NewEncoder(f)
	encoder.Indent("", "    ")
	if err = encoder.Encode(j.Testsuites); err != nil {
		return fmt.Errorf("failed to write junit report file: %w", err)
	}
	log.Printf("wrote junit report to %s", j.path)
	return nil
}

func (j *junitReport) collectTestCases() map[string]struct{} {
	cases := make(map[string]struct{})
	for _, suite := range j.Testsuites.Suites {
		for _, tc := range suite.Testcases {
			cases[tc.Name] = struct{}{}
		}
	}
	return cases
}

func (j *junitReport) collectTestCaseFailures() []string {
	var failures []string
	for _, suite := range j.Testsuites.Suites {
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
				// Check if this is a parent test case (ie prefix of next test).
				if j != len(suite.Testcases)-1 && strings.HasPrefix(suite.Testcases[j+1].Name, testCase.Name) {
					// Discard test case parents since they provide no value.
					continue
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
