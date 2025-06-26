package testrunner

import (
	"encoding/xml"
	"errors"
	"fmt"
	"iter"
	"log"
	"os"
	"strings"

	"github.com/emirpasic/gods/trees/redblacktree"
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

func generateForTimedoutTests(timedoutTests []string) *junitReport {
	var testcases []junit.Testcase
	for _, name := range timedoutTests {
		testcases = append(testcases, junit.Testcase{
			Name:    name + " (timeout)",
			Failure: &junit.Result{Message: "Timeout"},
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
	root := node{children: make(map[string]node)}

	for _, suite := range j.Testsuites.Suites {
		if suite.Failures == 0 {
			continue
		}
		for _, tc := range suite.Testcases {
			if tc.Failure == nil {
				continue
			}
			n := root
			for _, part := range strings.Split(tc.Name, "/") {
				child, ok := n.children[part]
				if !ok {
					child = node{children: make(map[string]node)}
					n.children[part] = child
				}
				n = child
			}
		}
	}

	leafFailures := make([]string, 0)

	// Walk the tree and find all leaf failures. The way Go test failures are reported in junit is that there's a
	// test case per suite and per test in that suite. Filter out any nodes that have children to find the most
	// specific failures to rerun.
	for path, n := range root.walk() {
		if len(n.children) > 0 {
			continue
		}
		leafFailures = append(leafFailures, path)
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
	
	// Collect all test case names into the tree
	testNameTree := redblacktree.NewWithStringComparator()
	for _, report := range reports {
		for _, suite := range report.Testsuites.Suites {
			for _, tc := range suite.Testcases {
				testNameTree.Put(tc.Name, struct{}{})
			}
		}
	}

	for i, report := range reports {
		combined.Tests += report.Testsuites.Tests
		combined.Errors += report.Testsuites.Errors
		combined.Failures += report.Testsuites.Failures
		combined.Skipped += report.Testsuites.Skipped
		combined.Disabled += report.Testsuites.Disabled
		combined.Time += report.Testsuites.Time

		// If the report is for a retry ...
		if i > 0 {
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

		suffix := fmt.Sprintf(" (retry %d)", i)
		for _, suite := range report.Testsuites.Suites {
			newSuite := suite // shallow copy
			if i > 0 {
				newSuite.Name += suffix
			}
			newSuite.Testcases = make([]junit.Testcase, 0, len(suite.Testcases))

			for _, test := range suite.Testcases {
				if isLeaf(test.Name, testNameTree) {
					testCopy := test
					if i > 0 {
						testCopy.Name += suffix
					}
					newSuite.Testcases = append(newSuite.Testcases, testCopy)
				}
			}

			combined.Suites = append(combined.Suites, newSuite)
		}
	}

	return &junitReport{
		Testsuites:    combined,
		reportingErrs: reportingErrs,
	}, nil
}

func isLeaf(name string, tree *redblacktree.Tree) bool {
	prefix := name + "/"
	node, _ := tree.Ceiling(prefix)
	if node != nil && strings.HasPrefix(node.Key.(string), prefix) {
		return false
	}
	return true
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
