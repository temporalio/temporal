// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package testrunner

import (
	"encoding/xml"
	"fmt"
	"iter"
	"log"
	"os"
	"strings"

	"github.com/jstemmer/go-junit-report/v2/junit"
)

type junitReport struct {
	path string
	junit.Testsuites
}

func (j *junitReport) read() {
	f, err := os.Open(j.path)
	if err != nil {
		log.Fatalf("failed to open junit report file: %v", err)
	}
	defer f.Close()

	decoder := xml.NewDecoder(f)
	if err = decoder.Decode(&j.Testsuites); err != nil {
		log.Fatalf("failed to read junit report file: %v", err)
	}
}

func (j *junitReport) generateForTimedoutTests(timedoutTests []string) {
	var testcases []junit.Testcase
	for _, name := range timedoutTests {
		testcases = append(testcases, junit.Testcase{
			Name:    name,
			Failure: &junit.Result{Message: "timeout"},
		})
	}
	j.Testsuites = junit.Testsuites{
		Suites: []junit.Testsuite{
			{
				Name:      "suite",
				Testcases: testcases,
			},
		},
	}
}

func (j *junitReport) write() {
	f, err := os.Create(j.path)
	if err != nil {
		log.Fatalf("failed to open junit report file: %v", err)
	}
	defer f.Close()

	encoder := xml.NewEncoder(f)
	encoder.Indent("", "    ")
	if err = encoder.Encode(j.Testsuites); err != nil {
		log.Fatalf("failed to write junit report file: %v", err)
	}
	log.Printf("wrote junit report to %s", j.path)
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

func mergeReports(reports []*junitReport) *junitReport {
	if len(reports) == 0 {
		log.Fatal("no reports to merge")
	}
	if len(reports) == 1 {
		return reports[0]
	}

	var combined junit.Testsuites
	combined.XMLName = reports[0].Testsuites.XMLName
	combined.Name = reports[0].Testsuites.Name
	combined.Suites = reports[0].Testsuites.Suites

	for i, report := range reports {
		combined.Tests += report.Testsuites.Tests
		combined.Errors += report.Testsuites.Errors
		combined.Failures += report.Testsuites.Failures
		combined.Skipped += report.Testsuites.Skipped
		combined.Disabled += report.Testsuites.Disabled
		combined.Time += report.Testsuites.Time

		// If the report is for a retry ...
		if i > 0 {
			// Run sanity check to make sure we rerun what we expect.
			casesTested := report.collectTestCases()
			casesMissing := make([]string, 0)
			previousReport := reports[i-1]
			for _, f := range previousReport.collectTestCaseFailures() {
				if _, ok := casesTested[f]; !ok {
					casesMissing = append(casesMissing, f)
				}
			}
			if len(casesMissing) > 0 {
				log.Fatalf("expected a rerun of all failures from the previous attempt, missing: %v", casesMissing)
			}

			// Append the test cases from the retry.
			for _, suite := range report.Testsuites.Suites {
				cpy := suite
				cpy.Name += fmt.Sprintf(" (retry %d)", i)
				cpy.Testcases = make([]junit.Testcase, 0, len(suite.Testcases))
				for _, test := range suite.Testcases {
					tcpy := test
					tcpy.Name += fmt.Sprintf(" (retry %d)", i)
					cpy.Testcases = append(cpy.Testcases, tcpy)
				}
				combined.Suites = append(combined.Suites, cpy)
			}
		}
	}

	return &junitReport{Testsuites: combined}
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
