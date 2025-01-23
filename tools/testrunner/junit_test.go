// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadJUnitReport(t *testing.T) {
	j := &junitReport{path: "testdata/junit-attempt-1.xml"}
	j.read()
	require.Len(t, j.Testsuites.Suites, 1)
	require.Equal(t, 2, j.Testsuites.Failures)
	require.Equal(t, []string{"TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument"}, j.failures())
}

func TestGenerateJUnitReportForTimedoutTests(t *testing.T) {
	out, err := os.CreateTemp("", "junit-report-*.xml")
	require.NoError(t, err)
	defer os.Remove(out.Name())

	j := &junitReport{path: out.Name()}
	j.generateForTimedoutTests([]string{
		"TestCallbacksSuite/TestWorkflowCallbacks_1",
		"TestCallbacksSuite/TestWorkflowCallbacks_2",
	})
	j.write()

	expectedReport, err := os.ReadFile("testdata/junit-timeout-output.xml")
	require.NoError(t, err)
	actualReport, err := os.ReadFile(out.Name())
	require.NoError(t, err)
	require.Equal(t, string(expectedReport), string(actualReport))
}

func TestNode(t *testing.T) {
	n := node{
		children: map[string]node{
			"a": {
				children: map[string]node{
					"b": {
						children: make(map[string]node),
					},
				},
			},
			"b": {
				children: make(map[string]node),
			},
		},
	}

	var paths []string
	for p := range n.walk() {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	require.Equal(t, []string{"a", "a/b", "b"}, paths)
}

func TestMergeReports(t *testing.T) {
	j1 := &junitReport{path: "testdata/junit-attempt-1.xml"}
	j1.read()
	j2 := &junitReport{path: "testdata/junit-attempt-2.xml"}
	j2.read()

	report := mergeReports([]*junitReport{j1, j2})

	suites := report.Testsuites.Suites
	require.Len(t, suites, 2)
	require.Equal(t, 4, report.Testsuites.Failures)
	require.Equal(t, "go.temporal.io/server/tests (retry 1)", suites[1].Name)
	require.Len(t, suites[1].Testcases, 2)
	require.Equal(t, "TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument (retry 1)", suites[1].Testcases[0].Name)
}
