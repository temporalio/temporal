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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestRunnerSanitizeAndParseArgs(t *testing.T) {
	t.Run("Passthrough", func(t *testing.T) {
		r := newRunner("gotestsum")
		args, err := r.sanitizeAndParseArgs([]string{"--junitfile", "f", "-foo", "bar"})
		require.NoError(t, err)
		require.Equal(t, []string{"-foo", "bar"}, args)
		require.Equal(t, "f", r.junitOutputPath)
	})
	t.Run("RetriesTwoArgs", func(t *testing.T) {
		r := newRunner("gotestsum")
		args, err := r.sanitizeAndParseArgs([]string{"--junitfile", "f", "-foo", "bar", "-retries", "3"})
		require.NoError(t, err)
		require.Equal(t, []string{"-foo", "bar"}, args)
		require.Equal(t, "f", r.junitOutputPath)
		require.Equal(t, 3, r.retries)
	})
	t.Run("RetriesOneArg", func(t *testing.T) {
		r := newRunner("gotestsum")
		args, err := r.sanitizeAndParseArgs([]string{"--junitfile", "f", "-foo", "bar", "-retries=3"})
		require.NoError(t, err)
		require.Equal(t, []string{"-foo", "bar"}, args)
		require.Equal(t, "f", r.junitOutputPath)
		require.Equal(t, 3, r.retries)
	})
	t.Run("RetriesTwoArgsInvalid", func(t *testing.T) {
		r := newRunner("gotestsum")
		_, err := r.sanitizeAndParseArgs([]string{"-foo", "bar", "-retries", "invalid"})
		require.ErrorContains(t, err, `strconv.Atoi: parsing "invalid"`)
	})
	t.Run("RetriesIncomplete", func(t *testing.T) {
		r := newRunner("gotestsum")
		_, err := r.sanitizeAndParseArgs([]string{"-foo", "bar", "-retries"})
		require.ErrorContains(t, err, "incomplete command line arguments: got -retries flag with no value")
	})
	t.Run("RetriesOneArgInvalid", func(t *testing.T) {
		r := newRunner("gotestsum")
		_, err := r.sanitizeAndParseArgs([]string{"-foo", "bar", "-retries=invalid"})
		require.ErrorContains(t, err, `strconv.Atoi: parsing "invalid"`)
	})
	t.Run("JuintfileSingleArg", func(t *testing.T) {
		r := newRunner("gotestsum")
		args, err := r.sanitizeAndParseArgs([]string{"-foo", "bar", "--junitfile=foo"})
		require.NoError(t, err)
		require.Equal(t, []string{"-foo", "bar"}, args)
		require.Equal(t, "foo", r.junitOutputPath)
	})
	t.Run("JunitfileIncomplete", func(t *testing.T) {
		r := newRunner("gotestsum")
		_, err := r.sanitizeAndParseArgs([]string{"-foo", "bar", "--junitfile"})
		require.ErrorContains(t, err, "incomplete command line arguments: got --junitfile flag with no value")
	})
	t.Run("DoubleDash", func(t *testing.T) {
		r := newRunner("gotestsum")
		args, err := r.sanitizeAndParseArgs([]string{"-foo", "bar", "--junitfile", "foo", "--", "-retries=3"})
		require.NoError(t, err)
		require.Equal(t, []string{"-foo", "bar", "--", "-retries=3"}, args)
		require.Equal(t, 0, r.retries)
	})
}

func TestAttemptRecordResultAndFailures(t *testing.T) {
	a := &attempt{
		junitXmlPath: "testdata/junit-attempt-1.xml",
	}
	err := a.recordResult(nil)
	require.NoError(t, err)
	require.Len(t, a.suites.Suites, 1)
	require.Equal(t, 2, a.suites.Failures)
	require.Equal(t, []string{"TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument"}, a.failures())
}

func TestCombineAttempts(t *testing.T) {
	r := newRunner("gotestsum")
	a1 := r.newAttempt()
	require.Equal(t, 1, a1.number)
	a1.junitXmlPath = "testdata/junit-attempt-1.xml"
	require.NoError(t, a1.recordResult(nil))
	a2 := r.newAttempt()
	require.Equal(t, 2, a2.number)
	a2.junitXmlPath = "testdata/junit-attempt-2.xml"
	require.NoError(t, a2.recordResult(nil))
	report := r.combineAttempts()

	require.Len(t, report.Suites, 2)
	require.Equal(t, 4, report.Failures)
	require.Equal(t, "go.temporal.io/server/tests (retry 1)", report.Suites[1].Name)
	require.Len(t, report.Suites[1].Testcases, 2)
	require.Equal(t, "TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument (retry 1)", report.Suites[1].Testcases[0].Name)
}

func TestStripRunFromArgs(t *testing.T) {
	t.Run("OneArg", func(t *testing.T) {
		args := stripRunFromArgs([]string{"-foo", "bar", "-run=A"})
		require.Equal(t, []string{"-foo", "bar"}, args)
	})

	t.Run("TwoArgs", func(t *testing.T) {
		args := stripRunFromArgs([]string{"-foo", "bar", "-run", "A"})
		require.Equal(t, []string{"-foo", "bar"}, args)
	})
}
