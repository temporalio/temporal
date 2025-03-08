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
	"testing"

	"github.com/stretchr/testify/require"
)

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
