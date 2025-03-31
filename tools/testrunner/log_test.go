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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTestTimeouts(t *testing.T) {
	logInput, err := os.ReadFile("testdata/timeout-input.log")
	require.NoError(t, err)
	logOutput, err := os.ReadFile("testdata/timeout-output.log")
	require.NoError(t, err)

	stacktrace, tests := parseTestTimeouts(string(logInput))
	require.Equal(t, []string{
		"TestActivityApiResetClientTestSuite",
		"TestNDCFuncTestSuite",
		"TestActivityApiStateReplicationSuite",
	}, tests)
	require.Equal(t, string(logOutput), stacktrace)
}
