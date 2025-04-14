// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package tdbg_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/tdbg"
)

// testFlagLookup helps to test the behaviour of the command line flags.
// It includes a testing object and a boolean to simulate auto confirmation scenarios.
type testFlagLookup struct {
	t           *testing.T
	autoConfirm bool
}

// testWriter is a mock writer used to capture and test the output of functions.
// It simulates writing to an output buffer and can be configured to return an error.
type testWriter struct {
	buffer bytes.Buffer
	err    error
}

// testReader is a mock reader to test the input to functions.
// It can be configured with a reader to simulate user input and an error for error handling scenarios.
type testReader struct {
	reader io.Reader
	err    error
}

// TestPrompter evaluates the functioning of tdbg.Prompter under different conditions.
// It tests for auto-confirmations, user responses, and error handling in both read and write operations.
func TestPrompter(t *testing.T) {
	// Define test cases with various scenarios including responses, errors, and expected outcomes.
	for _, tc := range []struct {
		name              string
		autoConfirm       bool
		response          string
		writerErr         error
		readerErr         error
		expectedPanic     string
		expectedExitCodes []int
		expectedPrompt    string
	}{
		// Test auto-confirmation without user interaction.
		{
			name:              "auto_confirm",
			autoConfirm:       true,
			expectedExitCodes: nil,
			expectedPrompt:    "",
		},
		// Simulate user entering 'yes' as response.
		{
			name:              "yes",
			autoConfirm:       false,
			response:          "y\n",
			expectedExitCodes: nil,
			expectedPrompt:    "test prompt [y/N]: ",
		},
		// Simulate user entering 'no' as response.
		{
			name:              "no",
			autoConfirm:       false,
			response:          "n\n",
			expectedExitCodes: []int{1},
			expectedPrompt:    "test prompt [y/N]: ",
		},
		// Test for error scenario in the writer component.
		{
			name:          "writer_error",
			writerErr:     errors.New("test writer error"),
			autoConfirm:   false,
			expectedPanic: "failed to write prompt: test writer error",
		},
		// Test for error scenario in the reader component.
		{
			name:          "reader_error",
			readerErr:     errors.New("test reader error"),
			autoConfirm:   false,
			expectedPanic: "failed to read prompt: test reader error",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Prepare the writer, reader and prompter for the test.
			var exitCodes []int
			writer := &testWriter{err: tc.writerErr}
			reader := &testReader{reader: bytes.NewBuffer([]byte(tc.response)), err: tc.readerErr}
			prompter := tdbg.NewPrompter(testFlagLookup{
				t:           t,
				autoConfirm: tc.autoConfirm,
			}, func(params *tdbg.PrompterParams) {
				// Set up the exit strategy and I/O for the prompter.
				params.Exiter = func(code int) {
					exitCodes = append(exitCodes, code)
				}
				params.Writer = writer
				params.Reader = reader
			})

			// Conduct the test based on the expected outcome: Panic or standard output.
			if tc.expectedPanic != "" {
				require.PanicsWithError(t, tc.expectedPanic, func() {
					prompter.Prompt("test prompt")
				})
			} else {
				prompter.Prompt("test prompt")
				assert.Equal(t, tc.expectedPrompt, writer.buffer.String())
				assert.Equal(t, tc.expectedExitCodes, exitCodes)
			}
		})
	}
}

// Write is the implementation of the io.Writer interface for testWriter.
// It writes bytes to the buffer or returns an error if configured to do so.
func (t *testWriter) Write(p []byte) (n int, err error) {
	if t.err != nil {
		return 0, t.err
	}
	return t.buffer.Write(p)
}

// Read is the implementation of the io.Reader interface for testReader.
// It reads bytes from the underlying reader or returns an error if configured to do so.
func (t *testReader) Read(p []byte) (n int, err error) {
	if t.err != nil {
		return 0, t.err
	}
	return t.reader.Read(p)
}

// Bool simulates the behavior of a command line flag lookup.
func (l testFlagLookup) Bool(name string) bool {
	require.Equal(l.t, tdbg.FlagYes, name)
	return l.autoConfirm
}
