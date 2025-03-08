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
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/maruel/panicparse/v2/stack"
)

// parseTestTimeouts parses the stdout of a test run and returns the stacktrace and names of tests that timed out.
func parseTestTimeouts(stdout string) (stacktrace string, timedoutTests []string) {
	lines := strings.Split(strings.ReplaceAll(stdout, "\r\n", "\n"), "\n")
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if strings.HasPrefix(line, "FAIL") {
			// ignore
		} else if strings.HasPrefix(line, "panic: test timed out after") {
			// parse names of tests that timed out
			for {
				i++
				line = strings.TrimSpace(lines[i])
				if strings.HasPrefix(line, "Test") {
					timedoutTests = append(timedoutTests, strings.Split(line, " ")[0])
				}
				if line == "" {
					break
				}
			}
		} else if len(timedoutTests) > 0 {
			// collect stracktrace
			stacktrace += line + "\n"
		}
	}

	stacktrace = fmt.Sprintf("%d timed out test(s):\n\t%v\n\n%v",
		len(timedoutTests), strings.Join(timedoutTests, "\n\t"), testOnlyStacktrace(stacktrace))
	return
}

// testOnlyStacktrace removes all but the test stacktraces from the full stacktrace.
func testOnlyStacktrace(stacktrace string) string {
	var res string
	snap, _, err := stack.ScanSnapshot(strings.NewReader(stacktrace), io.Discard, stack.DefaultOpts())
	if err != nil && err != io.EOF {
		return fmt.Sprintf("failed to parse stacktrace: %v", err)
	}
	if snap == nil {
		return "failed to find a stacktrace"
	}
	res = "abridged stacktrace:\n"
	for _, goroutine := range snap.Goroutines {
		shouldPrint := slices.ContainsFunc(goroutine.Stack.Calls, func(call stack.Call) bool {
			return strings.HasSuffix(call.RemoteSrcPath, "_test.go")
		})
		if shouldPrint {
			res += fmt.Sprintf("\tgoroutine %d [%v]:\n", goroutine.ID, goroutine.State)
			for _, call := range goroutine.Stack.Calls {
				file := call.RemoteSrcPath
				res += fmt.Sprintf("\t\t%s:%d\n", file, call.Line)
			}
			res += "\n"
		}
	}
	return res
}
