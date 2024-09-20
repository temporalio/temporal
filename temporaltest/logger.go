// The MIT License
//
// Copyright (c) 2021 Datadog, Inc.
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

package temporaltest

import (
	"testing"

	"go.temporal.io/sdk/log"
)

var _ log.Logger = &testLogger{}

// testLogger implements a Go SDK logger by writing to the test output.
//
// Text will be printed only if the test fails or the -test.v flag is set.
type testLogger struct {
	t *testing.T
}

func (tl *testLogger) logLevel(lvl, msg string, keyvals ...interface{}) {
	if tl.t == nil {
		return
	}
	args := []interface{}{lvl, msg}
	args = append(args, keyvals...)
	tl.t.Log(args...)
}

func (tl *testLogger) Debug(msg string, keyvals ...interface{}) {
	tl.logLevel("DEBUG", msg, keyvals)
}

func (tl *testLogger) Info(msg string, keyvals ...interface{}) {
	tl.logLevel("INFO ", msg, keyvals)
}

func (tl *testLogger) Warn(msg string, keyvals ...interface{}) {
	tl.logLevel("WARN ", msg, keyvals)
}

func (tl *testLogger) Error(msg string, keyvals ...interface{}) {
	tl.logLevel("ERROR", msg, keyvals)
}
