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

package runtime

import (
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// WaitGoRoutineWithFn waits for a go routine with the given function to appear within the duration.
func WaitGoRoutineWithFn(t testing.TB, fn any, maxDuration time.Duration) {
	t.Helper()

	targetFnName, ok := functionNameForPC(reflect.ValueOf(fn).Pointer())
	if !ok {
		t.Errorf("Invalid function %#v", fn)
	}

	attempt := 1
	require.Eventually(t,
		func() bool {
			stackRecords := make([]runtime.StackRecord, runtime.NumGoroutine()+10)
			stackRecordsLen, ok := runtime.GoroutineProfile(stackRecords)
			if !ok {
				t.Errorf("Size %d is too small for stack records. Need %d", len(stackRecords), stackRecordsLen)
			}

			for _, stackRecord := range stackRecords {
				frames := runtime.CallersFrames(stackRecord.Stack())
				for {
					frame, more := frames.Next()
					if strings.Contains(frame.Function, targetFnName) {
						t.Logf("Found %s function on %d attempt\n", frame.Function, attempt)
						return true
					}
					if !more {
						break
					}
				}
			}
			attempt++
			return false
		},
		maxDuration,
		1*time.Millisecond,
		"Function %s didn't appear in any go routine call stack after %s", targetFnName, maxDuration.String())
}

// PrintGoRoutines prints all go routines.
func PrintGoRoutines() {
	_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
}

func functionNameForPC(pc uintptr) (string, bool) {
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return "", false
	}
	elements := strings.Split(fn.Name(), ".")
	shortName := elements[len(elements)-1]
	return strings.TrimSuffix(shortName, "-fm"), true
}
