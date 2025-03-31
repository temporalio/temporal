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
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type (
	WaitOptions struct {
		CheckInterval time.Duration
		MaxDuration   time.Duration
		NumGoRoutines int
	}
)

var (
	defaultWaitOptions = WaitOptions{
		CheckInterval: 1 * time.Millisecond,
		MaxDuration:   1 * time.Second,
		NumGoRoutines: 1,
	}
)

func WithCheckInterval(checkInterval time.Duration) func(*WaitOptions) {
	return func(wo *WaitOptions) {
		wo.CheckInterval = checkInterval
	}
}

func WithMaxDuration(maxDuration time.Duration) func(*WaitOptions) {
	return func(wo *WaitOptions) {
		wo.MaxDuration = maxDuration
	}
}

func WithNumGoRoutines(numGoRoutines int) func(*WaitOptions) {
	return func(wo *WaitOptions) {
		wo.NumGoRoutines = numGoRoutines
	}
}

// WaitGoRoutineWithFn waits for a go routine with the given function to appear in call stacks,
// using different WaitOptions, and returns the number of attempts needed to find the go routine.
func WaitGoRoutineWithFn(t testing.TB, fn any, opts ...func(*WaitOptions)) int {
	wo := defaultWaitOptions
	for _, opt := range opts {
		opt(&wo)
	}

	fnName, err := functionName(fn)
	require.NoError(t, err)

	attempt := 1
	numFound := 0
	require.Eventually(t,
		func() bool {
			numFound, err = numGoRoutinesWithFn(fnName)
			require.NoError(t, err)
			if numFound == wo.NumGoRoutines {
				t.Logf("Found %s function %d times on %d attempt\n", fnName, numFound, attempt)
				return true
			}

			attempt++
			return false
		},
		wo.MaxDuration,
		wo.CheckInterval,
		"Function %s must be found %d times but was found %d times in all go routine call stacks after %s", fnName, wo.NumGoRoutines, numFound, wo.MaxDuration.String())
	return attempt
}

func AssertNoGoRoutineWithFn(t testing.TB, fn any) {
	fnName, err := functionName(fn)
	require.NoError(t, err)
	numFound, err := numGoRoutinesWithFn(fnName)
	require.NoError(t, err)
	require.Zero(t, numFound)
}

// PrintGoRoutines prints all go routines.
func PrintGoRoutines() {
	_ = pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
}

func numGoRoutinesWithFn(fnName string) (int, error) {
	// 20 is a buffer for go routines that might be created between the next 2 lines. 10 is not enough!
	stackRecords := make([]runtime.StackRecord, runtime.NumGoroutine()+20)
	stackRecordsLen, ok := runtime.GoroutineProfile(stackRecords)
	if !ok {
		return 0, errors.New(fmt.Sprintf("Size %d is too small for stack records. Need %d", len(stackRecords), stackRecordsLen))
	}

	numFound := 0
	for _, stackRecord := range stackRecords {
		frames := runtime.CallersFrames(stackRecord.Stack())
		for {
			frame, more := frames.Next()
			if strings.Contains(frame.Function, fnName) {
				numFound++
			}
			if !more {
				break
			}
		}
	}

	return numFound, nil
}

func functionName(fn any) (string, error) {
	if fnName, isString := fn.(string); isString {
		return fnName, nil
	}

	if fnName, isFunc := functionNameForPC(reflect.ValueOf(fn).Pointer()); isFunc {
		return fnName, nil
	}

	return "", errors.New(fmt.Sprintf("Invalid function %#v", fn))
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
