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

package testcore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/maruel/panicparse/v2/stack"
)

var (
	perTestTimeout     = 4 * time.Second
	drainTests         atomic.Bool
	drainTestTimeout   = perTestTimeout
	runningSuites      = make(map[uuid.UUID]*testing.T)
	runningSuitesMutex = sync.Mutex{}
)

type testTimeout struct {
	id       uuid.UUID
	updateCh chan time.Duration
	closeCh  chan struct{}
	resetCh  chan *testing.T
}

// This is one testTimeout per suite.
func newTestTimeout(currentT *testing.T, timeout time.Duration) *testTimeout {
	tt := &testTimeout{
		id:       uuid.New(),
		updateCh: make(chan time.Duration),
		closeCh:  make(chan struct{}),
		resetCh:  make(chan *testing.T),
	}

	// Extend the list of running suite.
	runningSuitesMutex.Lock()
	runningSuites[tt.id] = currentT
	runningSuitesMutex.Unlock()

	go func() {
		startedAt := time.Now()
		for {
			tick := time.NewTimer(time.Second)
			select {
			case newTimeout := <-tt.updateCh:
				timeout = newTimeout
			case newT := <-tt.resetCh:
				runningSuitesMutex.Lock()
				currentT = newT
				runningSuites[tt.id] = newT
				startedAt = time.Now()
				runningSuitesMutex.Unlock()
			case <-tt.closeCh:
				runningSuitesMutex.Lock()
				delete(runningSuites, tt.id)
				runningSuitesMutex.Unlock()
				return // exit goroutine
			case <-tick.C:
				if time.Now().After(startedAt.Add(timeout)) {
					drainTests.Store(true)

					// Cannot use t.Fatalf since it will block until the test completes.
					currentT.Logf("Test %v timed out after %v. To extend the time of the test, call s.SetTestTimeout().",
						currentT.Name(), timeout)

					// TODO: use per-test timeout here, too
					// Wait for other running tests to complete before exiting.
					// This helps present a clean JUnit report with only a single failure, in the best case.
					ctx, _ := context.WithTimeout(context.Background(), drainTestTimeout)
				drainLoop:
					for {
						runningSuitesMutex.Lock()
						testsToDrain := len(runningSuites) - 1
						runningSuitesMutex.Unlock()

						if testsToDrain == 0 {
							break drainLoop
						}
						currentT.Logf("Waiting for %d remaining tests to complete.", testsToDrain)

						select {
						case <-ctx.Done():
							currentT.Logf("Timed out while waiting for test to drain.")
							break drainLoop
						default:
							time.Sleep(100 * time.Millisecond)
						}
					}

					// Print stacktrace, only showing test goroutines.
					var traces string
					buf := make([]byte, 5<<20) // give it plenty of room
					runtime.Stack(buf, true)
					snap, _, err := stack.ScanSnapshot(bytes.NewReader(buf), io.Discard, stack.DefaultOpts())
					if err != nil && err != io.EOF {
						currentT.Logf("failed to parse stack trace: %v", err)
					} else {
						traces = "\nstacktrace:\n\n"
						for _, goroutine := range snap.Goroutines {
							var shouldPrint bool
							for _, line := range goroutine.Stack.Calls {
								if strings.HasPrefix(line.DirSrc, "tests/") {
									shouldPrint = true
									break
								}
							}
							if shouldPrint {
								traces += fmt.Sprintf("goroutine %d [%v]:\n", goroutine.ID, goroutine.State)
								for _, call := range goroutine.Stack.Calls {
									file := call.RelSrcPath
									traces += fmt.Sprintf("\t%s:%d\n", file, call.Line)
								}
								traces += "\n\n"
							}
						}
					}
					currentT.Log(traces)

					// `timeout` exits with code 124 to indicate a timeout - might as well do that here.
					// GitHub Actions will mark this test when it sees this exit code.
					//revive:disable-next-line:deep-exit
					os.Exit(124)
				}
			}
		}
	}()

	return tt
}

func (b testTimeout) set(d time.Duration) {
	b.updateCh <- d
}

func (b testTimeout) cancel() {
	b.closeCh <- struct{}{}
}

func (b testTimeout) reset(t *testing.T) {
	if drainTests.Load() {
		// A test already timed out, skip this one immediately.
		t.SkipNow()
		return
	}
	b.resetCh <- t
}
