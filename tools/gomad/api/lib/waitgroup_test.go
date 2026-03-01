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

package lib_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

func TestWaitGroup(t *testing.T) {

	t.Run("Do not wait when empty", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var wg SIMLIB.WaitGroup
			wg.Wait()
		})
	})

	t.Run("Wait for multiple goroutines", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int

			var wg SIMLIB.WaitGroup
			for i := 0; i < 3; i++ {
				wg.Add(1)
				SIMLANG.Go(func() {
					res = append(res, i)
					wg.Done()
				})
			}

			wg.Wait()

			require.ElementsMatch(t, res, []int{0, 1, 2})
		})
	})

	t.Run("Panic on negative counter", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var wg SIMLIB.WaitGroup

			require.PanicsWithValue(t, "negative counter in WaitGroup", func() {
				wg.Add(-1)
			})
		})
	})

	t.Run("Fanout", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var expected []int64
			var actual []int64
			taskCount := 1 + seed

			// single producer
			taskChannel := make(chan int64, taskCount)
			for i := int64(0); i < taskCount; i++ {
				SIMLANG.ChanSend(taskChannel, i)
				expected = append(expected, i)
			}
			SIMLANG.ChanClose(taskChannel)

			// fanout: multiple consumers
			var wg SIMLIB.WaitGroup
			resultChannel := make(chan int64, taskCount)
			for i := int64(0); i < 1+taskCount/2; i++ {
				wg.Add(1)
				SIMLANG.Go(func() {
					defer wg.Done()
					for {
						v, ok := SIMLANG.ChanRcvOk(taskChannel)
						if !ok {
							break
						}
						SIMLANG.ChanSend(resultChannel, v)
					}
				})
			}

			// wait until consumers are done
			SIMLANG.Go(func() {
				wg.Wait()
				SIMLANG.ChanClose(resultChannel)
			})

			// collect results
			for {
				v, ok := SIMLANG.ChanRcvOk(resultChannel)
				if !ok {
					break
				}
				actual = append(actual, v)
			}

			require.Len(t, actual, int(taskCount))
			require.ElementsMatch(t, actual, expected)
		})
	})
}
