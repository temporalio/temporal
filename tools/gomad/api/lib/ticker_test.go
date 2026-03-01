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
	"time"

	"github.com/stretchr/testify/require"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

func TestTicker(t *testing.T) {
	t.Run("NewTicker", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int64

			tick := SIMLIB.NewTicker(time.Minute)
			res = append(res, SIMLANG.ChanRcv(tick.C).UnixMilli())
			res = append(res, SIMLANG.ChanRcv(tick.C).UnixMilli())

			require.Equal(t, []int64{
				(1 * time.Minute).Milliseconds(),
				(2 * time.Minute).Milliseconds()}, res)
		})
	})

	t.Run("Stop", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int64
			ticker := SIMLIB.NewTicker(1 * time.Second)
			timeout := SIMLIB.NewTimer(1 * time.Minute)

		loop:
			for {
				selector := SIMLANG.Select(0, SIMLANG.RcvChan(ticker.C), nil, 0, SIMLANG.RcvChan(timeout.C), nil)
				switch selector.Case {
				case 0: // ticker
					ticker.Stop()
					res = append(res, SIMLIB.Now().UnixMilli())
				case 1: // timeout
					res = append(res, SIMLIB.Now().UnixMilli())
					break loop
				}
			}

			require.Equal(t, []int64{
				(1 * time.Second).Milliseconds(),
				(1 * time.Minute).Milliseconds()}, res)
		})
	})

	t.Run("Reset", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int64

			reset := false
			ticker := SIMLIB.NewTicker(1 * time.Second)
			timeout2s := SIMLIB.NewTimer(2*time.Second + 1*time.Millisecond) // +1ms to ensure it fires after ticker
			timeout3m := SIMLIB.NewTimer(3 * time.Minute)

		loop:
			for {
				selector := SIMLANG.Select(0, SIMLANG.RcvChan(ticker.C), nil, 0, SIMLANG.RcvChan(timeout2s.C), nil, 0, SIMLANG.RcvChan(timeout3m.C), nil)
				switch selector.Case {
				case 0: // ticker
					res = append(res, SIMLIB.Now().UnixMilli())
				case 1: // timeout2s
					if !reset {
						ticker.Reset(1 * time.Minute)
						reset = true
					}
				case 2: // timeout3m
					break loop
				}
			}

			require.Equal(t, []int64{
				(1 * time.Second).Milliseconds(),
				(2 * time.Second).Milliseconds(),
				(2*time.Second + 1*time.Millisecond + 1*time.Minute).Milliseconds(),
				(2*time.Second + 1*time.Millisecond + 2*time.Minute).Milliseconds()}, res)
		})
	})
}

func TestTick(t *testing.T) {
	testutil.StressRun(func(seed int64) {
		var res []int64

		c := SIMLIB.Tick(time.Minute)
		res = append(res, SIMLANG.ChanRcv(c).UnixMilli())
		res = append(res, SIMLANG.ChanRcv(c).UnixMilli())

		require.Equal(t, []int64{
			(1 * time.Minute).Milliseconds(),
			(2 * time.Minute).Milliseconds()}, res)
	})
}
