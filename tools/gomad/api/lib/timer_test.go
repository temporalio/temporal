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
	SIM "go.temporal.io/server/tools/gomad/runtime"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

func TestNewTimer(t *testing.T) {

	t.Run("Timer", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int64

			waitFor := func(d time.Duration) {
				timer := SIMLIB.NewTimer(d)
				res = append(res, SIMLANG.ChanRcv(timer.C).UnixMilli())
			}

			SIMLANG.Go(func() { waitFor(time.Hour) })
			SIMLANG.Go(func() { waitFor(time.Minute) })
			SIMLANG.Go(func() { waitFor(time.Second) })

			SIM.Join()

			require.Equal(t, []int64{
				time.Second.Milliseconds(),
				time.Minute.Milliseconds(),
				time.Hour.Milliseconds()}, res)
		})
	})

	t.Run("Non-Blocking Send", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			SIMLANG.Go(func() {
				SIMLIB.NewTimer(time.Minute)
				SIMLIB.Sleep(time.Hour)
			})

			SIM.Join()

			// all gouroutines finish despite Timer channel not being read
		})
	})

	t.Run("Stop", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			SIMLANG.Go(func() {
				timer := SIMLIB.NewTimer(0)
				require.False(t, timer.Stop()) // was fired immediately
				require.EqualValues(t, 0, SIMLANG.ChanRcv(timer.C).UnixMilli())

				timer = SIMLIB.NewTimer(-1)
				require.False(t, timer.Stop()) // was fired immediately
				require.EqualValues(t, 0, SIMLANG.ChanRcv(timer.C).UnixMilli())

				timer = SIMLIB.NewTimer(time.Second)
				require.True(t, timer.Stop())  // hasn't fired yet
				require.False(t, timer.Stop()) // already stopped

				timeout := SIMLIB.NewTimer(time.Minute)
				for {
					selector := SIMLANG.Select(0, SIMLANG.RcvChan(timer.C), nil, 0, SIMLANG.RcvChan(timeout.C), nil)
					switch selector.Case {
					case 0: // timer
						panic("never fires because it was stopped")
					case 1: // timeout
						return
					}
				}
			})

			SIM.Join()
		})
	})

	t.Run("Stop after Expired", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {

			SIMLANG.Go(func() {
				timer := SIMLIB.NewTimer(time.Hour)

				_ = SIMLANG.ChanRcv(timer.C)
				require.False(t, timer.Stop()) // just fired now

				SIMLIB.Sleep(time.Millisecond)
				require.False(t, timer.Stop()) // already fired
			})

			SIM.Join()
		})
	})

	t.Run("Reset", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {

			SIMLANG.Go(func() {
				timer := SIMLIB.NewTimer(time.Second)
				require.True(t, timer.Reset(time.Minute)) // before fired

				now := SIMLANG.ChanRcv(timer.C)
				require.Equal(t, time.Minute.Milliseconds(), now.UnixMilli())

				require.False(t, timer.Reset(time.Minute)) // after fired
			})

			SIM.Join()
		})
	})

	t.Run("Reset after Stopped", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {

			SIMLANG.Go(func() {
				timer := SIMLIB.NewTimer(time.Second)
				require.True(t, timer.Stop())
				require.False(t, timer.Reset(time.Minute))
			})

			SIM.Join()
		})
	})
}

// TODO: test it's non-blocking send

func TestAfterFunc(t *testing.T) {

	t.Run("AfterFunc", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var firedAt []int64

			SIMLANG.Go(func() {
				SIMLIB.AfterFunc(time.Minute,
					func() { firedAt = append(firedAt, SIMLIB.NowMs()) })
			})

			SIM.Join()

			require.Equal(t, []int64{
				time.Minute.Milliseconds(), // only once!
			}, firedAt)
		})
	})

	t.Run("Reset Before Fired", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var firedAt []int64

			SIMLANG.Go(func() {
				timer := SIMLIB.AfterFunc(time.Minute,
					func() { firedAt = append(firedAt, SIMLIB.NowMs()) })

				require.True(t,
					timer.Reset(time.Hour)) // overrides timer
			})

			SIM.Join()

			require.Equal(t, []int64{
				time.Hour.Milliseconds(), // only once!
			}, firedAt)
		})
	})

	t.Run("Reset After Fired", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var firedAt []int64

			SIMLANG.Go(func() {
				timer := SIMLIB.AfterFunc(time.Minute,
					func() { firedAt = append(firedAt, SIMLIB.NowMs()) })

				// timer fires!

				SIMLIB.Sleep(time.Minute + 1*time.Second) // 1s to ensure it's after AfterFunc

				require.False(t,
					timer.Reset(time.Hour))

				// timer fires again!
			})

			SIM.Join()

			require.Equal(t, []int64{
				time.Minute.Milliseconds(),
				(time.Minute + time.Hour + time.Second).Milliseconds(),
			}, firedAt)
		})
	})

	t.Run("Reset After Stop", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var firedAt []int64

			SIMLANG.Go(func() {
				timer := SIMLIB.AfterFunc(time.Minute,
					func() { firedAt = append(firedAt, SIMLIB.NowMs()) })
				timer.Stop()
				require.False(t,
					timer.Reset(time.Hour)) // overrides timer
			})

			SIM.Join()

			require.Equal(t, []int64{
				time.Hour.Milliseconds(),
			}, firedAt)
		})
	})
}
