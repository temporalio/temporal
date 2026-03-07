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

package lang_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIM "go.temporal.io/server/tools/gomad/runtime"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

func TestChannel(t *testing.T) {

	t.Run("send and receive", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			ch := make(chan string)

			SIMLANG.Go(func() {
				SIMLANG.ChanSend(ch, "hello")
			})

			res, ok := SIMLANG.ChanRcvOk(ch)
			require.True(t, ok)
			require.Equal(t, "hello", res)
		})
	})

	// regression test: ensure nil can be sent/received
	t.Run("send and receive nil message", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			ch := make(chan error)

			SIMLANG.Go(func() {
				SIMLANG.ChanSend(ch, nil)
			})

			res := SIMLANG.ChanRcv(ch)

			require.Nil(t, res)
		})
	})

	t.Run("send and receive multiple messages", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []string
			ch := make(chan string)

			SIMLANG.Go(func() {
				res = append(res, SIMLANG.ChanRcv(ch))
				SIMLANG.ChanSend(ch, "pong")
				res = append(res, SIMLANG.ChanRcv(ch))
				SIMLANG.ChanSend(ch, "pong")
			})

			SIMLANG.Go(func() {
				SIMLANG.ChanSend(ch, "ping")
				res = append(res, SIMLANG.ChanRcv(ch))
				SIMLANG.ChanSend(ch, "ping")
				res = append(res, SIMLANG.ChanRcv(ch))
			})

			SIM.Join()

			require.Equal(t, []string{"ping", "pong", "ping", "pong"}, res)
		})
	})

	t.Run("send and receive with single sender and multiple receivers", func(t *testing.T) {
		fn := func() []int {
			var res []int

			count := 3
			ch := make(chan int)

			// sender
			SIMLANG.Go(func() {
				for i := 0; i < count; i++ {
					SIMLANG.ChanSend(ch, i)
				}
			})

			// receivers
			for i := 0; i < count; i++ {
				SIMLANG.Go(func() {
					res = append(res, SIMLANG.ChanRcv(ch))
				})
			}

			SIM.Join()

			return res
		}

		t.Run("completes", func(t *testing.T) {
			testutil.StressRun(func(seed int64) {
				require.ElementsMatch(t, []int{0, 1, 2}, fn())
			})
		})

		t.Run("happens in deterministic order", func(t *testing.T) {
			for i := 0; i < testutil.TestRuns; i++ {
				var res1, res2 []int
				testutil.SingleRun(func(seed int64) { res1 = fn() }, int64(i))
				testutil.SingleRun(func(seed int64) { res2 = fn() }, int64(i))
				require.Equal(t, res1, res2)
			}
		})
	})

	t.Run("send and receive with single receiver and multiple senders", func(t *testing.T) {
		fn := func() []int {
			var res []int

			count := 3
			ch := make(chan int)

			// senders
			for i := 0; i < count; i++ {
				SIMLANG.Go(func() {
					SIMLANG.ChanSend(ch, i)
				})
			}

			// receiver
			SIMLANG.Go(func() {
				for i := 0; i < count; i++ {
					res = append(res, SIMLANG.ChanRcv(ch))
				}
			})

			SIM.Join()

			return res
		}

		t.Run("completes", func(t *testing.T) {
			testutil.StressRun(func(seed int64) {
				require.ElementsMatch(t, []int{0, 1, 2}, fn())
			})
		})

		t.Run("happens in deterministic order", func(t *testing.T) {
			for i := 0; i < testutil.TestRuns; i++ {
				var res1, res2 []int
				testutil.SingleRun(func(seed int64) { res1 = fn() }, int64(i))
				testutil.SingleRun(func(seed int64) { res2 = fn() }, int64(i))
				require.Equal(t, res1, res2)
			}
		})
	})

	t.Run("receive from closed channel", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			ch := make(chan string)

			SIMLANG.Go(func() {
				res, ok := SIMLANG.ChanRcvOk(ch) // unblocked by `ChanClose`
				require.Equal(t, "", res)
				require.False(t, ok)
			})

			SIMLANG.ChanClose(ch)
		})
	})

	t.Run("closing channel unblocks multiple goroutines", func(t *testing.T) {
		fn := func() []int {
			ch := make(chan string)

			var res []int
			// All goroutine spawning and the close must run cooperatively so
			// that scheduling is fully seed-deterministic. If spawned via
			// addFromNative, the delegation goroutine for ChanClose can race
			// with the receiver goroutines in the event queue, breaking
			// determinism across otherwise identical runs.
			ready := make(chan struct{})
			SIMLANG.InternalGo(func() {
				SIMLANG.Go(func() {
					SIMLANG.ChanRcvOk(ch)
					res = append(res, 0)
				})
				SIMLANG.Go(func() {
					SIMLANG.ChanRcvOk(ch)
					res = append(res, 1)
				})
				SIMLANG.Go(func() {
					SIMLANG.ChanRcvOk(ch)
					res = append(res, 2)
				})
				SIMLANG.ChanClose(ch)
				close(ready)
			})
			<-ready

			SIM.Join()

			return res
		}

		for i := 0; i < testutil.TestRuns; i++ {
			var res1, res2 []int
			testutil.SingleRun(func(seed int64) { res1 = fn() }, int64(i))
			testutil.SingleRun(func(seed int64) { res2 = fn() }, int64(i))
			require.Equal(t, res1, res2)
		}
	})

	t.Run("receive from nil channel", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var ch chan any

			require.PanicsWithValue(t, "receiving from a nil channel", func() {
				SIMLANG.ChanRcv(ch)
			})
		})
	})

	t.Run("send to closed channel", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			ch := make(chan string)
			SIMLANG.ChanClose(ch)

			require.PanicsWithValue(t, "sending to closed channel #1", func() {
				SIMLANG.ChanSend(ch, "hello")
			})
		})
	})

	t.Run("close channel twice", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			ch := make(chan string)
			SIMLANG.ChanClose(ch)

			require.PanicsWithValue(t, "closing already closed channel #1", func() {
				SIMLANG.ChanClose(ch)
			})
		})
	})

	t.Run("close channel again after make()", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			ch := make(chan string)
			SIMLANG.ChanClose(ch)

			ch = make(chan string)
			SIMLANG.ChanClose(ch)
		})
	})

	t.Run("send to nil channel", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var ch chan any

			require.PanicsWithValue(t, "sending to a nil channel", func() {
				SIMLANG.ChanSend(ch, true)
			})
		})
	})
}

func TestBufferedChannel(t *testing.T) {

	t.Run("send and receive", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			ch := make(chan string, 1+seed)

			SIMLANG.ChanSend(ch, "hello") // non-blocking
			res := SIMLANG.ChanRcv(ch)

			require.Equal(t, "hello", res)
		})
	})

	// regression test: ensure nil can be sent/received
	t.Run("send and receive nil message", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			ch := make(chan error, 1+seed)

			SIMLANG.ChanSend(ch, nil) // non-blocking
			res := SIMLANG.ChanRcv(ch)

			require.Nil(t, res)
		})
	})

	t.Run("send multiple messages, filling buffer once", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int
			messages := int(seed)
			ch := make(chan int, messages) // buffer = number of messages

			for i := 0; i < messages; i++ {
				SIMLANG.ChanSend(ch, i)
			}

			// close channel "randomly" - doesn't change the behaviour!
			if SIM.CurrentSimulator().Drng.Intn(2) == 1 {
				SIMLANG.ChanClose(ch)
			}

			for i := 0; i < messages; i++ {
				m, ok := SIMLANG.ChanRcvOk(ch)
				require.True(t, ok)
				res = append(res, m)
			}

			require.Equal(t, messages, len(res))
		})
	})

	t.Run("send multiple messages, filling buffer multiple times", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int
			messages := int(seed)
			ch := make(chan int, 1) // buffer = 1

			SIMLANG.Go(func() {
				for i := 0; i < messages; i++ {
					SIMLANG.ChanSend(ch, i)
				}
			})

			for i := 0; i < messages; i++ {
				res = append(res, SIMLANG.ChanRcv(ch))
			}

			require.Equal(t, messages, len(res))
		})
	})

	// TODO
	t.Run("Blocking", func(t *testing.T) {
		t.Skip()

		require.PanicsWithValue(t, "no more events but goroutines are not done: #0 (suspended)", func() {
			testutil.SingleRun(
				func(seed int64) {
					ch := make(chan int, 1)
					SIMLANG.ChanSend(ch, 1)
					SIMLANG.ChanSend(ch, 2)
				},
				0)
		})
	})
}
