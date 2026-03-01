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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
	SIM "go.temporal.io/server/tools/gomad/runtime"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

func TestLocker(t *testing.T) {
	testCases := []struct {
		name string
		fn   func(*testing.T, newLocker, int64)
	}{
		{
			name: "Lock",
			fn: func(t *testing.T, newLocker newLocker, seed int64) {
				var res []int

				SIMLANG.Go(func() {
					l := newLocker()
					l.Lock()
					defer l.Unlock()
					res = append(res, 1)
				})

				SIMLANG.Go(func() {
					l := newLocker()
					l.Lock()
					defer l.Unlock()
					res = append(res, 2)
				})

				SIM.Join()

				require.ElementsMatch(t, []int{1, 2}, res)
			},
		},
		//{
		//	name: "Deadlock",
		//	fn: func(t *testing.T, newLocker newLocker, seed int64) {
		//		SIMLANG.Go(func() {
		//			l := newLocker()
		//			l.Lock()
		//			l.Lock()
		//		})
		//
		//		require.PanicsWithValue(t, "no more events but goroutines are not done: #1 (suspended)", func() {
		//			SIM.Join()
		//		})
		//	},
		//},
		{
			name: "Invalid Unlock",
			fn: func(t *testing.T, newLocker newLocker, seed int64) {
				SIMLANG.Go(func() {
					l := newLocker()
					require.PanicsWithValue(t, "unlock of (never) locked mutex", func() {
						l.Unlock()
					})
				})

				SIM.Join()
			},
		},
		{
			name: "Concurrent",
			fn: func(t *testing.T, newLocker newLocker, seed int64) {
				var res []int
				channel := make(chan int)
				rounds := 1 + int(seed)

				l := newLocker()
				for i := 0; i < rounds; i++ {
					SIMLANG.Go(func() {
						l.Lock()
						defer l.Unlock()
						SIMLANG.ChanSend(channel, i)
					})
				}

				for i := 0; i < rounds; i++ {
					res = append(res, SIMLANG.ChanRcv(channel))
				}

				require.Equal(t, rounds, len(res))
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run("Mutex/"+tc.name, func(t *testing.T) {
			testutil.StressRun(func(seed int64) {
				tc.fn(t, func() sync.Locker {
					var mu SIMLIB.Mutex
					return &mu
				}, seed)
			})
		})

		t.Run("RWMutex/"+tc.name, func(t *testing.T) {
			testutil.StressRun(func(seed int64) {
				tc.fn(t, func() sync.Locker {
					var mu SIMLIB.RWMutex
					return &mu
				}, seed)
			})
		})
	}
}

type newLocker = func() sync.Locker

func TestMutex(t *testing.T) {
	testCases := []struct {
		name string
		fn   func(*testing.T, newMutex, int64)
	}{
		{
			name: "TryLock",
			fn: func(t *testing.T, newMutex newMutex, seed int64) {
				m := newMutex()
				require.True(t, m.TryLock())
				require.False(t, m.TryLock())
				m.Unlock()
				require.True(t, m.TryLock())
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run("Mutex/"+tc.name, func(t *testing.T) {
			testutil.StressRun(func(seed int64) {
				tc.fn(t, func() mutex {
					var mu SIMLIB.Mutex
					return &mu
				}, seed)
			})
		})

		t.Run("RWMutex/"+tc.name, func(t *testing.T) {
			testutil.StressRun(func(seed int64) {
				tc.fn(t, func() mutex {
					var mu SIMLIB.RWMutex
					return &mu
				}, seed)
			})
		})
	}
}

func TestRWMutex(t *testing.T) {
	t.Run("Exclusive Write Access", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []string

			var mu SIMLIB.RWMutex
			mu.Lock()

			l := mu.RLocker()
			SIMLANG.Go(func() {
				l.Lock()
				defer l.Unlock()
				res = append(res, "reader1")
			})
			SIMLANG.Go(func() {
				l.Lock()
				defer l.Unlock()
				res = append(res, "reader2")
			})
			SIMLANG.Go(func() {
				res = append(res, "writer")
				mu.Unlock()
			})

			SIM.Join()

			require.ElementsMatch(t, []string{"writer", "reader1", "reader2"}, res)
			require.Equal(t, "writer", res[0]) // always unblocks first
		})
	})

	t.Run("Shared Read Access", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []string

			var mu SIMLIB.RWMutex
			l := mu.RLocker()
			l.Lock() // reader1
			l.Lock() // reader2

			SIMLANG.Go(func() {
				defer l.Unlock()
				res = append(res, "reader1")
			})
			SIMLANG.Go(func() {
				defer l.Unlock()
				res = append(res, "reader2")
			})
			SIMLANG.Go(func() {
				mu.Lock()
				defer mu.Unlock()
				res = append(res, "writer")
			})

			SIM.Join()

			require.ElementsMatch(t, []string{"writer", "reader1", "reader2"}, res)
			require.Equal(t, "writer", res[2]) // always unblocks last
		})
	})
}

type newMutex = func() mutex

type mutex interface {
	Lock()
	Unlock()
	TryLock() bool
}

func TestCond(t *testing.T) {

	t.Run("Signal", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			rounds := int(seed + 1)
			cond := SIMLIB.NewCond(&SIMLIB.Mutex{})

			// producer
			var nums []int
			SIMLANG.Go(func() {
				for i := 0; i < rounds; i++ {
					cond.L.Lock()
					nums = append(nums, i)
					cond.L.Unlock()
					cond.Signal() // unblock waiting consumer
				}
			})

			// consumer
			var res int
			SIMLANG.Go(func() {
				cond.L.Lock()
				for len(nums) != rounds { // check in loop as lock might be lost again after Wait unblocked
					cond.Wait() // wait for producer signal
				}
				for _, num := range nums {
					res += num
				}
				cond.L.Unlock()
			})

			SIM.Join()

			require.Len(t, nums, rounds)
			expected := ((rounds - 1) * rounds) / 2
			require.Equal(t, expected, res)
		})
	})

	t.Run("Broadcast", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			rounds := int(seed + 1)
			cond := SIMLIB.NewCond(&SIMLIB.Mutex{})

			var counter int32
			for i := 0; i < rounds; i++ {
				SIMLANG.Go(func() {
					SIM.Sleep(1 * time.Second) // Simulate work

					cond.L.Lock()
					defer cond.L.Unlock()

					counter++

					if counter == counter {
						// all workers have reached the barrier
						cond.Broadcast()
					} else {
						// worker is waiting at the barrier
						cond.Wait()
					}
				})
			}

			SIM.Join()
		})
	})

	// TODO: call Broadcast() twice
}

func TestOnce(t *testing.T) {

	t.Run("once.Do", func(t *testing.T) {
		var res int

		var once SIMLIB.Once
		for i := 0; i < 5; i++ {
			once.Do(func() {
				res += 1
			})
		}

		require.Equal(t, 1, res)
	})

	t.Run("OnceFunc", func(t *testing.T) {
		var res int

		fn := SIMLIB.OnceFunc(func() { res += 1 })
		for i := 0; i < 5; i++ {
			fn()
		}

		require.Equal(t, 1, res)
	})

	t.Run("OnceValue", func(t *testing.T) {
		var res int

		fn := SIMLIB.OnceValue(func() int {
			res += 1
			return res
		})

		for i := 0; i < 5; i++ {
			fn()
		}

		require.Equal(t, 1, fn())
	})

	t.Run("OnceValues", func(t *testing.T) {
		var r1, r2 int

		fn := SIMLIB.OnceValues(func() (int, int) {
			r1 += 1
			r2 += 1
			return r1, r2
		})

		for i := 0; i < 5; i++ {
			fn()
		}

		ret1, ret2 := fn()
		require.Equal(t, 1, ret1)
		require.Equal(t, 1, ret2)
	})
}
