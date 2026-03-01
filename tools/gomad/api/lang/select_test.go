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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

func TestSelect(t *testing.T) {

	t.Run("Receive from single channel", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int
			ch := make(chan int)

			SIMLANG.Go(func() {
				SIMLANG.ChanSend(ch, 1)
				SIMLANG.ChanSend(ch, 2)
				SIMLANG.ChanSend(ch, 3)
			})

			for i := 0; i < 3; i++ {
				switch selector := SIMLANG.Select(0, SIMLANG.RcvChan(ch), nil); selector.Case {
				case 0:
					var v int
					selector.Assign(&v)
					res = append(res, v)
				}
			}

			require.Equal(t, []int{1, 2, 3}, res)
		})
	})

	t.Run("Receive nil from single channel", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			ch := make(chan error, 1)
			SIMLANG.ChanSend(ch, nil) // is `nil`!

			err := errors.New("some error")
			switch selector := SIMLANG.Select(0, SIMLANG.RcvChan(ch), nil); selector.Case {
			case 0:
				selector.Assign(&err)
			}

			require.Nil(t, err)
		})
	})

	t.Run("Receive from multiple channels", func(t *testing.T) {
		var visitedCase1, visitedCase2 bool

		testutil.StressRun(func(seed int64) {
			var res []int
			ch1 := make(chan int)
			ch2 := make(chan int)

			SIMLANG.Go(func() {
				SIMLANG.ChanSend(ch1, 1)
				SIMLANG.ChanSend(ch2, 2)
			})

			for i := 0; i < 2; i++ {
				switch selector := SIMLANG.Select(0, SIMLANG.RcvChan(ch1), nil, 0, SIMLANG.RcvChan(ch2), nil); selector.Case {
				case 0:
					var v int
					selector.Assign(&v)
					res = append(res, v)
					visitedCase1 = true
				case 1:
					var v int
					selector.Assign(&v)
					res = append(res, v)
					visitedCase2 = true
				}
			}

			require.Equal(t, []int{1, 2}, res)
		})

		require.True(t, visitedCase1)
		require.True(t, visitedCase2)
	})

	t.Run("Receive from channel with multiple cases", func(t *testing.T) {
		var visitedCase1, visitedCase2 bool

		testutil.StressRun(func(seed int64) {
			var res []int
			ch := make(chan int)

			SIMLANG.Go(func() {
				for i := 0; i < 3; i++ {
					SIMLANG.ChanSend(ch, i)
				}
			})

			for i := 0; i < 3; i++ {
				switch selector := SIMLANG.Select(0, SIMLANG.RcvChan(ch), nil, 0, SIMLANG.RcvChan(ch), nil); selector.Case { // same channel!
				case 0:
					var v int
					selector.Assign(&v)
					res = append(res, v)
					visitedCase1 = true
				case 1:
					var v int
					selector.Assign(&v)
					res = append(res, v)
					visitedCase2 = true
				}
			}

			require.Equal(t, []int{0, 1, 2}, res)
		})

		require.True(t, visitedCase1)
		require.True(t, visitedCase2)
	})

	t.Run("Send to single channel", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int
			ch := make(chan int)

			SIMLANG.Go(func() {
				for i := 0; i < 3; i++ {
					selSndFunc1 := func() any {
						return i
					}
					switch selector := SIMLANG.Select(1, SIMLANG.SndChan(ch), selSndFunc1); selector.Case {
					case 0:
					}
				}
			})

			res = append(res, SIMLANG.ChanRcv(ch))
			res = append(res, SIMLANG.ChanRcv(ch))
			res = append(res, SIMLANG.ChanRcv(ch))

			require.Equal(t, []int{0, 1, 2}, res)
		})
	})

	t.Run("Send to multiple channels", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int

			selSndChan1 := make(chan int)
			selSndFunc1 := func() any {
				return 1
			}
			selSndChan2 := make(chan int)
			selSndFunc2 := func() any {
				return 2
			}

			SIMLANG.Go(func() {
				for i := 0; i < 2; i++ {
					switch selector := SIMLANG.Select(1, SIMLANG.SndChan(selSndChan1), selSndFunc1, 1, SIMLANG.SndChan(selSndChan2), selSndFunc2); selector.Case {
					case 0:
					case 1:
					}
				}
			})

			res = append(res, SIMLANG.ChanRcv(selSndChan1))
			res = append(res, SIMLANG.ChanRcv(selSndChan2))

			require.Equal(t, []int{1, 2}, res)
		})
	})

	t.Run("Use default case", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []any

			selRcvChan := make(chan any)
			selSndChan := make(chan any)

			for i := 0; i < 2; i++ {
				switch selector := SIMLANG.Select(0, SIMLANG.RcvChan(selRcvChan), nil, 0, SIMLANG.SndChan(selSndChan), nil, nil); selector.Case {
				case 0:
					panic("unexpected")
				case 1:
					panic("unexpected")
				case 2:
					res = append(res, "default")
				}
			}

			require.Equal(t, []any{"default", "default"}, res)
		})
	})

	t.Run("Multiple selects", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []any
			ch := make(chan any)

			// sender
			SIMLANG.Go(func() {
				for i := 0; i < 3; i++ {
					msg := i
					selSndFunc := func() any {
						return msg
					}
					switch selector := SIMLANG.Select(1, SIMLANG.SndChan(ch), selSndFunc); selector.Case {
					case 0:
					}
				}
			})

			// receiver
			for i := 0; i < 3; i++ {
				switch selector := SIMLANG.Select(0, SIMLANG.RcvChan(ch), nil); selector.Case {
				case 0:
					var v int
					selector.Assign(&v)
					res = append(res, v)
				}
			}

			require.Equal(t, []any{0, 1, 2}, res)
		})
	})

	t.Run("Receive from channel until close", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int
			ch := make(chan int)

			SIMLANG.Go(func() {
				SIMLANG.ChanSend(ch, 0)
				SIMLANG.ChanSend(ch, 1)
				SIMLANG.ChanClose(ch)
			})

		loop:
			for {
				switch selector := SIMLANG.Select(0, SIMLANG.RcvChan(ch), nil); selector.Case {
				case 0:
					if !selector.Ok {
						break loop
					}
					var v int
					selector.Assign(&v)
					res = append(res, v)
				}
			}

			require.Equal(t, []int{0, 1}, res)
		})
	})

	t.Run("Nil channel is ignored", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var selNilChan chan int // nil channel
			selRcvChan := make(chan int)

			SIMLANG.Go(func() {
				SIMLANG.ChanSend(selRcvChan, 0)
			})

			for {
				switch selector := SIMLANG.Select(0, SIMLANG.RcvChan(selNilChan), nil, 0, SIMLANG.RcvChan(selRcvChan), nil); selector.Case {
				case 0:
					panic("not expected")
				case 1:
					return
				}
			}
		})
	})

	t.Run("Nil channel with default case", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var selNilChan chan string // nil channel

			switch selector := SIMLANG.Select(0, SIMLANG.RcvChan(selNilChan), nil, nil); selector.Case {
			case 0:
				panic("not expected")
			default:
			}
		})
	})

	t.Run("Set channel to nil", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []string

			ch := make(chan string, 2)
			SIMLANG.ChanSend(ch, "1st message")
			SIMLANG.ChanSend(ch, "2nd message") // won't be read

			for i := 0; i < 2; i++ {
				switch selector := SIMLANG.Select(0, SIMLANG.RcvChan(ch), nil, nil); selector.Case {
				case 0:
					var v string
					selector.Assign(&v)
					res = append(res, v)
					ch = nil // disable channel
				case 1:
					res = append(res, "default")
				}
			}

			require.Equal(t, []string{"1st message", "default"}, res)
		})
	})

	// TOOD: buffered channels
}
