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

func TestGoroutine(t *testing.T) {

	t.Run("Empty", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			// empty
		})
	})

	t.Run("Single goroutine", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var done bool

			SIMLANG.Go(func() {
				done = true
			})

			SIM.Join()

			require.True(t, done)
		})
	})

	t.Run("Multiple goroutines", func(t *testing.T) {
		testutil.StressRun(func(seed int64) {
			var res []int

			SIMLANG.Go(func() {
				res = append(res, 0)
			})

			SIMLANG.Go(func() {
				res = append(res, 1)
			})

			SIM.Join()

			require.ElementsMatch(t, []int{0, 1}, res)
		})
	})

	t.Run("Abort goroutine via hook", func(t *testing.T) {
		t.Skip()

		var res []int

		testutil.StressRun(
			func(seed int64) {
				SIMLANG.Go(func() {
					res = append(res, 1)
				})

				SIMLANG.Go(func() {
					res = append(res, 2)
				})

				SIM.Join()

				require.Len(t, res, 1)
			},
			SIM.Hook(func(i *SIM.Info) {
				// stop simulation once *first* goroutine is done
				if len(res) == 1 {
					close(i.Stop)
				}
			}))
	})
}
