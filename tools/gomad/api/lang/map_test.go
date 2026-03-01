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
	"go.temporal.io/server/tools/gomad/runtime/testutil"
)

func TestMap(t *testing.T) {

	t.Run("Init", func(t *testing.T) {
		fn := func() []string {
			m := SIMLANG.MapInit[string, int](
				"A", 0,
				"B", 1,
				"C", 2,
			)

			require.Equal(t, map[string]int{
				"A": 0,
				"B": 1,
				"C": 2,
			}, m)

			return SIMLANG.MapKeys(m)
		}

		for i := 0; i < testutil.TestRuns; i++ {
			var keys1, keys2 []string
			testutil.SingleRun(func(seed int64) { keys1 = fn() }, int64(i))
			testutil.SingleRun(func(seed int64) { keys2 = fn() }, int64(i))
			require.Equal(t, keys1, keys2)
		}
	})

	t.Run("Write", func(t *testing.T) {
		fn := func() []string {
			m := map[string]int{}
			m["A"] = 0
			SIMLANG.MapKey("A")
			m["B"] = 1
			SIMLANG.MapKey("B")
			m["C"] = 2
			SIMLANG.MapKey("C")

			require.Equal(t, map[string]int{
				"A": 0,
				"B": 1,
				"C": 2,
			}, m)

			return SIMLANG.MapKeys(m)
		}

		for i := 0; i < testutil.TestRuns; i++ {
			var keys1, keys2 []string
			testutil.SingleRun(func(seed int64) { keys1 = fn() }, int64(i))
			testutil.SingleRun(func(seed int64) { keys2 = fn() }, int64(i))
			require.Equal(t, keys1, keys2)
		}
	})
}
