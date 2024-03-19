// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package slicex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/slicex"
)

func TestLowerBoundFunc(t *testing.T) {
	for _, tc := range []struct {
		x      []int
		target int
		want   int
	}{
		{nil, 0, 0},
		{[]int{1, 2, 3, 4, 5}, 0, 0},
		{[]int{1, 2, 3, 4, 5}, 1, 0},
		{[]int{1, 2, 3, 4, 5}, 2, 1},
		{[]int{1, 2, 3, 4, 5}, 3, 2},
		{[]int{1, 2, 3, 4, 5}, 4, 3},
		{[]int{1, 2, 3, 4, 5}, 5, 4},
		{[]int{1, 2, 3, 4, 5}, 6, 5},
	} {
		got := slicex.LowerBoundFunc(tc.x, tc.target, func(a, b int) int {
			return a - b
		})
		assert.Equal(t, tc.want, got)
	}
}
