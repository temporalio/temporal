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

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepeatSlice(t *testing.T) {
	t.Run("when input slice is nil should return nil", func(t *testing.T) {
		got := RepeatSlice[int](nil, 5)
		require.Nil(t, got, "RepeatSlice produced non-nil slice from nil input")
	})
	t.Run("when input slice is empty should return empty", func(t *testing.T) {
		empty := []int{}
		got := RepeatSlice(empty, 5)
		require.Len(t, got, 0, "RepeatSlice filled empty slice")
	})
	t.Run("when requested repeat number equal 0 should return empty slice", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		got := RepeatSlice(xs, 0)
		require.Len(t, got, 0, "RepeatSlice with repeat count 0 returned non-empty slice")
	})
	t.Run("when requested repeat number is less than 0 should return empty slice", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		got := RepeatSlice(xs, -1)
		require.Len(t, got, 0, "RepeatSlice with repeat count -1 returned non-empty slice")
	})
	t.Run("when requested repeat number is 3 should return slice three times the input", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		got := RepeatSlice(xs, 3)
		require.Len(t, got, len(xs)*3, "RepeatSlice produced slice of wrong length: expected %d got %d", len(xs)*3, len(got))
		for i, v := range got {
			require.Equal(t, xs[i%len(xs)], v, "RepeatSlice wrong value in result: expected %d at index %d but got %d", xs[i%len(xs)], i, v)
		}
	})
	t.Run("should not change the input slice when truncating", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		_ = RepeatSlice(xs, 0)
		require.Len(t, xs, 5, "Repeat slice truncated the original slice: expected {1, 2, 3, 4, 5}, got %v", xs)
	})
	t.Run("should not change the input slice when replicating", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		_ = RepeatSlice(xs, 5)
		require.Len(t, xs, 5, "Repeat slice changed the original slice: expected {1, 2, 3, 4, 5}, got %v", xs)
	})
}

func TestMapSlice(t *testing.T) {
	t.Run("when given nil as slice should return nil", func(t *testing.T) {
		ys := MapSlice(nil, func(x int) uint32 { return uint32(x) })
		require.Nil(t, ys, "mapping over nil produced non nil got %v", ys)
	})
	t.Run("when given an empty slice should return empty slice", func(t *testing.T) {
		xs := []int{}
		var ys []uint32
		ys = MapSlice(xs, func(x int) uint32 { return uint32(x) })
		require.Len(t, ys, 0, "mapping over empty slice produced non empty slice got %v", ys)
	})
	t.Run("when given a slice and a function should apply function to every element of the original slice", func(t *testing.T) {
		xs := []int{1, 2, 3, 4, 5}
		ys := MapSlice(xs, func(x int) int { return x + 1 })
		for i, y := range ys {
			require.Equal(t, xs[i]+1, y, "mapping over slice did not apply function expected {2, 3, 4, 5} got %v", ys)
		}
	})
}
