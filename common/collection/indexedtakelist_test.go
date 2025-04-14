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

package collection_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/collection"
)

func TestIndexedTakeList(t *testing.T) {
	t.Parallel()

	type Value struct{ ID int }
	values := []Value{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}}
	indexer := func(v Value) int { return v.ID }

	list := collection.NewIndexedTakeList(values, indexer)

	t.Run("take absent", func(t *testing.T) {
		_, ok := list.Take(999)
		require.False(t, ok)
	})

	t.Run("take present", func(t *testing.T) {
		v, ok := list.Take(3)
		require.True(t, ok)
		require.Equal(t, 3, v.ID)
	})

	t.Run("take taken", func(t *testing.T) {
		_, ok := list.Take(3)
		require.False(t, ok)
	})

	t.Run("take remaining", func(t *testing.T) {
		allowed := []int{0, 1, 2, 4}
		remaining := list.TakeRemaining()
		require.Len(t, remaining, len(allowed))
		for _, v := range remaining {
			require.Contains(t, allowed, v.ID)
		}
	})

	t.Run("now empty", func(t *testing.T) {
		require.Empty(t, list.TakeRemaining())
	})
}
