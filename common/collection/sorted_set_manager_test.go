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

package collection_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/collection"
)

type element struct {
	key   string
	value int
}

func TestSortedSetManager_Add(t *testing.T) {
	m := newManager()
	var s []element
	s, ok := m.Add(s, "a", func() element { return element{"a", 1} })
	require.True(t, ok)
	s, ok = m.Add(s, "a", func() element { return element{"a", 2} })
	require.False(t, ok)
	assert.Len(t, s, 1)
	assert.Equal(t, "a", s[0].key)
}

func TestSortedSetManager_Get(t *testing.T) {
	m := newManager()
	var s []element
	s, ok := m.Add(s, "a", func() element { return element{"a", 1} })
	require.True(t, ok)
	e, ok := m.Get(s, "a")
	require.True(t, ok)
	assert.Equal(t, "a", e.key)
	_, ok = m.Get(s, "b")
	require.False(t, ok)
}

func TestSortedSetManager_Paginate(t *testing.T) {
	m := newManager()
	var s []element
	s, _ = m.Add(s, "a", func() element { return element{"a", 1} })
	s, _ = m.Add(s, "b", func() element { return element{"b", 2} })
	s, _ = m.Add(s, "c", func() element { return element{"c", 3} })
	page, lastKey := m.Paginate(s, "", 2)
	require.Len(t, page, 2)
	assert.Equal(t, "a", page[0].key)
	assert.Equal(t, "b", page[1].key)
	require.NotNil(t, lastKey)
	page, lastKey = m.Paginate(s, *lastKey, 2)
	require.Len(t, page, 1)
	assert.Equal(t, "c", page[0].key)
	require.Nil(t, lastKey)
}

func TestSortedSetManager_Update(t *testing.T) {
	m := newManager()
	var s []element
	s, ok := m.Add(s, "a", func() element { return element{"a", 1} })
	require.True(t, ok)
	s, ok = m.Update(s, "a", func(e element) element {
		e.value = 2
		return e
	})
	require.True(t, ok)
	e, ok := m.Get(s, "a")
	require.True(t, ok)
	assert.Equal(t, 2, e.value)
	s, ok = m.Update(s, "b", func(e element) element {
		e.value = 3
		return e
	})
	require.False(t, ok)
	require.Len(t, s, 1)
	assert.Equal(t, 2, s[0].value)
}

func TestSortedSetManager_Remove(t *testing.T) {
	m := newManager()
	var s []element
	s, _ = m.Add(s, "a", func() element { return element{"a", 1} })
	s, ok := m.Remove(s, "a")
	require.True(t, ok)
	s, ok = m.Remove(s, "a")
	require.False(t, ok)
	assert.Empty(t, s)
}

func newManager() *collection.SortedSetManager[[]element, element, string] {
	cmp := func(e element, k string) int {
		return strings.Compare(e.key, k)
	}
	key := func(e element) string {
		return e.key
	}
	return collection.NewSortedSetManager[[]element, element, string](cmp, key)
}
