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

package collection

import "slices"

// SortedSetManager provides CRUD functionality for in-memory sorted sets. Note that there's no Update method because
// you can just use the [SortedSetManager.Get] method and update that index directly.
type SortedSetManager[S ~[]E, E, K any] struct {
	cmp func(E, K) int
	key func(E) K
}

// NewSortedSetManager returns a new SortedSetManager with the given comparison function and key function.
func NewSortedSetManager[S ~[]E, E, K any](cmp func(E, K) int, key func(E) K) SortedSetManager[S, E, K] {
	return SortedSetManager[S, E, K]{cmp, key}
}

// Add adds a new element to the set. If the element is already in the set, it returns the set unchanged and false.
func (m SortedSetManager[S, E, K]) Add(set S, e E) (S, bool) {
	i, found := m.find(set, m.key(e))
	if found {
		return set, false
	}
	return slices.Insert(set, i, e), true
}

// Get returns the index of the element in the set that compares equal to key or -1 if no such element exists.
func (m SortedSetManager[S, E, K]) Get(set S, key K) int {
	i, found := m.find(set, key)
	if !found {
		return -1
	}
	return i
}

// Paginate returns up to n elements in the set that compare greater than gtKey. If there are more than n such elements,
// it also returns the key of the last element in the page. Otherwise, the second return value is nil.
func (m SortedSetManager[S, E, K]) Paginate(set S, gtKey K, n int) (S, *K) {
	i, exists := m.find(set, gtKey)
	if exists {
		i++
	}
	var (
		lastKey *K
		page    S
	)
	if i+n >= len(set) {
		page = set[i:]
	} else {
		tmp := m.key(set[i+n-1])
		lastKey = &tmp
		page = set[i : i+n]
	}
	return page, lastKey
}

// Remove removes an element from the set. If the element is not in the set, it returns the set unchanged and false.
func (m SortedSetManager[S, E, K]) Remove(set S, key K) (S, bool) {
	i, found := m.find(set, key)
	if !found {
		return set, false
	}
	return slices.Delete(set, i, i+1), true
}

func (m SortedSetManager[S, E, K]) find(set S, key K) (int, bool) {
	return slices.BinarySearchFunc(set, key, m.cmp)
}
