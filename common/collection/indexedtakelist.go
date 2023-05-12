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

package collection

import "math/big"

// IndexedTakeList holds a set of values that can only be observed by being
// removed from the set. It is possible for this set to contain duplicate values
// as long as each value maps to a distinct index.
type (
	IndexedTakeList[K comparable, V any] struct {
		values  []kv[K, V]
		removed big.Int
	}

	kv[K comparable, V any] struct {
		key   K
		value V
	}
)

// NewIndexedTakeList constructs a new IndexedTakeSet by applying the provided
// indexer to each of the provided values.
func NewIndexedTakeList[K comparable, V any](
	values []V,
	indexer func(V) K,
) *IndexedTakeList[K, V] {
	ret := &IndexedTakeList[K, V]{
		values: make([]kv[K, V], 0, len(values)),
	}
	for _, v := range values {
		ret.values = append(ret.values, kv[K, V]{key: indexer(v), value: v})
	}
	return ret
}

// Take finds a value in this set by its index and removes it, returning the
// value.
func (itl *IndexedTakeList[K, V]) Take(key K) (V, bool) {
	var zero V
	for i, kv := range itl.values {
		if kv.key != key {
			continue
		}
		if itl.removed.Bit(i) != 0 {
			return zero, false
		}
		itl.removed.SetBit(&itl.removed, i, 1)
		return kv.value, true
	}
	return zero, false
}

// TakeRemaining removes all remaining values from this set and returns them.
func (itl *IndexedTakeList[K, V]) TakeRemaining() []V {
	out := make([]V, 0, len(itl.values))
	for i, kv := range itl.values {
		if itl.removed.Bit(i) == 0 {
			out = append(out, kv.value)
		}
	}
	itl.values = nil
	return out
}
