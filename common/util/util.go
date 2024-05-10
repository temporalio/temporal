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

// util contains small standalone utility functions. This should have no
// dependencies on other server packages.
package util

import (
	"sort"
	"time"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/maps"
)

// MinTime returns the earlier of two given time.Time
func MinTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// MaxTime returns the later of two given time.Time
func MaxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// NextAlignedTime returns the earliest time after `t` that is aligned to an integer multiple
// of `align` since the unix epoch.
func NextAlignedTime(t time.Time, align time.Duration) time.Time {
	return time.Unix(0, (t.UnixNano()/int64(align)+1)*int64(align))
}

// SortSlice sorts the given slice of an ordered type.
// Sort is not guaranteed to be stable.
func SortSlice[S ~[]E, E constraints.Ordered](slice S) {
	sort.Slice(slice, func(i, j int) bool {
		return slice[i] < slice[j]
	})
}

// SliceHead returns the first n elements of s. n may be greater than len(s).
func SliceHead[S ~[]E, E any](s S, n int) S {
	if n < len(s) {
		return s[:n]
	}
	return s
}

// SliceTail returns the last n elements of s. n may be greater than len(s).
func SliceTail[S ~[]E, E any](s S, n int) S {
	if extra := len(s) - n; extra > 0 {
		return s[extra:]
	}
	return s
}

// CloneMapNonNil is like maps.Clone except it can't return nil, it will return an empty map instead.
func CloneMapNonNil[M ~map[K]V, K comparable, V any](m M) M {
	m = maps.Clone(m)
	if m == nil {
		m = make(M)
	}
	return m
}

// InverseMap creates the inverse map, ie., for a key-value map, it builds the value-key map.
func InverseMap[M ~map[K]V, K, V comparable](m M) map[V]K {
	if m == nil {
		return nil
	}
	invm := make(map[V]K, len(m))
	for k, v := range m {
		invm[v] = k
	}
	return invm
}

// MapConcurrent concurrently maps a function over input and fails fast on error.
func MapConcurrent[IN any, OUT any](input []IN, mapper func(IN) (OUT, error)) ([]OUT, error) {
	errorsCh := make(chan error, len(input))
	results := make([]OUT, len(input))

	for i, in := range input {
		i := i
		in := in
		go func() {
			var err error
			results[i], err = mapper(in)
			errorsCh <- err
		}()
	}
	for range input {
		if err := <-errorsCh; err != nil {
			return nil, err
		}
	}
	return results, nil
}

// MapSlice given slice xs []T and f(T) S produces slice []S by applying f to every element of xs
func MapSlice[T, S any](xs []T, f func(T) S) []S {
	if xs == nil {
		return nil
	}
	result := make([]S, len(xs))
	for i, s := range xs {
		result[i] = f(s)
	}
	return result
}

// FilterSlice iterates over elements of a slice, returning a new slice of all elements predicate returns true for.
func FilterSlice[T any](in []T, predicate func(T) bool) []T {
	var out []T
	for _, elem := range in {
		if predicate(elem) {
			out = append(out, elem)
		}
	}
	return out
}

// FoldSlice folds left a slice using given reducer function and initial value.
func FoldSlice[T any, A any](in []T, initializer A, reducer func(A, T) A) A {
	acc := initializer
	for _, val := range in {
		acc = reducer(acc, val)
	}
	return acc
}

// RepeatSlice given slice and a number (n) produces a new slice containing original slice n times
// if n is non-positive will produce nil
func RepeatSlice[T any](xs []T, n int) []T {
	if xs == nil || n <= 0 {
		return nil
	}
	ys := make([]T, n*len(xs))
	for i := 0; i < n; i++ {
		copy(ys[i*len(xs):], xs)
	}
	return ys
}

// Ptr returns a pointer to a copy of v.
func Ptr[T any](v T) *T {
	return &v
}
