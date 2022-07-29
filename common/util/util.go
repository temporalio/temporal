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
)

// Min returns the minimum of two comparable values.
func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

// Min returns the maximum of two comparable values.
func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// MinTime returns the earlier of two given time.Time
func MinTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// MinTime returns the later of two given time.Time
func MaxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
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
