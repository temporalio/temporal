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

// Package slicex (read "slice extensions") provides additional functionality for slices that is not provided by the
// standard library or the slices package.
package slicex

// LowerBoundFunc is like [slices.BinarySearchFunc], but for inequalities. It's the same as std::lower_bound in C++.
// It returns the first index in [0, len(x)] that does not compare less than target.
func LowerBoundFunc[S ~[]E, E, T any](x S, target T, cmp func(E, T) int) int {
	l, r := 0, len(x)
	for l < r {
		m := l + (r-l)/2
		if cmp(x[m], target) < 0 {
			l = m + 1
		} else {
			r = m
		}
	}
	return l
}
