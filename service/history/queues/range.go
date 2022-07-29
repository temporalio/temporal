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

package queues

import (
	"fmt"

	"go.temporal.io/server/service/history/tasks"
)

type (
	Range struct {
		InclusiveMin tasks.Key
		ExclusiveMax tasks.Key
	}
)

func NewRange(
	inclusiveMin tasks.Key,
	exclusiveMax tasks.Key,
) Range {
	if inclusiveMin.CompareTo(exclusiveMax) > 0 {
		panic(fmt.Sprintf("invalid task range, min %v is larger than max %v", inclusiveMin, exclusiveMax))
	}

	return Range{
		InclusiveMin: inclusiveMin,
		ExclusiveMax: exclusiveMax,
	}
}

func (r *Range) IsEmpty() bool {
	return r.InclusiveMin.CompareTo(r.ExclusiveMax) == 0
}

func (r *Range) ContainsKey(
	key tasks.Key,
) bool {
	return key.CompareTo(r.InclusiveMin) >= 0 &&
		key.CompareTo(r.ExclusiveMax) < 0
}

func (r *Range) ContainsRange(
	input Range,
) bool {
	return r.InclusiveMin.CompareTo(input.InclusiveMin) <= 0 &&
		r.ExclusiveMax.CompareTo(input.ExclusiveMax) >= 0
}

func (r *Range) CanSplit(
	key tasks.Key,
) bool {
	return r.ContainsKey(key) || r.ExclusiveMax.CompareTo(key) == 0
}

func (r *Range) Split(
	key tasks.Key,
) (left Range, right Range) {
	if !r.CanSplit(key) {
		panic(fmt.Sprintf("Unable to split range %v at %v", r, key))
	}

	return NewRange(r.InclusiveMin, key), NewRange(key, r.ExclusiveMax)
}

func (r *Range) CanMerge(
	input Range,
) bool {
	return r.InclusiveMin.CompareTo(input.ExclusiveMax) <= 0 &&
		r.ExclusiveMax.CompareTo(input.InclusiveMin) >= 0
}

func (r *Range) Merge(
	input Range,
) Range {
	if !r.CanMerge(input) {
		panic(fmt.Sprintf("Unable to merge range %v with incoming range %v", r, input))
	}

	return NewRange(
		tasks.MinKey(r.InclusiveMin, input.InclusiveMin),
		tasks.MaxKey(r.ExclusiveMax, input.ExclusiveMax),
	)
}

func (r *Range) Equals(
	input Range,
) bool {
	return r.InclusiveMin.CompareTo(input.InclusiveMin) == 0 &&
		r.ExclusiveMax.CompareTo(input.ExclusiveMax) == 0
}
