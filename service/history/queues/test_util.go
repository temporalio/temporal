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
	"math"
	"math/rand"
	"time"

	"golang.org/x/exp/slices"

	"go.temporal.io/server/service/history/tasks"
)

func NewRandomKey() tasks.Key {
	return tasks.NewKey(time.Unix(0, rand.Int63()), rand.Int63())
}

func NewRandomRange() Range {
	maxKey := NewRandomKey()
	minKey := tasks.NewKey(
		time.Unix(0, rand.Int63n(maxKey.FireTime.UnixNano())),
		rand.Int63n(maxKey.TaskID),
	)
	return NewRange(minKey, maxKey)
}

func NewRandomKeyInRange(
	r Range,
) tasks.Key {
	if r.IsEmpty() {
		panic("can not create key in range for an empty range")
	}

	minFireTimeUnixNano := r.InclusiveMin.FireTime.UnixNano()
	maxFireTimeUnixNano := r.ExclusiveMax.FireTime.UnixNano()
	minTaskID := r.InclusiveMin.TaskID
	maxTaskID := r.ExclusiveMax.TaskID

	if minFireTimeUnixNano == maxFireTimeUnixNano {
		return tasks.NewKey(
			r.InclusiveMin.FireTime,
			rand.Int63n(1+maxTaskID-minTaskID)+minTaskID,
		)
	}

	fireTime := time.Unix(0, rand.Int63n(1+maxFireTimeUnixNano-minFireTimeUnixNano)+minFireTimeUnixNano)
	if fireTime.Equal(r.InclusiveMin.FireTime) {
		return tasks.NewKey(
			fireTime,
			rand.Int63n(math.MaxInt64-minTaskID)+minTaskID,
		)
	}

	if fireTime.Equal(r.ExclusiveMax.FireTime) {
		return tasks.NewKey(
			fireTime,
			rand.Int63n(maxTaskID),
		)
	}

	return tasks.NewKey(fireTime, rand.Int63())
}

func NewRandomOrderedRangesInRange(
	r Range,
	numRanges int,
) []Range {
	ranges := []Range{r}
	for len(ranges) < numRanges {
		r := ranges[0]
		left, right := r.Split(NewRandomKeyInRange(r))
		left.ExclusiveMax.FireTime.Add(-time.Nanosecond)
		right.InclusiveMin.FireTime.Add(time.Nanosecond)
		ranges = append(ranges[1:], left, right)
	}

	slices.SortFunc(ranges, func(a, b Range) bool {
		return a.InclusiveMin.CompareTo(b.InclusiveMin) < 0
	})

	return ranges
}
