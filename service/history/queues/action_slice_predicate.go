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
	"go.temporal.io/server/service/history/tasks"
)

const (
	moveSliceDefaultReaderMinPendingTaskCount = 50
	moveSliceDefaultReaderMinSliceCount       = 3
)

var _ Action = (*slicePredicateAction)(nil)

type (
	// slicePredicateAction will move all slices in default reader
	// with non-universal predicate to the next reader so that upon restart
	// task loading for those slices won't blocking loading for other slices
	// in the default reader.
	//
	// Slice.ShrinkScope() will shrink its predicate when there's few
	// namespaces left. But those slices may still in the default reader.
	// If there are many such slices in the default reader, then upon restart
	// task loading for other namespace will be blocked. So we need to move those
	// slices to a different reader.
	//
	// NOTE: When there's no restart/shard movement, this movement won't affect
	// anything, as slice with non-universal predicate must have already loaded
	// all tasks into memory.
	slicePredicateAction struct {
		monitor        Monitor
		maxReaderCount int
	}
)

func newSlicePredicateAction(
	monitor Monitor,
	maxReaderCount int,
) *slicePredicateAction {
	return &slicePredicateAction{
		monitor:        monitor,
		maxReaderCount: maxReaderCount,
	}
}

func (a *slicePredicateAction) Name() string {
	return "slice-predicate"
}

func (a *slicePredicateAction) Run(readerGroup *ReaderGroup) {
	reader, ok := readerGroup.ReaderByID(DefaultReaderId)
	if !ok {
		return
	}

	if int64(a.maxReaderCount) <= DefaultReaderId+1 {
		return
	}

	sliceCount := a.monitor.GetSliceCount(DefaultReaderId)
	pendingTasks := 0
	hasNonUniversalPredicate := false
	reader.WalkSlices(func(s Slice) {
		pendingTasks += a.monitor.GetSlicePendingTaskCount(s)

		if !tasks.IsUniverisalPredicate(s.Scope().Predicate) {
			hasNonUniversalPredicate = true
		}
	})

	// only move slices when either default reader slice count or
	// pending task count is high
	if !hasNonUniversalPredicate ||
		(pendingTasks < moveSliceDefaultReaderMinPendingTaskCount &&
			sliceCount < moveSliceDefaultReaderMinSliceCount) {
		return
	}

	var moveSlices []Slice
	reader.SplitSlices(func(s Slice) (remaining []Slice, split bool) {
		if tasks.IsUniverisalPredicate(s.Scope().Predicate) {
			return []Slice{s}, false
		}

		moveSlices = append(moveSlices, s)
		return nil, true
	})

	if len(moveSlices) == 0 {
		return
	}

	nextReader := readerGroup.GetOrCreateReader(DefaultReaderId + 1)
	nextReader.MergeSlices(moveSlices...)
}
