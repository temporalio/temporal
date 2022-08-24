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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/tasks"
)

type (
	actionReaderStuck struct {
		attributes   *AlertAttributesReaderStuck
		completionFn actionCompletionFn
		logger       log.Logger
	}
)

func newReaderStuckAction(
	attributes *AlertAttributesReaderStuck,
	completionFn actionCompletionFn,
	logger log.Logger,
) *actionReaderStuck {
	return &actionReaderStuck{
		attributes:   attributes,
		completionFn: completionFn,
		logger:       logger,
	}
}

func (a *actionReaderStuck) Run(readerGroup *ReaderGroup) {
	defer a.completionFn()

	reader, ok := readerGroup.ReaderByID(a.attributes.ReaderID)
	if !ok {
		a.logger.Error("Failed to get queue with readerID for reader stuck action", tag.QueueReaderID(a.attributes.ReaderID))
		return
	}

	stuckRange := NewRange(
		a.attributes.CurrentWatermark,
		tasks.NewKey(
			a.attributes.CurrentWatermark.FireTime.Add(monitorWatermarkPrecision),
			a.attributes.CurrentWatermark.TaskID,
		),
	)

	var splitSlices []Slice
	reader.SplitSlices(func(s Slice) ([]Slice, bool) {
		r := s.Scope().Range
		if stuckRange.ContainsRange(r) {
			splitSlices = append(splitSlices, s)
			return nil, true
		}

		remaining := make([]Slice, 0, 2)
		if s.CanSplitByRange(stuckRange.InclusiveMin) {
			left, right := s.SplitByRange(stuckRange.InclusiveMin)
			remaining = append(remaining, left)
			s = right
		}

		if s.CanSplitByRange(stuckRange.ExclusiveMax) {
			left, right := s.SplitByRange(stuckRange.ExclusiveMax)
			remaining = append(remaining, right)
			s = left
		}

		splitSlices = append(splitSlices, s)
		return remaining, true
	})

	if len(splitSlices) == 0 {
		return
	}

	nextReader, ok := readerGroup.ReaderByID(a.attributes.ReaderID + 1)
	if ok {
		nextReader.MergeSlices(splitSlices...)
		return
	}

	readerGroup.NewReader(a.attributes.ReaderID+1, splitSlices...)
}
