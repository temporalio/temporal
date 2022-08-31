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
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/exp/slices"
)

const (
	targetLoadFactor = 0.8
)

type (
	actionSliceCount struct {
		attributes   *AlertAttributesSlicesCount
		monitor      Monitor
		completionFn actionCompletionFn
	}

	compactCandidate struct {
		slice    Slice
		distance tasks.Key
	}
)

func newSliceCountAction(
	attributes *AlertAttributesSlicesCount,
	monitor Monitor,
	completionFn actionCompletionFn,
) Action {
	return &actionSliceCount{
		attributes:   attributes,
		monitor:      monitor,
		completionFn: completionFn,
	}
}

func (a *actionSliceCount) Run(readerGroup *ReaderGroup) {
	defer a.completionFn()

	// first check if the alert is still valid
	if a.monitor.GetTotalSliceCount() <= a.attributes.CriticalSliceCount {
		return
	}

	// then try to shrink existing slices, which may reduce slice count
	readers := readerGroup.Readers()
	for _, reader := range readers {
		reader.ShrinkSlices()
	}
	currentSliceCount := a.monitor.GetTotalSliceCount()
	if currentSliceCount <= a.attributes.CriticalSliceCount {
		return
	}

	// have to compact (force merge) slices to reduce slice count
	preferredSliceCount := int(float64(a.attributes.CriticalSliceCount) * targetLoadFactor)
	numSliceToCompact := currentSliceCount - preferredSliceCount

	// find compact candidates by calculating distance between two slices
	// and sort candidates by distance
	candidates := make([]compactCandidate, 0, currentSliceCount)
	for readerID, reader := range readers {
		// first try only compacting slices in non-default reader
		if readerID == defaultReaderId {
			continue
		}

		candidates = a.appendCompactCandidatesForReader(candidates, reader)
	}
	a.sortCompactCandidates(candidates)

	sliceToCompact := make(map[Slice]struct{}, numSliceToCompact)
	for _, candidate := range candidates[:util.Min(numSliceToCompact, len(candidates))] {
		sliceToCompact[candidate.slice] = struct{}{}
	}

	// if there are not enough slices to compact in non-default readers,
	// have to compact slices in the default reader
	// note here the comparision is against CriticalSliceCount, not preferredSliceCount
	remainingToCompact := currentSliceCount - len(sliceToCompact) - a.attributes.CriticalSliceCount
	if remainingToCompact > 0 {
		candidates = a.appendCompactCandidatesForReader(nil, readers[defaultReaderId])
		a.sortCompactCandidates(candidates)
		for _, candidate := range candidates[:util.Min(remainingToCompact, len(candidates))] {
			sliceToCompact[candidate.slice] = struct{}{}
		}
	}

	// finally, perform the compaction
	for _, reader := range readers {
		reader.CompactSlices(func(s Slice) bool {
			_, ok := sliceToCompact[s]
			return ok
		})
	}
}

func (a *actionSliceCount) appendCompactCandidatesForReader(
	candidates []compactCandidate,
	reader Reader,
) []compactCandidate {
	// calculate distance between two slices
	var prevRange *Range
	reader.WalkSlices(func(s Slice) {
		currentRange := s.Scope().Range
		defer func() {
			prevRange = &currentRange
		}()

		if prevRange == nil {

			return
		}

		candidates = append(candidates, compactCandidate{
			slice:    s,
			distance: currentRange.InclusiveMin.Sub(prevRange.ExclusiveMax),
		})
	})

	return candidates
}

func (a *actionSliceCount) sortCompactCandidates(
	candidates []compactCandidate,
) {
	slices.SortFunc(candidates, func(this, that compactCandidate) bool {
		return this.distance.CompareTo(this.distance) < 0
	})
}
