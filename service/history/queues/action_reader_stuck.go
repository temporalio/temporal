package queues

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/tasks"
)

var _ Action = (*actionReaderStuck)(nil)

type (
	actionReaderStuck struct {
		attributes     *AlertAttributesReaderStuck
		maxReaderCount int64
		logger         log.Logger
	}
)

func newReaderStuckAction(
	attributes *AlertAttributesReaderStuck,
	maxReaderCount int,
	logger log.Logger,
) *actionReaderStuck {
	return &actionReaderStuck{
		attributes:     attributes,
		maxReaderCount: int64(maxReaderCount),
		logger:         logger,
	}
}

func (a *actionReaderStuck) Name() string {
	return "reader-stuck"
}

func (a *actionReaderStuck) Run(readerGroup *ReaderGroup) bool {
	nextReaderID := a.attributes.ReaderID + 1
	if nextReaderID >= a.maxReaderCount {
		a.logger.Info("Skipped reader stuck action, no lower priority reader available", tag.QueueReaderID(a.attributes.ReaderID))
		return false
	}

	reader, ok := readerGroup.ReaderByID(a.attributes.ReaderID)
	if !ok {
		a.logger.Info("Failed to get queue with readerID for reader stuck action", tag.QueueReaderID(a.attributes.ReaderID))
		return false
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
		if r.InclusiveMin.CompareTo(stuckRange.ExclusiveMax) >= 0 ||
			stuckRange.InclusiveMin.CompareTo(r.ExclusiveMax) >= 0 {
			return nil, false
		}

		if stuckRange.ContainsRange(r) {
			splitSlices = append(splitSlices, s)
			return nil, true
		}

		// s only partially overlaps the stuck range, so at least one of the bounds
		// below falls strictly inside s and splitting can not produce an empty slice.
		remaining := make([]Slice, 0, 2)
		if r.InclusiveMin.CompareTo(stuckRange.InclusiveMin) < 0 {
			left, right := s.SplitByRange(stuckRange.InclusiveMin)
			remaining = append(remaining, left)
			s = right
		}

		if s.Scope().Range.ExclusiveMax.CompareTo(stuckRange.ExclusiveMax) > 0 {
			left, right := s.SplitByRange(stuckRange.ExclusiveMax)
			remaining = append(remaining, right)
			s = left
		}

		splitSlices = append(splitSlices, s)
		return remaining, true
	})

	if len(splitSlices) == 0 {
		return false
	}

	nextReader := readerGroup.GetOrCreateReader(nextReaderID)
	nextReader.MergeSlices(splitSlices...)
	return true
}
