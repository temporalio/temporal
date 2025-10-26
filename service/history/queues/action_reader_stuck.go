package queues

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/tasks"
)

var _ Action = (*actionReaderStuck)(nil)

type (
	actionReaderStuck struct {
		attributes *AlertAttributesReaderStuck
		logger     log.Logger
	}
)

func newReaderStuckAction(
	attributes *AlertAttributesReaderStuck,
	logger log.Logger,
) *actionReaderStuck {
	return &actionReaderStuck{
		attributes: attributes,
		logger:     logger,
	}
}

func (a *actionReaderStuck) Name() string {
	return "reader-stuck"
}

func (a *actionReaderStuck) Run(readerGroup *ReaderGroup) bool {
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

		if len(remaining) == 0 {
			// s can't split by both min and max of stuck range,
			// and stuck range does not contain the range of s,
			// the only possible case is s is not overlapping with stuck range at all.
			return nil, false
		}

		splitSlices = append(splitSlices, s)
		return remaining, true
	})

	if len(splitSlices) == 0 {
		return false
	}

	nextReader := readerGroup.GetOrCreateReader(a.attributes.ReaderID + 1)
	nextReader.MergeSlices(splitSlices...)
	return true
}
