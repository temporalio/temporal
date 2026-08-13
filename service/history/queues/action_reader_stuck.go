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
		monitor        Monitor
		maxReaderCount int
		logger         log.Logger
	}
)

func newReaderStuckAction(
	attributes *AlertAttributesReaderStuck,
	monitor Monitor,
	maxReaderCount int,
	logger log.Logger,
) *actionReaderStuck {
	return &actionReaderStuck{
		attributes:     attributes,
		monitor:        monitor,
		maxReaderCount: maxReaderCount,
		logger:         logger,
	}
}

func (a *actionReaderStuck) Name() string {
	return "reader-stuck"
}

func (a *actionReaderStuck) Run(readerGroup *ReaderGroup) bool {
	nextReaderID := a.attributes.ReaderID + 1
	if nextReaderID >= int64(a.maxReaderCount) {
		return a.decline()
	}

	reader, ok := readerGroup.ReaderByID(a.attributes.ReaderID)
	if !ok {
		a.logger.Info("Failed to get queue with readerID for reader stuck action", tag.QueueReaderID(a.attributes.ReaderID))
		return a.decline()
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
		if r.CanSplitStrictly(stuckRange.InclusiveMin) {
			left, right := s.SplitByRange(stuckRange.InclusiveMin)
			remaining = append(remaining, left)
			s = right
		}

		if r.CanSplitStrictly(stuckRange.ExclusiveMax) {
			left, right := s.SplitByRange(stuckRange.ExclusiveMax)
			remaining = append(remaining, right)
			s = left
		}

		if len(remaining) == 0 {
			// s lies outside the stuck range or only abuts it, so any split here
			// would produce an empty slice.
			return nil, false
		}

		splitSlices = append(splitSlices, s)
		return remaining, true
	})

	if len(splitSlices) == 0 {
		return a.decline()
	}

	nextReader := readerGroup.GetOrCreateReader(nextReaderID)
	nextReader.MergeSlices(splitSlices...)
	return true
}

// decline silences the alert before reporting that nothing was mitigated.
// Nothing about the reader changed, so the next read at the same watermark would
// otherwise re-alert immediately and checkpoint the queue on every read.
func (a *actionReaderStuck) decline() bool {
	a.monitor.SilenceAlert(AlertTypeReaderStuck)
	return false
}
