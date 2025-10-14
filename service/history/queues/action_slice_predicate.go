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

func (a *slicePredicateAction) Run(readerGroup *ReaderGroup) bool {
	reader, ok := readerGroup.ReaderByID(DefaultReaderId)
	if !ok {
		return false
	}

	if int64(a.maxReaderCount) <= DefaultReaderId+1 {
		return false
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
		return false
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
		return false
	}

	nextReader := readerGroup.GetOrCreateReader(DefaultReaderId + 1)
	nextReader.MergeSlices(moveSlices...)
	return true
}
