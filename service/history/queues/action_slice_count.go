package queues

import (
	"slices"

	"go.temporal.io/server/service/history/tasks"
)

var _ Action = (*actionSliceCount)(nil)

type (
	actionSliceCount struct {
		attributes *AlertAttributesSlicesCount
		monitor    Monitor
	}

	compactCandidate struct {
		slice    Slice
		distance tasks.Key
	}
)

func newSliceCountAction(
	attributes *AlertAttributesSlicesCount,
	monitor Monitor,
) *actionSliceCount {
	return &actionSliceCount{
		attributes: attributes,
		monitor:    monitor,
	}
}

func (a *actionSliceCount) Name() string {
	return "slice-count"
}

func (a *actionSliceCount) Run(readerGroup *ReaderGroup) (actionTaken bool) {
	// first check if the alert is still valid
	if a.monitor.GetTotalSliceCount() <= a.attributes.CriticalSliceCount {
		return false
	}

	// then try to shrink existing slices, which may reduce slice count
	readers := readerGroup.Readers()
	for _, reader := range readers {
		reader.ShrinkSlices()
	}
	currentSliceCount := a.monitor.GetTotalSliceCount()
	if currentSliceCount <= a.attributes.CriticalSliceCount {
		return false
	}

	// have to compact (force merge) slices to reduce slice count
	preferredSliceCount := int(float64(a.attributes.CriticalSliceCount) * targetLoadFactor)

	isDefaultReader := func(readerID int64) bool { return readerID == DefaultReaderId }
	isNotDefaultReader := func(readerID int64) bool { return !isDefaultReader(readerID) }
	isUniversalPredicate := func(s Slice) bool { return tasks.IsUniverisalPredicate(s.Scope().Predicate) }
	isNotUniversalPredicate := func(s Slice) bool { return !isUniversalPredicate(s) }

	// peform compaction in four stages:
	// 1. compact slices in non-default reader with non-universal predicate
	// 2. compact slices in default reader with non-universal predicate
	// 3. compact slices in non-default reader with universal predicate
	// 4. compact slices in default reader with universal predicate
	//
	// Main reason for treating universal predicate separately is that upon compaction,
	// the resulting predicate will be universal as well. Then in the worst case,
	// one slice with universal predicate may "infect" all other slices and result in
	// a very large slice with universal predicate and upon shard reload, all tasks
	// in the slice needs to be reprocessed.
	// So compact slices with non-univerisal predicate first to minimize the impact
	// on other namespaces upon shard reload.

	actionTaken = true
	if a.findAndCompactLowSliceCount(
		readers,
		isNotDefaultReader,
		isNotUniversalPredicate,
		preferredSliceCount,
	) {
		return actionTaken
	}

	if a.findAndCompactLowSliceCount(
		readers,
		isDefaultReader,
		isNotUniversalPredicate,
		preferredSliceCount,
	) {
		return actionTaken
	}

	if a.findAndCompactLowSliceCount(
		readers,
		isNotDefaultReader,
		isUniversalPredicate,
		a.attributes.CriticalSliceCount,
	) {
		return actionTaken
	}

	_ = a.findAndCompactLowSliceCount(
		readers,
		isDefaultReader,
		isUniversalPredicate,
		a.attributes.CriticalSliceCount,
	)
	return actionTaken
}

func (a *actionSliceCount) findAndCompactLowSliceCount(
	readers map[int64]Reader,
	readerPredicate func(int64) bool,
	slicePredicate SlicePredicate,
	targetSliceCount int,
) bool {
	currentSliceCount := a.monitor.GetTotalSliceCount()
	candidates := make([]compactCandidate, 0, currentSliceCount)
	for readerID, reader := range readers {
		if !readerPredicate(readerID) {
			continue
		}

		candidates = a.appendCompactCandidatesForReader(candidates, reader, slicePredicate)
	}

	sliceToCompact := a.pickCompactCandidates(candidates, currentSliceCount-targetSliceCount)

	for readerID, reader := range readers {
		if !readerPredicate(readerID) {
			continue
		}

		reader.CompactSlices(func(s Slice) bool {
			_, ok := sliceToCompact[s]
			return ok
		})
	}

	return a.monitor.GetTotalSliceCount() <= targetSliceCount
}

func (a *actionSliceCount) appendCompactCandidatesForReader(
	candidates []compactCandidate,
	reader Reader,
	slicePredicate SlicePredicate,
) []compactCandidate {
	// find compact candidates by calculating distance between two slices
	// and sort candidates by distance
	var prevRange *Range
	prevEligible := false
	reader.WalkSlices(func(s Slice) {
		currentRange := s.Scope().Range
		currentEligible := slicePredicate(s)
		defer func() {
			prevRange = &currentRange
			prevEligible = currentEligible
		}()

		if prevRange == nil || !prevEligible || !currentEligible {
			return
		}

		candidates = append(candidates, compactCandidate{
			slice:    s,
			distance: currentRange.InclusiveMin.Sub(prevRange.ExclusiveMax),
		})
	})

	return candidates
}

func (a *actionSliceCount) pickCompactCandidates(
	candidates []compactCandidate,
	numSliceToCompact int,
) map[Slice]struct{} {
	slices.SortFunc(candidates, func(this, that compactCandidate) int {
		return this.distance.CompareTo(that.distance)
	})

	sliceToCompact := make(map[Slice]struct{}, numSliceToCompact)
	for _, candidate := range candidates[:min(numSliceToCompact, len(candidates))] {
		sliceToCompact[candidate.slice] = struct{}{}
	}

	return sliceToCompact
}
