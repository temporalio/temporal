package fc

import (
	"cmp"
	"slices"
	"time"
)

// MaxLimiters is the maximum total limiters that can apply to any task. (Currently this includes whole-queue
// limiters, in the future we may allow whole-queue limiters to be separate from this limit.)
const MaxLimiters = 3

const betterWakePriorityThreshold = 60 * time.Second

// wakePriority is a namespace-scoped value that we can order waiters by. Currently we use a
// combination of priority key and task age.
type wakePriority int64

// makeWakePriority
func makeWakePriority(pri int32, age time.Time) wakePriority {
	// pri is [1-60] (matching.maxPriorityLevels), so we can fit pri and age into 63 bits:
	// 00000000[--pri-][----------------------age---------------------]
	pri = min(max(pri, 0), 255)
	return wakePriority(pri)<<48 | wakePriority(age.UnixMilli())
}

// betterWakePriority returns true if a beats b in priority by a "significant" amount.
// Note that this is always true if a has a lower priority portion than b.
func betterWakePriority(a, b wakePriority) bool {
	return b-a > wakePriority(betterWakePriorityThreshold.Milliseconds())
}

// sortLimiters sorts limiters in place into a canonical order, so two partitions don't get in
// a quasi-deadlock by using opposite orders.
func sortLimiters(limiters []Limiter) {
	slices.SortFunc(limiters, func(a, b Limiter) int {
		if v := cmp.Compare(a.Type, b.Type); v != 0 {
			return v
		}
		return cmp.Compare(a.Key, b.Key)
	})
}
