package queues

import (
	"math"
	"math/rand"
	"slices"
	"time"

	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

func NewRandomKey() tasks.Key {
	return tasks.NewKey(time.Unix(0, rand.Int63()).UTC(), rand.Int63())
}

func NewRandomRange() Range {
	maxKey := NewRandomKey()
	minKey := tasks.NewKey(
		time.Unix(0, rand.Int63n(maxKey.FireTime.UnixNano())).UTC(),
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
		left.ExclusiveMax.FireTime = left.ExclusiveMax.FireTime.Add(-time.Nanosecond)
		right.InclusiveMin.FireTime = right.InclusiveMin.FireTime.Add(time.Nanosecond)
		ranges = append(ranges[1:], left, right)
	}

	slices.SortFunc(ranges, func(a, b Range) int {
		return a.InclusiveMin.CompareTo(b.InclusiveMin)
	})

	return ranges
}

func NewRandomScopes(
	numScopes int,
) []Scope {
	ranges := NewRandomOrderedRangesInRange(
		NewRandomRange(),
		numScopes,
	)

	scopes := make([]Scope, 0, 10)
	for _, r := range ranges {
		scopes = append(scopes, NewScope(r, predicates.Universal[tasks.Task]()))
	}

	return scopes
}
