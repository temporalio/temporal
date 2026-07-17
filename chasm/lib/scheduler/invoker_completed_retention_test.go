package scheduler_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// completedStart builds a completed BufferedStart whose close time is derived
// from age (larger age = older).
func completedStart(base time.Time, age int) *schedulespb.BufferedStart {
	return &schedulespb.BufferedStart{
		RequestId: fmt.Sprintf("completed-%d", age),
		Completed: &schedulespb.CompletedResult{
			CloseTime: timestamppb.New(base.Add(-time.Duration(age) * time.Minute)),
		},
	}
}

// When more than recentActionCount completed starts are present, retention keeps
// only the newest recentActionCount, drops the oldest, and preserves all
// non-completed starts.
func TestApplyCompletedRetention_TrimsOldestBeyondLimit(t *testing.T) {
	base := time.Now()
	limit := scheduler.RecentActionCount
	excess := 3

	// Newest-first: age 0 is newest, age (limit+excess-1) is oldest. Add them in
	// oldest-first order to prove retention sorts by close time rather than slice
	// position.
	var completed []*schedulespb.BufferedStart
	for age := limit + excess - 1; age >= 0; age-- {
		completed = append(completed, completedStart(base, age))
	}

	// More than `limit` pending starts, to show the retention limit applies only
	// to completed starts and never trims non-completed ones.
	pendingCount := limit + excess
	var pending []*schedulespb.BufferedStart
	for i := range pendingCount {
		pending = append(pending, &schedulespb.BufferedStart{RequestId: fmt.Sprintf("pending-%d", i)})
	}

	invoker := &scheduler.Invoker{
		InvokerState: &schedulerpb.InvokerState{
			// Interleave the pending starts so we also cover ordering.
			BufferedStarts: append(pending, completed...),
		},
	}

	invoker.ApplyCompletedRetention()

	var keptCompleted, keptPending []*schedulespb.BufferedStart
	for _, s := range invoker.BufferedStarts {
		if s.GetCompleted() != nil {
			keptCompleted = append(keptCompleted, s)
		} else {
			keptPending = append(keptPending, s)
		}
	}

	// Non-completed starts are always retained, regardless of the limit.
	require.Len(t, keptPending, pendingCount)

	// Only the newest `limit` completed starts survive.
	require.Len(t, keptCompleted, limit)
	kept := make(map[string]bool, len(keptCompleted))
	for _, s := range keptCompleted {
		kept[s.RequestId] = true
	}
	for age := range limit {
		require.True(t, kept[fmt.Sprintf("completed-%d", age)],
			"newest completed start (age %d) should be retained", age)
	}
	for age := limit; age < limit+excess; age++ {
		require.False(t, kept[fmt.Sprintf("completed-%d", age)],
			"oldest completed start (age %d) should be trimmed", age)
	}
}

// At or below the limit, retention is a no-op: every completed start is kept.
func TestApplyCompletedRetention_KeepsAllWithinLimit(t *testing.T) {
	base := time.Now()
	count := scheduler.RecentActionCount // exactly at the limit

	var completed []*schedulespb.BufferedStart
	for age := range count {
		completed = append(completed, completedStart(base, age))
	}

	invoker := &scheduler.Invoker{
		InvokerState: &schedulerpb.InvokerState{BufferedStarts: completed},
	}

	invoker.ApplyCompletedRetention()

	require.Len(t, invoker.BufferedStarts, count,
		"no completed start should be trimmed when at or below the limit")
}
