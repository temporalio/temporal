package scheduler_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBackfillCapacityNativeControl(t *testing.T) {
	testBackfillCapacityProgress(t, 450)
}

func TestBackfillCapacityProgress(t *testing.T) {
	for _, n := range []int{451, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) { testBackfillCapacityProgress(t, n) })
	}
}

func testBackfillCapacityProgress(t *testing.T, count int) {
	t.Helper()
	now := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	ts := clock.NewEventTimeSource().Update(now)
	spec := defaultSchedule()
	spec.State.Paused = true
	e := newSchedulerTestEngine(t, spec, withEngineTimeSource(ts))
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		for range count {
			s.NewRangeBackfiller(ctx, &schedulepb.BackfillRequest{
				StartTime: timestamppb.New(now), EndTime: timestamppb.New(now),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			})
		}
		return nil
	}))
	seen := make(map[string]string)
	remaining := count
	for round := 0; round < 50 && remaining > 0; round++ {
		buffered := 0
		require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
			i := s.Invoker.Get(ctx)
			buffered = len(i.BufferedStarts)
			require.LessOrEqual(t, buffered, 460, "shared half-buffer minus generator reserve plus retained-history allowance")
			for _, start := range i.BufferedStarts {
				require.NotContains(t, seen, start.RequestId)
				seen[start.RequestId] = start.WorkflowId
			}
			i.BufferedStarts = nil
			remaining = len(s.Backfillers)
			return nil
		}))
		if remaining == 0 {
			break
		}
		require.Positive(t, buffered, "seed=capacity-%d: empty buffer and %d ranges must make progress", count, remaining)
		ts.Update(ts.Now().Add(time.Hour))
		_, err := e.engine.FirePureTasks(e.rootRef, ts.Now())
		require.NoError(t, err)
	}
	require.Zero(t, remaining)
	require.Len(t, seen, count)
}
