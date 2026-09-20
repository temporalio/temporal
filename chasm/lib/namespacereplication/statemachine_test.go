package namespacereplication

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
)

// TestPeerRetryBackoff pins the capped-exponential backoff schedule for peer
// retries: base doubling per attempt, clamped at peerRetryMaxInterval, and
// overflow-safe for large attempt counts.
func TestPeerRetryBackoff(t *testing.T) {
	testCases := []struct {
		name    string
		attempt int32
		want    time.Duration
	}{
		{name: "attempt below 1 uses base", attempt: 0, want: peerRetryBaseInterval},
		{name: "negative attempt uses base", attempt: -3, want: peerRetryBaseInterval},
		{name: "attempt 1 = base", attempt: 1, want: peerRetryBaseInterval},
		{name: "attempt 2 = 2x base", attempt: 2, want: 2 * peerRetryBaseInterval},
		{name: "attempt 3 = 4x base", attempt: 3, want: 4 * peerRetryBaseInterval},
		{name: "attempt 9 still under cap", attempt: 9, want: 256 * peerRetryBaseInterval},
		{name: "attempt 10 exceeds cap -> clamped", attempt: 10, want: peerRetryMaxInterval},
		{name: "attempt 20 clamped", attempt: 20, want: peerRetryMaxInterval},
		{name: "very large attempt clamped (overflow-safe)", attempt: 1000, want: peerRetryMaxInterval},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := peerRetryBackoff(tc.attempt)
			require.Equal(t, tc.want, got)
			require.LessOrEqual(t, got, peerRetryMaxInterval)
			require.GreaterOrEqual(t, got, peerRetryBaseInterval)
		})
	}
}

func TestTransitionLocalCommittedSchedulesOutboundTasks(t *testing.T) {
	c := NewNamespaceMutationComponent(&namespacereplicationpb.NamespaceMutation{PeerCells: []string{"cellB", "cellC"}})
	ctx := &chasm.MockMutableContext{}

	now := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	require.NoError(t, TransitionLocalCommitted.Apply(c, ctx, EventLocalCommitted{Time: now}))
	require.Len(t, ctx.Tasks, 2)
	for i, cell := range []string{"cellB", "cellC"} {
		task, ok := ctx.Tasks[i].Payload.(*namespacereplicationpb.ApplyPeerTask)
		require.True(t, ok)
		require.Equal(t, cell, task.GetTargetCell())
		require.Equal(t, cell, ctx.Tasks[i].Attributes.Destination)
		require.True(t, ctx.Tasks[i].Attributes.ScheduledTime.IsZero())
	}
}

func TestTransitionLocalShadowSkippedSchedulesOutboundTasks(t *testing.T) {
	c := NewNamespaceMutationComponent(&namespacereplicationpb.NamespaceMutation{PeerCells: []string{"cellB"}})
	ctx := &chasm.MockMutableContext{}

	now := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	require.NoError(t, TransitionLocalShadowSkipped.Apply(c, ctx, EventLocalShadowSkipped{Time: now}))
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_SKIPPED_SHADOW, c.GetLocalApply().GetOutcome())
	require.Len(t, ctx.Tasks, 1)
}

func TestTransitionPeerRetryUsesPureTimerTask(t *testing.T) {
	c := NewNamespaceMutationComponent(&namespacereplicationpb.NamespaceMutation{PeerCells: []string{"cellB"}})
	c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
	ctx := &chasm.MockMutableContext{}
	now := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)

	require.NoError(t, TransitionPeerRetry.Apply(c, ctx, EventPeerRetry{
		Time:       now,
		TargetCell: "cellB",
		Attempt:    1,
		Err:        errors.New("down"),
	}))
	require.Len(t, ctx.Tasks, 1)
	task, ok := ctx.Tasks[0].Payload.(*namespacereplicationpb.ApplyPeerBackoffTask)
	require.True(t, ok)
	require.Equal(t, "cellB", task.GetTargetCell())
	require.Equal(t, int32(1), task.GetAttempt())
	require.Empty(t, ctx.Tasks[0].Attributes.Destination)
	require.Equal(t, now.Add(peerRetryBackoff(1)), ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestApplyPeerBackoffTaskReenqueuesOutboundTask(t *testing.T) {
	c := NewNamespaceMutationComponent(&namespacereplicationpb.NamespaceMutation{PeerCells: []string{"cellB"}})
	c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
	c.PeerApply["cellB"].AttemptCount = 2
	h := newApplyPeerBackoffTaskHandler()
	task := &namespacereplicationpb.ApplyPeerBackoffTask{TargetCell: "cellB", Attempt: 2}

	valid, err := h.Validate(nil, c, chasm.TaskInvocation{}, task)
	require.NoError(t, err)
	require.True(t, valid)

	ctx := &chasm.MockMutableContext{}
	require.NoError(t, h.Execute(ctx, c, chasm.TaskAttributes{}, task))
	require.Len(t, ctx.Tasks, 1)
	peerTask, ok := ctx.Tasks[0].Payload.(*namespacereplicationpb.ApplyPeerTask)
	require.True(t, ok)
	require.Equal(t, "cellB", peerTask.GetTargetCell())
	require.Equal(t, int32(2), peerTask.GetAttempt())
	require.Equal(t, "cellB", ctx.Tasks[0].Attributes.Destination)
	require.True(t, ctx.Tasks[0].Attributes.ScheduledTime.IsZero())
}
