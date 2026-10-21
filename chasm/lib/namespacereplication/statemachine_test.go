package namespacereplication

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
)

func TestLocalRetryBackoff(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		attempt int32
		want    time.Duration
	}{
		{name: "attempt below one uses base", attempt: 0, want: time.Second},
		{name: "attempt one uses base", attempt: 1, want: time.Second},
		{name: "attempt two doubles", attempt: 2, want: 2 * time.Second},
		{name: "attempt three doubles again", attempt: 3, want: 4 * time.Second},
		{name: "attempt nine remains below cap", attempt: 9, want: 256 * time.Second},
		{name: "attempt ten is capped", attempt: 10, want: 5 * time.Minute},
		{name: "large attempt is capped", attempt: 1000, want: 5 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, localRetryBackoff(tc.attempt))
		})
	}
}

func TestTransitionLocalRetrySchedulesDurableTimer(t *testing.T) {
	c := NewNamespaceMutationComponent(&namespacereplicationpb.NamespaceMutation{})
	ctx := &chasm.MockMutableContext{}
	now := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	delay := 2 * time.Second

	require.NoError(t, TransitionLocalRetry.Apply(c, ctx, EventLocalRetry{
		Time:     now,
		Attempts: 2,
		Delay:    delay,
		Err:      errors.New("outcome unresolved"),
	}))

	status := c.GetLocalApply()
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING, status.GetOutcome())
	require.Equal(t, int32(2), status.GetAttemptCount())
	require.Equal(t, now, status.GetFirstAttemptAt().AsTime())
	require.Equal(t, now, status.GetLastAttemptAt().AsTime())
	require.Equal(t, "outcome unresolved", status.GetLastFailure().GetMessage())
	require.Len(t, ctx.Tasks, 1)
	task, ok := ctx.Tasks[0].Payload.(*namespacereplicationpb.ApplyLocalTask)
	require.True(t, ok)
	require.Equal(t, int32(2), task.GetAttempt())
	require.Empty(t, ctx.Tasks[0].Attributes.Destination)
	require.Equal(t, now.Add(delay), ctx.Tasks[0].Attributes.ScheduledTime)
}

// TestPeerRetryBackoff pins the capped-exponential backoff schedule for peer
// retries: base doubling per attempt, clamped at the configured maximum, and
// overflow-safe for large attempt counts.
func TestPeerRetryBackoff(t *testing.T) {
	t.Parallel()

	policy := defaultPeerRetryPolicy()
	testCases := []struct {
		name    string
		attempt int32
		want    time.Duration
	}{
		{name: "attempt below 1 uses base", attempt: 0, want: policy.initialInterval},
		{name: "negative attempt uses base", attempt: -3, want: policy.initialInterval},
		{name: "attempt 1 uses base", attempt: 1, want: policy.initialInterval},
		{name: "attempt 2 doubles", attempt: 2, want: 2 * policy.initialInterval},
		{name: "attempt 3 doubles again", attempt: 3, want: 4 * policy.initialInterval},
		{name: "attempt 9 remains below cap", attempt: 9, want: 256 * policy.initialInterval},
		{name: "attempt 10 is clamped", attempt: 10, want: policy.maximumInterval},
		{name: "attempt 20 is clamped", attempt: 20, want: policy.maximumInterval},
		{name: "very large attempt is clamped", attempt: 1000, want: policy.maximumInterval},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := peerRetryBackoff(tc.attempt, policy)
			require.Equal(t, tc.want, got)
			require.LessOrEqual(t, got, policy.maximumInterval)
			require.GreaterOrEqual(t, got, policy.initialInterval)
		})
	}
}

func TestPeerRetryBackoffUsesConfiguredIntervals(t *testing.T) {
	t.Parallel()

	policy := peerRetryPolicy{
		initialInterval: time.Nanosecond,
		maximumInterval: time.Hour,
	}
	require.Equal(t, time.Duration(1<<20), peerRetryBackoff(21, policy))
	require.Equal(t, time.Hour, peerRetryBackoff(64, policy))

	overflowPolicy := peerRetryPolicy{
		initialInterval: time.Duration(1 << 62),
		maximumInterval: time.Duration((1 << 63) - 1),
	}
	require.Equal(t, overflowPolicy.maximumInterval, peerRetryBackoff(2, overflowPolicy))
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

func TestTransitionPeerRetryUsesPureTimerTask(t *testing.T) {
	c := NewNamespaceMutationComponent(&namespacereplicationpb.NamespaceMutation{PeerCells: []string{"cellB"}})
	c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
	ctx := &chasm.MockMutableContext{}
	now := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	delay := 17 * time.Second

	require.NoError(t, TransitionPeerRetry.Apply(c, ctx, EventPeerRetry{
		Time:       now,
		TargetCell: "cellB",
		Attempts:   1,
		Delay:      delay,
		Err:        errors.New("down"),
	}))
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE, c.GetPeerApply()["cellB"].GetOutcome())
	require.Len(t, ctx.Tasks, 1)
	task, ok := ctx.Tasks[0].Payload.(*namespacereplicationpb.ApplyPeerBackoffTask)
	require.True(t, ok)
	require.Equal(t, "cellB", task.GetTargetCell())
	require.Equal(t, int32(1), task.GetAttempt())
	require.Empty(t, ctx.Tasks[0].Attributes.Destination)
	require.Equal(t, now.Add(delay), ctx.Tasks[0].Attributes.ScheduledTime)
}

func TestApplyPeerBackoffTaskReenqueuesOutboundTask(t *testing.T) {
	c := NewNamespaceMutationComponent(&namespacereplicationpb.NamespaceMutation{PeerCells: []string{"cellB"}})
	c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
	c.PeerApply["cellB"].Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
	c.PeerApply["cellB"].AttemptCount = 2
	h := newApplyPeerBackoffTaskHandler()
	task := &namespacereplicationpb.ApplyPeerBackoffTask{TargetCell: "cellB", Attempt: 2}

	valid, err := h.Validate(nil, c, chasm.TaskInvocation{}, task)
	require.NoError(t, err)
	require.True(t, valid)

	ctx := &chasm.MockMutableContext{}
	require.NoError(t, h.Execute(ctx, c, chasm.TaskAttributes{}, task))
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, c.GetPeerApply()["cellB"].GetOutcome())
	valid, err = h.Validate(nil, c, chasm.TaskInvocation{}, task)
	require.NoError(t, err)
	require.False(t, valid)
	require.Len(t, ctx.Tasks, 1)
	peerTask, ok := ctx.Tasks[0].Payload.(*namespacereplicationpb.ApplyPeerTask)
	require.True(t, ok)
	require.Equal(t, "cellB", peerTask.GetTargetCell())
	require.Equal(t, int32(2), peerTask.GetAttempt())
	require.Equal(t, "cellB", ctx.Tasks[0].Attributes.Destination)
	require.True(t, ctx.Tasks[0].Attributes.ScheduledTime.IsZero())
}
