package namespacereplication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/testing/protorequire"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestPeerRetryTimerInvalidatesAfterDispatch(t *testing.T) {
	t.Parallel()

	env := newNsreplTestEnv(t)
	ref := env.start(env.mutationUpdate("cellB"), func(c *NamespaceMutationComponent) {
		c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
		c.PeerApply["cellB"].Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE
		c.PeerApply["cellB"].AttemptCount = 2
	})
	task := &namespacereplicationpb.ApplyPeerBackoffTask{TargetCell: "cellB", Attempt: 2}
	attrs := chasm.TaskAttributes{ScheduledTime: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	dropped, err := chasmtest.ExecutePureTask(env.engineCtx, env.engine, env.read(ref), env.backoffHandler, attrs, task)
	require.NoError(t, err)
	require.False(t, dropped)
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, env.read(ref).GetPeerApply()["cellB"].GetOutcome())
	tasks, err := env.engine.Tasks(ref)
	require.NoError(t, err)
	require.Len(t, tasks[historytasks.CategoryOutbound], 1)

	dropped, err = chasmtest.ExecutePureTask(env.engineCtx, env.engine, env.read(ref), env.backoffHandler, attrs, task)
	require.NoError(t, err)
	require.True(t, dropped)
	tasks, err = env.engine.Tasks(ref)
	require.NoError(t, err)
	require.Len(t, tasks[historytasks.CategoryOutbound], 1)
}

func TestPeerRetryCycle(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	timeSource := clock.NewEventTimeSource().Update(now)
	env := newNsreplTestEnvWithOptions(t, chasmtest.WithTimeSource(timeSource))
	ref := env.startCommitted("cellB")
	env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil).Times(3)
	gomock.InOrder(
		env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("peer down")),
		env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("peer still down")),
		env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
			&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil),
	)

	for attempt := int32(0); attempt < 2; attempt++ {
		task := &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: attempt}
		err := env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{Destination: "cellB"}, task)
		var destinationDown *queueserrors.DestinationDownError
		require.ErrorAs(t, err, &destinationDown)
		c := env.read(ref)
		peer := c.GetPeerApply()["cellB"]
		require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE, peer.GetOutcome())
		require.Equal(t, attempt+1, peer.GetAttemptCount())
		require.False(t, c.allPeersTerminal())

		// Even a delivery carrying the next generation cannot run during backoff.
		valid, err := env.peerHandler.Validate(nil, c, chasm.TaskInvocation{},
			&namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: attempt + 1})
		require.NoError(t, err)
		require.False(t, valid)
		if attempt > 0 {
			dropped, err := chasmtest.ExecutePureTask(env.engineCtx, env.engine, c, env.backoffHandler,
				chasm.TaskAttributes{ScheduledTime: now}, &namespacereplicationpb.ApplyPeerBackoffTask{TargetCell: "cellB", Attempt: attempt})
			require.NoError(t, err)
			require.True(t, dropped)
		}

		timeSource.Advance(time.Minute)
		_, err = env.engine.FirePureTasks(ref, timeSource.Now())
		require.NoError(t, err)
		c = env.read(ref)
		require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, c.GetPeerApply()["cellB"].GetOutcome())
		require.Equal(t, attempt+1, c.GetPeerApply()["cellB"].GetAttemptCount())
		valid, err = env.peerHandler.Validate(nil, c, chasm.TaskInvocation{},
			&namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: attempt + 1})
		require.NoError(t, err)
		require.True(t, valid)
	}

	require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{Destination: "cellB"},
		&namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB", Attempt: 2}))
	c := env.read(ref)
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, c.GetStatus())
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED, c.GetPeerApply()["cellB"].GetOutcome())
	require.Equal(t, int32(3), c.GetPeerApply()["cellB"].GetAttemptCount())
}

func TestPeerRetryReadyIgnoresStaleOrDispatchedPeers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		outcome namespacereplicationpb.PeerApplyOutcome
		attempt int32
	}{
		{"already dispatched", namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, 2},
		{"stale attempt", namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE, 3},
		{"terminal peer", namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewNamespaceMutationComponent(&namespacereplicationpb.NamespaceMutation{PeerCells: []string{"cellB"}})
			c.PeerApply["cellB"].Outcome = tc.outcome
			c.PeerApply["cellB"].AttemptCount = tc.attempt
			ctx := &chasm.MockMutableContext{}
			require.NoError(t, TransitionPeerRetryReady.Apply(c, ctx, EventPeerRetryReady{TargetCell: "cellB", Attempt: 2}))
			protorequire.ProtoEqual(t, &namespacereplicationpb.PeerApplyStatus{Outcome: tc.outcome, AttemptCount: tc.attempt}, c.GetPeerApply()["cellB"])
			require.Empty(t, ctx.Tasks)
		})
	}
}
