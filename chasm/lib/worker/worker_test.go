package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
)

// TestRecordHeartbeat verifies the initial lease and renewal using CHASM time.
func TestRecordHeartbeat(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0).UTC()
	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return now },
	}}
	group := newWorkerGroup()
	require.NoError(t, group.recordHeartbeat(ctx, nil, time.Minute))
	protorequire.ProtoEqual(t, &workerstatepb.WorkerGroupState{
		Status: workerstatepb.WORKER_GROUP_STATUS_ACTIVE,
	}, group.WorkerGroupState)
	require.Equal(t, []chasm.MockTask{{
		Component:  group,
		Attributes: chasm.TaskAttributes{ScheduledTime: now.Add(time.Minute)},
		Payload:    &workerstatepb.LeaseExpiryTask{},
	}}, ctx.Tasks)

	// A later heartbeat extends the lease and schedules expiry at the new deadline.
	now = now.Add(30 * time.Second)
	require.NoError(t, group.recordHeartbeat(ctx, nil, time.Minute))
	require.Len(t, ctx.Tasks, 2)
	require.Same(t, group, ctx.Tasks[1].Component)
	require.Equal(t, now.Add(time.Minute), ctx.Tasks[1].Attributes.ScheduledTime)
}

func TestRecordHeartbeatInvalidDuration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		leaseDuration time.Duration
	}{
		{name: "zero lease"},
		{name: "negative lease", leaseDuration: -time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := &chasm.MockMutableContext{}
			group := newWorkerGroup()
			require.NoError(t, group.recordHeartbeat(ctx, nil, time.Minute))
			before := common.CloneProto(group.WorkerGroupState)

			// An invalid renewal must preserve the existing lease and schedule no task.
			err := group.recordHeartbeat(ctx, nil, tc.leaseDuration)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			protorequire.ProtoEqual(t, before, group.WorkerGroupState)
			require.Len(t, ctx.Tasks, 1)
		})
	}
}

func TestRecordHeartbeatInactiveGroup(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	group := newWorkerGroup()
	require.NoError(t, group.recordHeartbeat(ctx, nil, time.Minute))
	_, err := group.Terminate(ctx, chasm.TerminateComponentRequest{})
	require.NoError(t, err)
	before := common.CloneProto(group.WorkerGroupState)

	// A heartbeat cannot reopen the current run after it closes.
	err = group.recordHeartbeat(ctx, nil, time.Minute)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	protorequire.ProtoEqual(t, before, group.WorkerGroupState)
	require.Len(t, ctx.Tasks, 1)
}
