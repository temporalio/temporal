package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRecordHeartbeat(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0).UTC()
	ctx := &chasm.MockMutableContext{MockContext: chasm.MockContext{
		HandleNow: func(chasm.Component) time.Time { return now },
	}}
	first := workerHeartbeat("worker-1")
	second := workerHeartbeat("worker-2")
	// Create worker children under one shared lease.
	group := newWorkerGroup()
	require.NoError(t, group.recordHeartbeat(ctx, []*workerpb.WorkerHeartbeat{first, second}, time.Minute))
	protorequire.ProtoEqual(t, &workerstatepb.WorkerGroupState{
		Status:              workerstatepb.WORKER_GROUP_STATUS_ACTIVE,
		LeaseExpirationTime: timestamppb.New(now.Add(time.Minute)),
	}, group.WorkerGroupState)
	require.Len(t, group.Workers, 2)
	require.Contains(t, group.Workers, first.WorkerInstanceKey)
	require.Contains(t, group.Workers, second.WorkerInstanceKey)
	secondWorker := group.Workers[second.WorkerInstanceKey].Get(ctx)
	require.Equal(t, []chasm.MockTask{{
		Component:  group,
		Attributes: chasm.TaskAttributes{ScheduledTime: now.Add(time.Minute)},
		Payload:    &workerstatepb.LeaseExpiryTask{},
	}}, ctx.Tasks)

	// Add a worker and renew the lease, retaining omitted and existing workers.
	now = now.Add(30 * time.Second)
	third := workerHeartbeat("worker-3")
	require.NoError(t, group.recordHeartbeat(ctx, []*workerpb.WorkerHeartbeat{second, third}, time.Minute))
	require.Len(t, group.Workers, 3)
	require.Contains(t, group.Workers, first.WorkerInstanceKey)
	require.Contains(t, group.Workers, third.WorkerInstanceKey)
	require.Same(t, secondWorker, group.Workers[second.WorkerInstanceKey].Get(ctx))
	require.Equal(t, now.Add(time.Minute), group.GetLeaseExpirationTime().AsTime())
	require.Len(t, ctx.Tasks, 2)
	require.Same(t, group, ctx.Tasks[1].Component)
	require.Equal(t, now.Add(time.Minute), ctx.Tasks[1].Attributes.ScheduledTime)
}

func TestRecordHeartbeatInvalidBatch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		heartbeats    []*workerpb.WorkerHeartbeat
		leaseDuration time.Duration
	}{
		{name: "empty batch", leaseDuration: time.Minute},
		{name: "nil heartbeat", heartbeats: []*workerpb.WorkerHeartbeat{nil}, leaseDuration: time.Minute},
		{name: "missing worker key", heartbeats: []*workerpb.WorkerHeartbeat{workerHeartbeat("")}, leaseDuration: time.Minute},
		{name: "zero lease", heartbeats: []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-2")}},
		{name: "negative lease", heartbeats: []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-2")}, leaseDuration: -time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := &chasm.MockMutableContext{}
			first := workerHeartbeat("worker-1")
			group := newWorkerGroup()
			require.NoError(t, group.recordHeartbeat(ctx, []*workerpb.WorkerHeartbeat{first}, time.Minute))
			before := common.CloneProto(group.WorkerGroupState)
			err := group.recordHeartbeat(ctx, tc.heartbeats, tc.leaseDuration)
			require.Error(t, err)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			protorequire.ProtoEqual(t, before, group.WorkerGroupState)
			require.Len(t, group.Workers, 1)
			require.Len(t, ctx.Tasks, 1)
		})
	}
}

func TestRecordHeartbeatDuplicateWorker(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	first := workerHeartbeat("worker-1")
	// Duplicate entries create one worker component and share one lease.
	group := newWorkerGroup()
	require.NoError(t, group.recordHeartbeat(ctx, []*workerpb.WorkerHeartbeat{first, first}, time.Minute))
	require.Len(t, group.Workers, 1)
	require.Equal(t, ctx.Tasks[0].Attributes.ScheduledTime.UTC(), group.GetLeaseExpirationTime().AsTime())
	require.Len(t, ctx.Tasks, 1)
}

func TestRecordHeartbeatInactiveGroup(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	group := newWorkerGroup()
	require.NoError(t, group.recordHeartbeat(ctx, []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-1")}, time.Minute))
	_, err := group.Terminate(ctx, chasm.TerminateComponentRequest{})
	require.NoError(t, err)
	before := common.CloneProto(group.WorkerGroupState)
	err = group.recordHeartbeat(ctx, []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-2")}, time.Minute)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	protorequire.ProtoEqual(t, before, group.WorkerGroupState)
	require.Len(t, group.Workers, 1)
	require.Contains(t, group.Workers, "worker-1")
	require.Len(t, ctx.Tasks, 1)
}

func workerHeartbeat(key string) *workerpb.WorkerHeartbeat {
	return &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: key,
	}
}
