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
	first := workerHeartbeat("worker-1", "group-1")
	second := workerHeartbeat("worker-2", "group-1")
	group, err := newWorkerGroup(ctx, "group-1", []*workerpb.WorkerHeartbeat{first, second}, time.Minute)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, &workerstatepb.WorkerGroupState{
		WorkerGroupingKey:   "group-1",
		Status:              workerstatepb.WORKER_GROUP_STATUS_ACTIVE,
		LeaseExpirationTime: timestamppb.New(now.Add(time.Minute)),
	}, group.WorkerGroupState)
	require.Len(t, group.Workers, 2)
	protorequire.ProtoEqual(t, first, group.Workers["worker-1"].Get(ctx).WorkerHeartbeat)
	protorequire.ProtoEqual(t, second, group.Workers["worker-2"].Get(ctx).WorkerHeartbeat)
	require.Equal(t, []chasm.MockTask{{
		Component:  group,
		Attributes: chasm.TaskAttributes{ScheduledTime: now.Add(time.Minute)},
		Payload:    &workerstatepb.LeaseExpiryTask{},
	}}, ctx.Tasks)

	now = now.Add(30 * time.Second)
	updated := common.CloneProto(first)
	updated.WorkerIdentity = "updated-identity"
	third := workerHeartbeat("worker-3", "group-1")
	require.NoError(t, group.recordHeartbeat(ctx, []*workerpb.WorkerHeartbeat{updated, second, third}, time.Minute))
	require.Equal(t, now.Add(time.Minute), group.GetLeaseExpirationTime().AsTime())
	require.Len(t, group.Workers, 3)
	protorequire.ProtoEqual(t, updated, group.Workers["worker-1"].Get(ctx).WorkerHeartbeat)
	protorequire.ProtoEqual(t, second, group.Workers["worker-2"].Get(ctx).WorkerHeartbeat)
	protorequire.ProtoEqual(t, third, group.Workers["worker-3"].Get(ctx).WorkerHeartbeat)
	require.Len(t, ctx.Tasks, 2)
	require.Same(t, group, ctx.Tasks[1].Component)
	require.Equal(t, now.Add(time.Minute), ctx.Tasks[1].Attributes.ScheduledTime)

	updated.WorkerIdentity = "changed-after-heartbeat"
	require.Equal(t, "updated-identity", group.Workers["worker-1"].Get(ctx).GetWorkerHeartbeat().GetWorkerIdentity())
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
		{name: "missing worker key", heartbeats: []*workerpb.WorkerHeartbeat{workerHeartbeat("", "group-1")}, leaseDuration: time.Minute},
		{name: "missing grouping key", heartbeats: []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-2", "")}, leaseDuration: time.Minute},
		{name: "mixed groups", heartbeats: []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-2", "group-1"), workerHeartbeat("worker-3", "group-2")}, leaseDuration: time.Minute},
		{name: "zero lease", heartbeats: []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-2", "group-1")}},
		{name: "negative lease", heartbeats: []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-2", "group-1")}, leaseDuration: -time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := &chasm.MockMutableContext{}
			first := workerHeartbeat("worker-1", "group-1")
			group, err := newWorkerGroup(ctx, "group-1", []*workerpb.WorkerHeartbeat{first}, time.Minute)
			require.NoError(t, err)
			before := common.CloneProto(group.WorkerGroupState)
			err = group.recordHeartbeat(ctx, tc.heartbeats, tc.leaseDuration)
			require.Error(t, err)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
			protorequire.ProtoEqual(t, before, group.WorkerGroupState)
			require.Len(t, group.Workers, 1)
			protorequire.ProtoEqual(t, first, group.Workers["worker-1"].Get(ctx).WorkerHeartbeat)
			require.Len(t, ctx.Tasks, 1)
		})
	}
}

func TestRecordHeartbeatDuplicateWorker(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	first := workerHeartbeat("worker-1", "group-1")
	last := common.CloneProto(first)
	last.WorkerIdentity = "last-identity"
	group, err := newWorkerGroup(ctx, "group-1", []*workerpb.WorkerHeartbeat{first, last}, time.Minute)
	require.NoError(t, err)
	require.Len(t, group.Workers, 1)
	protorequire.ProtoEqual(t, last, group.Workers["worker-1"].Get(ctx).WorkerHeartbeat)
}

func TestNewWorkerGroupMissingKey(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	group, err := newWorkerGroup(ctx, "", []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-1", "")}, time.Minute)
	require.Nil(t, group)
	require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	require.Empty(t, ctx.Tasks)
}

func TestRecordHeartbeatInactiveGroup(t *testing.T) {
	t.Parallel()
	ctx := &chasm.MockMutableContext{}
	group, err := newWorkerGroup(ctx, "group-1", []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-1", "group-1")}, time.Minute)
	require.NoError(t, err)
	_, err = group.Terminate(ctx, chasm.TerminateComponentRequest{})
	require.NoError(t, err)
	before := common.CloneProto(group.WorkerGroupState)
	err = group.recordHeartbeat(ctx, []*workerpb.WorkerHeartbeat{workerHeartbeat("worker-2", "group-1")}, time.Minute)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
	protorequire.ProtoEqual(t, before, group.WorkerGroupState)
	require.Len(t, group.Workers, 1)
	require.Len(t, ctx.Tasks, 1)
}

func workerHeartbeat(key, groupingKey string) *workerpb.WorkerHeartbeat {
	return &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: key,
		HostInfo:          &workerpb.WorkerHostInfo{WorkerGroupingKey: groupingKey},
	}
}
