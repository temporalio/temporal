package migration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCheckReplicationOnceCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name           string
		checkpointIDs  map[int32]int64
		checkpointAge  time.Duration
		allowedLagging time.Duration
		rpcDuration    time.Duration
		requiredMin    int64
		ackID          int64
		unsetWatermark bool
		missingRemote  bool
		wantReady      bool
	}{
		{
			name: "quiet gap", checkpointIDs: map[int32]int64{1: 20},
			checkpointAge: time.Second, allowedLagging: 5 * time.Second,
			requiredMin: 10, ackID: 20, wantReady: true,
		},
		{
			name: "old backlog followed by fresh writes", checkpointIDs: map[int32]int64{1: 30},
			checkpointAge: time.Second, allowedLagging: 5 * time.Second,
			requiredMin: 10, ackID: 20,
		},
		{
			name: "at expiration boundary", checkpointIDs: map[int32]int64{1: 20},
			checkpointAge: 5 * time.Second, allowedLagging: 5 * time.Second,
			requiredMin: 10, ackID: 20, wantReady: true,
		},
		{
			name: "expired", checkpointIDs: map[int32]int64{1: 20},
			checkpointAge: 5*time.Second + time.Nanosecond, allowedLagging: 5 * time.Second,
			requiredMin: 10, ackID: 20,
		},
		{
			name: "expires during RPC", checkpointIDs: map[int32]int64{1: 20},
			checkpointAge: time.Second, allowedLagging: 5 * time.Second, rpcDuration: 6 * time.Second,
			requiredMin: 10, ackID: 20,
		},
		{
			name: "below required floor", checkpointIDs: map[int32]int64{1: 20},
			checkpointAge: time.Second, allowedLagging: 5 * time.Second,
			requiredMin: 21, ackID: 20,
		},
		{
			name: "uninitialized watermark", checkpointIDs: map[int32]int64{1: 0},
			checkpointAge: time.Second, allowedLagging: 5 * time.Second,
			unsetWatermark: true,
		},
		{
			name: "missing checkpoint shard", checkpointIDs: map[int32]int64{2: 20},
			checkpointAge: time.Second, allowedLagging: 5 * time.Second,
			requiredMin: 10, ackID: 20,
		},
		{
			name: "no checkpoint", allowedLagging: 5 * time.Second,
			requiredMin: 10, ackID: 20,
		},
		{
			name: "zero tolerance", checkpointIDs: map[int32]int64{1: 20},
			checkpointAge: time.Second, requiredMin: 10, ackID: 20,
		},
		{
			name: "negative tolerance", checkpointIDs: map[int32]int64{1: 20},
			checkpointAge: time.Second, allowedLagging: -time.Second,
			requiredMin: 10, ackID: 20,
		},
		{
			name: "future checkpoint", checkpointIDs: map[int32]int64{1: 20},
			checkpointAge: -time.Second, allowedLagging: 5 * time.Second,
			requiredMin: 10, ackID: 20,
		},
		{
			name: "missing remote", checkpointIDs: map[int32]int64{1: 20},
			checkpointAge: time.Second, allowedLagging: 5 * time.Second,
			requiredMin: 10, ackID: 20, missingRemote: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1000, 0)
			timeSource := clock.NewEventTimeSource().Update(now)
			checkpoint := replicationCheckpoint{
				now: timeSource.Now, requestStart: now.Add(-tc.checkpointAge), maxTaskIDs: tc.checkpointIDs,
			}
			shard := checkpointTestShard(1, 50, tc.ackID)
			if tc.unsetWatermark {
				shard.RemoteClusters["remote"].AckedTaskVisibilityTime = nil
			}
			if tc.missingRemote {
				shard.RemoteClusters = nil
			}
			client := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
			client.EXPECT().GetReplicationStatus(gomock.Any(), gomock.Any()).DoAndReturn(
				func(context.Context, *historyservice.GetReplicationStatusRequest, ...grpc.CallOption) (*historyservice.GetReplicationStatusResponse, error) {
					timeSource.Advance(tc.rpcDuration)
					return &historyservice.GetReplicationStatusResponse{Shards: []*historyservice.ShardReplicationStatus{shard}}, nil
				},
			)
			a := activities{HistoryClient: client, Logger: log.NewNoopLogger(), MetricsHandler: metrics.NoopMetricsHandler}
			ready, err := a.checkReplicationOnce(context.Background(), WaitReplicationRequest{
				ShardCount: 1, RemoteCluster: "remote", AllowedLagging: tc.allowedLagging,
				WaitForTaskIds: map[int32]int64{1: tc.requiredMin},
			}, &checkpoint)
			if tc.missingRemote {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantReady, ready)
			if tc.rpcDuration > 0 {
				require.Equal(t, now, checkpoint.requestStart)
				require.False(t, checkpoint.isFresh(timeSource.Now(), tc.allowedLagging))
			}
		})
	}
}

func TestCheckReplicationOnceRetainsAndReplacesCheckpoint(t *testing.T) {
	now := time.Unix(1000, 0)
	timeSource := clock.NewEventTimeSource().Update(now)
	checkpoint := replicationCheckpoint{now: timeSource.Now}
	client := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
	var response *historyservice.GetReplicationStatusResponse
	client.EXPECT().GetReplicationStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, *historyservice.GetReplicationStatusRequest, ...grpc.CallOption) (*historyservice.GetReplicationStatusResponse, error) {
			return response, nil
		},
	).AnyTimes()
	a := activities{HistoryClient: client, Logger: log.NewNoopLogger(), MetricsHandler: metrics.NoopMetricsHandler}
	req := WaitReplicationRequest{
		ShardCount: 2, RemoteCluster: "remote", AllowedLagging: 5 * time.Second,
		WaitForTaskIds: map[int32]int64{1: 1, 2: 2},
	}
	for _, step := range []struct {
		elapsed         time.Duration
		maxIDs          [2]int64
		ackIDs          [2]int64
		checkpointIDs   [2]int64
		checkpointStart time.Duration
		wantReady       bool
	}{
		{0, [2]int64{10, 20}, [2]int64{1, 2}, [2]int64{10, 20}, 0, false},
		{2 * time.Second, [2]int64{20, 30}, [2]int64{10, 19}, [2]int64{10, 20}, 0, false},
		{3 * time.Second, [2]int64{30, 40}, [2]int64{9, 20}, [2]int64{10, 20}, 0, false},
		{4 * time.Second, [2]int64{40, 50}, [2]int64{10, 20}, [2]int64{10, 20}, 0, true},
		{6 * time.Second, [2]int64{50, 60}, [2]int64{10, 20}, [2]int64{50, 60}, 6 * time.Second, false},
		{7 * time.Second, [2]int64{60, 70}, [2]int64{50, 59}, [2]int64{50, 60}, 6 * time.Second, false},
		{8 * time.Second, [2]int64{70, 80}, [2]int64{49, 60}, [2]int64{50, 60}, 6 * time.Second, false},
		{9 * time.Second, [2]int64{80, 90}, [2]int64{50, 60}, [2]int64{50, 60}, 6 * time.Second, true},
	} {
		timeSource.Update(now.Add(step.elapsed))
		response = &historyservice.GetReplicationStatusResponse{Shards: []*historyservice.ShardReplicationStatus{
			checkpointTestShard(1, step.maxIDs[0], step.ackIDs[0]),
			checkpointTestShard(2, step.maxIDs[1], step.ackIDs[1]),
		}}
		ready, err := a.checkReplicationOnce(context.Background(), req, &checkpoint)
		require.NoError(t, err)
		require.Equal(t, step.wantReady, ready, "poll at %v", step.elapsed)
		require.Equal(t, map[int32]int64{1: step.checkpointIDs[0], 2: step.checkpointIDs[1]}, checkpoint.maxTaskIDs)
		require.Equal(t, now.Add(step.checkpointStart), checkpoint.requestStart)
	}

	newAttempt := replicationCheckpoint{now: timeSource.Now}
	ready, err := a.checkReplicationOnce(context.Background(), req, &newAttempt)
	require.NoError(t, err)
	require.False(t, ready)
}

func checkpointTestShard(shardID int32, maxID, ackID int64) *historyservice.ShardReplicationStatus {
	return &historyservice.ShardReplicationStatus{
		ShardId: shardID, MaxReplicationTaskId: maxID,
		MaxReplicationTaskVisibilityTime: timestamppb.New(time.Unix(1000, 0)),
		RemoteClusters: map[string]*historyservice.ShardReplicationStatusPerCluster{
			"remote": {AckedTaskId: ackID, AckedTaskVisibilityTime: timestamppb.New(time.Unix(900, 0))},
		},
	}
}
