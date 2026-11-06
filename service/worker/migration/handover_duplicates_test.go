package migration

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
)

func TestCheckHandoverOnceDuplicateShards(t *testing.T) {
	t.Parallel()
	req := waitHandoverRequest{Namespace: "namespace", RemoteCluster: "target", ShardCount: 2}
	ready := shardResp(1, req.RemoteCluster, 20, handoverAt(req.Namespace, 20))
	lagging := shardResp(1, req.RemoteCluster, 5, handoverAt(req.Namespace, 20))
	newerLagging := shardResp(1, req.RemoteCluster, 30, handoverAt(req.Namespace, 45))
	missingInfo := shardResp(1, req.RemoteCluster, 30, nil)
	otherShard := shardResp(2, req.RemoteCluster, 20, handoverAt(req.Namespace, 20))
	readySnapshot := wideevents.NewHandoverLagSnapshot(2, 0)
	readySnapshot.ReadyCount = 2
	laggingSnapshot := wideevents.NewHandoverLagSnapshot(2, 0)
	laggingSnapshot.ReadyCount = 1
	laggingSnapshot.NotReadyCount = 1
	laggingSnapshot.MaxLaggingTasks = 15
	laggingSnapshot.MaxLaggingTasksShardID = 1
	laggingSnapshot.AddLaggingShard(1, 15)
	missingInfoSnapshot := wideevents.NewHandoverLagSnapshot(2, 1)
	missingInfoSnapshot.ReadyCount = 1
	missingInfoSnapshot.NotReadyCount = 1
	missingInfoSnapshot.AddLaggingShard(1, 0)

	for _, tc := range []struct {
		name     string
		shards   []*historyservice.ShardReplicationStatus
		done     bool
		snapshot wideevents.HandoverLagSnapshot
		err      string
	}{
		{
			name:     "all reports ready",
			shards:   []*historyservice.ShardReplicationStatus{ready, otherShard, ready},
			done:     true,
			snapshot: readySnapshot,
		},
		{
			name:     "newer ack last",
			shards:   []*historyservice.ShardReplicationStatus{lagging, otherShard, ready},
			done:     true,
			snapshot: readySnapshot,
		},
		{
			name:     "newer ack first",
			shards:   []*historyservice.ShardReplicationStatus{ready, otherShard, lagging},
			done:     true,
			snapshot: readySnapshot,
		},
		{
			name:     "newer ack retains its handover watermark",
			shards:   []*historyservice.ShardReplicationStatus{ready, otherShard, newerLagging},
			snapshot: laggingSnapshot,
		},
		{
			name:     "older ack cannot replace newer handover watermark",
			shards:   []*historyservice.ShardReplicationStatus{newerLagging, otherShard, ready},
			snapshot: laggingSnapshot,
		},
		{
			name:     "duplicate lagging reports counted once",
			shards:   []*historyservice.ShardReplicationStatus{lagging, otherShard, lagging},
			snapshot: laggingSnapshot,
		},
		{
			name:     "stale report missing handover info",
			shards:   []*historyservice.ShardReplicationStatus{shardResp(1, req.RemoteCluster, 5, nil), otherShard, ready},
			done:     true,
			snapshot: readySnapshot,
		},
		{
			name:     "newer report missing handover info",
			shards:   []*historyservice.ShardReplicationStatus{ready, otherShard, missingInfo},
			snapshot: missingInfoSnapshot,
		},
		{
			name:     "duplicate missing handover info counted once",
			shards:   []*historyservice.ShardReplicationStatus{missingInfo, otherShard, missingInfo},
			snapshot: missingInfoSnapshot,
		},
		{
			name:   "duplicates cannot hide missing shard",
			shards: []*historyservice.ShardReplicationStatus{ready, ready},
			err:    "GetReplicationStatus returns 1 shards, expecting 2",
		},
		{
			name:   "too many unique shards",
			shards: []*historyservice.ShardReplicationStatus{ready, otherShard, shardResp(3, req.RemoteCluster, 20, handoverAt(req.Namespace, 20))},
			err:    "GetReplicationStatus returns 3 shards, expecting 2",
		},
		{
			name:   "missing remote cluster still errors",
			shards: []*historyservice.ShardReplicationStatus{ready, otherShard, {ShardId: 1}},
			err:    "GetReplicationStatus response for shard 1 does not contains remote cluster target",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			historyClient := historyservicemock.NewMockHistoryServiceClient(gomock.NewController(t))
			historyClient.EXPECT().GetReplicationStatus(gomock.Any(), gomock.Any()).Return(replicationStatusResp(tc.shards...), nil)
			a := &activities{
				HistoryClient:  historyClient,
				Logger:         log.NewNoopLogger(),
				MetricsHandler: metrics.NoopMetricsHandler,
			}
			var snapshot wideevents.HandoverLagSnapshot
			done, err := a.checkHandoverOnce(t.Context(), req, &snapshot)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				require.False(t, done)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.done, done)
			require.Equal(t, tc.snapshot, snapshot)
		})
	}
}
