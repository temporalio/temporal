package migration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	commonlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
)

const lagSummaryTestNsName = "handover-lag-summary-test-ns"

// checkHandoverOnce resets the snapshot each poll, so the summary sees only the final state.
func TestCheckHandoverOnceSnapshotIsLastPollOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	historyClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
	a := &activities{
		HistoryClient:  historyClient,
		Logger:         commonlog.NewNoopLogger(),
		MetricsHandler: metrics.NoopMetricsHandler,
	}
	req := waitHandoverRequest{Namespace: lagSummaryTestNsName, RemoteCluster: "target", ShardCount: 2}

	// Poll 1: shard 1 is behind, shard 2 has no handover info yet.
	historyClient.EXPECT().GetReplicationStatus(gomock.Any(), gomock.Any()).Return(
		replicationStatusResp(
			shardResp(1, "target", 5, handoverAt(req.Namespace, 20)),
			shardResp(2, "target", 5, nil),
		), nil)

	var snapshot wideevents.HandoverLagSnapshot
	done, err := a.checkHandoverOnce(context.Background(), req, &snapshot)
	require.NoError(t, err)
	require.False(t, done)
	require.Zero(t, snapshot.ReadyCount)
	require.Equal(t, 2, snapshot.NotReadyCount)
	require.Equal(t, 1, snapshot.MissingHandoverInfoCount)
	require.EqualValues(t, 15, snapshot.MaxLaggingTasks)
	require.Len(t, snapshot.LaggingShards, 2)

	// Poll 2: both caught up. The stale laggards from poll 1 must be gone.
	historyClient.EXPECT().GetReplicationStatus(gomock.Any(), gomock.Any()).Return(
		replicationStatusResp(
			shardResp(1, "target", 20, handoverAt(req.Namespace, 20)),
			shardResp(2, "target", 20, handoverAt(req.Namespace, 20)),
		), nil)

	done, err = a.checkHandoverOnce(context.Background(), req, &snapshot)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, 2, snapshot.ReadyCount)
	require.Zero(t, snapshot.NotReadyCount)
	require.Empty(t, snapshot.LaggingShards)
}

func replicationStatusResp(shards ...*historyservice.ShardReplicationStatus) *historyservice.GetReplicationStatusResponse {
	return &historyservice.GetReplicationStatusResponse{Shards: shards}
}

func shardResp(shardID int32, remote string, ackedTaskID int64, handover map[string]*historyservice.HandoverNamespaceInfo) *historyservice.ShardReplicationStatus {
	return &historyservice.ShardReplicationStatus{
		ShardId: shardID,
		RemoteClusters: map[string]*historyservice.ShardReplicationStatusPerCluster{
			remote: {AckedTaskId: ackedTaskID},
		},
		HandoverNamespaces: handover,
	}
}

func handoverAt(ns string, taskID int64) map[string]*historyservice.HandoverNamespaceInfo {
	return map[string]*historyservice.HandoverNamespaceInfo{
		ns: {HandoverReplicationTaskId: taskID},
	}
}
