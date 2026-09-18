package migration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	commonlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
)

const lagSummaryTestNsName = "handover-lag-summary-test-ns"

type handoverEventCaptureLogger struct {
	embedded.Logger
	records []otellog.Record
}

func (l *handoverEventCaptureLogger) Emit(_ context.Context, record otellog.Record) {
	l.records = append(l.records, record)
}

func (l *handoverEventCaptureLogger) Enabled(context.Context, otellog.EnabledParameters) bool {
	return true
}

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

func TestEmitHandoverIncompleteDisabled(t *testing.T) {
	lg := &handoverEventCaptureLogger{}
	a := &activities{
		EventLogger:                  lg,
		emitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(false),
	}

	a.emitHandoverIncomplete(
		waitHandoverRequest{Namespace: lagSummaryTestNsName, RemoteCluster: "target"},
		&wideevents.HandoverLagSnapshot{NotReadyCount: 1},
		time.Second,
		context.Canceled,
	)
	require.Empty(t, lg.records)
}

func TestEmitHandoverIncompleteEnabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespace(namespace.Name(lagSummaryTestNsName)).Return(
		namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{
			Id:   "namespace-id",
			Name: lagSummaryTestNsName,
		}, nil, "cluster-a"),
		nil,
	)
	lg := &handoverEventCaptureLogger{}
	a := &activities{
		NamespaceRegistry:            registry,
		EventLogger:                  lg,
		emitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(true),
	}

	a.emitHandoverIncomplete(
		waitHandoverRequest{Namespace: lagSummaryTestNsName, RemoteCluster: "target"},
		&wideevents.HandoverLagSnapshot{NotReadyCount: 1},
		time.Second,
		context.Canceled,
	)
	require.Len(t, lg.records, 1)
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
