package migration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	commonlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
)

const (
	lagSummaryTestNsID   = "deadbeef-0000-0000-0000-000000000002"
	lagSummaryTestNsName = "handover-lag-summary-test-ns"
)

// eventCaptureLogger records emitted wide events for assertions.
type eventCaptureLogger struct {
	embedded.Logger
	records []log.Record
}

func (c *eventCaptureLogger) Emit(_ context.Context, r log.Record) { c.records = append(c.records, r) }

func (c *eventCaptureLogger) Enabled(context.Context, log.EnabledParameters) bool { return true }

func (c *eventCaptureLogger) attrs(rec log.Record) map[string]log.Value {
	got := map[string]log.Value{}
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		got[kv.Key] = kv.Value
		return true
	})
	return got
}

// details decodes a record's details attribute, which wideevents emits as compact JSON.
func (c *eventCaptureLogger) details(t *testing.T, i int) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(c.attrs(c.records[i])["details"].AsString()), &out))
	return out
}

func newLagSummaryTestActivities(t *testing.T, lg log.Logger) *activities {
	t.Helper()
	ns := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: lagSummaryTestNsID, Name: lagSummaryTestNsName},
		nil, false, nil, 0,
	)
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespace(namespace.Name(lagSummaryTestNsName)).Return(ns, nil).AnyTimes()
	return &activities{EventLogger: lg, NamespaceRegistry: registry}
}

func lagSummaryTestRequest() waitHandoverRequest {
	return waitHandoverRequest{Namespace: lagSummaryTestNsName, RemoteCluster: "target"}
}

// A handover that completes leaves no laggards, so the wait emits nothing at all.
func TestEmitHandoverLagSummarySilentWhenReady(t *testing.T) {
	lg := &eventCaptureLogger{}
	a := newLagSummaryTestActivities(t, lg)

	a.emitHandoverLagSummary(lagSummaryTestRequest(), &handoverLagSnapshot{totalShards: 4}, time.Second, nil)

	require.Empty(t, lg.records)
}

func TestEmitHandoverLagSummaryNamesLaggingShards(t *testing.T) {
	lg := &eventCaptureLogger{}
	a := newLagSummaryTestActivities(t, lg)

	snapshot := &handoverLagSnapshot{
		totalShards:              4,
		readyCount:               2,
		notReadyCount:            2,
		missingHandoverInfoCount: 1,
		maxLaggingTasks:          42,
		maxLaggingTasksShardID:   3,
		laggingShards: []laggingShard{
			{ShardID: 1, LaggingTasks: 0},
			{ShardID: 3, LaggingTasks: 42},
		},
	}
	a.emitHandoverLagSummary(lagSummaryTestRequest(), snapshot, 30*time.Second, context.Canceled)

	require.Len(t, lg.records, 1)
	require.Equal(t, wideevents.PhaseHandoverIncomplete, lg.attrs(lg.records[0])["phase"].AsString())
	require.Equal(t, lagSummaryTestNsID, lg.attrs(lg.records[0])["namespace_id"].AsString())

	d := lg.details(t, 0)
	require.EqualValues(t, 4, d["total_shards"])
	require.EqualValues(t, 2, d["ready_count"])
	require.EqualValues(t, 2, d["not_ready_count"])
	require.EqualValues(t, 1, d["missing_handover_info_count"])
	require.EqualValues(t, 42, d["max_lagging_tasks"])
	require.EqualValues(t, 3, d["max_lagging_tasks_shard_id"])
	require.EqualValues(t, 30, d["elapsed_seconds"])
	require.Equal(t, "context_canceled", d["exit_reason"])
	require.Equal(t, "target", d["remote_cluster"])
	require.Equal(t, false, d["lagging_shards_truncated"])

	shards := d["lagging_shards"].([]any)
	require.Len(t, shards, 2)
	require.EqualValues(t, 1, shards[0].(map[string]any)["shard_id"])
	require.EqualValues(t, 42, shards[1].(map[string]any)["lagging_tasks"])
}

// A failed GetReplicationStatus and a killed wait point at different problems, so they are
// distinguishable in the event.
func TestEmitHandoverLagSummaryExitReason(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"canceled", context.Canceled, "context_canceled"},
		{"deadline", context.DeadlineExceeded, "deadline_exceeded"},
		{"rpc", errors.New("shard 3 missing remote cluster"), "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lg := &eventCaptureLogger{}
			a := newLagSummaryTestActivities(t, lg)

			a.emitHandoverLagSummary(
				lagSummaryTestRequest(),
				&handoverLagSnapshot{totalShards: 1, notReadyCount: 1},
				time.Second,
				tc.err,
			)

			require.Len(t, lg.records, 1)
			d := lg.details(t, 0)
			require.Equal(t, tc.want, d["exit_reason"])
			require.Equal(t, tc.err.Error(), d["exit_error"])
		})
	}
}

// The per-shard list is capped, but the count above it is the true one.
func TestEmitHandoverLagSummaryTruncates(t *testing.T) {
	lg := &eventCaptureLogger{}
	a := newLagSummaryTestActivities(t, lg)

	snapshot := &handoverLagSnapshot{totalShards: 4096, notReadyCount: 4096}
	for i := 0; i < maxLaggingShardsInSummary; i++ {
		snapshot.laggingShards = append(snapshot.laggingShards, laggingShard{ShardID: int32(i), LaggingTasks: 1})
	}
	a.emitHandoverLagSummary(lagSummaryTestRequest(), snapshot, time.Second, context.Canceled)

	d := lg.details(t, 0)
	require.EqualValues(t, 4096, d["not_ready_count"])
	require.Len(t, d["lagging_shards"].([]any), maxLaggingShardsInSummary)
	require.Equal(t, true, d["lagging_shards_truncated"])
}

// checkHandoverOnce overwrites the snapshot every poll, so what the deferred summary sees is the
// final state of the wait and not an accumulation across polls.
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

	var snapshot handoverLagSnapshot
	done, err := a.checkHandoverOnce(context.Background(), req, &snapshot)
	require.NoError(t, err)
	require.False(t, done)
	require.Zero(t, snapshot.readyCount)
	require.Equal(t, 2, snapshot.notReadyCount)
	require.Equal(t, 1, snapshot.missingHandoverInfoCount)
	require.EqualValues(t, 15, snapshot.maxLaggingTasks)
	require.Len(t, snapshot.laggingShards, 2)

	// Poll 2: both caught up. The stale laggards from poll 1 must be gone.
	historyClient.EXPECT().GetReplicationStatus(gomock.Any(), gomock.Any()).Return(
		replicationStatusResp(
			shardResp(1, "target", 20, handoverAt(req.Namespace, 20)),
			shardResp(2, "target", 20, handoverAt(req.Namespace, 20)),
		), nil)

	done, err = a.checkHandoverOnce(context.Background(), req, &snapshot)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, 2, snapshot.readyCount)
	require.Zero(t, snapshot.notReadyCount)
	require.Empty(t, snapshot.laggingShards)
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
