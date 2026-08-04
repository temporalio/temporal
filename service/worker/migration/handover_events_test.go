package migration

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

const (
	readinessTestNsID   = "deadbeef-0000-0000-0000-000000000002"
	readinessTestNsName = "handover-readiness-test-ns"
)

// eventCaptureLogger records emitted wide events for assertions.
type eventCaptureLogger struct {
	embedded.Logger
	records []log.Record
}

func (c *eventCaptureLogger) Emit(_ context.Context, r log.Record) { c.records = append(c.records, r) }

func (c *eventCaptureLogger) Enabled(context.Context, log.EnabledParameters) bool { return true }

// details decodes each record's details attribute, which wideevents emits as compact JSON.
func (c *eventCaptureLogger) details(t *testing.T) []map[string]any {
	t.Helper()
	out := make([]map[string]any, 0, len(c.records))
	for _, rec := range c.records {
		var raw string
		rec.WalkAttributes(func(kv log.KeyValue) bool {
			if kv.Key == "details" {
				raw = kv.Value.AsString()
			}
			return true
		})
		var d map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &d))
		out = append(out, d)
	}
	return out
}

func newReadinessTestActivities(t *testing.T, lg log.Logger) *activities {
	t.Helper()
	ns := namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: readinessTestNsID, Name: readinessTestNsName},
		nil, false, nil, 0,
	)
	registry := namespace.NewMockRegistry(gomock.NewController(t))
	registry.EXPECT().GetNamespace(namespace.Name(readinessTestNsName)).Return(ns, nil).AnyTimes()
	return &activities{EventLogger: lg, NamespaceRegistry: registry}
}

// checkHandoverOnce runs on a 1s loop, so a shard that stays not-ready across polls must emit
// once, not once per poll. Only the transitions in and out of not-ready are events.
func TestEmitShardHandoverReadinessOnlyOnTransitions(t *testing.T) {
	lg := &eventCaptureLogger{}
	a := newReadinessTestActivities(t, lg)
	req := waitHandoverRequest{Namespace: readinessTestNsName, RemoteCluster: "target"}
	notReadyShards := make(map[int32]bool)

	notReady := shardStatus{shardID: 3, laggingTasks: 42, isReady: false}
	ready := shardStatus{shardID: 3, laggingTasks: 0, isReady: true}

	// A shard that is ready from the first poll onward never emits.
	for i := 0; i < 3; i++ {
		a.emitShardHandoverReadiness(req, shardStatus{shardID: 1, isReady: true}, notReadyShards)
	}
	require.Empty(t, lg.records)

	// Going not-ready emits once, however many polls it stays that way.
	for i := 0; i < 5; i++ {
		a.emitShardHandoverReadiness(req, notReady, notReadyShards)
	}
	require.Len(t, lg.records, 1)

	// Clearing emits once more.
	for i := 0; i < 5; i++ {
		a.emitShardHandoverReadiness(req, ready, notReadyShards)
	}
	require.Len(t, lg.records, 2)

	// Flapping back emits again.
	a.emitShardHandoverReadiness(req, notReady, notReadyShards)
	require.Len(t, lg.records, 3)

	d := lg.details(t)
	require.EqualValues(t, 3, d[0]["shard_id"])
	require.Equal(t, false, d[0]["ready"])
	require.EqualValues(t, 42, d[0]["lagging_tasks"])
	require.Equal(t, "target", d[0]["remote_cluster"])
	require.Equal(t, readinessTestNsID, namespaceIDAttr(lg.records[0]))
	require.Equal(t, true, d[1]["ready"])
	require.Equal(t, false, d[2]["ready"])
}

// Each shard is tracked independently.
func TestEmitShardHandoverReadinessPerShard(t *testing.T) {
	lg := &eventCaptureLogger{}
	a := newReadinessTestActivities(t, lg)
	req := waitHandoverRequest{Namespace: readinessTestNsName, RemoteCluster: "target"}
	notReadyShards := make(map[int32]bool)

	for shardID := int32(0); shardID < 4; shardID++ {
		a.emitShardHandoverReadiness(req, shardStatus{shardID: shardID, laggingTasks: 1}, notReadyShards)
	}
	require.Len(t, lg.records, 4)
	require.Len(t, notReadyShards, 4)

	// Only shard 2 clears.
	a.emitShardHandoverReadiness(req, shardStatus{shardID: 2, isReady: true}, notReadyShards)
	require.Len(t, lg.records, 5)
	require.Len(t, notReadyShards, 3)
	require.NotContains(t, notReadyShards, int32(2))
}

// namespaceIDAttr reads the record's namespace_id identity attribute.
func namespaceIDAttr(rec log.Record) string {
	var out string
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		if kv.Key == "namespace_id" {
			out = kv.Value.AsString()
		}
		return true
	})
	return out
}
