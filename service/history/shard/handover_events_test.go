package shard

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	commonlog "go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
)

const (
	handoverEventsTestShardID = int32(7)
	handoverEventsTestNsID    = "deadbeef-0000-0000-0000-000000000001"
	handoverEventsTestNsName  = "handover-events-test-ns"
)

// eventCaptureLogger records emitted wide events for assertions.
type eventCaptureLogger struct {
	embedded.Logger
	records []log.Record
}

func (c *eventCaptureLogger) Emit(_ context.Context, r log.Record) { c.records = append(c.records, r) }

func (c *eventCaptureLogger) Enabled(context.Context, log.EnabledParameters) bool { return true }

// phases returns each record's phase, in order.
func (c *eventCaptureLogger) phases() []string {
	out := make([]string, 0, len(c.records))
	for _, rec := range c.records {
		out = append(out, recordAttrs(rec)["phase"].AsString())
	}
	return out
}

func recordAttrs(rec log.Record) map[string]log.Value {
	got := map[string]log.Value{}
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		got[kv.Key] = kv.Value
		return true
	})
	return got
}

// recordDetails decodes the details attribute, emitted as compact JSON.
func recordDetails(t *testing.T, rec log.Record) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(recordAttrs(rec)["details"].AsString()), &out))
	return out
}

func newHandoverEventsTracker(t *testing.T, lg log.Logger, maxTaskID int64, stateErr error, enabled bool) *defaultHandoverTracker {
	t.Helper()
	clusterMetadata := cluster.NewMockMetadata(gomock.NewController(t))
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	tracker := NewDefaultHandoverTrackerFactory()(HandoverTrackerParams{
		ShardID:                      handoverEventsTestShardID,
		ClusterMetadata:              clusterMetadata,
		GetMaxReplicationTaskID:      func() int64 { return maxTaskID },
		ErrorByStateFn:               func() error { return stateErr },
		NotifyReplicationFn:          func(int64) {},
		Logger:                       commonlog.NewNoopLogger(),
		EventLogger:                  lg,
		EmitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(enabled),
	})
	return tracker.(*defaultHandoverTracker)
}

func handoverEventsNamespace(state enumspb.ReplicationState) *namespace.Namespace {
	return namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: handoverEventsTestNsID, Name: handoverEventsTestNsName},
		&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters:          []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
			State:             state,
		},
		1,
	)
}

func TestHandoverWatermarkEvents(t *testing.T) {
	lg := &eventCaptureLogger{}
	tracker := newHandoverEventsTracker(t, lg, 100, nil, true)

	inHandover := handoverEventsNamespace(enumspb.REPLICATION_STATE_HANDOVER)

	// Taking the watermark emits once.
	tracker.UpdateHandoverState(inHandover, false)
	require.Equal(t, []string{wideevents.PhaseHandoverWatermarkSet}, lg.phases())

	d := recordDetails(t, lg.records[0])
	require.EqualValues(t, handoverEventsTestShardID, d["shard_id"])
	require.EqualValues(t, 100, d["max_replication_task_id"])
	require.Equal(t, false, d["pending"])
	require.Equal(t, wideevents.WatermarkAdded, d["reason"])
	require.Equal(t, handoverEventsTestNsID, recordAttrs(lg.records[0])["namespace_id"].AsString())
	require.Equal(t, handoverEventsTestNsName, recordAttrs(lg.records[0])["namespace"].AsString())

	// A repeat notification at the same version is not a mutation, so it emits nothing.
	tracker.UpdateHandoverState(inHandover, false)
	require.Len(t, lg.records, 1)

	// A newer notification version advances the watermark.
	tracker.UpdateHandoverState(inHandover.Clone(namespace.WithNotificationVersion(2)), false)
	require.Equal(t, []string{wideevents.PhaseHandoverWatermarkSet, wideevents.PhaseHandoverWatermarkSet}, lg.phases())
	require.Equal(t, wideevents.WatermarkUpdated, recordDetails(t, lg.records[1])["reason"])

	// Leaving handover drops the watermark.
	tracker.UpdateHandoverState(handoverEventsNamespace(enumspb.REPLICATION_STATE_NORMAL), false)
	require.Equal(t, wideevents.PhaseHandoverWatermarkRemoved, lg.phases()[2])
	require.Equal(t, false, recordDetails(t, lg.records[2])["deleted_from_db"])
}

// The non-handover branch runs for every namespace notification on every shard, so it must only
// emit on a real removal.
func TestHandoverWatermarkRemovedOnlyWhenTracked(t *testing.T) {
	lg := &eventCaptureLogger{}
	tracker := newHandoverEventsTracker(t, lg, 100, nil, true)

	for range 3 {
		tracker.UpdateHandoverState(handoverEventsNamespace(enumspb.REPLICATION_STATE_NORMAL), false)
	}

	require.Empty(t, lg.records)
}

// An unacquired shard stores the sentinel, reported as pending; resolving it later emits nothing.
func TestHandoverWatermarkPendingThenResolved(t *testing.T) {
	lg := &eventCaptureLogger{}
	tracker := newHandoverEventsTracker(t, lg, 100, errors.New("shard status unknown"), true)

	tracker.UpdateHandoverState(handoverEventsNamespace(enumspb.REPLICATION_STATE_HANDOVER), false)
	require.Len(t, lg.records, 1)
	require.Equal(t, true, recordDetails(t, lg.records[0])["pending"])

	tracker.ResolvePendingTaskIDs(555)
	require.Len(t, lg.records, 1)
	require.EqualValues(t, 555, tracker.handoverNamespaces[handoverEventsTestNsName].MaxReplicationTaskID)
}

func TestHandoverWatermarkEventsDisabled(t *testing.T) {
	lg := &eventCaptureLogger{}
	tracker := newHandoverEventsTracker(t, lg, 100, nil, false)

	tracker.UpdateHandoverState(handoverEventsNamespace(enumspb.REPLICATION_STATE_HANDOVER), false)
	require.True(t, tracker.IsInHandover(handoverEventsTestNsName, ""))
	require.Empty(t, lg.records)

	tracker.UpdateHandoverState(handoverEventsNamespace(enumspb.REPLICATION_STATE_NORMAL), false)
	require.False(t, tracker.IsInHandover(handoverEventsTestNsName, ""))
	require.Empty(t, lg.records)
}
