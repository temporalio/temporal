package replication

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// laneEventFields are the fields of a lane event that show where a lane's ownership begins
// and ends relative to the default lane.
var laneEventFields = []string{
	"lane-key", "lane-scope-min", "lane-scope-end", "lane-cursor", "lane-acked",
	"lane-pending", "lane-retiring", "lane-paused", "default-reserved-cursor", "default-completed-cursor",
}

type loggedEvent struct {
	message string
	fields  map[string]any
}

func loggedEvents(logs *observer.ObservedLogs, fields []string) []loggedEvent {
	var events []loggedEvent
	for _, entry := range logs.All() {
		context := entry.ContextMap()
		selected := make(map[string]any, len(fields))
		for _, field := range fields {
			if value, ok := context[field]; ok {
				selected[field] = value
			}
		}
		events = append(events, loggedEvent{message: entry.Message, fields: selected})
	}
	return events
}

func laneEvent(message string, scopeEnd, cursor, acked int64, pending, retiring, paused bool, defaultReserved, defaultCompleted int64) loggedEvent {
	return loggedEvent{message: message, fields: map[string]any{
		"lane-key":                 "a",
		"lane-scope-min":           int64(150),
		"lane-scope-end":           scopeEnd,
		"lane-cursor":              cursor,
		"lane-acked":               acked,
		"lane-pending":             pending,
		"lane-retiring":            retiring,
		"lane-paused":              paused,
		"default-reserved-cursor":  defaultReserved,
		"default-completed-cursor": defaultCompleted,
	}}
}

func TestSenderLaneRegistryLogsEachLaneTransition(t *testing.T) {
	t.Parallel()
	core, logs := observer.New(zap.InfoLevel)
	r, err := newSenderLaneRegistry(100, nil, log.NewZapLogger(zap.New(core)))
	require.NoError(t, err)
	ack := func(laneID string, watermark int64, command enumsspb.ReplicationFlowControlCommand) []lanePauseChange {
		return r.ObserveAcks(map[string]*replicationspb.ReplicationState{
			laneID: {InclusiveLowWatermark: watermark, FlowControlCommand: command},
		})
	}

	// A default range in flight keeps the new lane pending until the range completes.
	_, acquired := r.AcquireDefault(200)
	require.True(t, acquired)
	lane, created, err := r.Create("a", namespaceLaneFilter("a"), 150, nil)
	require.NoError(t, err)
	require.True(t, created)
	r.ReleaseDefault(200, true)

	require.Equal(t, []lanePauseChange{{logicalKey: "a", paused: true}}, ack(lane.id, 180, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE))
	require.Empty(t, ack(lane.id, 180, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE))
	require.Equal(t, []lanePauseChange{{logicalKey: "a", paused: false}}, ack(lane.id, 180, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME))

	// A default reservation ahead of the lane keeps its retirement boundary open until the
	// lane has sent through it.
	_, acquired = r.AcquireDefault(300)
	require.True(t, acquired)
	require.True(t, r.RequestRetirement("a"))
	_, acquired = r.Acquire(lane.id)
	require.True(t, acquired)
	r.AdvanceLaneCursor(lane.id, 300)
	r.Release(lane.id)
	r.ReleaseDefault(300, true)
	ack(lane.id, 300, enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME)
	_, completed := r.CompleteRetirement(lane.id)
	require.True(t, completed)

	const open = int64(math.MaxInt64)
	require.Equal(t, []loggedEvent{
		laneEvent("Created a replication lane.", open, 150, 0, true, false, false, 200, 100),
		laneEvent("Completed the default lane handoff to a replication lane.", open, 200, 0, false, false, false, 200, 200),
		laneEvent("Receiver paused a replication lane.", open, 200, 180, false, false, true, 200, 200),
		laneEvent("Receiver resumed a replication lane.", open, 200, 180, false, false, false, 200, 200),
		laneEvent("Requested retirement of a replication lane.", open, 200, 180, false, true, false, 300, 200),
		laneEvent("Fixed the retirement boundary of a replication lane.", 300, 300, 180, false, true, false, 300, 200),
		laneEvent("Retired a replication lane.", 300, 300, 300, false, true, false, 300, 300),
	}, loggedEvents(logs, laneEventFields))
}

func TestSenderLaneRegistryLogsRestoredAndDroppedLanes(t *testing.T) {
	t.Parallel()
	source, err := newSenderLaneRegistry(100, nil, log.NewNoopLogger())
	require.NoError(t, err)
	lane, _, err := source.Create("a", namespaceLaneFilter("a"), 150, nil)
	require.NoError(t, err)
	source.ObserveAcks(map[string]*replicationspb.ReplicationState{lane.id: {InclusiveLowWatermark: 170}})
	state := source.BuildReaderState(syncReplicationState(400, 400, 400))

	core, logs := observer.New(zap.InfoLevel)
	restored, err := newSenderLaneRegistry(laneDefaultCursor(state), state.Lanes, log.NewZapLogger(zap.New(core)))
	require.NoError(t, err)
	restored.Forget("a")
	restored.RequireDefaultRecovery(170)

	fields := []string{"lane-key", "lane-scope-min", "lane-scope-end", "default-replay-floor"}
	require.Equal(t, []loggedEvent{
		{message: "Restored a replication lane.", fields: map[string]any{"lane-key": "a", "lane-scope-min": int64(170), "lane-scope-end": int64(math.MaxInt64)}},
		{message: "Dropped a restored replication lane.", fields: map[string]any{"lane-key": "a", "lane-scope-min": int64(170), "lane-scope-end": int64(math.MaxInt64)}},
		{message: "Required a default lane replay before any lane handoff.", fields: map[string]any{"default-replay-floor": int64(170)}},
	}, loggedEvents(logs, fields))
}

func TestManualLaneStreamPolicyLogsRateChanges(t *testing.T) {
	t.Parallel()
	dc := dynamicconfig.NewMemoryClient()
	setEntries := func(entries ...dynamicconfig.ReplicationLaneIsolationEntry) {
		dc.OverrideSetting(dynamicconfig.ReplicationLaneIsolation, append([]dynamicconfig.ReplicationLaneIsolationEntry{}, entries...))
	}
	core, logs := observer.New(zap.InfoLevel)
	policy, err := NewManualLaneStreamPolicyFactory(dynamicconfig.NewCollection(dc, log.NewNoopLogger()), log.NewZapLogger(zap.New(core))).
		NewStreamPolicy(LaneStream{RemoteCluster: "remote", ShardID: 1})
	require.NoError(t, err)
	evaluate := func() {
		_, err := policy.Evaluate(LaneSignals{})
		require.NoError(t, err)
	}

	// Lanes that are added or withdrawn do not change a rate.
	setEntries(dynamicconfig.ReplicationLaneIsolationEntry{RemoteCluster: "remote", NamespaceID: "ns-a", TasksPerSecond: 10})
	evaluate()
	setEntries(
		dynamicconfig.ReplicationLaneIsolationEntry{RemoteCluster: "remote", NamespaceID: "ns-a", TasksPerSecond: 20},
		dynamicconfig.ReplicationLaneIsolationEntry{RemoteCluster: "remote", NamespaceID: "ns-b", TasksPerSecond: 5},
	)
	evaluate()
	evaluate()
	setEntries()
	evaluate()

	require.Equal(t, []loggedEvent{{
		message: "Changed the rate of a replication lane.",
		fields: map[string]any{
			"lane-key":                  namespaceLaneKey("ns-a"),
			"previous-tasks-per-second": float64(10),
			"tasks-per-second":          float64(20),
		},
	}}, loggedEvents(logs, []string{"lane-key", "previous-tasks-per-second", "tasks-per-second"}))
}
