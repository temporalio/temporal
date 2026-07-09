package wideevents

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log"
)

// attrMap indexes a payload's emitted attributes by key for assertions.
func attrMap(kvs []log.KeyValue) map[string]log.Value {
	m := make(map[string]log.Value, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value
	}
	return m
}

func TestReplicationLifecycleEventName(t *testing.T) {
	require.Equal(t, "replication_lifecycle", ReplicationLifecyclePayload{}.EventName())
}

func TestReplicationLifecycleEncodeSent(t *testing.T) {
	p := ReplicationLifecyclePayload{
		Phase:           ReplicationSent,
		TaskType:        ReplTaskSyncVersionedTransition,
		Shard:           3,
		NamespaceID:     "ns-id",
		WorkflowID:      "wf-id",
		RunID:           "run-id",
		FailoverVersion: 5,
		TransitionCount: 7,
		IsFirstSync:     true,
		FirstEventID:    1,
		NextEventID:     8,
	}
	f := attrMap(p.Attributes())

	require.Equal(t, "sent", f["phase"].AsString())
	require.True(t, f["is_first_sync"].AsBool())
	require.Equal(t, int64(1), f["first_event_id"].AsInt64())
	require.Equal(t, int64(8), f["next_event_id"].AsInt64())
	require.Equal(t, ReplTaskSyncVersionedTransition, f["task_type"].AsString())
	require.Equal(t, int64(3), f["shard"].AsInt64())
	require.Equal(t, "ns-id", f["namespace_id"].AsString())
	require.Equal(t, "wf-id", f["workflow_id"].AsString())
	require.Equal(t, "run-id", f["run_id"].AsString())
	require.Equal(t, int64(5), f["failover_version"].AsInt64())
	require.Equal(t, int64(7), f["transition_count"].AsInt64())
	// applied-only fields must be absent for the sent phase.
	_, ok := f["outcome"]
	require.False(t, ok)
}

func TestReplicationLifecycleEncodeApplied(t *testing.T) {
	p := ReplicationLifecyclePayload{
		Phase:              ReplicationApplied,
		TaskType:           ReplTaskSyncVersionedTransition,
		Shard:              1,
		NamespaceID:        "ns-id",
		WorkflowID:         "wf-id",
		RunID:              "run-id",
		State:              "Running",
		Status:             "Unspecified",
		AppliedNextEventID: 10,
		TransitionHistory:  []VersionedTransitionEntry{{FailoverVersion: 5, TransitionCount: 7}},
		LastEventID:        9,
		LastEventVersion:   5,
		Outcome:            "applied",
		NewExecutionRunID:  "new-run",
		SignalCount:        6,
		UpdateCount:        2,
	}
	f := attrMap(p.Attributes())

	require.Equal(t, "applied", f["phase"].AsString())
	require.Equal(t, "new-run", f["new_execution_run_id"].AsString())
	require.Equal(t, int64(6), f["signal_count"].AsInt64())
	require.Equal(t, int64(2), f["update_count"].AsInt64())
	// zero-valued applied summary fields must be omitted.
	_, ok := f["activity_count"]
	require.False(t, ok)
	require.Equal(t, "applied", f["outcome"].AsString())
	require.Equal(t, "Running", f["state"].AsString())
	require.Equal(t, "Unspecified", f["status"].AsString())
	require.Equal(t, int64(10), f["applied_next_event_id"].AsInt64())
	// composite fields are emitted as a compact JSON string.
	require.JSONEq(t, `[{"failover_version":5,"transition_count":7}]`, f["transition_history"].AsString())
	require.Equal(t, int64(9), f["last_event_id"].AsInt64())
	require.Equal(t, int64(5), f["last_event_version"].AsInt64())
	// sent-only fields must be absent.
	_, ok = f["is_first_sync"]
	require.False(t, ok)
}

func TestEmitReplicationLifecycleNilSafe(t *testing.T) {
	require.NotPanics(t, func() {
		Emit(nil, ReplicationLifecyclePayload{Phase: ReplicationSent})
	})
}

// fullyPopulatedReplication returns a payload with every field set to a non-zero value so that
// Attributes exercises every conditional branch. Phase-specific fields are only emitted for their
// phase, so callers must union the results across all phases to see the full field set.
func fullyPopulatedReplication(phase ReplicationPhase) ReplicationLifecyclePayload {
	return ReplicationLifecyclePayload{
		Phase:               phase,
		TaskType:            ReplTaskSyncVersionedTransition,
		Shard:               1,
		Namespace:           "ns",
		NamespaceID:         "ns-id",
		WorkflowID:          "wf-id",
		RunID:               "run-id",
		FailoverVersion:     5,
		TransitionCount:     7,
		ParentWorkflowID:    "p-wf",
		ParentRunID:         "p-run",
		ParentInitiatedID:   3,
		Details:             map[string]any{"k": "v"},
		NewRunID:            "new-run",
		IsFirstSync:         true,
		FirstEventID:        1,
		NextEventID:         8,
		Attempt:             2,
		EventVersionHistory: []VersionHistoryEntry{{EventID: 9, Version: 5}},
		State:               "Running",
		Status:              "Unspecified",
		AppliedNextEventID:  10,
		TransitionHistory:   []VersionedTransitionEntry{{FailoverVersion: 5, TransitionCount: 7}},
		LastEventID:         9,
		LastEventVersion:    5,
		Outcome:             "applied",
		Error:               "boom",
		NewExecutionRunID:   "ne-run",
		ResetRunID:          "reset-run",
		SignalCount:         6,
		ActivityCount:       4,
		UserTimerCount:      3,
		ChildExecutionCount: 2,
		UpdateCount:         1,
	}
}

// TestReplicationLifecycleFieldSetLocked pins the complete set of field names ReplicationLifecycle
// can emit, across every phase. This set is the event's published wire contract that downstream
// consumers depend on. If this test fails you have added, removed, or renamed an emitted field:
// do so deliberately, get the change reviewed, and then update `want` to match.
func TestReplicationLifecycleFieldSetLocked(t *testing.T) {
	want := []string{
		"phase", "task_type", "shard", "namespace", "namespace_id", "workflow_id", "run_id",
		"failover_version", "transition_count",
		"parent_workflow_id", "parent_run_id", "parent_initiated_id",
		"details",
		"new_run_id", "is_first_sync", "first_event_id", "next_event_id",
		"attempt",
		"event_version_history",
		"outcome", "error", "state", "status", "applied_next_event_id",
		"transition_history", "last_event_id", "last_event_version",
		"new_execution_run_id", "reset_run_id",
		"signal_count", "activity_count", "user_timer_count", "child_execution_count", "update_count",
	}

	got := make(map[string]struct{})
	for _, phase := range []ReplicationPhase{ReplicationSent, ReplicationExecuting, ReplicationApplied} {
		for _, kv := range fullyPopulatedReplication(phase).Attributes() {
			got[kv.Key] = struct{}{}
		}
	}
	gotKeys := make([]string, 0, len(got))
	for k := range got {
		gotKeys = append(gotKeys, k)
	}

	require.ElementsMatch(t, want, gotKeys,
		"ReplicationLifecycle emitted-field set changed; this alters the event's published wire "+
			"contract. Make the change deliberately, get it reviewed, then update `want`.")
}
