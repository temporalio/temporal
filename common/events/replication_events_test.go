package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// captureEncoder records all encoded fields for assertions.
type captureEncoder struct {
	fields map[string]any
}

func newCaptureEncoder() *captureEncoder {
	return &captureEncoder{fields: make(map[string]any)}
}

func (e *captureEncoder) String(key, value string)      { e.fields[key] = value }
func (e *captureEncoder) Int64(key string, v int64)     { e.fields[key] = v }
func (e *captureEncoder) Float64(key string, v float64) { e.fields[key] = v }
func (e *captureEncoder) Bool(key string, v bool)       { e.fields[key] = v }
func (e *captureEncoder) Any(key string, v any)         { e.fields[key] = v }

func TestReplicationLifecycleRegisteredName(t *testing.T) {
	require.Equal(t, "replication_lifecycle", ReplicationLifecycle.Name())
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
	enc := newCaptureEncoder()
	p.Encode(enc)

	require.Equal(t, "sent", enc.fields["phase"])
	require.Equal(t, true, enc.fields["is_first_sync"])
	require.Equal(t, int64(1), enc.fields["first_event_id"])
	require.Equal(t, int64(8), enc.fields["next_event_id"])
	require.Equal(t, ReplTaskSyncVersionedTransition, enc.fields["task_type"])
	require.Equal(t, int64(3), enc.fields["shard"])
	require.Equal(t, "ns-id", enc.fields["namespace_id"])
	require.Equal(t, "wf-id", enc.fields["workflow_id"])
	require.Equal(t, "run-id", enc.fields["run_id"])
	require.Equal(t, int64(5), enc.fields["failover_version"])
	require.Equal(t, int64(7), enc.fields["transition_count"])
	// verify-only fields must be absent for non-verify task type.
	_, ok := enc.fields["verify_next_event_id"]
	require.False(t, ok)
}

func TestReplicationLifecycleEncodeApplied(t *testing.T) {
	p := ReplicationLifecyclePayload{
		Phase:                ReplicationApplied,
		TaskType:             ReplTaskSyncVersionedTransition,
		Shard:                1,
		NamespaceID:          "ns-id",
		WorkflowID:           "wf-id",
		RunID:                "run-id",
		State:                "Running",
		Status:               "Unspecified",
		AppliedNextEventID:   10,
		TransitionHistoryLen: 2,
		LastEventID:          9,
		Outcome:              "applied",
		NewExecutionRunID:    "new-run",
		SignalCount:          6,
		UpdateCount:          2,
	}
	enc := newCaptureEncoder()
	p.Encode(enc)

	require.Equal(t, "applied", enc.fields["phase"])
	require.Equal(t, "new-run", enc.fields["new_execution_run_id"])
	require.Equal(t, int64(6), enc.fields["signal_count"])
	require.Equal(t, int64(2), enc.fields["update_count"])
	// zero-valued applied summary fields must be omitted.
	_, ok := enc.fields["activity_count"]
	require.False(t, ok)
	require.Equal(t, "applied", enc.fields["outcome"])
	require.Equal(t, "Running", enc.fields["state"])
	require.Equal(t, "Unspecified", enc.fields["status"])
	require.Equal(t, int64(10), enc.fields["applied_next_event_id"])
	require.Equal(t, int64(2), enc.fields["transition_history_len"])
	require.Equal(t, int64(9), enc.fields["last_event_id"])
	// sent-only fields must be absent.
	_, ok = enc.fields["is_force_replication"]
	require.False(t, ok)
}

func TestEmitReplicationLifecycleNilSafe(t *testing.T) {
	require.NotPanics(t, func() {
		EmitReplicationLifecycle(nil, ReplicationLifecyclePayload{Phase: ReplicationSent})
	})
}

// fullyPopulatedReplication returns a payload with every field set to a non-zero value so that
// Encode exercises every conditional branch. Phase-specific fields are only emitted for their
// phase, so callers must union the results across all phases to see the full field set.
func fullyPopulatedReplication(phase ReplicationPhase) ReplicationLifecyclePayload {
	return ReplicationLifecyclePayload{
		Phase:                phase,
		TaskType:             ReplTaskSyncVersionedTransition,
		Shard:                1,
		Namespace:            "ns",
		NamespaceID:          "ns-id",
		WorkflowID:           "wf-id",
		RunID:                "run-id",
		FailoverVersion:      5,
		TransitionCount:      7,
		ParentWorkflowID:     "p-wf",
		ParentRunID:          "p-run",
		ParentInitiatedID:    3,
		Details:              map[string]any{"k": "v"},
		NewRunID:             "new-run",
		IsFirstSync:          true,
		FirstEventID:         1,
		NextEventID:          8,
		Attempt:              2,
		State:                "Running",
		Status:               "Unspecified",
		AppliedNextEventID:   10,
		TransitionHistoryLen: 2,
		LastEventID:          9,
		Outcome:              "applied",
		Error:                "boom",
		NewExecutionRunID:    "ne-run",
		ResetRunID:           "reset-run",
		SignalCount:          6,
		ActivityCount:        4,
		UserTimerCount:       3,
		ChildExecutionCount:  2,
		UpdateCount:          1,
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
		"outcome", "error", "state", "status", "applied_next_event_id",
		"transition_history_len", "last_event_id",
		"new_execution_run_id", "reset_run_id",
		"signal_count", "activity_count", "user_timer_count", "child_execution_count", "update_count",
	}

	got := make(map[string]struct{})
	for _, phase := range []ReplicationPhase{ReplicationSent, ReplicationExecuting, ReplicationApplied} {
		enc := newCaptureEncoder()
		fullyPopulatedReplication(phase).Encode(enc)
		for k := range enc.fields {
			got[k] = struct{}{}
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
