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
		Phase:              ReplicationSent,
		TaskType:           ReplTaskSyncVersionedTransition,
		Shard:              3,
		NamespaceID:        "ns-id",
		WorkflowID:         "wf-id",
		RunID:              "run-id",
		FailoverVersion:    5,
		TransitionCount:    7,
		IsForceReplication: true,
		ArchetypeID:        42,
		IsFirstTask:        true,
		IsFirstSync:        true,
		FirstEventID:       1,
		NextEventID:        8,
	}
	enc := newCaptureEncoder()
	p.Encode(enc)

	require.Equal(t, "sent", enc.fields["phase"])
	require.Equal(t, true, enc.fields["is_first_task"])
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
	require.Equal(t, true, enc.fields["is_force_replication"])
	require.Equal(t, int64(42), enc.fields["archetype_id"])
	// verify-only fields must be absent for non-verify task type.
	_, ok := enc.fields["verify_next_event_id"]
	require.False(t, ok)
}

func TestReplicationLifecycleEncodeApplied(t *testing.T) {
	p := ReplicationLifecyclePayload{
		Phase:                  ReplicationApplied,
		TaskType:               ReplTaskSyncVersionedTransition,
		Shard:                  1,
		NamespaceID:            "ns-id",
		WorkflowID:             "wf-id",
		RunID:                  "run-id",
		State:                  "Running",
		Status:                 "Unspecified",
		AppliedNextEventID:     10,
		LastWriteVersion:       2,
		AppliedFailoverVersion: 3,
		AppliedTransitionCount: 4,
		TransitionHistoryLen:   2,
		LastEventID:            9,
		LastEventVersion:       3,
		Outcome:                "applied",
		StateTransitionCount:   11,
		NewExecutionRunID:      "new-run",
		WorkflowWasReset:       true,
		StartTime:              "2026-06-29T00:00:00Z",
		SignalCount:            6,
		UpdateCount:            2,
	}
	enc := newCaptureEncoder()
	p.Encode(enc)

	require.Equal(t, "applied", enc.fields["phase"])
	require.Equal(t, int64(11), enc.fields["state_transition_count"])
	require.Equal(t, "new-run", enc.fields["new_execution_run_id"])
	require.Equal(t, true, enc.fields["workflow_was_reset"])
	require.Equal(t, "2026-06-29T00:00:00Z", enc.fields["start_time"])
	require.Equal(t, int64(6), enc.fields["signal_count"])
	require.Equal(t, int64(2), enc.fields["update_count"])
	// zero-valued applied summary fields must be omitted.
	_, ok := enc.fields["activity_count"]
	require.False(t, ok)
	require.Equal(t, "applied", enc.fields["outcome"])
	require.Equal(t, "Running", enc.fields["state"])
	require.Equal(t, "Unspecified", enc.fields["status"])
	require.Equal(t, int64(10), enc.fields["applied_next_event_id"])
	require.Equal(t, int64(2), enc.fields["last_write_version"])
	require.Equal(t, int64(3), enc.fields["applied_failover_version"])
	require.Equal(t, int64(4), enc.fields["applied_transition_count"])
	require.Equal(t, int64(2), enc.fields["transition_history_len"])
	require.Equal(t, int64(9), enc.fields["last_event_id"])
	require.Equal(t, int64(3), enc.fields["last_event_version"])
	// sent-only fields must be absent.
	_, ok = enc.fields["is_force_replication"]
	require.False(t, ok)
}

func TestEmitReplicationLifecycleNilSafe(t *testing.T) {
	require.NotPanics(t, func() {
		EmitReplicationLifecycle(nil, ReplicationLifecyclePayload{Phase: ReplicationSent})
	})
}
