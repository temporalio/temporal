package events

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

var ReplicationLifecycle = NewEventDef("replication_lifecycle")

type ReplicationPhase string

const (
	ReplicationSent      ReplicationPhase = "sent"
	ReplicationExecuting ReplicationPhase = "executing"
	ReplicationApplied   ReplicationPhase = "applied"
)

const (
	ReplTaskSyncWorkflowState         = "sync_workflow_state"
	ReplTaskSyncVersionedTransition   = "sync_versioned_transition"
	ReplTaskVerifyVersionedTransition = "verify_versioned_transition"
)

type ReplicationLifecyclePayload struct {
	Phase    ReplicationPhase
	TaskType string
	Shard    int32
	// task identity + cross-phase join key: (namespace_id, workflow_id, run_id, transition_count, task_type)
	Namespace       string
	NamespaceID     string
	WorkflowID      string
	RunID           string
	FailoverVersion int64
	TransitionCount int64
	// parent info: populated at every phase where mutable state is in hand (sent + applied) when
	// this workflow is itself a child. Only the child->parent direction is emitted (a child points
	// at its parent); the parent->children direction is intentionally not recorded here.
	ParentWorkflowID  string
	ParentRunID       string
	ParentInitiatedID int64
	Details           map[string]any
	// sent-only
	NewRunID     string
	IsFirstSync  bool
	FirstEventID int64
	NextEventID  int64
	// received-only
	Attempt int32
	// applied-only: post-apply mutable-state SUMMARY (no blob)
	State                string
	Status               string
	AppliedNextEventID   int64
	TransitionHistoryLen int32
	LastEventID          int64
	Outcome              string
	Error                string
	NewExecutionRunID    string
	ResetRunID           string
	SignalCount          int64
	ActivityCount        int64
	UserTimerCount       int64
	ChildExecutionCount  int64
	UpdateCount          int64
}

func (p ReplicationLifecyclePayload) Encode(enc Encoder) {
	enc.String("phase", string(p.Phase))
	enc.String("task_type", p.TaskType)
	enc.Int64("shard", int64(p.Shard))
	enc.String("namespace", p.Namespace)
	enc.String("namespace_id", p.NamespaceID)
	enc.String("workflow_id", p.WorkflowID)
	enc.String("run_id", p.RunID)
	if p.FailoverVersion != 0 || p.TransitionCount != 0 {
		enc.Int64("failover_version", p.FailoverVersion)
		enc.Int64("transition_count", p.TransitionCount)
	}
	// parent fields are phase-independent: emitted on any phase that populated them (sent +
	// applied). Guards keep them absent when not applicable (e.g. executing, or a workflow that is
	// not a child).
	if p.ParentWorkflowID != "" {
		enc.String("parent_workflow_id", p.ParentWorkflowID)
		enc.String("parent_run_id", p.ParentRunID)
		if p.ParentInitiatedID != 0 {
			enc.Int64("parent_initiated_id", p.ParentInitiatedID)
		}
	}
	if len(p.Details) > 0 {
		enc.Any("details", p.Details)
	}
	switch p.Phase {
	case ReplicationSent:
		p.encodeSent(enc)
	case ReplicationExecuting:
		enc.Int64("attempt", int64(p.Attempt))
	case ReplicationApplied:
		p.encodeApplied(enc)
	default:
	}
}

func (p ReplicationLifecyclePayload) encodeSent(enc Encoder) {
	if p.NewRunID != "" {
		enc.String("new_run_id", p.NewRunID)
	}
	enc.Bool("is_first_sync", p.IsFirstSync)
	if p.FirstEventID != 0 {
		enc.Int64("first_event_id", p.FirstEventID)
	}
	if p.NextEventID != 0 {
		enc.Int64("next_event_id", p.NextEventID)
	}
}

func (p ReplicationLifecyclePayload) encodeApplied(enc Encoder) {
	enc.String("outcome", p.Outcome)
	if p.Error != "" {
		enc.String("error", p.Error)
	}
	if p.State == "" {
		return
	}
	enc.String("state", p.State)
	enc.String("status", p.Status)
	enc.Int64("applied_next_event_id", p.AppliedNextEventID)
	enc.Int64("transition_history_len", int64(p.TransitionHistoryLen))
	enc.Int64("last_event_id", p.LastEventID)
	if p.NewExecutionRunID != "" {
		enc.String("new_execution_run_id", p.NewExecutionRunID)
	}
	if p.ResetRunID != "" {
		enc.String("reset_run_id", p.ResetRunID)
	}
	if p.SignalCount != 0 {
		enc.Int64("signal_count", p.SignalCount)
	}
	if p.ActivityCount != 0 {
		enc.Int64("activity_count", p.ActivityCount)
	}
	if p.UserTimerCount != 0 {
		enc.Int64("user_timer_count", p.UserTimerCount)
	}
	if p.ChildExecutionCount != 0 {
		enc.Int64("child_execution_count", p.ChildExecutionCount)
	}
	if p.UpdateCount != 0 {
		enc.Int64("update_count", p.UpdateCount)
	}
}

// PopulateParentInfo records the parent workflow identity when info describes a child workflow.
// A nil info or a workflow with no parent is a no-op, so call sites never need to guard.
func (p *ReplicationLifecyclePayload) PopulateParentInfo(info *persistencespb.WorkflowExecutionInfo) {
	if info.GetParentWorkflowId() == "" {
		return
	}
	p.ParentWorkflowID = info.GetParentWorkflowId()
	p.ParentRunID = info.GetParentRunId()
	p.ParentInitiatedID = info.GetParentInitiatedId()
}

// EmitReplicationLifecycle is a nil-safe helper.
func EmitReplicationLifecycle(h Handler, p ReplicationLifecyclePayload) {
	if h == nil {
		return
	}
	ReplicationLifecycle.With(h).Emit(p)
}
