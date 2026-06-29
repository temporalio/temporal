package events

var ReplicationLifecycle = NewEventDef("replication_lifecycle",
	WithField("phase", FieldString),
	WithField("task_type", FieldString),
	WithField("shard", FieldInt64),
	WithField("namespace", FieldString),
	WithField("namespace_id", FieldString),
	WithField("workflow_id", FieldString),
	WithField("run_id", FieldString),
	WithField("failover_version", FieldInt64),
	WithField("transition_count", FieldInt64),
)

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
	// sent-only
	IsForceReplication bool
	ArchetypeID        uint32
	VerifyNextEventID  int64
	NewRunID           string
	IsFirstTask        bool
	IsFirstSync        bool
	FirstEventID       int64
	NextEventID        int64
	// received-only
	Attempt int32
	// applied-only: post-apply mutable-state SUMMARY (no blob)
	State                     string
	Status                    string
	AppliedNextEventID        int64
	LastWriteVersion          int64
	AppliedFailoverVersion    int64
	AppliedTransitionCount    int64
	TransitionHistoryLen      int32
	LastEventID               int64
	LastEventVersion          int64
	Outcome                   string
	Error                     string
	StateTransitionCount      int64
	NewExecutionRunID         string
	SuccessorRunID            string
	FirstExecutionRunID       string
	OriginalExecutionRunID    string
	ResetRunID                string
	WorkflowWasReset          bool
	StartTime                 string
	ExecutionTime             string
	CloseTime                 string
	LastUpdateFailoverVersion int64
	LastUpdateTransitionCount int64
	SignalCount               int64
	ActivityCount             int64
	ChildExecutionCount       int64
	UpdateCount               int64
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
	switch p.Phase {
	case ReplicationSent:
		enc.Bool("is_force_replication", p.IsForceReplication)
		if p.ArchetypeID != 0 {
			enc.Int64("archetype_id", int64(p.ArchetypeID))
		}
		if p.TaskType == ReplTaskVerifyVersionedTransition {
			enc.Int64("verify_next_event_id", p.VerifyNextEventID)
			if p.NewRunID != "" {
				enc.String("new_run_id", p.NewRunID)
			}
		}
		enc.Bool("is_first_task", p.IsFirstTask)
		enc.Bool("is_first_sync", p.IsFirstSync)
		if p.FirstEventID != 0 {
			enc.Int64("first_event_id", p.FirstEventID)
		}
		if p.NextEventID != 0 {
			enc.Int64("next_event_id", p.NextEventID)
		}
	case ReplicationExecuting:
		enc.Int64("attempt", int64(p.Attempt))
	case ReplicationApplied:
		enc.String("outcome", p.Outcome)
		if p.Error != "" {
			enc.String("error", p.Error)
		}
		if p.State != "" {
			enc.String("state", p.State)
			enc.String("status", p.Status)
			enc.Int64("applied_next_event_id", p.AppliedNextEventID)
			enc.Int64("last_write_version", p.LastWriteVersion)
			enc.Int64("applied_failover_version", p.AppliedFailoverVersion)
			enc.Int64("applied_transition_count", p.AppliedTransitionCount)
			enc.Int64("transition_history_len", int64(p.TransitionHistoryLen))
			enc.Int64("last_event_id", p.LastEventID)
			enc.Int64("last_event_version", p.LastEventVersion)
			if p.StateTransitionCount != 0 {
				enc.Int64("state_transition_count", p.StateTransitionCount)
			}
			if p.NewExecutionRunID != "" {
				enc.String("new_execution_run_id", p.NewExecutionRunID)
			}
			if p.SuccessorRunID != "" {
				enc.String("successor_run_id", p.SuccessorRunID)
			}
			if p.FirstExecutionRunID != "" {
				enc.String("first_execution_run_id", p.FirstExecutionRunID)
			}
			if p.OriginalExecutionRunID != "" {
				enc.String("original_execution_run_id", p.OriginalExecutionRunID)
			}
			if p.ResetRunID != "" {
				enc.String("reset_run_id", p.ResetRunID)
			}
			if p.WorkflowWasReset {
				enc.Bool("workflow_was_reset", true)
			}
			if p.StartTime != "" {
				enc.String("start_time", p.StartTime)
			}
			if p.ExecutionTime != "" {
				enc.String("execution_time", p.ExecutionTime)
			}
			if p.CloseTime != "" {
				enc.String("close_time", p.CloseTime)
			}
			if p.LastUpdateFailoverVersion != 0 {
				enc.Int64("last_update_failover_version", p.LastUpdateFailoverVersion)
			}
			if p.LastUpdateTransitionCount != 0 {
				enc.Int64("last_update_transition_count", p.LastUpdateTransitionCount)
			}
			if p.SignalCount != 0 {
				enc.Int64("signal_count", p.SignalCount)
			}
			if p.ActivityCount != 0 {
				enc.Int64("activity_count", p.ActivityCount)
			}
			if p.ChildExecutionCount != 0 {
				enc.Int64("child_execution_count", p.ChildExecutionCount)
			}
			if p.UpdateCount != 0 {
				enc.Int64("update_count", p.UpdateCount)
			}
		}
	}
}

// EmitReplicationLifecycle is a nil-safe helper.
func EmitReplicationLifecycle(h Handler, p ReplicationLifecyclePayload) {
	if h == nil {
		return
	}
	ReplicationLifecycle.With(h).Emit(p)
}
