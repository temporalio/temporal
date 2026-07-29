package wideevents

import (
	"go.opentelemetry.io/otel/log"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// ReplicationLifecycleEventName is the stable event name for the ReplicationLifecycle wide event,
// which traces a replication task sent -> executing -> applied.
const ReplicationLifecycleEventName = "replication_lifecycle"

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
	NewRunID string
	// SourceTaskID is the sender's replication-queue task id for this event, recorded so a "sent"
	// event can be correlated with the source cluster's replication queue.
	SourceTaskID int64
	IsFirstSync  bool
	FirstEventID int64
	NextEventID  int64
	// received-only
	Attempt int32
	// event_version_history is the (event_id, version) branch. It is emitted on executing (from the
	// task) and applied (from the resulting mutable state); the version disambiguates which history
	// branch the events are on.
	EventVersionHistory []VersionHistoryEntry
	// applied-only: post-apply mutable-state SUMMARY (no blob)
	State               string
	Status              string
	AppliedNextEventID  int64
	TransitionHistory   []VersionedTransitionEntry
	LastEventID         int64
	LastEventVersion    int64
	Outcome             string
	Error               string
	NewExecutionRunID   string
	ResetRunID          string
	SignalCount         int64
	ActivityCount       int64
	UserTimerCount      int64
	ChildExecutionCount int64
	UpdateCount         int64
}

// VersionedTransitionEntry is one entry of a workflow's transition history.
type VersionedTransitionEntry struct {
	FailoverVersion int64 `json:"failover_version"`
	TransitionCount int64 `json:"transition_count"`
}

// VersionHistoryEntry is one (event_id, version) point of an event version history branch.
type VersionHistoryEntry struct {
	EventID int64 `json:"event_id"`
	Version int64 `json:"version"`
}

func (p ReplicationLifecyclePayload) EventName() string { return ReplicationLifecycleEventName }

func (p ReplicationLifecyclePayload) Attributes() []log.KeyValue {
	attrs := []log.KeyValue{
		log.String("phase", string(p.Phase)),
		log.String("task_type", p.TaskType),
		log.Int64("shard", int64(p.Shard)),
		log.String("namespace", p.Namespace),
		log.String("namespace_id", p.NamespaceID),
		log.String("workflow_id", p.WorkflowID),
		log.String("run_id", p.RunID),
	}
	if p.FailoverVersion != 0 || p.TransitionCount != 0 {
		attrs = append(attrs,
			log.Int64("failover_version", p.FailoverVersion),
			log.Int64("transition_count", p.TransitionCount),
		)
	}
	// parent fields are phase-independent: emitted on any phase that populated them (sent +
	// applied). Guards keep them absent when not applicable (e.g. executing, or a workflow that is
	// not a child).
	if p.ParentWorkflowID != "" {
		attrs = append(attrs,
			log.String("parent_workflow_id", p.ParentWorkflowID),
			log.String("parent_run_id", p.ParentRunID),
		)
		if p.ParentInitiatedID != 0 {
			attrs = append(attrs, log.Int64("parent_initiated_id", p.ParentInitiatedID))
		}
	}
	if len(p.Details) > 0 {
		attrs = append(attrs, jsonAttr("details", p.Details))
	}
	if len(p.EventVersionHistory) > 0 {
		attrs = append(attrs, jsonAttr("event_version_history", p.EventVersionHistory))
	}
	switch p.Phase {
	case ReplicationSent:
		attrs = p.appendSent(attrs)
	case ReplicationExecuting:
		attrs = append(attrs, log.Int64("attempt", int64(p.Attempt)))
	case ReplicationApplied:
		attrs = p.appendApplied(attrs)
	default:
	}
	return attrs
}

func (p ReplicationLifecyclePayload) appendSent(attrs []log.KeyValue) []log.KeyValue {
	if p.NewRunID != "" {
		attrs = append(attrs, log.String("new_run_id", p.NewRunID))
	}
	if p.SourceTaskID != 0 {
		attrs = append(attrs, log.Int64("source_task_id", p.SourceTaskID))
	}
	attrs = append(attrs, log.Bool("is_first_sync", p.IsFirstSync))
	if p.FirstEventID != 0 {
		attrs = append(attrs, log.Int64("first_event_id", p.FirstEventID))
	}
	if p.NextEventID != 0 {
		attrs = append(attrs, log.Int64("next_event_id", p.NextEventID))
	}
	return attrs
}

func (p ReplicationLifecyclePayload) appendApplied(attrs []log.KeyValue) []log.KeyValue {
	attrs = append(attrs, log.String("outcome", p.Outcome))
	if p.Error != "" {
		attrs = append(attrs, log.String("error", p.Error))
	}
	if p.State == "" {
		return attrs
	}
	attrs = append(attrs,
		log.String("state", p.State),
		log.String("status", p.Status),
		log.Int64("applied_next_event_id", p.AppliedNextEventID),
	)
	if len(p.TransitionHistory) > 0 {
		attrs = append(attrs, jsonAttr("transition_history", p.TransitionHistory))
	}
	attrs = append(attrs, log.Int64("last_event_id", p.LastEventID))
	if p.LastEventVersion != 0 {
		attrs = append(attrs, log.Int64("last_event_version", p.LastEventVersion))
	}
	if p.NewExecutionRunID != "" {
		attrs = append(attrs, log.String("new_execution_run_id", p.NewExecutionRunID))
	}
	if p.ResetRunID != "" {
		attrs = append(attrs, log.String("reset_run_id", p.ResetRunID))
	}
	if p.SignalCount != 0 {
		attrs = append(attrs, log.Int64("signal_count", p.SignalCount))
	}
	if p.ActivityCount != 0 {
		attrs = append(attrs, log.Int64("activity_count", p.ActivityCount))
	}
	if p.UserTimerCount != 0 {
		attrs = append(attrs, log.Int64("user_timer_count", p.UserTimerCount))
	}
	if p.ChildExecutionCount != 0 {
		attrs = append(attrs, log.Int64("child_execution_count", p.ChildExecutionCount))
	}
	if p.UpdateCount != 0 {
		attrs = append(attrs, log.Int64("update_count", p.UpdateCount))
	}
	return attrs
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
