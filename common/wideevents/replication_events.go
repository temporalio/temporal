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
	// ReplicationSkipped is a terminal source-side phase: the sender could not build ("convert")
	// the task after exhausting retries and skipped it (advanced the watermark past it) instead of
	// wedging the stream. Such a task never reaches executing/applied, so this is where its trace
	// ends. See StreamSenderImpl.recordStuckTaskSkipped.
	ReplicationSkipped ReplicationPhase = "skipped"
)

const (
	ReplTaskSyncWorkflowState         = "sync_workflow_state"
	ReplTaskSyncVersionedTransition   = "sync_versioned_transition"
	ReplTaskVerifyVersionedTransition = "verify_versioned_transition"
	ReplTaskDeleteExecution           = "delete_execution"
	// The three below have no sent/executing/applied event today (emitReplicationSent does not build
	// them), but a raw queue task of these types can still be skipped, so the skipped event maps them
	// to keep task_type a single vocabulary across every phase.
	ReplTaskHistory      = "history"
	ReplTaskSyncActivity = "sync_activity"
	ReplTaskSyncHSM      = "sync_hsm"
)

// A mutation carries only what changed within (ExclusiveStartVT, HydratedVT]; a snapshot carries
// whole mutable state and has no lower bound.
const (
	ArtifactKindSnapshot = "snapshot"
	ArtifactKindMutation = "mutation"
)

type ReplicationLifecyclePayload struct {
	// Phase is sent, executing, applied or skipped.
	Phase ReplicationPhase
	// TaskType is which replication task this is.
	TaskType string
	// Shard is the SENDING shard on sent, and the TARGET shard on executing and applied.
	Shard int32
	// Namespace is the namespace name, resolved best-effort.
	Namespace string
	// NamespaceID/WorkflowID/RunID identify the execution being replicated.
	NamespaceID string
	WorkflowID  string
	RunID       string
	// FailoverVersion/TransitionCount are the TASK's versioned transition, and with the identity above
	// the cross-phase join key. On sent this describes the queue entry, not the payload; see HydratedVT.
	FailoverVersion int64
	TransitionCount int64
	// ParentWorkflowID/ParentRunID/ParentInitiatedID point at this workflow's parent when it is a child,
	// on any phase holding mutable state. Only child->parent is recorded, never parent->children.
	ParentWorkflowID  string
	ParentRunID       string
	ParentInitiatedID int64
	// Details carries phase-specific extras, e.g. verify's expected-vs-actual comparison.
	Details map[string]any

	// SourceCluster/SourceShard/SourceTaskID identify the sending cluster, the sending shard and the
	// sender's replication-queue task id; together they are the cross-cluster join key.
	SourceCluster string
	SourceShard   int32
	SourceTaskID  int64

	// sent-only

	// NewRunID is the successor run a verify task targets, or whose event 1 a sync task also shipped.
	NewRunID string
	// IsFirstSync marks the first sync of a newly created run.
	IsFirstSync bool
	// FirstEventID/NextEventID are the task's own event range, not what the artifact carried.
	FirstEventID int64
	NextEventID  int64

	// What the task carried (sent-only). Not recoverable later: the bounds come from the sender's
	// in-memory per-(run, target) progress cache, and per-entity stamps get overwritten.

	// TargetCluster is the target streamed to; progress is cached per target. Also emitted on skipped,
	// where it is the passive now missing the dropped task's state.
	TargetCluster string
	// Priority is the stream it went out on. Also emitted on skipped.
	Priority string
	// ArtifactKind is snapshot (whole state) or mutation (a diff).
	ArtifactKind string
	// ExclusiveStartVT is the diff's exclusive lower bound. Nil for a snapshot.
	ExclusiveStartVT *VersionedTransitionEntry
	// HydratedVT is the versioned transition of the state serialized; >= the task's own VT.
	HydratedVT *VersionedTransitionEntry
	// ShippedFirstEventID/Version is the first event carried; the version says which branch it is on.
	ShippedFirstEventID      int64
	ShippedFirstEventVersion int64
	// ShippedLastEventID/Version is the last event carried. Its version can differ from the first
	// when the shipped range crosses a failover.
	ShippedLastEventID      int64
	ShippedLastEventVersion int64

	// executing-only

	// Attempt is which delivery attempt this is; > 0 means the apply is being retried. On a skipped
	// event it is instead the source-side send/convert attempts exhausted before the task was dropped.
	Attempt int32

	// EventVersionHistory is the (event_id, version) branch, from the task on executing and from
	// mutable state on applied. The version says which history branch the events are on.
	EventVersionHistory []VersionHistoryEntry

	// applied-only: a summary of post-apply mutable state, never a blob

	// State/Status are the execution state and status after applying.
	State  string
	Status string
	// AppliedNextEventID is the next event id after applying.
	AppliedNextEventID int64
	// TransitionHistory is where the target landed; its last entry is this event's versioned transition.
	TransitionHistory []VersionedTransitionEntry
	// LastEventID/LastEventVersion are the last event on the target's current branch after applying.
	LastEventID      int64
	LastEventVersion int64
	// Outcome is applied, verified, resend_needed or error. Failed applies emit no event at all.
	Outcome string
	// Error is the failure message when Outcome is error.
	Error string
	// NewExecutionRunID is the continue-as-new successor, ResetRunID the reset successor.
	NewExecutionRunID string
	ResetRunID        string
	// Counts are the post-apply pending totals, useful for spotting unbounded state growth.
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
	if p.SourceCluster != "" {
		attrs = append(attrs, log.String("source_cluster", p.SourceCluster))
	}
	if p.SourceShard != 0 {
		attrs = append(attrs, log.Int64("source_shard", int64(p.SourceShard)))
	}
	if p.SourceTaskID != 0 {
		attrs = append(attrs, log.Int64("source_task_id", p.SourceTaskID))
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
	case ReplicationSkipped:
		attrs = p.appendSkipped(attrs)
	default:
	}
	return attrs
}

func (p ReplicationLifecyclePayload) appendSent(attrs []log.KeyValue) []log.KeyValue {
	if p.NewRunID != "" {
		attrs = append(attrs, log.String("new_run_id", p.NewRunID))
	}
	attrs = append(attrs, log.Bool("is_first_sync", p.IsFirstSync))
	if p.FirstEventID != 0 {
		attrs = append(attrs, log.Int64("first_event_id", p.FirstEventID))
	}
	if p.NextEventID != 0 {
		attrs = append(attrs, log.Int64("next_event_id", p.NextEventID))
	}
	if p.TargetCluster != "" {
		attrs = append(attrs, log.String("target_cluster", p.TargetCluster))
	}
	if p.Priority != "" {
		attrs = append(attrs, log.String("priority", p.Priority))
	}
	if p.ArtifactKind != "" {
		attrs = append(attrs, log.String("artifact_kind", p.ArtifactKind))
	}
	if p.ExclusiveStartVT != nil {
		attrs = append(attrs, jsonAttr("exclusive_start_versioned_transition", p.ExclusiveStartVT))
	}
	if p.HydratedVT != nil {
		attrs = append(attrs, jsonAttr("hydrated_versioned_transition", p.HydratedVT))
	}
	if p.ShippedFirstEventID != 0 {
		attrs = append(attrs,
			log.Int64("shipped_first_event_id", p.ShippedFirstEventID),
			log.Int64("shipped_first_event_version", p.ShippedFirstEventVersion),
			log.Int64("shipped_last_event_id", p.ShippedLastEventID),
			log.Int64("shipped_last_event_version", p.ShippedLastEventVersion),
		)
	}
	return attrs
}

// appendSkipped emits the fields for a terminal "skipped" event: target_cluster identifies the
// passive now missing the dropped state, priority tells whether it was live traffic (high) or force
// replication (low), attempt records how many convert attempts were exhausted before giving up, and
// error carries the last convert failure so an operator can investigate. The source_cluster /
// source_shard / source_task_id join key back to the source queue is emitted by Attributes for every
// phase, so it is not repeated here.
func (p ReplicationLifecyclePayload) appendSkipped(attrs []log.KeyValue) []log.KeyValue {
	if p.TargetCluster != "" {
		attrs = append(attrs, log.String("target_cluster", p.TargetCluster))
	}
	if p.Priority != "" {
		attrs = append(attrs, log.String("priority", p.Priority))
	}
	attrs = append(attrs, log.Int64("attempt", int64(p.Attempt)))
	if p.Error != "" {
		attrs = append(attrs, log.String("error", p.Error))
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
