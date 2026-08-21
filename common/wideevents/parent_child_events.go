package wideevents

import "go.opentelemetry.io/otel/log"

// ParentChildLifecycleEventName identifies critical transitions and anomalies between parent and child workflows.
const ParentChildLifecycleEventName = "parent_child_lifecycle"

type ParentChildPhase string

type ParentChildOutcome string

const (
	ParentChildPhaseChildStart              ParentChildPhase = "child_start"
	ParentChildPhaseVerifyFirstWorkflowTask ParentChildPhase = "verify_first_workflow_task"
	ParentChildPhaseRecordChildCompletion   ParentChildPhase = "record_child_completion"
	ParentChildPhaseVerifyChildCompletion   ParentChildPhase = "verify_child_completion"
	ParentChildPhaseParentResend            ParentChildPhase = "parent_resend"
)

const (
	ParentChildOutcomeWorkflowAlreadyExists    ParentChildOutcome = "workflow_already_exists"
	ParentChildOutcomeNotFoundIgnored          ParentChildOutcome = "not_found_ignored"
	ParentChildOutcomeChildNotFound            ParentChildOutcome = "child_not_found"
	ParentChildOutcomeFirstWorkflowTaskMissing ParentChildOutcome = "first_workflow_task_missing"
	ParentChildOutcomeNotFound                 ParentChildOutcome = "not_found"
	ParentChildOutcomeCompletionMissing        ParentChildOutcome = "completion_missing"
	ParentChildOutcomeScheduled                ParentChildOutcome = "scheduled"
	ParentChildOutcomeStarted                  ParentChildOutcome = "started"
	ParentChildOutcomeVerified                 ParentChildOutcome = "verified"
	ParentChildOutcomeIgnored                  ParentChildOutcome = "ignored"
	ParentChildOutcomeSucceeded                ParentChildOutcome = "succeeded"
	ParentChildOutcomeSourceNotFound           ParentChildOutcome = "source_not_found"
	ParentChildOutcomeDeduplicated             ParentChildOutcome = "deduplicated"
	ParentChildOutcomeLimited                  ParentChildOutcome = "limited"
	ParentChildOutcomeFailed                   ParentChildOutcome = "failed"
)

// ParentChildLifecyclePayload carries the relation and local task identity needed to correlate an
// event with its replication lifecycle and terminal task disposition.
type ParentChildLifecyclePayload struct {
	Phase   ParentChildPhase
	Outcome ParentChildOutcome

	LocalCluster string
	LocalShard   int32

	ParentNamespaceID string
	ParentWorkflowID  string
	ParentRunID       string

	ChildNamespaceID string
	ChildWorkflowID  string
	ChildRunID       string

	ParentInitiatedID      int64
	ParentInitiatedVersion int64

	LocalTaskID      int64
	LocalTaskType    string
	LocalTaskVersion int64

	Error     string
	ErrorType string
	Details   map[string]any
}

func (p ParentChildLifecyclePayload) EventName() string { return ParentChildLifecycleEventName }

func (p ParentChildLifecyclePayload) Attributes() []log.KeyValue {
	attrs := []log.KeyValue{
		log.String("phase", string(p.Phase)),
		log.String("outcome", string(p.Outcome)),
		log.String("local_cluster", p.LocalCluster),
		log.Int64("local_shard", int64(p.LocalShard)),
		log.String("parent_namespace_id", p.ParentNamespaceID),
		log.String("parent_workflow_id", p.ParentWorkflowID),
		log.String("parent_run_id", p.ParentRunID),
		log.String("child_namespace_id", p.ChildNamespaceID),
		log.String("child_workflow_id", p.ChildWorkflowID),
		log.String("child_run_id", p.ChildRunID),
	}
	if p.ParentInitiatedID != 0 || p.ParentInitiatedVersion != 0 {
		attrs = append(attrs,
			log.Int64("parent_initiated_id", p.ParentInitiatedID),
			log.Int64("parent_initiated_version", p.ParentInitiatedVersion),
		)
	}
	if p.LocalTaskID != 0 || p.LocalTaskType != "" {
		attrs = append(attrs,
			log.Int64("local_task_id", p.LocalTaskID),
			log.String("local_task_type", p.LocalTaskType),
			log.Int64("local_task_version", p.LocalTaskVersion),
		)
	}
	if p.Error != "" {
		attrs = append(attrs, log.String("error", p.Error))
	}
	if p.ErrorType != "" {
		attrs = append(attrs, log.String("error_type", p.ErrorType))
	}
	if len(p.Details) > 0 {
		attrs = append(attrs, jsonAttr("details", p.Details))
	}
	return attrs
}
