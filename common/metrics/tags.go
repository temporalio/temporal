package metrics

import (
	"fmt"
	"strconv"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/util"
)

const (
	gitRevisionTag   = "git_revision"
	buildDateTag     = "build_date"
	buildVersionTag  = "build_version"
	buildPlatformTag = "build_platform"
	goVersionTag     = "go_version"

	instance       = "instance"
	namespace      = "namespace"
	namespaceID    = "namespace_id"
	namespaceState = "namespace_state"
	sourceCluster  = "source_cluster"
	targetCluster  = "target_cluster"
	fromCluster    = "from_cluster"
	toCluster      = "to_cluster"
	taskQueue      = "taskqueue"
	workflowType   = "workflowType"
	activityType   = "activityType"
	commandType    = "commandType"
	serviceName    = "service_name"
	actionType     = "action_type"
	workerVersion  = "worker_version"
	destination    = "destination"
	// Generic reason tag can be used anywhere a reason is needed.
	reason = "reason"
	// See server.api.enums.v1.ReplicationTaskType
	replicationTaskType                            = "replicationTaskType"
	replicationTaskPriority                        = "replicationTaskPriority"
	taskExpireStage                                = "task_expire_stage"
	versioningBehavior                             = "versioning_behavior"
	continueAsNewVersioningBehavior                = "continue_as_new_versioning_behavior"
	suggestContinueAsNewReasonTooManyUpdates       = "suggest_continue_as_new_reason_too_many_updates"
	suggestContinueAsNewReasonTooManyHistoryEvents = "suggest_continue_as_new_reason_too_many_history_events"
	suggestContinueAsNewReasonHistorySizeTooLarge  = "suggest_continue_as_new_reason_history_size_too_large"
	suggestContinueAsNewReasonTargetVersionChanged = "suggest_continue_as_new_reason_target_version_changed"
	isFirstAttempt                                 = "first-attempt"
	workflowStatus                                 = "workflow_status"
	behaviorBefore                                 = "behavior_before"
	behaviorAfter                                  = "behavior_after"
	runInitiator                                   = "run_initiator"
	fromUnversioned                                = "from_unversioned"
	toUnversioned                                  = "to_unversioned"
	queryTypeTag                                   = "query_type"
	namespaceAllValue                              = "all"
	unknownValue                                   = "_unknown_"
	totalMetricSuffix                              = "_total"
	tagExcludedValue                               = "_tag_excluded_"
	falseValue                                     = "false"
	trueValue                                      = "true"
	errorPrefix                                    = "*"

	queryTypeStackTrace       = "__stack_trace"
	queryTypeOpenSessions     = "__open_sessions"
	queryTypeWorkflowMetadata = "__temporal_workflow_metadata"
	queryTypeUserDefined      = "__user_defined"

	newRun      = "new"
	existingRun = "existing"
	childRun    = "child"
	canRun      = "can"
	retryRun    = "retry"
	cronRun     = "cron"
	unknownRun  = "unknown"
)

// Tag is a struct to define metrics tags
type Tag struct {
	Key   string
	Value string
}

func (v Tag) String() string {
	return fmt.Sprintf("tag{key: %q, value: %q}", v.Key, v.Value)
}

// NamespaceTag returns a new namespace tag. For timers, this also ensures that we
// dual emit the metric with the all tag. If a blank namespace is provided then
// this converts that to an unknown namespace.
func NamespaceTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: namespace, Value: value}
}

// NamespaceIDTag returns a new namespace ID tag.
func NamespaceIDTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: namespaceID, Value: value}
}

var namespaceUnknownTag = Tag{Key: namespace, Value: unknownValue}

// NamespaceUnknownTag returns a new namespace:unknown tag-value
func NamespaceUnknownTag() Tag {
	return namespaceUnknownTag
}

// NamespaceStateTag returns a new namespace state tag.
func NamespaceStateTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: namespaceState, Value: value}
}

var taskQueueUnknownTag = Tag{Key: taskQueue, Value: unknownValue}

// TaskQueueUnknownTag returns a new taskqueue:unknown tag-value
func TaskQueueUnknownTag() Tag {
	return taskQueueUnknownTag
}

// InstanceTag returns a new instance tag
func InstanceTag(value string) Tag {
	return Tag{Key: instance, Value: value}
}

// SourceClusterTag returns a new source cluster tag.
func SourceClusterTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: sourceCluster, Value: value}
}

// TargetClusterTag returns a new target cluster tag.
func TargetClusterTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: targetCluster, Value: value}
}

// FromClusterIDTag returns a new from cluster tag.
func FromClusterIDTag(value int32) Tag {
	return Tag{Key: fromCluster, Value: strconv.FormatInt(int64(value), 10)}
}

// ToClusterIDTag returns a new to cluster tag.
func ToClusterIDTag(value int32) Tag {
	return Tag{Key: toCluster, Value: strconv.FormatInt(int64(value), 10)}
}

// UnsafeTaskQueueTag returns a new task queue tag.
// WARNING: Do not use this function directly in production code as it may create high number of unique task queue tag
// values that can trouble the observability stack. Instead, use one of the following helper functions and pass a proper
// breakdown boolean (typically based on the task queue dynamic configs):
// - `workflow.PerTaskQueueFamilyScope`
// - `tqid.PerTaskQueueFamilyScope`
// - `tqid.PerTaskQueueScope`
// - `tqid.PerTaskQueuePartitionScope`
func UnsafeTaskQueueTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: taskQueue, Value: value}
}

func TaskQueueTypeTag(tqType enumspb.TaskQueueType) Tag {
	return Tag{Key: TaskTypeTagName, Value: tqType.String()}
}

// Consider passing the value of "metrics.breakdownByBuildID" dynamic config to this function.
func WorkerVersionTag(version string, versionBreakdown bool) Tag {
	if version == "" {
		version = "__unversioned__"
	} else if !versionBreakdown {
		version = "__versioned__"
	}
	return Tag{Key: workerVersion, Value: version}
}

// WorkflowTypeTag returns a new workflow type tag.
func WorkflowTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: workflowType, Value: value}
}

// ActivityTypeTag returns a new activity type tag.
func ActivityTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: activityType, Value: value}
}

// CommandTypeTag returns a new command type tag.
func CommandTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: commandType, Value: value}
}

// Returns a new service role tag.
func ServiceRoleTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: ServiceRoleTagName, Value: value}
}

// Returns a new failure type tag
func FailureTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: FailureTagName, Value: value}
}

func FirstAttemptTag(attempt int32) Tag {
	value := falseValue
	if attempt == 1 {
		value = trueValue
	}
	return Tag{Key: isFirstAttempt, Value: value}
}

func FailureSourceTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: FailureSourceTagName, Value: value}
}

func TaskCategoryTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: TaskCategoryTagName, Value: value}
}

func TaskTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: TaskTypeTagName, Value: value}
}

func PartitionTag(partition string) Tag {
	return Tag{Key: PartitionTagName, Value: partition}
}

func TaskPriorityTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: TaskPriorityTagName, Value: value}
}

func MatchingTaskPriorityTag(value int32) Tag {
	priStr := ""
	if value != 0 {
		priStr = strconv.FormatInt(int64(value), 10)
	}
	return Tag{Key: TaskPriorityTagName, Value: priStr}
}

func QueueReaderIDTag(readerID int64) Tag {
	return Tag{Key: QueueReaderIDTagName, Value: strconv.Itoa(int(readerID))}
}

func QueueActionTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: QueueActionTagName, Value: value}
}

func QueueTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: QueueTypeTagName, Value: value}
}

func VisibilityPluginNameTag(value string) Tag {
	if value == "" {
		value = unknownValue
	}
	return Tag{Key: visibilityPluginNameTagName, Value: value}
}

func VisibilityIndexNameTag(value string) Tag {
	if value == "" {
		value = unknownValue
	}
	return Tag{Key: visibilityIndexNameTagName, Value: value}
}

func WorkerPluginNameTag(value string) Tag {
	return Tag{Key: WorkerPluginNameTagName, Value: value}
}

// VersionedTag represents whether a loaded task queue manager represents a specific version set or build ID or not.
func VersionedTag(versioned string) Tag {
	return Tag{Key: versionedTagName, Value: versioned}
}

func ServiceErrorTypeTag(err error) Tag {
	return Tag{Key: ErrorTypeTagName, Value: strings.TrimPrefix(util.ErrorType(err), errorPrefix)}
}

func OutcomeTag(outcome string) Tag {
	return Tag{Key: outcomeTagName, Value: outcome}
}

func NexusMethodTag(value string) Tag {
	return Tag{Key: nexusMethodTagName, Value: value}
}

func NexusEndpointTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return Tag{Key: nexusEndpointTagName, Value: value}
}

func NexusServiceTag(value string) Tag {
	return Tag{Key: nexusServiceTagName, Value: value}
}

func NexusOperationTag(value string) Tag {
	return Tag{Key: nexusOperationTagName, Value: value}
}

// HttpStatusTag returns a new httpStatusTag.
func HttpStatusTag(value int) Tag {
	return Tag{Key: httpStatusTagName, Value: strconv.Itoa(value)}
}

func ResourceExhaustedCauseTag(cause enumspb.ResourceExhaustedCause) Tag {
	return Tag{Key: resourceExhaustedTag, Value: cause.String()}
}

func ResourceExhaustedScopeTag(scope enumspb.ResourceExhaustedScope) Tag {
	return Tag{Key: resourceExhaustedScopeTag, Value: scope.String()}
}

func ServiceNameTag(value primitives.ServiceName) Tag {
	return Tag{Key: serviceName, Value: string(value)}
}

func ActionType(value string) Tag {
	return Tag{Key: actionType, Value: value}
}

func OperationTag(value string) Tag {
	return Tag{Key: OperationTagName, Value: value}
}

func StringTag(key string, value string) Tag {
	return Tag{Key: key, Value: value}
}

func CacheTypeTag(value string) Tag {
	return Tag{Key: CacheTypeTagName, Value: value}
}

func PriorityTag(value locks.Priority) Tag {
	return Tag{Key: PriorityTagName, Value: strconv.Itoa(int(value))}
}

// ReasonString is just a string but the special type is defined here to remind callers of ReasonTag to limit the
// cardinality of possible reasons.
type ReasonString string

// ReasonTag is a generic tag can be used anywhere a reason is needed.
// Make sure that the value is of limited cardinality.
func ReasonTag(value ReasonString) Tag {
	return Tag{Key: reason, Value: string(value)}
}

// ReplicationTaskTypeTag returns a new replication task type tag.
func ReplicationTaskTypeTag(value enumsspb.ReplicationTaskType) Tag {
	return Tag{Key: replicationTaskType, Value: value.String()}
}

// ReplicationTaskPriorityTag returns a replication task priority tag.
func ReplicationTaskPriorityTag(value enumsspb.TaskPriority) Tag {
	return Tag{Key: replicationTaskPriority, Value: value.String()}
}

// DestinationTag is a tag for metrics emitted by outbound task executors for the task's destination.
func DestinationTag(value string) Tag {
	return Tag{Key: destination, Value: value}
}

func VersioningBehaviorTag(behavior enumspb.VersioningBehavior) Tag {
	return Tag{Key: versioningBehavior, Value: behavior.String()}
}

func ContinueAsNewVersioningBehaviorTag(canBehavior enumspb.ContinueAsNewVersioningBehavior) Tag {
	return Tag{Key: continueAsNewVersioningBehavior, Value: canBehavior.String()}
}

func SuggestContinueAsNewReasonTargetVersionChangedTag(present bool) Tag {
	v := falseValue
	if present {
		v = trueValue
	}
	return Tag{Key: suggestContinueAsNewReasonTargetVersionChanged, Value: v}
}

func SuggestContinueAsNewReasonTooManyUpdatesTag(present bool) Tag {
	v := falseValue
	if present {
		v = trueValue
	}
	return Tag{Key: suggestContinueAsNewReasonTooManyUpdates, Value: v}
}

func SuggestContinueAsNewReasonTooManyHistoryEventsTag(present bool) Tag {
	v := falseValue
	if present {
		v = trueValue
	}
	return Tag{Key: suggestContinueAsNewReasonTooManyHistoryEvents, Value: v}
}

func SuggestContinueAsNewReasonHistorySizeTooLargeTag(present bool) Tag {
	v := falseValue
	if present {
		v = trueValue
	}
	return Tag{Key: suggestContinueAsNewReasonHistorySizeTooLarge, Value: v}
}

func WorkflowStatusTag(status string) Tag {
	return Tag{Key: workflowStatus, Value: status}
}

func QueryTypeTag(queryType string) Tag {
	if queryType == queryTypeStackTrace || queryType == queryTypeOpenSessions || queryType == queryTypeWorkflowMetadata {
		return Tag{Key: queryTypeTag, Value: queryType}
	}
	// group all user defined queries into a single tag value
	return Tag{Key: queryTypeTag, Value: queryTypeUserDefined}
}

func VersioningBehaviorBeforeOverrideTag(behavior enumspb.VersioningBehavior) Tag {
	return Tag{Key: behaviorBefore, Value: behavior.String()}
}

func VersioningBehaviorAfterOverrideTag(behavior enumspb.VersioningBehavior) Tag {
	return Tag{Key: behaviorAfter, Value: behavior.String()}
}

// RunInitiatorTag creates a tag indicating how a workflow run was initiated.
// It handles both new workflow runs and continuations from previous runs.
// When attributes is nil (e.g. during AddWorkflowExecutionOptionsUpdatedEvent),
// it returns a tag indicating an existing run.
func RunInitiatorTag(prevRunID string, attributes *historypb.WorkflowExecutionStartedEventAttributes) Tag {
	if attributes == nil {
		return Tag{Key: runInitiator, Value: existingRun}
	} else if attributes.GetParentWorkflowExecution() != nil {
		return Tag{Key: runInitiator, Value: childRun}
	}

	switch attributes.GetInitiator() {
	case enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED:
		return Tag{Key: runInitiator, Value: newRun}
	case enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW:
		return Tag{Key: runInitiator, Value: canRun}
	case enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY:
		return Tag{Key: runInitiator, Value: retryRun}
	case enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE:
		return Tag{Key: runInitiator, Value: cronRun}
	default:
		return Tag{Key: runInitiator, Value: unknownRun}
	}
}

func FromUnversionedTag(version string) Tag {
	if version == "_unversioned_" {
		return Tag{Key: fromUnversioned, Value: trueValue}
	}
	return Tag{Key: fromUnversioned, Value: falseValue}
}

func ToUnversionedTag(version string) Tag {
	if version == "_unversioned_" {
		return Tag{Key: toUnversioned, Value: trueValue}
	}
	return Tag{Key: toUnversioned, Value: falseValue}
}

var TaskExpireStageReadTag = Tag{Key: taskExpireStage, Value: "read"}
var TaskExpireStageMemoryTag = Tag{Key: taskExpireStage, Value: "memory"}
var TaskInvalidTag = Tag{Key: taskExpireStage, Value: "invalid"}

func PersistenceDBKindTag(kind string) Tag {
	return Tag{Key: PersistenceDBKindTagName, Value: kind}
}

func HeaderCallsiteTag(kind string) Tag {
	return Tag{Key: headerCallsiteTagName, Value: kind}
}
