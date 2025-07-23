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
	workerBuildId  = "worker-build-id"
	destination    = "destination"
	// Generic reason tag can be used anywhere a reason is needed.
	reason = "reason"
	// See server.api.enums.v1.ReplicationTaskType
	replicationTaskType     = "replicationTaskType"
	replicationTaskPriority = "replicationTaskPriority"
	taskExpireStage         = "task_expire_stage"
	versioningBehavior      = "versioning_behavior"
	isFirstAttempt          = "first-attempt"
	workflowStatus          = "workflow_status"
	behaviorBefore          = "behavior_before"
	behaviorAfter           = "behavior_after"
	runInitiator            = "run_initiator"
	fromUnversioned         = "from_unversioned"
	toUnversioned           = "to_unversioned"
	queryTypeTag            = "query_type"
	namespaceAllValue       = "all"
	unknownValue            = "_unknown_"
	totalMetricSuffix       = "_total"
	tagExcludedValue        = "_tag_excluded_"
	falseValue              = "false"
	trueValue               = "true"
	errorPrefix             = "*"

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

// Tag is an interface to define metrics tags
type Tag interface {
	Key() string
	Value() string
}

type (
	tagImpl struct {
		key   string
		value string
	}
)

func (v *tagImpl) Key() string {
	return v.key
}

func (v *tagImpl) Value() string {
	return v.value
}

func (v *tagImpl) String() string {
	return fmt.Sprintf("tag{key: %q, value: %q}", v.key, v.value)
}

// NamespaceTag returns a new namespace tag. For timers, this also ensures that we
// dual emit the metric with the all tag. If a blank namespace is provided then
// this converts that to an unknown namespace.
func NamespaceTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{
		key:   namespace,
		value: value,
	}
}

// NamespaceIDTag returns a new namespace ID tag.
func NamespaceIDTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{
		key:   namespaceID,
		value: value,
	}
}

var namespaceUnknownTag = &tagImpl{key: namespace, value: unknownValue}

// NamespaceUnknownTag returns a new namespace:unknown tag-value
func NamespaceUnknownTag() Tag {
	return namespaceUnknownTag
}

// NamespaceStateTag returns a new namespace state tag.
func NamespaceStateTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{
		key:   namespaceState,
		value: value,
	}
}

var taskQueueUnknownTag = &tagImpl{key: taskQueue, value: unknownValue}

// TaskQueueUnknownTag returns a new taskqueue:unknown tag-value
func TaskQueueUnknownTag() Tag {
	return taskQueueUnknownTag
}

// InstanceTag returns a new instance tag
func InstanceTag(value string) Tag {
	return &tagImpl{key: instance, value: value}
}

// SourceClusterTag returns a new source cluster tag.
func SourceClusterTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: sourceCluster, value: value}
}

// TargetClusterTag returns a new target cluster tag.
func TargetClusterTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: targetCluster, value: value}
}

// FromClusterIDTag returns a new from cluster tag.
func FromClusterIDTag(value int32) Tag {
	return &tagImpl{key: fromCluster, value: strconv.FormatInt(int64(value), 10)}
}

// ToClusterIDTag returns a new to cluster tag.
func ToClusterIDTag(value int32) Tag {
	return &tagImpl{key: toCluster, value: strconv.FormatInt(int64(value), 10)}
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
	return &tagImpl{key: taskQueue, value: value}
}

func TaskQueueTypeTag(tqType enumspb.TaskQueueType) Tag {
	return &tagImpl{key: TaskTypeTagName, value: tqType.String()}
}

// Consider passing the value of "metrics.breakdownByBuildID" dynamic config to this function.
func WorkerBuildIdTag(buildId string, buildIdBreakdown bool) Tag {
	if buildId == "" {
		buildId = "__unversioned__"
	} else if !buildIdBreakdown {
		buildId = "__versioned__"
	}
	return &tagImpl{key: workerBuildId, value: buildId}
}

// WorkflowTypeTag returns a new workflow type tag.
func WorkflowTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: workflowType, value: value}
}

// ActivityTypeTag returns a new activity type tag.
func ActivityTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: activityType, value: value}
}

// CommandTypeTag returns a new command type tag.
func CommandTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: commandType, value: value}
}

// Returns a new service role tag.
func ServiceRoleTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: ServiceRoleTagName, value: value}
}

// Returns a new failure type tag
func FailureTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: FailureTagName, value: value}
}

func FirstAttemptTag(attempt int32) Tag {
	value := falseValue
	if attempt == 1 {
		value = trueValue
	}
	return &tagImpl{key: isFirstAttempt, value: value}
}

func FailureSourceTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: FailureSourceTagName, value: value}
}

func TaskCategoryTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: TaskCategoryTagName, value: value}
}

func TaskTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: TaskTypeTagName, value: value}
}

func PartitionTag(partition string) Tag {
	return &tagImpl{key: PartitionTagName, value: partition}
}

func TaskPriorityTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: TaskPriorityTagName, value: value}
}

func QueueReaderIDTag(readerID int64) Tag {
	return &tagImpl{key: QueueReaderIDTagName, value: strconv.Itoa(int(readerID))}
}

func QueueActionTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: QueueActionTagName, value: value}
}

func QueueTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: QueueTypeTagName, value: value}
}

func VisibilityPluginNameTag(value string) Tag {
	if value == "" {
		value = unknownValue
	}
	return &tagImpl{key: visibilityPluginNameTagName, value: value}
}

func VisibilityIndexNameTag(value string) Tag {
	if value == "" {
		value = unknownValue
	}
	return &tagImpl{key: visibilityIndexNameTagName, value: value}
}

// VersionedTag represents whether a loaded task queue manager represents a specific version set or build ID or not.
func VersionedTag(versioned string) Tag {
	return &tagImpl{key: versionedTagName, value: versioned}
}

func ServiceErrorTypeTag(err error) Tag {
	return &tagImpl{key: ErrorTypeTagName, value: strings.TrimPrefix(util.ErrorType(err), errorPrefix)}
}

func OutcomeTag(outcome string) Tag {
	return &tagImpl{key: outcomeTagName, value: outcome}
}

func NexusMethodTag(value string) Tag {
	return &tagImpl{key: nexusMethodTagName, value: value}
}

func NexusEndpointTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: nexusEndpointTagName, value: value}
}

func NexusServiceTag(value string) Tag {
	return &tagImpl{key: nexusServiceTagName, value: value}
}

func NexusOperationTag(value string) Tag {
	return &tagImpl{key: nexusOperationTagName, value: value}
}

// HttpStatusTag returns a new httpStatusTag.
func HttpStatusTag(value int) Tag {
	return &tagImpl{key: httpStatusTagName, value: strconv.Itoa(value)}
}

func ResourceExhaustedCauseTag(cause enumspb.ResourceExhaustedCause) Tag {
	return &tagImpl{key: resourceExhaustedTag, value: cause.String()}
}

func ResourceExhaustedScopeTag(scope enumspb.ResourceExhaustedScope) Tag {
	return &tagImpl{key: resourceExhaustedScopeTag, value: scope.String()}
}

func ServiceNameTag(value primitives.ServiceName) Tag {
	return &tagImpl{key: serviceName, value: string(value)}
}

func ActionType(value string) Tag {
	return &tagImpl{key: actionType, value: value}
}

func OperationTag(value string) Tag {
	return &tagImpl{key: OperationTagName, value: value}
}

func StringTag(key string, value string) Tag {
	return &tagImpl{key: key, value: value}
}

func CacheTypeTag(value string) Tag {
	return &tagImpl{key: CacheTypeTagName, value: value}
}

func PriorityTag(value locks.Priority) Tag {
	return &tagImpl{key: PriorityTagName, value: strconv.Itoa(int(value))}
}

// ReasonString is just a string but the special type is defined here to remind callers of ReasonTag to limit the
// cardinality of possible reasons.
type ReasonString string

// ReasonTag is a generic tag can be used anywhere a reason is needed.
// Make sure that the value is of limited cardinality.
func ReasonTag(value ReasonString) Tag {
	return &tagImpl{key: reason, value: string(value)}
}

// ReplicationTaskTypeTag returns a new replication task type tag.
func ReplicationTaskTypeTag(value enumsspb.ReplicationTaskType) Tag {
	return &tagImpl{key: replicationTaskType, value: value.String()}
}

// ReplicationTaskPriorityTag returns a replication task priority tag.
func ReplicationTaskPriorityTag(value enumsspb.TaskPriority) Tag {
	return &tagImpl{key: replicationTaskPriority, value: value.String()}
}

// DestinationTag is a tag for metrics emitted by outbound task executors for the task's destination.
func DestinationTag(value string) Tag {
	return &tagImpl{
		key:   destination,
		value: value,
	}
}

func VersioningBehaviorTag(behavior enumspb.VersioningBehavior) Tag {
	return &tagImpl{versioningBehavior, behavior.String()}
}

func WorkflowStatusTag(status string) Tag {
	return &tagImpl{key: workflowStatus, value: status}
}

func QueryTypeTag(queryType string) Tag {
	if queryType == queryTypeStackTrace || queryType == queryTypeOpenSessions || queryType == queryTypeWorkflowMetadata {
		return &tagImpl{key: queryTypeTag, value: queryType}
	}
	// group all user defined queries into a single tag value
	return &tagImpl{key: queryTypeTag, value: queryTypeUserDefined}
}

func VersioningBehaviorBeforeOverrideTag(behavior enumspb.VersioningBehavior) Tag {
	return &tagImpl{key: behaviorBefore, value: behavior.String()}
}

func VersioningBehaviorAfterOverrideTag(behavior enumspb.VersioningBehavior) Tag {
	return &tagImpl{key: behaviorAfter, value: behavior.String()}
}

// RunInitiatorTag creates a tag indicating how a workflow run was initiated.
// It handles both new workflow runs and continuations from previous runs.
// When attributes is nil (e.g. during AddWorkflowExecutionOptionsUpdatedEvent),
// it returns a tag indicating an existing run.
func RunInitiatorTag(prevRunID string, attributes *historypb.WorkflowExecutionStartedEventAttributes) Tag {
	if attributes == nil {
		return &tagImpl{key: runInitiator, value: existingRun}
	} else if attributes.GetParentWorkflowExecution() != nil {
		return &tagImpl{key: runInitiator, value: childRun}
	}

	switch attributes.GetInitiator() {
	case enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED:
		return &tagImpl{key: runInitiator, value: newRun}
	case enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW:
		return &tagImpl{key: runInitiator, value: canRun}
	case enumspb.CONTINUE_AS_NEW_INITIATOR_RETRY:
		return &tagImpl{key: runInitiator, value: retryRun}
	case enumspb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE:
		return &tagImpl{key: runInitiator, value: cronRun}
	default:
		return &tagImpl{key: runInitiator, value: unknownRun}
	}
}

func FromUnversionedTag(version string) Tag {
	if version == "_unversioned_" {
		return &tagImpl{key: fromUnversioned, value: trueValue}
	}
	return &tagImpl{key: fromUnversioned, value: falseValue}
}

func ToUnversionedTag(version string) Tag {
	if version == "_unversioned_" {
		return &tagImpl{key: toUnversioned, value: trueValue}
	}
	return &tagImpl{key: toUnversioned, value: falseValue}
}

var TaskExpireStageReadTag Tag = &tagImpl{key: taskExpireStage, value: "read"}
var TaskExpireStageMemoryTag Tag = &tagImpl{key: taskExpireStage, value: "memory"}
var TaskInvalidTag Tag = &tagImpl{key: taskExpireStage, value: "invalid"}

func PersistenceDBKindTag(kind string) Tag {
	return &tagImpl{key: PersistenceDBKindTagName, value: kind}
}
