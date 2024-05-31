// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

import (
	"strconv"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"

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
	namespaceState = "namespace_state"
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
	// Generic reason tag can be used anywhere a reason is needed.
	reason = "reason"
	// See server.api.enums.v1.ReplicationTaskType
	replicationTaskType = "replicationTaskType"

	namespaceAllValue = "all"
	unknownValue      = "_unknown_"
	totalMetricSuffix = "_total"
	tagExcludedValue  = "_tag_excluded_"

	errorPrefix = "*"
)

// Tag is an interface to define metrics tags
type Tag interface {
	Key() string
	Value() string
}

var StickyTaskQueueTag = TaskQueueTag("__sticky__")

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

// TaskQueueTag returns a new task queue tag.
func TaskQueueTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: taskQueue, value: sanitizer.Value(value)}
}

func TaskQueueTypeTag(tqType enumspb.TaskQueueType) Tag {
	return &tagImpl{key: TaskTypeTagName, value: tqType.String()}
}

func WorkerBuildIdTag(buildId string) Tag {
	if buildId == "" {
		buildId = "_unversioned_"
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

func PartitionTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: PartitionTypeName, value: value}
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

// VersionedTag represents whether a loaded task queue manager represents a specific version set or build ID or not.
func VersionedTag(versioned string) Tag {
	return &tagImpl{key: versionedTagName, value: versioned}
}

func ServiceErrorTypeTag(err error) Tag {
	return &tagImpl{key: ErrorTypeTagName, value: strings.TrimPrefix(util.ErrorType(err), errorPrefix)}
}

func NexusOutcomeTag(outcome string) Tag {
	return &tagImpl{key: nexusOutcomeTagName, value: outcome}
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

// HttpStatusTag returns a new httpStatusTag.
func HttpStatusTag(value int) Tag {
	return &tagImpl{key: httpStatusTagName, value: strconv.Itoa(value)}
}

func ResourceExhaustedCauseTag(cause enumspb.ResourceExhaustedCause) Tag {
	return &tagImpl{key: resourceExhaustedTag, value: cause.String()}
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
