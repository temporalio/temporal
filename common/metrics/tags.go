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
	"fmt"
	"strconv"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
)

const (
	gitRevisionTag   = "git_revision"
	buildDateTag     = "build_date"
	buildVersionTag  = "build_version"
	buildPlatformTag = "build_platform"
	goVersionTag     = "go_version"

	instance      = "instance"
	namespace     = "namespace"
	targetCluster = "target_cluster"
	taskQueue     = "taskqueue"
	workflowType  = "workflowType"
	activityType  = "activityType"
	commandType   = "commandType"
	serviceName   = "service_name"
	actionType    = "action_type"

	namespaceAllValue = "all"
	unknownValue      = "_unknown_"
	totalMetricSuffix = "_total"
	tagExcludedValue  = "_tag_excluded_"

	getType     = "%T"
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

func TaskPriorityTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: TaskPriorityTagName, value: value}
}

func QueueTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return &tagImpl{key: QueueTypeTagName, value: value}
}

func VisibilityTypeTag(value string) Tag {
	if value == "" {
		value = unknownValue
	}
	return &tagImpl{key: visibilityTypeTagName, value: value}
}

func ServiceErrorTypeTag(err error) Tag {
	return &tagImpl{key: ErrorTypeTagName, value: strings.TrimPrefix(fmt.Sprintf(getType, err), errorPrefix)}
}

var (
	standardVisibilityTypeTag = VisibilityTypeTag(standardVisibilityTagValue)
	advancedVisibilityTypeTag = VisibilityTypeTag(advancedVisibilityTagValue)
)

func StandardVisibilityTypeTag() Tag {
	return standardVisibilityTypeTag
}

func AdvancedVisibilityTypeTag() Tag {
	return advancedVisibilityTypeTag
}

// HttpStatusTag returns a new httpStatusTag.
func HttpStatusTag(value int) Tag {
	return &tagImpl{key: httpStatusTagName, value: strconv.Itoa(value)}
}

func ResourceExhaustedCauseTag(cause enumspb.ResourceExhaustedCause) Tag {
	return &tagImpl{key: resourceExhaustedTag, value: cause.String()}
}

func ServiceTypeTag(value string) Tag {
	return &tagImpl{key: serviceName, value: value}
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
