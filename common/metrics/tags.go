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

const (
	gitRevisionTag   = "git_revision"
	gitBranchTag     = "git_branch"
	buildDateTag     = "build_date"
	gitTagTag        = "git_tag"
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

	namespaceAllValue = "all"
	unknownValue      = "_unknown_"
	totalMetricSuffix = "_total"
)

// Tag is an interface to define metrics tags
type Tag interface {
	Key() string
	Value() string
}

var StickyTaskQueueTag = TaskQueueTag("__sticky__")

type (
	namespaceTag struct {
		value string
	}

	namespaceUnknownTag struct{}

	taskQueueUnknownTag struct{}

	instanceTag struct {
		value string
	}

	targetClusterTag struct {
		value string
	}

	taskQueueTag struct {
		value string
	}

	workflowTypeTag struct {
		value string
	}

	activityTypeTag struct {
		value string
	}

	commandTypeTag struct {
		value string
	}

	serviceRoleTag struct {
		value string
	}

	statsTypeTag struct {
		value string
	}

	failureTag struct {
		value string
	}

	taskTypeTag struct {
		value string
	}

	queueTypeTag struct {
		value string
	}

	visibilityTypeTag struct {
		value string
	}
)

// NamespaceTag returns a new namespace tag. For timers, this also ensures that we
// dual emit the metric with the all tag. If a blank namespace is provided then
// this converts that to an unknown namespace.
func NamespaceTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return namespaceTag{value: value}
}

// Key returns the key of the namespace tag
func (d namespaceTag) Key() string {
	return namespace
}

// Value returns the value of a namespace tag
func (d namespaceTag) Value() string {
	return d.value
}

// NamespaceUnknownTag returns a new namespace:unknown tag-value
func NamespaceUnknownTag() Tag {
	return namespaceUnknownTag{}
}

// Key returns the key of the taskqueue unknown tag
func (d taskQueueUnknownTag) Key() string {
	return taskQueue
}

// Value returns the value of the taskqueue unknown tag
func (d taskQueueUnknownTag) Value() string {
	return unknownValue
}

// TaskQueueUnknownTag returns a new taskqueue:unknown tag-value
func TaskQueueUnknownTag() Tag {
	return taskQueueUnknownTag{}
}

// Key returns the key of the namespace unknown tag
func (d namespaceUnknownTag) Key() string {
	return namespace
}

// Value returns the value of the namespace unknown tag
func (d namespaceUnknownTag) Value() string {
	return unknownValue
}

// InstanceTag returns a new instance tag
func InstanceTag(value string) Tag {
	return instanceTag{value: value}
}

// Key returns the key of the instance tag
func (i instanceTag) Key() string {
	return instance
}

// Value returns the value of a instance tag
func (i instanceTag) Value() string {
	return i.value
}

// TargetClusterTag returns a new target cluster tag.
func TargetClusterTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return targetClusterTag{value: value}
}

// Key returns the key of the target cluster tag
func (d targetClusterTag) Key() string {
	return targetCluster
}

// Value returns the value of a target cluster tag
func (d targetClusterTag) Value() string {
	return d.value
}

// TaskQueueTag returns a new task queue tag.
func TaskQueueTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return taskQueueTag{sanitizer.Value(value)}
}

// Key returns the key of the task queue tag
func (d taskQueueTag) Key() string {
	return taskQueue
}

// Value returns the value of the task queue tag
func (d taskQueueTag) Value() string {
	return d.value
}

// WorkflowTypeTag returns a new workflow type tag.
func WorkflowTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return workflowTypeTag{value: value}
}

// Key returns the key of the workflow type tag
func (d workflowTypeTag) Key() string {
	return workflowType
}

// Value returns the value of the workflow type tag
func (d workflowTypeTag) Value() string {
	return d.value
}

// ActivityTypeTag returns a new activity type tag.
func ActivityTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return activityTypeTag{value: value}
}

// Key returns the key of the activity type tag
func (d activityTypeTag) Key() string {
	return activityType
}

// Value returns the value of the activity type tag
func (d activityTypeTag) Value() string {
	return d.value
}

// CommandTypeTag returns a new command type tag.
func CommandTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return commandTypeTag{value: value}
}

// Key returns the key of the command type tag
func (d commandTypeTag) Key() string {
	return commandType
}

// Value returns the value of the command type tag
func (d commandTypeTag) Value() string {
	return d.value
}

// Returns a new service role tag.
func ServiceRoleTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return serviceRoleTag{value: value}
}

// Key returns the key of the service role tag
func (d serviceRoleTag) Key() string {
	return ServiceRoleTagName
}

// Value returns the value of the service role tag
func (d serviceRoleTag) Value() string {
	return d.value
}

// Returns a new stats type tag
func StatsTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return statsTypeTag{value: value}
}

// Key returns the key of the stats type tag
func (d statsTypeTag) Key() string {
	return StatsTypeTagName
}

// Value returns the value of the stats type tag
func (d statsTypeTag) Value() string {
	return d.value
}

// Returns a new failure type tag
func FailureTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return failureTag{value: value}
}

// Key returns the key of the tag
func (d failureTag) Key() string {
	return FailureTagName
}

// Value returns the value of the tag
func (d failureTag) Value() string {
	return d.value
}

func TaskTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return taskTypeTag{value: value}

}

func QueueTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return queueTypeTag{value: value}
}

// Key returns the key of the tag
func (d taskTypeTag) Key() string {
	return TaskTypeTagName
}

// Value returns the value of the tag
func (d taskTypeTag) Value() string {
	return d.value
}

func VisibilityTypeTag(value string) Tag {
	if value == "" {
		value = unknownValue
	}
	return visibilityTypeTag{value: value}
}

func StandardVisibilityTypeTag() Tag {
	return visibilityTypeTag{value: standardVisibilityTagValue}
}

func AdvancedVisibilityTypeTag() Tag {
	return visibilityTypeTag{value: advancedVisibilityTagValue}
}

// Key returns the key of the tag
func (d visibilityTypeTag) Key() string {
	return visibilityTypeTagName
}

// Value returns the value of the tag
func (d visibilityTypeTag) Value() string {
	return d.value
}

func (d queueTypeTag) Key() string {
	return QueueTypeTagName
}

func (d queueTypeTag) Value() string {
	return d.value
}
