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
	revisionTag     = "revision"
	branchTag       = "branch"
	buildDateTag    = "build_date"
	buildVersionTag = "build_version"
	goVersionTag    = "go_version"

	instance      = "instance"
	namespace     = "namespace"
	targetCluster = "target_cluster"
	taskList      = "tasklist"
	workflowType  = "workflowType"
	activityType  = "activityType"

	namespaceAllValue = "all"
	unknownValue      = "_unknown_"
)

// Tag is an interface to define metrics tags
type Tag interface {
	Key() string
	Value() string
}

type (
	namespaceTag struct {
		value string
	}

	namespaceUnknownTag struct{}

	instanceTag struct {
		value string
	}

	targetClusterTag struct {
		value string
	}

	taskListTag struct {
		value string
	}

	workflowTypeTag struct {
		value string
	}

	activityTypeTag struct {
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
	return namespaceTag{value}
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
	return instanceTag{value}
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
	return targetClusterTag{value}
}

// Key returns the key of the target cluster tag
func (d targetClusterTag) Key() string {
	return targetCluster
}

// Value returns the value of a target cluster tag
func (d targetClusterTag) Value() string {
	return d.value
}

// TaskListTag returns a new task list tag.
func TaskListTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return taskListTag{value}
}

// Key returns the key of the task list tag
func (d taskListTag) Key() string {
	return taskList
}

// Value returns the value of the task list tag
func (d taskListTag) Value() string {
	return d.value
}

// WorkflowTypeTag returns a new workflow type tag.
func WorkflowTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return workflowTypeTag{value}
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
	return activityTypeTag{value}
}

// Key returns the key of the activity type tag
func (d activityTypeTag) Key() string {
	return activityType
}

// Value returns the value of the activity type tag
func (d activityTypeTag) Value() string {
	return d.value
}
