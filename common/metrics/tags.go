// Copyright (c) 2017 Uber Technologies, Inc.
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
	domain        = "domain"
	targetCluster = "target_cluster"
	taskList      = "tasklist"
	workflowType  = "workflowType"
	activityType  = "activityType"
	decisionType  = "decisionType"
	invariantType = "invariantType"

	domainAllValue = "all"
	unknownValue   = "_unknown_"
)

// Tag is an interface to define metrics tags
type Tag interface {
	Key() string
	Value() string
}

type (
	domainTag struct {
		value string
	}

	domainUnknownTag struct{}

	taskListUnknownTag struct{}

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

	decisionTypeTag struct {
		value string
	}

	invariantTypeTag struct {
		value string
	}
)

// DomainTag returns a new domain tag. For timers, this also ensures that we
// dual emit the metric with the all tag. If a blank domain is provided then
// this converts that to an unknown domain.
func DomainTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return domainTag{value}
}

// Key returns the key of the domain tag
func (d domainTag) Key() string {
	return domain
}

// Value returns the value of a domain tag
func (d domainTag) Value() string {
	return d.value
}

// DomainUnknownTag returns a new domain:unknown tag-value
func DomainUnknownTag() Tag {
	return domainUnknownTag{}
}

// Key returns the key of the domain unknown tag
func (d taskListUnknownTag) Key() string {
	return domain
}

// Value returns the value of the domain unknown tag
func (d taskListUnknownTag) Value() string {
	return unknownValue
}

// TaskListUnknownTag returns a new tasklist:unknown tag-value
func TaskListUnknownTag() Tag {
	return taskListUnknownTag{}
}

// Key returns the key of the domain unknown tag
func (d domainUnknownTag) Key() string {
	return domain
}

// Value returns the value of the domain unknown tag
func (d domainUnknownTag) Value() string {
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
	return taskListTag{sanitizer.Value(value)}
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

// DecisionTypeTag returns a new decision type tag.
func DecisionTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return decisionTypeTag{value}
}

// Key returns the key of the decision type tag
func (d decisionTypeTag) Key() string {
	return decisionType
}

// Value returns the value of the decision type tag
func (d decisionTypeTag) Value() string {
	return d.value
}

// InvariantTypeTag returns a new invariant type tag.
func InvariantTypeTag(value string) Tag {
	if len(value) == 0 {
		value = unknownValue
	}
	return invariantTypeTag{value}
}

// Key returns the key of invariant type tag
func (d invariantTypeTag) Key() string {
	return invariantType
}

// Value returns the value of invariant type tag
func (d invariantTypeTag) Value() string {
	return d.value
}
