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

	instanceTag struct {
		value string
	}

	targetClusterTag struct {
		value string
	}

	taskListTag struct {
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
