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
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package metrics

import (
	"strconv"

	"go.temporal.io/server/common/tqid"
)

const (
	omitted = "__omitted__"
	normal  = "__normal__"
	sticky  = "__sticky__"
)

// GetPerTaskQueueFamilyScope returns "namespace" and "taskqueue" tags. "taskqueue" will be "__omitted__" if
// taskQueueBreakdown is false.
func GetPerTaskQueueFamilyScope(
	handler Handler,
	namespaceName string,
	taskQueueFamily *tqid.TaskQueueFamily,
	taskQueueBreakdown bool,
	tags ...Tag,
) Handler {
	metricTaskQueueName := omitted
	if taskQueueBreakdown {
		metricTaskQueueName = taskQueueFamily.Name()
	}

	tags = append(tags, NamespaceTag(namespaceName), UnsafeTaskQueueTag(metricTaskQueueName))
	return handler.WithTags(tags...)
}

// GetPerTaskQueueScope returns GetPerTaskQueueFamilyScope plus the "task_type" tag.
func GetPerTaskQueueScope(
	handler Handler,
	namespaceName string,
	taskQueue *tqid.TaskQueue,
	taskQueueBreakdown bool,
	tags ...Tag,
) Handler {
	return GetPerTaskQueueFamilyScope(handler, namespaceName, taskQueue.Family(), taskQueueBreakdown,
		append(tags, TaskQueueTypeTag(taskQueue.TaskType()))...)
}

// GetPerTaskQueuePartitionIDScope is similar to GetPerTaskQueuePartitionTypeScope, except that the partition tag will
// hold the normal partition ID if partitionIDBreakdown is true.
func GetPerTaskQueuePartitionIDScope(
	handler Handler,
	namespaceName string,
	partition tqid.Partition,
	taskQueueBreakdown bool,
	partitionIDBreakdown bool,
	tags ...Tag,
) Handler {
	var value string
	if partition == nil {
		value = unknownValue
	} else if normalPartition, ok := partition.(*tqid.NormalPartition); ok {
		if partitionIDBreakdown {
			value = strconv.Itoa(normalPartition.PartitionId())
		} else {
			value = normal
		}
	} else {
		value = sticky
	}

	return GetPerTaskQueueScope(handler, namespaceName, partition.TaskQueue(), taskQueueBreakdown,
		append(tags, PartitionTag(value))...)
}

// GetPerTaskQueuePartitionTypeScope returns GetPerTaskQueueScope scope plus a "partition" tag which
// can be "__normal__", "__sticky__", or "_unknown_".
func GetPerTaskQueuePartitionTypeScope(
	handler Handler,
	namespaceName string,
	partition tqid.Partition,
	taskQueueBreakdown bool,
	tags ...Tag,
) Handler {
	var value string
	if partition == nil {
		value = unknownValue
	} else if _, ok := partition.(*tqid.NormalPartition); ok {
		value = normal
	} else {
		value = sticky
	}

	return GetPerTaskQueueScope(handler, namespaceName, partition.TaskQueue(), taskQueueBreakdown,
		append(tags, PartitionTag(value))...)
}
