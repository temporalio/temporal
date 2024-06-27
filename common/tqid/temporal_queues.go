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

package tqid

import (
	"strconv"

	"go.temporal.io/server/common/metrics"
)

func GetPerTaskQueueScope(
	handler metrics.Handler,
	namespaceName string,
	taskQueue *TaskQueue,
	taskQueueBreakdown bool,
) metrics.Handler {
	metricTaskQueueName := "__omitted__"
	if taskQueueBreakdown {
		metricTaskQueueName = taskQueue.Name()
	}
	return handler.WithTags(
		metrics.NamespaceTag(namespaceName),
		metrics.TaskQueueTag(metricTaskQueueName),
		metrics.TaskQueueTypeTag(taskQueue.TaskType()),
	)
}

func GetPerTaskQueuePartitionScope(
	handler metrics.Handler,
	namespaceName string,
	partition Partition,
	taskQueueBreakdown bool,
	partitionIDBreakdown bool,
) metrics.Handler {
	h := GetPerTaskQueueScope(handler, namespaceName, partition.TaskQueue(), taskQueueBreakdown)

	var value string
	if partition == nil {
		value = "_unknown_"
	} else if normalPartition, ok := partition.(*NormalPartition); ok {
		if partitionIDBreakdown {
			value = strconv.Itoa(normalPartition.PartitionId())
		} else {
			value = "__normal__"
		}
	} else {
		value = "__sticky__"
	}

	return h.WithTags(metrics.PartitionTag(value))
}
