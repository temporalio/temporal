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

package matching

import (
	"math/rand"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqname"
	"go.temporal.io/server/common/util"
)

type (
	// LoadBalancer is the interface for implementers of
	// component that distributes add/poll api calls across
	// available task queue partitions when possible
	LoadBalancer interface {
		// PickWritePartition returns the task queue partition for adding
		// an activity or workflow task. The input is the name of the
		// original task queue (with no partition info). When forwardedFrom
		// is non-empty, this call is forwardedFrom from a child partition
		// to a parent partition in which case, no load balancing should be
		// performed
		PickWritePartition(
			namespaceID namespace.ID,
			taskQueue taskqueuepb.TaskQueue,
			taskQueueType enumspb.TaskQueueType,
			forwardedFrom string,
		) string

		// PickReadPartition returns the task queue partition to send a poller to.
		// Input is name of the original task queue as specified by caller. When
		// forwardedFrom is non-empty, no load balancing should be done.
		PickReadPartition(
			namespaceID namespace.ID,
			taskQueue taskqueuepb.TaskQueue,
			taskQueueType enumspb.TaskQueueType,
			forwardedFrom string,
		) string
	}

	defaultLoadBalancer struct {
		namespaceIDToName   func(id namespace.ID) (namespace.Name, error)
		nReadPartitions     dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		nWritePartitions    dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters
		forceReadPartition  dynamicconfig.IntPropertyFn
		forceWritePartition dynamicconfig.IntPropertyFn
	}
)

// NewLoadBalancer returns an instance of matching load balancer that
// can help distribute api calls across task queue partitions
func NewLoadBalancer(
	namespaceIDToName func(id namespace.ID) (namespace.Name, error),
	dc *dynamicconfig.Collection,
) LoadBalancer {
	lb := &defaultLoadBalancer{
		namespaceIDToName:   namespaceIDToName,
		nReadPartitions:     dc.GetTaskQueuePartitionsProperty(dynamicconfig.MatchingNumTaskqueueReadPartitions),
		nWritePartitions:    dc.GetTaskQueuePartitionsProperty(dynamicconfig.MatchingNumTaskqueueWritePartitions),
		forceReadPartition:  dc.GetIntProperty(dynamicconfig.TestMatchingLBForceReadPartition, -1),
		forceWritePartition: dc.GetIntProperty(dynamicconfig.TestMatchingLBForceReadPartition, -1),
	}
	return lb
}

func (lb *defaultLoadBalancer) PickWritePartition(
	namespaceID namespace.ID,
	taskQueue taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
	forwardedFrom string,
) string {
	return lb.pickPartition(namespaceID, taskQueue, taskQueueType, forwardedFrom, lb.nWritePartitions, lb.forceWritePartition)
}

func (lb *defaultLoadBalancer) PickReadPartition(
	namespaceID namespace.ID,
	taskQueue taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
	forwardedFrom string,
) string {
	return lb.pickPartition(namespaceID, taskQueue, taskQueueType, forwardedFrom, lb.nReadPartitions, lb.forceReadPartition)
}

func (lb *defaultLoadBalancer) pickPartition(
	namespaceID namespace.ID,
	taskQueue taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
	forwardedFrom string,
	nPartitions dynamicconfig.IntPropertyFnWithTaskQueueInfoFilters,
	force dynamicconfig.IntPropertyFn,
) string {
	if forwardedFrom != "" || taskQueue.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY {
		return taskQueue.GetName()
	}

	tqName, err := tqname.FromBaseName(taskQueue.GetName())

	// this should never happen when forwardedFrom is empty
	if err != nil {
		return taskQueue.GetName()
	}

	if n := force(); n >= 0 {
		return tqName.WithPartition(n).FullName()
	}

	nsName, err := lb.namespaceIDToName(namespaceID)
	if err != nil {
		return taskQueue.GetName()
	}

	n := util.Max(1, nPartitions(nsName.String(), tqName.BaseNameString(), taskQueueType))
	return tqName.WithPartition(rand.Intn(n)).FullName()
}
