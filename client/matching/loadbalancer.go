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
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqname"
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
			taskQueue *taskqueuepb.TaskQueue,
			taskQueueType enumspb.TaskQueueType,
			forwardedFrom string,
		) string

		// PickReadPartition returns the task queue partition to send a poller to.
		// Input is name of the original task queue as specified by caller. When
		// forwardedFrom is non-empty, no load balancing should be done.
		PickReadPartition(
			namespaceID namespace.ID,
			taskQueue *taskqueuepb.TaskQueue,
			taskQueueType enumspb.TaskQueueType,
			forwardedFrom string,
		) *pollToken
	}

	defaultLoadBalancer struct {
		namespaceIDToName   func(id namespace.ID) (namespace.Name, error)
		nReadPartitions     dynamicconfig.IntPropertyFnWithTaskQueueFilter
		nWritePartitions    dynamicconfig.IntPropertyFnWithTaskQueueFilter
		forceReadPartition  dynamicconfig.IntPropertyFn
		forceWritePartition dynamicconfig.IntPropertyFn

		lock         sync.RWMutex
		taskQueueLBs map[taskQueueKey]*tqLoadBalancer
	}

	// Keeps track of polls per partition. Sends a poll to the partition with the fewest polls
	tqLoadBalancer struct {
		taskQueue    taskQueueKey
		pollerCounts []int // keep track of poller count of each partition
		lock         sync.Mutex
	}

	taskQueueKey struct {
		NamespaceID namespace.ID
		Name        tqname.Name
		Type        enumspb.TaskQueueType
	}

	pollToken struct {
		fullName    string
		partitionID int
		balancer    *tqLoadBalancer
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
		forceWritePartition: dc.GetIntProperty(dynamicconfig.TestMatchingLBForceWritePartition, -1),
		lock:                sync.RWMutex{},
		taskQueueLBs:        make(map[taskQueueKey]*tqLoadBalancer),
	}
	return lb
}

func (lb *defaultLoadBalancer) PickWritePartition(
	namespaceID namespace.ID,
	taskQueue *taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
	forwardedFrom string,
) string {
	if forwardedFrom != "" || taskQueue.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY {
		return taskQueue.GetName()
	}

	tqName, err := tqname.FromBaseName(taskQueue.GetName())

	// this should never happen when forwardedFrom is empty
	if err != nil {
		return taskQueue.GetName()
	}

	if n := lb.forceWritePartition(); n >= 0 {
		return tqName.WithPartition(n).FullName()
	}

	nsName, err := lb.namespaceIDToName(namespaceID)
	if err != nil {
		return taskQueue.GetName()
	}

	n := max(1, lb.nWritePartitions(nsName.String(), tqName.BaseNameString(), taskQueueType))
	return tqName.WithPartition(rand.Intn(n)).FullName()
}

// PickReadPartition picks a partition for poller to poll task from, and keeps load balanced between partitions.
// Caller is responsible to call pollToken.Release() after complete the poll.
func (lb *defaultLoadBalancer) PickReadPartition(
	namespaceID namespace.ID,
	taskQueue *taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
	forwardedFrom string,
) *pollToken {
	if forwardedFrom != "" || taskQueue.Kind == enumspb.TASK_QUEUE_KIND_STICKY {
		// no partition for sticky task queue and forwarded request
		return &pollToken{fullName: taskQueue.GetName()}
	}

	parsedName, err := tqname.Parse(taskQueue.GetName())
	if err != nil || err == nil && !parsedName.IsRoot() {
		// parse error or partition already picked, use as-is
		return &pollToken{fullName: taskQueue.GetName()}
	}

	tqlb := lb.getTaskQueueLoadBalancer(namespaceID, parsedName, taskQueueType)

	// For read path it's safer to return global default partition count instead of root partition, when we fail to
	// map namespace ID to name.
	var partitionCount = dynamicconfig.GlobalDefaultNumTaskQueuePartitions

	namespaceName, err := lb.namespaceIDToName(namespaceID)
	if err == nil {
		partitionCount = lb.nReadPartitions(string(namespaceName), parsedName.BaseNameString(), taskQueueType)
	}

	return tqlb.pickReadPartition(partitionCount, lb.forceReadPartition())
}

func (lb *defaultLoadBalancer) getTaskQueueLoadBalancer(
	namespaceID namespace.ID, parsedName tqname.Name, tqType enumspb.TaskQueueType,
) *tqLoadBalancer {
	key := taskQueueKey{NamespaceID: namespaceID, Name: parsedName, Type: tqType}

	lb.lock.RLock()
	tqlb, ok := lb.taskQueueLBs[key]
	lb.lock.RUnlock()
	if ok {
		return tqlb
	}

	lb.lock.Lock()
	tqlb, ok = lb.taskQueueLBs[key]
	if !ok {
		tqlb = newTaskQueueLoadBalancer(key)
		lb.taskQueueLBs[key] = tqlb
	}
	lb.lock.Unlock()
	return tqlb
}

func newTaskQueueLoadBalancer(key taskQueueKey) *tqLoadBalancer {
	return &tqLoadBalancer{
		taskQueue: key,
	}
}

func (b *tqLoadBalancer) pickReadPartition(partitionCount int, forcedPartition int) *pollToken {
	b.lock.Lock()
	defer b.lock.Unlock()

	// ensure we reflect dynamic config change if it ever happens
	b.ensurePartitionCountLocked(partitionCount)

	partitionID := forcedPartition

	if partitionID < 0 {
		partitionID = b.pickReadPartitionWithFewestPolls(partitionCount)
	}

	b.pollerCounts[partitionID]++

	return &pollToken{
		fullName:    b.taskQueue.Name.WithPartition(partitionID).FullName(),
		partitionID: partitionID,
		balancer:    b,
	}
}

// caller to ensure that lock is obtained before call this function
func (b *tqLoadBalancer) pickReadPartitionWithFewestPolls(partitionCount int) int {
	// pick a random partition to start with
	startPartitionID := rand.Intn(partitionCount)
	pickedPartitionID := startPartitionID
	minPollerCount := b.pollerCounts[pickedPartitionID]
	for i := 1; i < partitionCount && minPollerCount > 0; i++ {
		currPartitionID := (startPartitionID + i) % int(partitionCount)
		if b.pollerCounts[currPartitionID] < minPollerCount {
			pickedPartitionID = currPartitionID
			minPollerCount = b.pollerCounts[currPartitionID]
		}
	}

	return pickedPartitionID
}

// caller to ensure that lock is obtained before call this function
func (b *tqLoadBalancer) ensurePartitionCountLocked(partitionCount int) {
	if len(b.pollerCounts) == partitionCount {
		return
	}

	if len(b.pollerCounts) < partitionCount {
		// add more partition entries
		for i := len(b.pollerCounts); i < partitionCount; i++ {
			b.pollerCounts = append(b.pollerCounts, 0)
		}
	} else {
		// truncate existing partition entries
		b.pollerCounts = b.pollerCounts[:partitionCount]
	}
}

func (b *tqLoadBalancer) Release(partitionID int) {
	b.lock.Lock()
	defer b.lock.Unlock()
	// partitionID could be out of range if dynamic config reduce taskQueue partition count
	if len(b.pollerCounts) > partitionID && b.pollerCounts[partitionID] > 0 {
		b.pollerCounts[partitionID]--
	}
}

func (t *pollToken) Release() {
	if t.balancer != nil {
		// t.balancer == nil is valid for example sticky task queue.
		t.balancer.Release(t.partitionID)
	}
}

func (t *pollToken) GetFullName() string {
	return t.fullName
}
