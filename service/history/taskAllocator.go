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

package history

import (
	"sync"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
)

type (
	taskAllocator interface {
		verifyActiveTask(taskNamespaceID namespace.ID, task interface{}) bool
		verifyFailoverActiveTask(targetNamespaceIDs map[string]struct{}, taskNamespaceID namespace.ID, task interface{}) bool
		verifyStandbyTask(standbyCluster string, taskNamespaceID namespace.ID, task interface{}) bool
		lock()
		unlock()
	}

	taskAllocatorImpl struct {
		currentClusterName string
		shard              shard.Context
		namespaceRegistry  namespace.Registry
		logger             log.Logger

		locker sync.RWMutex
	}
)

// newTaskAllocator create a new task allocator
func newTaskAllocator(shard shard.Context) taskAllocator {
	return &taskAllocatorImpl{
		currentClusterName: shard.GetClusterMetadata().GetCurrentClusterName(),
		shard:              shard,
		namespaceRegistry:  shard.GetNamespaceRegistry(),
		logger:             shard.GetLogger(),
	}
}

// verifyActiveTask, will return true if task activeness check is successful
func (t *taskAllocatorImpl) verifyActiveTask(taskNamespaceID namespace.ID, task interface{}) bool {
	t.locker.RLock()
	defer t.locker.RUnlock()

	namespaceEntry, err := t.namespaceRegistry.GetNamespaceByID(taskNamespaceID)
	if err != nil {
		t.logger.Debug("Unable to find namespace, process task.", tag.WorkflowNamespaceID(taskNamespaceID.String()), tag.Value(task))
		return true
	}
	if namespaceEntry.IsGlobalNamespace() && t.currentClusterName != namespaceEntry.ActiveClusterName() {
		// timer task does not belong to cluster name
		t.logger.Debug("Namespace is not active, skip task.", tag.WorkflowNamespaceID(taskNamespaceID.String()), tag.Value(task))
		return false
	}
	t.logger.Debug("Namespace is active, process task.", tag.WorkflowNamespaceID(taskNamespaceID.String()), tag.Value(task))
	return true
}

// verifyFailoverActiveTask, will return true if task activeness check is successful
func (t *taskAllocatorImpl) verifyFailoverActiveTask(targetNamespaceIDs map[string]struct{}, taskNamespaceID namespace.ID, task interface{}) bool {
	_, ok := targetNamespaceIDs[taskNamespaceID.String()]
	if ok {
		t.logger.Debug("Failover Namespace is active, process task.", tag.WorkflowNamespaceID(taskNamespaceID.String()), tag.Value(task))
		return true
	}
	t.logger.Debug("Failover Namespace is not active, skip task.", tag.WorkflowNamespaceID(taskNamespaceID.String()), tag.Value(task))
	return false
}

// verifyStandbyTask, will return true if task standbyness check is successful
func (t *taskAllocatorImpl) verifyStandbyTask(standbyCluster string, taskNamespaceID namespace.ID, task interface{}) bool {
	t.locker.RLock()
	defer t.locker.RUnlock()

	namespaceEntry, err := t.namespaceRegistry.GetNamespaceByID(taskNamespaceID)
	if err != nil {
		t.logger.Debug("Unable to find namespace, skip task.", tag.WorkflowNamespaceID(taskNamespaceID.String()), tag.Value(task))
		return false
	}
	if !namespaceEntry.IsGlobalNamespace() {
		// non global namespace, timer task does not belong here
		t.logger.Debug("Namespace is not global, skip task.", tag.WorkflowNamespaceID(taskNamespaceID.String()), tag.Value(task))
		return false
	} else if namespaceEntry.IsGlobalNamespace() && namespaceEntry.ActiveClusterName() != standbyCluster {
		// timer task does not belong here
		t.logger.Debug("Namespace is not standby, skip task.", tag.WorkflowNamespaceID(taskNamespaceID.String()), tag.Value(task))
		return false
	}
	t.logger.Debug("Namespace is standby, process task.", tag.WorkflowNamespaceID(taskNamespaceID.String()), tag.Value(task))
	return true
}

// lock block all task allocation
func (t *taskAllocatorImpl) lock() {
	t.locker.Lock()
}

// unlock resume the task allocator
func (t *taskAllocatorImpl) unlock() {
	t.locker.Unlock()
}
