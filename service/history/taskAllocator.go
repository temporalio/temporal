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

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	taskAllocator interface {
		verifyActiveTask(taskNamespaceID string, task interface{}) (bool, error)
		verifyFailoverActiveTask(targetNamespaceIDs map[string]struct{}, taskNamespaceID string, task interface{}) (bool, error)
		verifyStandbyTask(standbyCluster string, taskNamespaceID string, task interface{}) (bool, error)
		lock()
		unlock()
	}

	taskAllocatorImpl struct {
		currentClusterName string
		shard              ShardContext
		namespaceCache     cache.NamespaceCache
		logger             log.Logger

		locker sync.RWMutex
	}
)

// newTaskAllocator create a new task allocator
func newTaskAllocator(shard ShardContext) taskAllocator {
	return &taskAllocatorImpl{
		currentClusterName: shard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              shard,
		namespaceCache:     shard.GetNamespaceCache(),
		logger:             shard.GetLogger(),
	}
}

// verifyActiveTask, will return true if task activeness check is successful
func (t *taskAllocatorImpl) verifyActiveTask(taskNamespaceID string, task interface{}) (bool, error) {
	t.locker.RLock()
	defer t.locker.RUnlock()

	namespaceEntry, err := t.namespaceCache.GetNamespaceByID(taskNamespaceID)
	if err != nil {
		// it is possible that the namespace is deleted
		// we should treat that namespace as active
		if _, ok := err.(*serviceerror.NotFound); !ok {
			t.logger.Warn("Cannot find namespace", tag.WorkflowNamespaceID(taskNamespaceID))
			return false, err
		}
		t.logger.Warn("Cannot find namespace, default to process task.", tag.WorkflowNamespaceID(taskNamespaceID), tag.Value(task))
		return true, nil
	}
	if namespaceEntry.IsGlobalNamespace() && t.currentClusterName != namespaceEntry.GetReplicationConfig().ActiveClusterName {
		// timer task does not belong to cluster name
		t.logger.Debug("Namespace is not active, skip task.", tag.WorkflowNamespaceID(taskNamespaceID), tag.Value(task))
		return false, nil
	}
	t.logger.Debug("Namespace is active, process task.", tag.WorkflowNamespaceID(taskNamespaceID), tag.Value(task))
	return true, nil
}

// verifyFailoverActiveTask, will return true if task activeness check is successful
func (t *taskAllocatorImpl) verifyFailoverActiveTask(targetNamespaceIDs map[string]struct{}, taskNamespaceID string, task interface{}) (bool, error) {
	_, ok := targetNamespaceIDs[taskNamespaceID]
	if ok {
		t.logger.Debug("Failover Namespace is active, process task.", tag.WorkflowNamespaceID(taskNamespaceID), tag.Value(task))
		return true, nil
	}
	t.logger.Debug("Failover Namespace is not active, skip task.", tag.WorkflowNamespaceID(taskNamespaceID), tag.Value(task))
	return false, nil
}

// verifyStandbyTask, will return true if task standbyness check is successful
func (t *taskAllocatorImpl) verifyStandbyTask(standbyCluster string, taskNamespaceID string, task interface{}) (bool, error) {
	t.locker.RLock()
	defer t.locker.RUnlock()

	namespaceEntry, err := t.namespaceCache.GetNamespaceByID(taskNamespaceID)
	if err != nil {
		// it is possible that the namespace is deleted
		// we should treat that namespace as not active
		if _, ok := err.(*serviceerror.NotFound); !ok {
			t.logger.Warn("Cannot find namespace", tag.WorkflowNamespaceID(taskNamespaceID))
			return false, err
		}
		t.logger.Warn("Cannot find namespace, default to not process task.", tag.WorkflowNamespaceID(taskNamespaceID), tag.Value(task))
		return false, nil
	}
	if !namespaceEntry.IsGlobalNamespace() {
		// non global namespace, timer task does not belong here
		t.logger.Debug("Namespace is not global, skip task.", tag.WorkflowNamespaceID(taskNamespaceID), tag.Value(task))
		return false, nil
	} else if namespaceEntry.IsGlobalNamespace() && namespaceEntry.GetReplicationConfig().ActiveClusterName != standbyCluster {
		// timer task does not belong here
		t.logger.Debug("Namespace is not standby, skip task.", tag.WorkflowNamespaceID(taskNamespaceID), tag.Value(task))
		return false, nil
	}
	t.logger.Debug("Namespace is standby, process task.", tag.WorkflowNamespaceID(taskNamespaceID), tag.Value(task))
	return true, nil
}

// lock block all task allocation
func (t *taskAllocatorImpl) lock() {
	t.locker.Lock()
}

// unlock resume the task allocator
func (t *taskAllocatorImpl) unlock() {
	t.locker.Unlock()
}
