package history

import (
	"sync"

	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
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
