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

package history

import (
	"sync"

	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
)

type (
	taskAllocator interface {
		verifyActiveTask(taskDomainID string, task interface{}) (bool, error)
		verifyFailoverActiveTask(targetDomainIDs map[string]struct{}, taskDomainID string, task interface{}) (bool, error)
		verifyStandbyTask(standbyCluster string, taskDomainID string, task interface{}) (bool, error)
		lock()
		unlock()
	}

	taskAllocatorImpl struct {
		currentClusterName string
		shard              ShardContext
		domainCache        cache.DomainCache
		logger             bark.Logger

		locker sync.RWMutex
	}
)

// newTaskAllocator create a new task allocator
func newTaskAllocator(shard ShardContext) taskAllocator {
	return &taskAllocatorImpl{
		currentClusterName: shard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              shard,
		domainCache:        shard.GetDomainCache(),
		logger:             shard.GetLogger(),
	}
}

// verifyActiveTask, will return true if task activeness check is successful
func (t *taskAllocatorImpl) verifyActiveTask(taskDomainID string, task interface{}) (bool, error) {
	t.locker.RLock()
	defer t.locker.RUnlock()

	domainEntry, err := t.domainCache.GetDomainByID(taskDomainID)
	if err != nil {
		// it is possible that the domain is deleted
		// we should treat that domain as active
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			t.logger.Warnf("Cannot find domainID: %v, err: %v.", taskDomainID)
			return false, err
		}
		t.logger.Warnf("Cannot find domainID: %v, default to process task: %v.", taskDomainID, task)
		return true, nil
	}
	if domainEntry.IsGlobalDomain() && t.currentClusterName != domainEntry.GetReplicationConfig().ActiveClusterName {
		// timer task does not belong to cluster name
		t.logger.Debugf("DomainID: %v is not active, skip task: %v.", taskDomainID, task)
		return false, nil
	}
	t.logger.Debugf("DomainID: %v is active, process task: %v.", taskDomainID, task)
	return true, nil
}

// verifyFailoverActiveTask, will return true if task activeness check is successful
func (t *taskAllocatorImpl) verifyFailoverActiveTask(targetDomainIDs map[string]struct{}, taskDomainID string, task interface{}) (bool, error) {
	_, ok := targetDomainIDs[taskDomainID]
	if ok {
		t.logger.Debugf("Failover DomainID: %v is active, process task: %v.", taskDomainID, task)
		return true, nil
	}
	t.logger.Debugf("Failover DomainID: %v is not active, skip task: %v.", taskDomainID, task)
	return false, nil
}

// verifyStandbyTask, will return true if task standbyness check is successful
func (t *taskAllocatorImpl) verifyStandbyTask(standbyCluster string, taskDomainID string, task interface{}) (bool, error) {
	t.locker.RLock()
	defer t.locker.RUnlock()

	domainEntry, err := t.domainCache.GetDomainByID(taskDomainID)
	if err != nil {
		// it is possible that the domain is deleted
		// we should treat that domain as not active
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			t.logger.Warnf("Cannot find domainID: %v, err: %v.", taskDomainID)
			return false, err
		}
		t.logger.Warnf("Cannot find domainID: %v, default to not process task: %v.", taskDomainID, task)
		return false, nil
	}
	if !domainEntry.IsGlobalDomain() {
		// non global domain, timer task does not belong here
		t.logger.Debugf("DomainID: %v is not global, skip task: %v.", taskDomainID, task)
		return false, nil
	} else if domainEntry.IsGlobalDomain() && domainEntry.GetReplicationConfig().ActiveClusterName != standbyCluster {
		// timer task does not belong here
		t.logger.Debugf("DomainID: %v is not standby, skip task: %v.", taskDomainID, task)
		return false, nil
	}
	t.logger.Debugf("DomainID: %v is standby, process task: %v.", taskDomainID, task)
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
