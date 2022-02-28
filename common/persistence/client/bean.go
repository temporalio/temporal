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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination bean_mock.go

package client

import (
	"sync"

	"go.temporal.io/server/common/persistence"
)

type (
	// Bean in an collection of persistence manager
	Bean interface {
		Close()

		GetClusterMetadataManager() persistence.ClusterMetadataManager
		GetMetadataManager() persistence.MetadataManager
		GetTaskManager() persistence.TaskManager
		GetNamespaceReplicationQueue() persistence.NamespaceReplicationQueue
		GetShardManager() persistence.ShardManager
		GetExecutionManager() persistence.ExecutionManager
	}

	// BeanImpl stores persistence managers
	BeanImpl struct {
		sync.RWMutex

		clusterMetadataManager    persistence.ClusterMetadataManager
		metadataManager           persistence.MetadataManager
		taskManager               persistence.TaskManager
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
		shardManager              persistence.ShardManager
		executionManager          persistence.ExecutionManager

		factory  Factory
		isClosed bool
	}
)

var _ Bean = (*BeanImpl)(nil)

// NewBeanFromFactory crate a new store bean using factory
func NewBeanFromFactory(
	factory Factory,
) (*BeanImpl, error) {
	clusterMetadataMgr, err := factory.NewClusterMetadataManager()
	if err != nil {
		return nil, err
	}

	metadataMgr, err := factory.NewMetadataManager()
	if err != nil {
		return nil, err
	}

	taskMgr, err := factory.NewTaskManager()
	if err != nil {
		return nil, err
	}

	namespaceReplicationQueue, err := factory.NewNamespaceReplicationQueue()
	if err != nil {
		return nil, err
	}

	shardMgr, err := factory.NewShardManager()
	if err != nil {
		return nil, err
	}

	executionManager, err := factory.NewExecutionManager()
	if err != nil {
		return nil, err
	}

	return NewBean(
		factory,
		clusterMetadataMgr,
		metadataMgr,
		taskMgr,
		namespaceReplicationQueue,
		shardMgr,
		executionManager,
	), nil
}

// NewBean create a new store bean
func NewBean(
	factory Factory,
	clusterMetadataManager persistence.ClusterMetadataManager,
	metadataManager persistence.MetadataManager,
	taskManager persistence.TaskManager,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	shardManager persistence.ShardManager,
	executionManager persistence.ExecutionManager,
) *BeanImpl {
	return &BeanImpl{
		factory:                   factory,
		clusterMetadataManager:    clusterMetadataManager,
		metadataManager:           metadataManager,
		taskManager:               taskManager,
		namespaceReplicationQueue: namespaceReplicationQueue,
		shardManager:              shardManager,
		executionManager:          executionManager,
		isClosed:                  false,
	}
}

// GetClusterMetadataManager get ClusterMetadataManager
func (s *BeanImpl) GetClusterMetadataManager() persistence.ClusterMetadataManager {
	s.RLock()
	defer s.RUnlock()

	return s.clusterMetadataManager
}

// GetMetadataManager get MetadataManager
func (s *BeanImpl) GetMetadataManager() persistence.MetadataManager {

	s.RLock()
	defer s.RUnlock()

	return s.metadataManager
}

// GetTaskManager get TaskManager
func (s *BeanImpl) GetTaskManager() persistence.TaskManager {

	s.RLock()
	defer s.RUnlock()

	return s.taskManager
}

// GetNamespaceReplicationQueue get NamespaceReplicationQueue
func (s *BeanImpl) GetNamespaceReplicationQueue() persistence.NamespaceReplicationQueue {
	s.RLock()
	defer s.RUnlock()

	return s.namespaceReplicationQueue
}

// GetShardManager get ShardManager
func (s *BeanImpl) GetShardManager() persistence.ShardManager {

	s.RLock()
	defer s.RUnlock()

	return s.shardManager
}

// GetExecutionManager get ExecutionManager
func (s *BeanImpl) GetExecutionManager() persistence.ExecutionManager {
	s.RLock()
	defer s.RUnlock()

	return s.executionManager
}

// Close cleanup connections
func (s *BeanImpl) Close() {
	s.Lock()
	defer s.Unlock()

	if s.isClosed {
		return
	}

	s.clusterMetadataManager.Close()
	s.metadataManager.Close()
	s.taskManager.Close()
	s.namespaceReplicationQueue.Stop()
	s.shardManager.Close()
	s.executionManager.Close()

	s.factory.Close()
	s.isClosed = true
}
