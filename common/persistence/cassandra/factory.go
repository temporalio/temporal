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

package cassandra

import (
	"sync"

	"github.com/gocql/gocql"

	"go.temporal.io/server/common/cassandra"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/service/config"
)

type (
	// Factory vends datastore implementations backed by cassandra
	Factory struct {
		sync.RWMutex
		cfg              config.Cassandra
		clusterName      string
		logger           log.Logger
		execStoreFactory *executionStoreFactory
	}
	executionStoreFactory struct {
		session *gocql.Session
		logger  log.Logger
	}
)

// NewFactory returns an instance of a factory object which can be used to create
// datastores that are backed by cassandra
func NewFactory(cfg config.Cassandra, clusterName string, logger log.Logger) *Factory {
	return &Factory{
		cfg:         cfg,
		clusterName: clusterName,
		logger:      logger,
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return newTaskPersistence(f.cfg, f.logger)
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return newShardPersistence(f.cfg, f.clusterName, f.logger)
}

// NewHistoryV2Store returns a new history store
func (f *Factory) NewHistoryV2Store() (p.HistoryStore, error) {
	return newHistoryV2Persistence(f.cfg, f.logger)
}

// NewMetadataStore returns a metadata store that understands only v2
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	return newMetadataPersistenceV2(f.cfg, f.clusterName, f.logger)
}

// NewClusterMetadataStore returns a metadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return newClusterMetadataInstance(f.cfg, f.logger)
}

// NewExecutionStore returns an ExecutionStore for a given shardID
func (f *Factory) NewExecutionStore(shardID int) (p.ExecutionStore, error) {
	factory, err := f.executionStoreFactory()
	if err != nil {
		return nil, err
	}
	return factory.new(shardID)
}

// NewVisibilityStore returns a visibility store
func (f *Factory) NewVisibilityStore() (p.VisibilityStore, error) {
	return newVisibilityPersistence(f.cfg, f.logger)
}

// NewQueue returns a new queue backed by cassandra
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return newQueue(f.cfg, f.logger, queueType)
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()
	if f.execStoreFactory != nil {
		f.execStoreFactory.close()
	}
}

func (f *Factory) executionStoreFactory() (*executionStoreFactory, error) {
	f.RLock()
	if f.execStoreFactory != nil {
		f.RUnlock()
		return f.execStoreFactory, nil
	}
	f.RUnlock()
	f.Lock()
	defer f.Unlock()
	if f.execStoreFactory != nil {
		return f.execStoreFactory, nil
	}

	factory, err := newExecutionStoreFactory(f.cfg, f.logger)
	if err != nil {
		return nil, err
	}
	f.execStoreFactory = factory
	return f.execStoreFactory, nil
}

// newExecutionStoreFactory is used to create an instance of ExecutionStoreFactory implementation
func newExecutionStoreFactory(cfg config.Cassandra, logger log.Logger) (*executionStoreFactory, error) {
	cluster := cassandra.NewCassandraCluster(cfg)
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = cfg.Consistency.GetConsistency()
	cluster.SerialConsistency = cfg.Consistency.GetSerialConsistency()
	cluster.Timeout = defaultSessionTimeout
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &executionStoreFactory{session: session, logger: logger}, nil
}

func (f *executionStoreFactory) close() {
	f.session.Close()
}

// new implements ExecutionStoreFactory interface
func (f *executionStoreFactory) new(shardID int) (p.ExecutionStore, error) {
	pmgr, err := NewWorkflowExecutionPersistence(shardID, f.session, f.logger)
	if err != nil {
		return nil, err
	}
	return pmgr, nil
}
