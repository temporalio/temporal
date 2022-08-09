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

package sql

import (
	"fmt"
	"sync"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

type (
	// Factory vends store objects backed by MySQL
	Factory struct {
		cfg         config.SQL
		mainDBConn  DbConn
		clusterName string
		logger      log.Logger
	}

	// DbConn represents a logical mysql connection - its a
	// wrapper around the standard sql connection pool with
	// additional reference counting
	DbConn struct {
		dbKind   sqlplugin.DbKind
		cfg      *config.SQL
		resolver resolver.ServiceResolver

		sqlplugin.DB

		sync.Mutex
		refCnt int
	}
)

// NewFactory returns an instance of a factory object which can be used to create
// datastores backed by any kind of SQL store
func NewFactory(
	cfg config.SQL,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
) *Factory {
	return &Factory{
		cfg:         cfg,
		clusterName: clusterName,
		logger:      logger,
		mainDBConn:  NewRefCountedDBConn(sqlplugin.DbKindMain, &cfg, r),
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	conn, err := f.mainDBConn.Get()
	if err != nil {
		return nil, err
	}
	return newTaskPersistence(conn, f.cfg.TaskScanPartitions, f.logger)
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	conn, err := f.mainDBConn.Get()
	if err != nil {
		return nil, err
	}
	return newShardPersistence(conn, f.clusterName, f.logger)
}

// NewMetadataStore returns a new metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	conn, err := f.mainDBConn.Get()
	if err != nil {
		return nil, err
	}
	return newMetadataPersistenceV2(conn, f.clusterName, f.logger)
}

// NewClusterMetadataStore returns a new ClusterMetadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	conn, err := f.mainDBConn.Get()
	if err != nil {
		return nil, err
	}
	return newClusterMetadataPersistence(conn, f.logger)
}

// NewExecutionStore returns a new ExecutionStore
func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	conn, err := f.mainDBConn.Get()
	if err != nil {
		return nil, err
	}
	return NewSQLExecutionStore(conn, f.logger)
}

// NewQueue returns a new queue backed by sql
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	conn, err := f.mainDBConn.Get()
	if err != nil {
		return nil, err
	}

	return newQueue(conn, f.logger, queueType)
}

// Close closes the factory
func (f *Factory) Close() {
	f.mainDBConn.ForceClose()
}

// NewRefCountedDBConn returns a  logical mysql connection that
// uses reference counting to decide when to close the
// underlying connection object. The reference count gets incremented
// everytime get() is called and decremented everytime Close() is called
func NewRefCountedDBConn(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
) DbConn {
	return DbConn{
		dbKind:   dbKind,
		cfg:      cfg,
		resolver: r,
	}
}

// Get returns a mysql db connection and increments a reference count
// this method will create a new connection, if an existing connection
// does not exist
func (c *DbConn) Get() (sqlplugin.DB, error) {
	c.Lock()
	defer c.Unlock()
	if c.refCnt == 0 {
		conn, err := NewSQLDB(c.dbKind, c.cfg, c.resolver)
		if err != nil {
			return nil, err
		}
		c.DB = conn
	}
	c.refCnt++
	return c, nil
}

// ForceClose ignores reference counts and shutsdown the underlying connection pool
func (c *DbConn) ForceClose() {
	c.Lock()
	defer c.Unlock()
	if c.DB != nil {
		err := c.DB.Close()
		if err != nil {
			fmt.Println("failed to close database connection, may leak some connection", err)
		}
	}
	c.refCnt = 0
}

// Close closes the underlying connection if the reference count becomes zero
func (c *DbConn) Close() error {
	c.Lock()
	defer c.Unlock()
	c.refCnt--
	if c.refCnt == 0 {
		err := c.DB.Close()
		c.DB = nil
		return err
	}
	return nil
}
