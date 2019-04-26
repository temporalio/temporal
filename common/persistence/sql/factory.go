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

package sql

import (
	"sync"

	"github.com/uber/cadence/common/log"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/storage"
	"github.com/uber/cadence/common/persistence/sql/storage/sqldb"
	"github.com/uber/cadence/common/service/config"
)

// Factory vends store objects backed by MySQL
type Factory struct {
	sync.RWMutex
	db          sqldb.Interface
	cfg         config.SQL
	clusterName string
	logger      log.Logger
}

// NewFactory returns an instance of a factory object which can be used to create
// datastores backed by any kind of SQL store
func NewFactory(cfg config.SQL, clusterName string, logger log.Logger) *Factory {
	return &Factory{cfg: cfg, clusterName: clusterName, logger: logger}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	conn, err := f.conn()
	if err != nil {
		return nil, err
	}
	return newTaskPersistence(conn, f.cfg.NumShards, f.logger)
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	conn, err := f.conn()
	if err != nil {
		return nil, err
	}
	return newShardPersistence(conn, f.clusterName, f.logger)
}

// NewHistoryStore returns a new history store
func (f *Factory) NewHistoryStore() (p.HistoryStore, error) {
	conn, err := f.conn()
	if err != nil {
		return nil, err
	}
	return newHistoryPersistence(conn, f.logger)
}

// NewHistoryV2Store returns a new history store
func (f *Factory) NewHistoryV2Store() (p.HistoryV2Store, error) {
	conn, err := f.conn()
	if err != nil {
		return nil, err
	}
	return newHistoryV2Persistence(conn, f.logger)
}

// NewMetadataStore returns a new metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	conn, err := f.conn()
	if err != nil {
		return nil, err
	}
	return newMetadataPersistenceV2(conn, f.clusterName, f.logger)
}

// NewMetadataStoreV1 returns the default metadatastore
func (f *Factory) NewMetadataStoreV1() (p.MetadataStore, error) {
	return f.NewMetadataStore()
}

// NewMetadataStoreV2 returns the default metadatastore
func (f *Factory) NewMetadataStoreV2() (p.MetadataStore, error) {
	return f.NewMetadataStore()
}

// NewExecutionStore returns an ExecutionStore for a given shardID
func (f *Factory) NewExecutionStore(shardID int) (p.ExecutionStore, error) {
	conn, err := f.conn()
	if err != nil {
		return nil, err
	}
	return NewSQLExecutionStore(conn, f.logger, shardID)
}

// NewVisibilityStore returns a visibility store
func (f *Factory) NewVisibilityStore() (p.VisibilityStore, error) {
	return NewSQLVisibilityStore(f.cfg, f.logger)
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()
	if f.db != nil {
		f.db.Close()
	}
}

func (f *Factory) conn() (sqldb.Interface, error) {
	f.RLock()
	if f.db != nil {
		f.RUnlock()
		return f.db, nil
	}
	f.RUnlock()
	f.Lock()
	defer f.Unlock()
	var err error
	f.db, err = storage.NewSQLDB(&f.cfg)
	return f.db, err
}
