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
	"errors"

	"fmt"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/uber-common/bark"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
)

type (
	// Factory vends store objects backed by MySQL
	Factory struct {
		sync.RWMutex
		cfg              config.SQL
		clusterName      string
		logger           bark.Logger
		execStoreFactory *executionStoreFactory
	}
	executionStoreFactory struct {
		db     *sqlx.DB
		logger bark.Logger
	}
)

var errNoVisibility = errors.New("visibility not supported by SQL datastore")

// NewFactory returns an instance of a factory object which can be used to create
// datastores backed by any kind of SQL store
func NewFactory(cfg config.SQL, clusterName string, logger bark.Logger) *Factory {
	return &Factory{cfg: cfg, clusterName: clusterName, logger: logger}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return newTaskPersistence(f.cfg, f.logger)
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return newShardPersistence(f.cfg, f.clusterName, f.logger)
}

// NewHistoryStore returns a new history store
func (f *Factory) NewHistoryStore() (p.HistoryStore, error) {
	return newHistoryPersistence(f.cfg, f.logger)
}

// NewHistoryV2Store returns a new history store
func (f *Factory) NewHistoryV2Store() (p.HistoryV2Store, error) {
	return nil, fmt.Errorf("Not implemented")
}

// NewMetadataStore returns a new metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	return newMetadataPersistenceV2(f.cfg, f.clusterName, f.logger)
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
	factory, err := f.newExecutionStoreFactory()
	if err != nil {
		return nil, err
	}
	return factory.new(shardID)
}

// NewVisibilityStore returns a visibility store
func (f *Factory) NewVisibilityStore() (p.VisibilityStore, error) {
	return nil, errNoVisibility
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()
	if f.execStoreFactory != nil {
		f.execStoreFactory.close()
	}
}

// newExecutionStoreFactory returns a new instance of a factory that vends
// execution stores. This factory exist to make sure all of the execution
// managers reuse the same underlying db connection / object and that closing
// one closes all of them
func (f *Factory) newExecutionStoreFactory() (*executionStoreFactory, error) {
	f.RLock()
	if f.execStoreFactory != nil {
		f.RUnlock()
		return f.execStoreFactory, nil
	}
	f.RUnlock()
	f.Lock()
	defer f.Unlock()
	var err error
	f.execStoreFactory, err = newExecutionStoreFactory(f.cfg, f.logger)
	return f.execStoreFactory, err
}

func newExecutionStoreFactory(cfg config.SQL, logger bark.Logger) (*executionStoreFactory, error) {
	db, err := newConnection(cfg)
	if err != nil {
		return nil, err
	}
	return &executionStoreFactory{
		db:     db,
		logger: logger,
	}, nil
}

func (f *executionStoreFactory) new(shardID int) (p.ExecutionStore, error) {
	return NewSQLMatchingPersistence(f.db, f.logger, shardID)
}

// close closes the factory
func (f *executionStoreFactory) close() {
	if f.db != nil {
		f.db.Close()
	}
}
