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

	"github.com/uber-common/bark"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
)

type (
	// Factory vends store objects backed by MySQL
	Factory struct {
		cfg         config.SQL
		clusterName string
		logger      bark.Logger
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
	return newTaskPersistence(f.cfg)
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return newShardPersistence(f.cfg, f.clusterName, f.logger)
}

// NewHistoryStore returns a new history store
func (f *Factory) NewHistoryStore() (p.HistoryStore, error) {
	return newHistoryPersistence(f.cfg, f.logger)
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
	// todo: this is only a placeholder
	return NewSQLMatchingPersistence(f.cfg, f.logger)
}

// NewVisibilityStore returns a visibility store
func (f *Factory) NewVisibilityStore() (p.VisibilityStore, error) {
	return nil, errNoVisibility
}

// Close closes the factory
func (f *Factory) Close() {}
