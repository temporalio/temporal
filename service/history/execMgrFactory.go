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
	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/config"
)

// executionMgrFactory is an implementation of
// persistence.ExecutionManagerFactory interface
type executionMgrFactory struct {
	config        *config.Cassandra
	logger        bark.Logger
	metricsClient metrics.Client
}

// NewExecutionManagerFactory builds and returns a factory object
func NewExecutionManagerFactory(config *config.Cassandra,
	logger bark.Logger, mClient metrics.Client) persistence.ExecutionManagerFactory {

	return &executionMgrFactory{
		config:        config,
		logger:        logger,
		metricsClient: mClient,
	}
}

// CreateExecutionManager implements ExecutionManagerFactory interface
func (factory *executionMgrFactory) CreateExecutionManager(shardID int) (persistence.ExecutionManager, error) {

	mgr, err := persistence.NewCassandraWorkflowExecutionPersistence(
		factory.config.Hosts,
		factory.config.Datacenter,
		factory.config.Keyspace,
		shardID,
		factory.logger)

	if err != nil {
		return nil, err
	}

	tags := map[string]string{
		metrics.ShardTagName: metrics.AllShardsTagValue,
	}
	return persistence.NewWorkflowExecutionPersistenceClient(mgr, factory.metricsClient.Tagged(tags)), nil
}
