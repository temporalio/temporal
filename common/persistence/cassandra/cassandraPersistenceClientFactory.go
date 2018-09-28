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

package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	p "github.com/uber/cadence/common/persistence"
)

type (
	cassandraPersistenceClientFactory struct {
		session       *gocql.Session
		metricsClient metrics.Client
		rateLimiter   common.TokenBucket
		logger        bark.Logger
	}
)

// NewPersistenceClientFactory is used to create an instance of ExecutionManagerFactory implementation
func NewPersistenceClientFactory(hosts string, port int, user, password, dc string, keyspace string,
	numConns int, logger bark.Logger, rateLimiter common.TokenBucket, metricsClient metrics.Client) (p.ExecutionManagerFactory, error) {
	cluster := NewCassandraCluster(hosts, port, user, password, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout
	cluster.NumConns = numConns

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraPersistenceClientFactory{session: session, logger: logger, metricsClient: metricsClient, rateLimiter: rateLimiter}, nil
}

// CreateExecutionManager implements ExecutionManagerFactory interface
func (f *cassandraPersistenceClientFactory) CreateExecutionManager(shardID int) (p.ExecutionManager, error) {
	pmgr, err := NewWorkflowExecutionPersistence(shardID, f.session, f.logger)

	if err != nil {
		return nil, err
	}

	mgr := p.NewExecutionManagerImpl(pmgr)

	if f.rateLimiter != nil {
		mgr = p.NewWorkflowExecutionPersistenceRateLimitedClient(mgr, f.rateLimiter, f.logger)
	}

	if f.metricsClient == nil {
		return mgr, nil
	}

	return p.NewWorkflowExecutionPersistenceMetricsClient(mgr, f.metricsClient, f.logger), nil
}

// Close releases the underlying resources held by this object
func (f *cassandraPersistenceClientFactory) Close() {
	if f.session != nil {
		f.session.Close()
	}
}
