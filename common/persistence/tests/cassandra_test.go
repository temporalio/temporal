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

package tests

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/cassandra"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/environment"
)

// TODO merge the initialization with existing persistence setup
const (
	testCassandraClusterName = "temporal_cassandra_cluster"

	testCassandraUser               = "temporal"
	testCassandraPassword           = "temporal"
	testCassandraDatabaseNamePrefix = "test_"
	testCassandraDatabaseNameSuffix = "temporal_persistence"
)

func TestCassandraExecutionMutableStateStoreSuite(t *testing.T) {
	cfg := newCassandraConfig()
	SetUpCassandraDatabase(cfg)
	SetUpCassandraSchema(cfg)
	logger := log.NewNoopLogger()
	factory := cassandra.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testCassandraClusterName,
		logger,
	)
	shardStore, err := factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	defer func() {
		factory.Close()
		TearDownCassandraKeyspace(cfg)
	}()

	s := NewExecutionMutableStateSuite(t, shardStore, executionStore, logger)
	suite.Run(t, s)
}

func TestCassandraHistoryStoreSuite(t *testing.T) {
	cfg := newCassandraConfig()
	SetUpCassandraDatabase(cfg)
	SetUpCassandraSchema(cfg)
	logger := log.NewNoopLogger()
	factory := cassandra.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testCassandraClusterName,
		logger,
	)
	store, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	defer func() {
		factory.Close()
		TearDownCassandraKeyspace(cfg)
	}()

	s := NewHistoryEventsSuite(t, store, logger)
	suite.Run(t, s)
}

func TestCassandraTaskQueueSuite(t *testing.T) {
	cfg := newCassandraConfig()
	SetUpCassandraDatabase(cfg)
	SetUpCassandraSchema(cfg)
	logger := log.NewNoopLogger()
	factory := cassandra.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testCassandraClusterName,
		logger,
	)
	taskQueueStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	defer func() {
		factory.Close()
		TearDownCassandraKeyspace(cfg)
	}()

	s := NewTaskQueueSuite(t, taskQueueStore, logger)
	suite.Run(t, s)
}

func TestCassandraTaskQueueTaskSuite(t *testing.T) {
	cfg := newCassandraConfig()
	SetUpCassandraDatabase(cfg)
	SetUpCassandraSchema(cfg)
	logger := log.NewNoopLogger()
	factory := cassandra.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testCassandraClusterName,
		logger,
	)
	taskQueueStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	defer func() {
		factory.Close()
		TearDownCassandraKeyspace(cfg)
	}()

	s := NewTaskQueueTaskSuite(t, taskQueueStore, logger)
	suite.Run(t, s)
}

// newCassandraConfig returns a new Cassandra config for test
func newCassandraConfig() *config.Cassandra {
	return &config.Cassandra{
		User:     testCassandraUser,
		Password: testCassandraPassword,
		Hosts:    environment.GetCassandraAddress(),
		Port:     environment.GetCassandraPort(),
		Keyspace: testCassandraDatabaseNamePrefix + shuffle.String(testCassandraDatabaseNameSuffix),
	}
}
