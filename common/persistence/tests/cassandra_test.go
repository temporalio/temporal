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

	"go.uber.org/zap/zaptest"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/serialization"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/resolver"
)

func setUpCassandraTest(t *testing.T) (CassandraTestData, func()) {
	var testData CassandraTestData
	testData.Cfg = NewCassandraConfig()
	testData.Logger = log.NewZapLogger(zaptest.NewLogger(t))
	SetUpCassandraDatabase(testData.Cfg, testData.Logger)
	SetUpCassandraSchema(testData.Cfg, testData.Logger)

	testData.Factory = cassandra.NewFactory(
		*testData.Cfg,
		resolver.NewNoopResolver(),
		testCassandraClusterName,
		testData.Logger,
	)

	tearDown := func() {
		testData.Factory.Close()
		TearDownCassandraKeyspace(testData.Cfg)
	}

	return testData, tearDown
}

func TestCassandraExecutionMutableStateStoreSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewExecutionMutableStateSuite(t, shardStore, executionStore, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraExecutionMutableStateTaskStoreSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestCassandraHistoryStoreSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewHistoryEventsSuite(t, store, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraTaskQueueSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestCassandraTaskQueueTaskSuite(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create Cassandra DB: %v", err)
	}

	s := NewTaskQueueTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}
