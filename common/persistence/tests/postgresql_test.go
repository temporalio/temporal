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

	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	sqltests "go.temporal.io/server/common/persistence/sql/sqlplugin/tests"
	"go.temporal.io/server/common/resolver"
)

func TestPostgreSQLShardStoreSuite(t *testing.T) {
	testData, tearDown := setUpPostgreSQLTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewShardSuite(
		t,
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestPostgreSQLExecutionMutableStateStoreSuite(t *testing.T) {
	testData, tearDown := setUpPostgreSQLTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestPostgreSQLExecutionMutableStateTaskStoreSuite(t *testing.T) {
	testData, tearDown := setUpPostgreSQLTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
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

func TestPostgreSQLHistoryStoreSuite(t *testing.T) {
	testData, tearDown := setUpPostgreSQLTest(t)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewHistoryEventsSuite(t, store, testData.Logger)
	suite.Run(t, s)
}

func TestPostgreSQLTaskQueueSuite(t *testing.T) {
	testData, tearDown := setUpPostgreSQLTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestPostgreSQLTaskQueueTaskSuite(t *testing.T) {
	testData, tearDown := setUpPostgreSQLTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewTaskQueueTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestPostgreSQLVisibilityPersistenceSuite(t *testing.T) {
	s := &VisibilityPersistenceSuite{
		TestBase: persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQLTestClusterOption()),
	}
	suite.Run(t, s)
}

func TestPostgreSQL12VisibilityPersistenceSuite(t *testing.T) {
	s := &VisibilityPersistenceSuite{
		TestBase: persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQL12TestClusterOption()),
	}
	suite.Run(t, s)
}

// TODO: Merge persistence-tests into the tests directory.

func TestPostgreSQLHistoryV2PersistenceSuite(t *testing.T) {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestPostgreSQLMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestPostgreSQLClusterMetadataPersistence(t *testing.T) {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

// TODO flaky test in buildkite
// https://go.temporal.io/server/issues/2877
/*
FAIL: TestPostgreSQLQueuePersistence/TestNamespaceReplicationQueue (0.26s)
        queuePersistenceTest.go:102:
            	Error Trace:	queuePersistenceTest.go:102
            	Error:      	Not equal:
            	            	expected: 99
            	            	actual  : 98
            	Test:       	TestPostgreSQLQueuePersistence/TestNamespaceReplicationQueue
*/
//func TestPostgreSQLQueuePersistence(t *testing.T) {
//	s := new(persistencetests.QueuePersistenceSuite)
//	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQLTestClusterOption())
//	s.TestBase.Setup()
//	suite.Run(t, s)
//}

func TestPostgreSQL12HistoryV2PersistenceSuite(t *testing.T) {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQL12TestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestPostgreSQL12MetadataPersistenceSuiteV2(t *testing.T) {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQL12TestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestPostgreSQL12ClusterMetadataPersistence(t *testing.T) {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQL12TestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

// SQL store tests

func TestPostgreSQLNamespaceSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLQueueMessageSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewQueueMessageSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLQueueMetadataSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewQueueMetadataSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLMatchingTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryShardSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryNodeSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryNodeSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryTreeSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryTreeSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryCurrentExecutionSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryCurrentExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryTransferTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryTransferTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryTimerTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryTimerTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryReplicationTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryReplicationTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryVisibilityTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryVisibilityTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryReplicationDLQTaskSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryReplicationDLQTaskSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionBufferSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionActivitySuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionTimerSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionSignalSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLVisibilitySuite(t *testing.T) {
	cfg := NewPostgreSQLConfig()
	SetupPostgreSQLDatabase(cfg)
	SetupPostgreSQLSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(cfg)
	}()

	s := sqltests.NewVisibilitySuite(t, store)
	suite.Run(t, s)
}

func TestPostgreSQLClosedConnectionError(t *testing.T) {
	testData, tearDown := setUpPostgreSQLTest(t)
	defer tearDown()

	s := newConnectionSuite(t, testData.Factory)
	suite.Run(t, s)
}
