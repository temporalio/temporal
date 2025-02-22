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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	sqltests "go.temporal.io/server/common/persistence/sql/sqlplugin/tests"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/tidb"
	"go.temporal.io/server/common/resolver"
)

func TestTiDBShardStoreSuite(t *testing.T) {
	testData, tearDown := setUpTiDBTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}

	s := NewShardSuite(
		t,
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestTiDBExecutionMutableStateStoreSuite(t *testing.T) {
	testData, tearDown := setUpTiDBTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}

	s := NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		&persistence.HistoryBranchUtilImpl{},
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestTiDBExecutionMutableStateTaskStoreSuite(t *testing.T) {
	testData, tearDown := setUpTiDBTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
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

func TestTiDBHistoryStoreSuite(t *testing.T) {
	testData, tearDown := setUpTiDBTest(t)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}

	s := NewHistoryEventsSuite(t, store, testData.Logger)
	suite.Run(t, s)
}

func TestTiDBTaskQueueSuite(t *testing.T) {
	testData, tearDown := setUpTiDBTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		testData.Factory.Close()
		TearDownTiDBDatabase(testData.Cfg)
	}()

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestTiDBTaskQueueTaskSuite(t *testing.T) {
	testData, tearDown := setUpTiDBTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}

	s := NewTaskQueueTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestTiDBVisibilityPersistenceSuite(t *testing.T) {
	s := &VisibilityPersistenceSuite{
		TestBase: persistencetests.NewTestBaseWithSQL(persistencetests.GetTiDBTestClusterOption()),
	}
	suite.Run(t, s)
}

// TODO: Merge persistence-tests into the tests directory.

func TestTiDBHistoryV2PersistenceSuite(t *testing.T) {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetTiDBTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestTiDBMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetTiDBTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestTiDBQueuePersistence(t *testing.T) {
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetTiDBTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestTiDBClusterMetadataPersistence(t *testing.T) {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetTiDBTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

// SQL Store tests

func TestTiDBNamespaceSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBQueueMessageSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewQueueMessageSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBQueueMetadataSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewQueueMetadataSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBMatchingTaskSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryShardSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryNodeSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryNodeSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryTreeSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryTreeSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryCurrentExecutionSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryCurrentExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryExecutionSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryTransferTaskSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryTransferTaskSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryTimerTaskSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryTimerTaskSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryReplicationTaskSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryReplicationTaskSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryVisibilityTaskSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryVisibilityTaskSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryReplicationDLQTaskSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryReplicationDLQTaskSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryExecutionBufferSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryExecutionActivitySuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryExecutionTimerSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryExecutionSignalSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

func TestTiDBVisibilitySuite(t *testing.T) {
	cfg := NewTiDBConfig()
	SetupTiDBDatabase(cfg)
	SetupTiDBSchema(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver())
	if err != nil {
		t.Fatalf("unable to create TiDB DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownTiDBDatabase(cfg)
	}()

	s := sqltests.NewVisibilitySuite(t, store)
	suite.Run(t, s)
}

func TestTiDBClosedConnectionError(t *testing.T) {
	testData, tearDown := setUpTiDBTest(t)
	defer tearDown()

	s := newConnectionSuite(t, testData.Factory)
	suite.Run(t, s)
}

func TestTiDBQueueV2(t *testing.T) {
	testData, tearDown := setUpTiDBTest(t)
	t.Cleanup(tearDown)
	RunQueueV2TestSuiteForSQL(t, testData.Factory)
}

func TestTiDBNexusIncomingServicePersistence(t *testing.T) {
	testData, tearDown := setUpTiDBTest(t)
	defer tearDown()

	store, err := testData.Factory.NewNexusIncomingServiceStore()
	if err != nil {
		t.Fatalf("unable to create TiDB NexusIncomingServiceStore: %v", err)
	}

	tableVersion := atomic.Int64{}
	t.Run("Generic", func(t *testing.T) {
		RunNexusIncomingServiceTestSuite(t, store, &tableVersion)
	})
}
