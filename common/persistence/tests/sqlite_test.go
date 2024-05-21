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
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	sqltests "go.temporal.io/server/common/persistence/sql/sqlplugin/tests"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/environment"
)

// TODO merge the initialization with existing persistence setup
const (
	testSQLiteClusterName = "temporal_sqlite_cluster"
	testSQLiteSchemaDir   = "../../../schema/sqlite/v3" // specify if mode is not "memory"
)

// NewSQLiteMemoryConfig returns a new SQLite config for test
func NewSQLiteMemoryConfig() *config.SQL {
	return &config.SQL{
		User:              "",
		Password:          "",
		ConnectAddr:       environment.GetLocalhostIP(),
		ConnectProtocol:   "tcp",
		PluginName:        "sqlite",
		DatabaseName:      "default",
		ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
	}
}

// NewSQLiteMemoryConfig returns a new SQLite config for test
func NewSQLiteFileConfig() *config.SQL {
	return &config.SQL{
		User:              "",
		Password:          "",
		ConnectAddr:       environment.GetLocalhostIP(),
		ConnectProtocol:   "tcp",
		PluginName:        "sqlite",
		DatabaseName:      "test_" + persistencetests.GenerateRandomDBName(3),
		ConnectAttributes: map[string]string{"cache": "private"},
	}
}

func SetupSQLiteDatabase(cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		panic(fmt.Sprintf("unable to create SQLite admin DB: %v", err))
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		panic(fmt.Sprintf("unable to create SQLite database: %v", err))
	}

	LoadSchema(db, path.Join(testSQLiteSchemaDir, "temporal", "schema.sql"))
	LoadSchema(db, path.Join(testSQLiteSchemaDir, "visibility", "schema.sql"))
}

func LoadSchema(db sqlplugin.AdminDB, schemaFile string) {
	statements, err := persistence.LoadAndSplitQuery([]string{schemaFile})
	if err != nil {
		panic(fmt.Sprintf("LoadSchema %+v", tag.Error(err)))
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			panic(fmt.Sprintf("LoadSchema %+v", tag.Error(err)))
		}
	}
}

func TestSQLiteExecutionMutableStateStoreSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	shardStore, err := factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		&persistence.HistoryBranchUtilImpl{},
		logger,
	)
	suite.Run(t, s)
}

func TestSQLiteExecutionMutableStateTaskStoreSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	shardStore, err := factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		logger,
	)
	suite.Run(t, s)
}

func TestSQLiteHistoryStoreSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	store, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewHistoryEventsSuite(t, store, logger)
	suite.Run(t, s)
}

func TestSQLiteTaskQueueSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	taskQueueStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewTaskQueueSuite(t, taskQueueStore, logger)
	suite.Run(t, s)
}

func TestSQLiteTaskQueueTaskSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	taskQueueStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewTaskQueueTaskSuite(t, taskQueueStore, logger)
	suite.Run(t, s)
}

func TestSQLiteFileExecutionMutableStateStoreSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	defer func() {
		assert.NoError(t, os.Remove(cfg.DatabaseName))
	}()
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	shardStore, err := factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewExecutionMutableStateSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		&persistence.HistoryBranchUtilImpl{},
		logger,
	)
	suite.Run(t, s)
}

func TestSQLiteFileExecutionMutableStateTaskStoreSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	defer func() {
		assert.NoError(t, os.Remove(cfg.DatabaseName))
	}()
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	shardStore, err := factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	executionStore, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		logger,
	)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryStoreSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	defer func() {
		assert.NoError(t, os.Remove(cfg.DatabaseName))
	}()
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	store, err := factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewHistoryEventsSuite(t, store, logger)
	suite.Run(t, s)
}

func TestSQLiteFileTaskQueueSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	defer func() {
		assert.NoError(t, os.Remove(cfg.DatabaseName))
	}()
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	taskQueueStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewTaskQueueSuite(t, taskQueueStore, logger)
	suite.Run(t, s)
}

func TestSQLiteFileTaskQueueTaskSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	defer func() {
		assert.NoError(t, os.Remove(cfg.DatabaseName))
	}()
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	taskQueueStore, err := factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		factory.Close()
	}()

	s := NewTaskQueueTaskSuite(t, taskQueueStore, logger)
	suite.Run(t, s)
}

// TODO: Merge persistence-tests into the tests directory.

func TestSQLiteVisibilityPersistenceSuite(t *testing.T) {
	s := new(VisibilityPersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetSQLiteMemoryTestClusterOption())
	suite.Run(t, s)
}

func TestSQLiteHistoryV2PersistenceSuite(t *testing.T) {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetSQLiteMemoryTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestSQLiteMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetSQLiteMemoryTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestSQLiteClusterMetadataPersistence(t *testing.T) {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetSQLiteMemoryTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestSQLiteQueuePersistence(t *testing.T) {
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetSQLiteMemoryTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryV2PersistenceSuite(t *testing.T) {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetSQLiteFileTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestSQLiteFileMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetSQLiteFileTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestSQLiteFileClusterMetadataPersistence(t *testing.T) {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetSQLiteFileTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestSQLiteFileQueuePersistence(t *testing.T) {
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetSQLiteFileTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

// SQL store tests

func TestSQLiteNamespaceSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteQueueMessageSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewQueueMessageSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteQueueMetadataSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewQueueMetadataSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteMatchingTaskSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryShardSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryNodeSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryNodeSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryTreeSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryTreeSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryCurrentExecutionSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryCurrentExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryTransferTaskSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryTransferTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryTimerTaskSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryTimerTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryReplicationTaskSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryReplicationTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryVisibilityTaskSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryVisibilityTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryReplicationDLQTaskSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryReplicationDLQTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionBufferSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionActivitySuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionTimerSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionSignalSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteVisibilitySuite(t *testing.T) {
	cfg := NewSQLiteMemoryConfig()
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	s := sqltests.NewVisibilitySuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileNamespaceSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileQueueMessageSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewQueueMessageSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileQueueMetadataSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewQueueMetadataSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileMatchingTaskSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewMatchingTaskQueueSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryShardSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryNodeSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryNodeSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryTreeSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryTreeSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryCurrentExecutionSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryCurrentExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryTransferTaskSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryTransferTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryTimerTaskSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryTimerTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryReplicationTaskSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryReplicationTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryVisibilityTaskSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryVisibilityTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryReplicationDLQTaskSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryReplicationDLQTaskSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionBufferSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionActivitySuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionTimerSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionSignalSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteFileVisibilitySuite(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create SQLite DB: %v", err)
	}
	defer os.Remove(cfg.DatabaseName)

	s := sqltests.NewVisibilitySuite(t, store)
	suite.Run(t, s)
}

func TestSQLiteQueueV2(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	t.Cleanup(func() {
		factory.Close()
		assert.NoError(t, os.Remove(cfg.DatabaseName))
	})
	RunQueueV2TestSuiteForSQL(t, factory)
}

func TestSQLiteNexusEndpointPersistence(t *testing.T) {
	cfg := NewSQLiteFileConfig()
	SetupSQLiteDatabase(cfg)
	logger := log.NewNoopLogger()
	factory := sql.NewFactory(
		*cfg,
		resolver.NewNoopResolver(),
		testSQLiteClusterName,
		logger,
		metrics.NoopMetricsHandler,
	)
	t.Cleanup(func() {
		factory.Close()
		assert.NoError(t, os.Remove(cfg.DatabaseName))
	})
	RunNexusEndpointTestSuiteForSQL(t, factory)
}
