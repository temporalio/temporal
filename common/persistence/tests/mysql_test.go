package tests

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	sqltests "go.temporal.io/server/common/persistence/sql/sqlplugin/tests"
	"go.temporal.io/server/common/resolver"
)

func TestMySQLShardStoreSuite(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}

	s := NewShardSuite(
		t,
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestMySQLExecutionMutableStateStoreSuite(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
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

func TestMySQLExecutionMutableStateTaskStoreSuite(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
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

func TestMySQLHistoryStoreSuite(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}

	s := NewHistoryEventsSuite(t, store, testData.Logger)
	suite.Run(t, s)
}

func TestMySQLTaskQueueSuite(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		testData.Factory.Close()
		TearDownMySQLDatabase(t, testData.Cfg)
	}()

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestMySQLFairTaskQueueSuite(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewFairTaskStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		testData.Factory.Close()
		TearDownMySQLDatabase(t, testData.Cfg)
	}()

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger) // same suite, different store
	suite.Run(t, s)
}

func TestMySQLTaskQueueTaskSuite(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}

	s := NewTaskQueueTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestMySQLTaskQueueFairTaskSuite(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewFairTaskStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}

	s := NewTaskQueueFairTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestMySQLTaskQueueUserDataSuite(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}

	s := NewTaskQueueUserDataSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestMySQLVisibilityPersistenceSuite(t *testing.T) {
	s := &VisibilityPersistenceSuite{
		TestBase: persistencetests.NewTestBaseWithSQL(persistencetests.GetMySQLTestClusterOption()),
	}
	suite.Run(t, s)
}

// TODO: Merge persistence-tests into the tests directory.

func TestMySQLHistoryV2PersistenceSuite(t *testing.T) {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetMySQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestMySQLMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetMySQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestMySQLQueuePersistence(t *testing.T) {
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetMySQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

func TestMySQLClusterMetadataPersistence(t *testing.T) {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetMySQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(t, s)
}

// SQL Store tests

func TestMySQLNamespaceSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewNamespaceSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLQueueMessageSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewQueueMessageSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLQueueMetadataSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewQueueMetadataSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLMatchingTaskSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewMatchingTaskSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLMatchingTaskV2Suite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewMatchingTaskV2Suite(t, store)
	suite.Run(t, s)
}

func TestMySQLMatchingTaskQueueSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewMatchingTaskQueueSuite(t, store, sqlplugin.MatchingTaskVersion1)
	suite.Run(t, s)
}

func TestMySQLMatchingFairTaskQueueSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewMatchingTaskQueueSuite(t, store, sqlplugin.MatchingTaskVersion2)
	suite.Run(t, s)
}

func TestMySQLHistoryShardSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryShardSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryNodeSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryNodeSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryTreeSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryTreeSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryCurrentExecutionSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryCurrentExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryTransferTaskSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryTransferTaskSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryTimerTaskSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryTimerTaskSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryReplicationTaskSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryReplicationTaskSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryVisibilityTaskSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryVisibilityTaskSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryReplicationDLQTaskSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryReplicationDLQTaskSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionBufferSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionBufferSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionActivitySuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionActivitySuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionChildWorkflowSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionChildWorkflowSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionTimerSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionTimerSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionChasmSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionChasmSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionRequestCancelSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionRequestCancelSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionSignalSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLHistoryExecutionSignalRequestSuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalRequestSuite(t, store)
	suite.Run(t, s)
}

func TestMySQLVisibilitySuite(t *testing.T) {
	cfg := NewMySQLConfig()
	SetupMySQLDatabase(t, cfg)
	SetupMySQLSchema(t, cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMySQLDatabase(t, cfg)
	}()

	s := sqltests.NewVisibilitySuite(t, store)
	suite.Run(t, s)
}

func TestMySQLClosedConnectionError(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	s := newConnectionSuite(t, testData.Factory)
	suite.Run(t, s)
}

func TestMySQLQueueV2(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	t.Cleanup(tearDown)
	RunQueueV2TestSuiteForSQL(t, testData.Factory)
}

func TestMySQLNexusEndpointPersistence(t *testing.T) {
	testData, tearDown := setUpMySQLTest(t)
	defer tearDown()

	store, err := testData.Factory.NewNexusEndpointStore()
	if err != nil {
		t.Fatalf("unable to create MySQL NexusEndpointStore: %v", err)
	}

	tableVersion := atomic.Int64{}
	t.Run("Generic", func(t *testing.T) {
		RunNexusEndpointTestSuite(t, store, &tableVersion)
	})
}
