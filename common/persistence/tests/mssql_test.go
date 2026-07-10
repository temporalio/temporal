package tests

import (
	"math"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/mssql" // register plugins
	sqltests "go.temporal.io/server/common/persistence/sql/sqlplugin/tests"
	"go.temporal.io/server/common/resolver"
)

type MSSQLSuite struct {
	suite.Suite
	pluginName   string
	connectAttrs map[string]string
}

func (p *MSSQLSuite) TestMSSQLShardStoreSuite() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}

	s := NewShardSuite(
		p.T(),
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLExecutionMutableStateStoreSuite() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}

	s := NewExecutionMutableStateSuite(
		p.T(),
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLExecutionMutableStateTaskStoreSuite() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}

	s := NewExecutionMutableStateTaskSuite(
		p.T(),
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryStoreSuite() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}

	s := NewHistoryEventsSuite(p.T(), store, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLTaskQueueSuite() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}

	s := NewTaskQueueSuite(p.T(), taskQueueStore, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLFairTaskQueueSuite() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewFairTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}

	s := NewTaskQueueSuite(p.T(), taskQueueStore, testData.Logger) // same suite, different store
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLTaskQueueTaskSuite() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}

	s := NewTaskQueueTaskSuite(p.T(), taskQueueStore, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLTaskQueueFairTaskSuite() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewFairTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}

	s := NewTaskQueueFairTaskSuite(p.T(), taskQueueStore, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLTaskQueueUserDataSuite() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}

	s := NewTaskQueueUserDataSuite(p.T(), taskQueueStore, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLVisibilityPersistenceSuite() {
	s := &VisibilityPersistenceSuite{
		TestBase: persistencetests.NewTestBaseWithSQL(
			persistencetests.GetMSSQLTestClusterOption(p.pluginName, p.connectAttrs),
		),
	}
	suite.Run(p.T(), s)
}

// TODO: Merge persistence-tests into the tests directory.

func (p *MSSQLSuite) TestMSSQLHistoryV2PersistenceSuite() {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(
		persistencetests.GetMSSQLTestClusterOption(p.pluginName, p.connectAttrs),
	)
	s.Setup(nil)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLMetadataPersistenceSuiteV2() {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithSQL(
		persistencetests.GetMSSQLTestClusterOption(p.pluginName, p.connectAttrs),
	)
	s.Setup(nil)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLClusterMetadataPersistence() {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(
		persistencetests.GetMSSQLTestClusterOption(p.pluginName, p.connectAttrs),
	)
	s.Setup(nil)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLQueuePersistence() {
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(
		persistencetests.GetMSSQLTestClusterOption(p.pluginName, p.connectAttrs),
	)
	s.Setup(nil)
	suite.Run(p.T(), s)
}

// SQL store tests

func (p *MSSQLSuite) TestMSSQLNamespaceSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewNamespaceSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLQueueMessageSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewQueueMessageSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLQueueMetadataSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewQueueMetadataSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLMatchingTaskSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewMatchingTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLMatchingTaskV2Suite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewMatchingTaskV2Suite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLMatchingTaskQueueSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewMatchingTaskQueueSuite(p.T(), store, sqlplugin.MatchingTaskVersion1)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLMatchingFairTaskQueueSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewMatchingTaskQueueSuite(p.T(), store, sqlplugin.MatchingTaskVersion2)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryShardSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryShardSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryNodeSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryNodeSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryTreeSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryTreeSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryCurrentExecutionSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryCurrentExecutionSuite(p.T(), store, chasm.WorkflowArchetypeID)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryCurrentChasmExecutionSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryCurrentExecutionSuite(p.T(), store, math.MaxUint32)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryExecutionSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryTransferTaskSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryTransferTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryTimerTaskSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryTimerTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryReplicationTaskSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryReplicationTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryVisibilityTaskSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryVisibilityTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryReplicationDLQTaskSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryReplicationDLQTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryExecutionBufferSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionBufferSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryExecutionActivitySuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionActivitySuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryExecutionChildWorkflowSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionChildWorkflowSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryExecutionTimerSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionTimerSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryExecutionChasmSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)

	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionChasmSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryExecutionRequestCancelSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionRequestCancelSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryExecutionSignalSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLHistoryExecutionSignalRequestSuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalRequestSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLVisibilitySuite() {
	cfg := NewMSSQLConfig(p.pluginName, p.connectAttrs)
	SetupMSSQLDatabase(p.T(), cfg)
	SetupMSSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MSSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownMSSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewVisibilitySuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestMSSQLClosedConnectionError() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	defer tearDown()

	s := newConnectionSuite(p.T(), testData.Factory)
	suite.Run(p.T(), s)
}

func (p *MSSQLSuite) TestPGQueueV2() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	p.T().Cleanup(tearDown)
	RunQueueV2TestSuiteForSQL(p.T(), testData.Factory)
}

func (p *MSSQLSuite) TestMSSQLNexusEndpointPersistence() {
	testData, tearDown := setUpMSSQLTest(p.T(), p.pluginName, p.connectAttrs)
	p.T().Cleanup(tearDown)
	RunNexusEndpointTestSuiteForSQL(p.T(), testData.Factory)
}

func TestMSSQL(t *testing.T) {
	t.Parallel()
	s := &MSSQLSuite{pluginName: "mssql2019"}
	suite.Run(t, s)
}
