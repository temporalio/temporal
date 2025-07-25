package tests

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql" // register plugins
	sqltests "go.temporal.io/server/common/persistence/sql/sqlplugin/tests"
	"go.temporal.io/server/common/resolver"
)

type PostgreSQLSuite struct {
	suite.Suite
	pluginName string
}

func (p *PostgreSQLSuite) TestPostgreSQLShardStoreSuite() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewShardSuite(
		p.T(),
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLExecutionMutableStateStoreSuite() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewExecutionMutableStateSuite(
		p.T(),
		shardStore,
		executionStore,
		serialization.NewSerializer(),
		&persistence.HistoryBranchUtilImpl{},
		testData.Logger,
	)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLExecutionMutableStateTaskStoreSuite() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
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

func (p *PostgreSQLSuite) TestPostgreSQLHistoryStoreSuite() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewHistoryEventsSuite(p.T(), store, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLTaskQueueSuite() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewTaskQueueSuite(p.T(), taskQueueStore, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLFairTaskQueueSuite() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewFairTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewTaskQueueSuite(p.T(), taskQueueStore, testData.Logger) // same suite, different store
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLTaskQueueTaskSuite() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewTaskQueueTaskSuite(p.T(), taskQueueStore, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLTaskQueueFairTaskSuite() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewFairTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewTaskQueueFairTaskSuite(p.T(), taskQueueStore, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLTaskQueueUserDataSuite() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}

	s := NewTaskQueueUserDataSuite(p.T(), taskQueueStore, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLVisibilityPersistenceSuite() {
	s := &VisibilityPersistenceSuite{
		TestBase: persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQLTestClusterOption()),
	}
	suite.Run(p.T(), s)
}

// TODO: Merge persistence-tests into the tests directory.

func (p *PostgreSQLSuite) TestPostgreSQLHistoryV2PersistenceSuite() {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLMetadataPersistenceSuiteV2() {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLClusterMetadataPersistence() {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLQueuePersistence() {
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetPostgreSQLTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(p.T(), s)
}

// SQL store tests

func (p *PostgreSQLSuite) TestPostgreSQLNamespaceSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewNamespaceSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLQueueMessageSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewQueueMessageSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLQueueMetadataSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewQueueMetadataSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLMatchingTaskSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewMatchingTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLMatchingTaskV2Suite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewMatchingTaskV2Suite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLMatchingTaskQueueSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewMatchingTaskQueueSuite(p.T(), store, sqlplugin.MatchingTaskVersion1)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLMatchingFairTaskQueueSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create PostgreSQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewMatchingTaskQueueSuite(p.T(), store, sqlplugin.MatchingTaskVersion2)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryShardSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryShardSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryNodeSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryNodeSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryTreeSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryTreeSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryCurrentExecutionSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryCurrentExecutionSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryExecutionSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryTransferTaskSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryTransferTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryTimerTaskSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryTimerTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryReplicationTaskSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryReplicationTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryVisibilityTaskSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryVisibilityTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryReplicationDLQTaskSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryReplicationDLQTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryExecutionBufferSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionBufferSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryExecutionActivitySuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionActivitySuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryExecutionChildWorkflowSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionChildWorkflowSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryExecutionTimerSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionTimerSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgresHistoryExecutionChasmSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)

	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create Postgres DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionChasmSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryExecutionRequestCancelSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionRequestCancelSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryExecutionSignalSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLHistoryExecutionSignalRequestSuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalRequestSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLVisibilitySuite() {
	cfg := NewPostgreSQLConfig(p.pluginName)
	SetupPostgreSQLDatabase(p.T(), cfg)
	SetupPostgreSQLSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownPostgreSQLDatabase(p.T(), cfg)
	}()

	s := sqltests.NewVisibilitySuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPostgreSQLClosedConnectionError() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	defer tearDown()

	s := newConnectionSuite(p.T(), testData.Factory)
	suite.Run(p.T(), s)
}

func (p *PostgreSQLSuite) TestPGQueueV2() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	p.T().Cleanup(tearDown)
	RunQueueV2TestSuiteForSQL(p.T(), testData.Factory)
}

func (p *PostgreSQLSuite) TestPostgreSQLNexusEndpointPersistence() {
	testData, tearDown := setUpPostgreSQLTest(p.T(), p.pluginName)
	p.T().Cleanup(tearDown)
	RunNexusEndpointTestSuiteForSQL(p.T(), testData.Factory)
}

func TestPQ(t *testing.T) {
	s := &PostgreSQLSuite{pluginName: "postgres12"}
	suite.Run(t, s)
}

func TestPGX(t *testing.T) {
	s := &PostgreSQLSuite{pluginName: "postgres12_pgx"}
	suite.Run(t, s)
}
