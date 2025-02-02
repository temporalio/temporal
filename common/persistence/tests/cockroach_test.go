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
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	p "go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/cockroach"
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/cockroach" // register plugins
	sqltests "go.temporal.io/server/common/persistence/sql/sqlplugin/tests"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/environment"
	"go.uber.org/zap/zaptest"
)

type CockroachSuite struct {
	suite.Suite
	pluginName string
}

// TODO merge the initialization with existing persistence setup
const (
	testCockroachClusterName = "temporal_cockroach_cluster"

	testCockroachUser               = "temporal"
	testCockroachPassword           = "temporal"
	testCockroachConnectionProtocol = "tcp"
	testCockroachDatabaseNamePrefix = "test_"
	testCockroachDatabaseNameSuffix = "temporal_persistence"

	// TODO hard code this dir for now
	//  need to merge persistence test config / initialization in one place
	testCockroachExecutionSchema  = "../../../schema/cockroach/temporal/schema.sql"
	testCockroachVisibilitySchema = "../../../schema/cockroach/visibility/schema.sql"
)

type (
	CockroachTestData struct {
		Cfg     *config.SQL
		Factory *sql.Factory
		Logger  log.Logger
		Metrics *metricstest.Capture
	}
)

func setUpCockroachTest(t *testing.T, pluginName string) (CockroachTestData, func()) {
	var testData CockroachTestData
	testData.Cfg = NewCockroachConfig(pluginName)
	testData.Logger = log.NewZapLogger(zaptest.NewLogger(t))
	mh := metricstest.NewCaptureHandler()
	testData.Metrics = mh.StartCapture()
	SetupCockroachDatabase(t, testData.Cfg)
	SetupCockroachSchema(t, testData.Cfg)

	testData.Factory = sql.NewFactory(
		*testData.Cfg,
		resolver.NewNoopResolver(),
		testCockroachClusterName,
		testData.Logger,
		mh,
	)

	tearDown := func() {
		testData.Factory.Close()
		mh.StopCapture(testData.Metrics)
		TearDownCockroachDatabase(t, testData.Cfg)
	}

	return testData, tearDown
}

func (p *CockroachSuite) TestCockroachShardStoreSuite() {
	testData, tearDown := setUpCockroachTest(p.T(), p.pluginName)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}

	s := NewShardSuite(
		p.T(),
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachExecutionMutableStateStoreSuite() {
	testData, tearDown := setUpCockroachTest(p.T(), p.pluginName)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
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

func (p *CockroachSuite) TestCockroachExecutionMutableStateTaskStoreSuite() {
	testData, tearDown := setUpCockroachTest(p.T(), p.pluginName)
	defer tearDown()

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}
	executionStore, err := testData.Factory.NewExecutionStore()
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
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

func (p *CockroachSuite) TestCockroachHistoryStoreSuite() {
	testData, tearDown := setUpCockroachTest(p.T(), p.pluginName)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}

	s := NewHistoryEventsSuite(p.T(), store, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachTaskQueueSuite() {
	testData, tearDown := setUpCockroachTest(p.T(), p.pluginName)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}

	s := NewTaskQueueSuite(p.T(), taskQueueStore, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachTaskQueueTaskSuite() {
	testData, tearDown := setUpCockroachTest(p.T(), p.pluginName)
	defer tearDown()

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}

	s := NewTaskQueueTaskSuite(p.T(), taskQueueStore, testData.Logger)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachVisibilityPersistenceSuite() {
	s := &VisibilityPersistenceSuite{
		TestBase: persistencetests.NewTestBaseWithSQL(persistencetests.GetCockroachTestClusterOption()),
	}
	suite.Run(p.T(), s)
}

// TODO: Merge persistence-tests into the tests directory.

func (p *CockroachSuite) TestCockroachHistoryV2PersistenceSuite() {
	s := new(persistencetests.HistoryV2PersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetCockroachTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachMetadataPersistenceSuiteV2() {
	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetCockroachTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachClusterMetadataPersistence() {
	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetCockroachTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachQueuePersistence() {
	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseWithSQL(persistencetests.GetCockroachTestClusterOption())
	s.TestBase.Setup(nil)
	suite.Run(p.T(), s)
}

// SQL store tests

func (p *CockroachSuite) TestCockroachNamespaceSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewNamespaceSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachQueueMessageSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewQueueMessageSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachQueueMetadataSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewQueueMetadataSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachMatchingTaskSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewMatchingTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachMatchingTaskQueueSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewMatchingTaskQueueSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryShardSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryShardSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryNodeSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryNodeSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryTreeSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryTreeSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryCurrentExecutionSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create Cockroach DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryCurrentExecutionSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryExecutionSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryTransferTaskSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryTransferTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryTimerTaskSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryTimerTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryReplicationTaskSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryReplicationTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryVisibilityTaskSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryVisibilityTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryReplicationDLQTaskSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryReplicationDLQTaskSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryExecutionBufferSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionBufferSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryExecutionActivitySuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionActivitySuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryExecutionChildWorkflowSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionChildWorkflowSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryExecutionTimerSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionTimerSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryExecutionRequestCancelSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionRequestCancelSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryExecutionSignalSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachHistoryExecutionSignalRequestSuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindMain, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewHistoryExecutionSignalRequestSuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachVisibilitySuite() {
	cfg := NewCockroachConfig(p.pluginName)
	SetupCockroachDatabase(p.T(), cfg)
	SetupCockroachSchema(p.T(), cfg)
	store, err := sql.NewSQLDB(sqlplugin.DbKindVisibility, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		p.T().Fatalf("unable to create MySQL DB: %v", err)
	}
	defer func() {
		_ = store.Close()
		TearDownCockroachDatabase(p.T(), cfg)
	}()

	s := sqltests.NewVisibilitySuite(p.T(), store)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestCockroachClosedConnectionError() {
	testData, tearDown := setUpCockroachTest(p.T(), p.pluginName)
	defer tearDown()

	s := newConnectionSuite(p.T(), testData.Factory)
	suite.Run(p.T(), s)
}

func (p *CockroachSuite) TestQueueV2() {
	testData, tearDown := setUpCockroachTest(p.T(), p.pluginName)
	p.T().Cleanup(tearDown)
	RunQueueV2TestSuiteForSQL(p.T(), testData.Factory)
}

func (p *CockroachSuite) TestCockroachNexusEndpointPersistence() {
	testData, tearDown := setUpCockroachTest(p.T(), p.pluginName)
	p.T().Cleanup(tearDown)
	RunNexusEndpointTestSuiteForSQL(p.T(), testData.Factory)
}

func TestCockroach(t *testing.T) {
	s := &CockroachSuite{pluginName: cockroach.PluginName}
	suite.Run(t, s)
}

// NewCockroachConfig returns a new Cockroach config for test
func NewCockroachConfig(pluginName string) *config.SQL {
	return &config.SQL{
		User:     testCockroachUser,
		Password: testCockroachPassword,
		ConnectAddr: net.JoinHostPort(
			environment.GetCockroachAddress(),
			strconv.Itoa(environment.GetCockroachPort()),
		),
		ConnectProtocol: testCockroachConnectionProtocol,
		PluginName:      pluginName,
		DatabaseName:    testCockroachDatabaseNamePrefix + shuffle.String(testCockroachDatabaseNameSuffix),
	}
}

func SetupCockroachDatabase(t *testing.T, cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Cockroach admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = db.CreateDatabase(cfg.DatabaseName)
	if err != nil {
		t.Fatalf("unable to create Cockroach database: %v", err)
	}
}

func SetupCockroachSchema(t *testing.T, cfg *config.SQL) {
	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, cfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Cockroach admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	schemaPath, err := filepath.Abs(testCockroachExecutionSchema)
	if err != nil {
		t.Fatal(err)
	}

	statements, err := p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		t.Fatal(err)
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}

	schemaPath, err = filepath.Abs(testCockroachVisibilitySchema)
	if err != nil {
		t.Fatal(err)
	}

	statements, err = p.LoadAndSplitQuery([]string{schemaPath})
	if err != nil {
		t.Fatal(err)
	}

	for _, stmt := range statements {
		if err = db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
}

func TearDownCockroachDatabase(t *testing.T, cfg *config.SQL) {
	adminCfg := *cfg
	// NOTE need to connect with empty name to create new database
	adminCfg.DatabaseName = ""

	db, err := sql.NewSQLAdminDB(sqlplugin.DbKindUnknown, &adminCfg, resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	if err != nil {
		t.Fatalf("unable to create Cockroach admin DB: %v", err)
	}
	defer func() { _ = db.Close() }()

	err = db.DropDatabase(cfg.DatabaseName)
	if err != nil {
		t.Fatalf("unable to drop Cockroach database: %v", err)
	}
}
