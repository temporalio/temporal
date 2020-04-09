package mysql

import (
	"testing"

	"github.com/stretchr/testify/suite"

	pt "github.com/temporalio/temporal/common/persistence/persistence-tests"
)

func TestSQLHistoryV2PersistenceSuite(t *testing.T) {
	s := new(pt.HistoryV2PersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(GetTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLMatchingPersistenceSuite(t *testing.T) {
	s := new(pt.MatchingPersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(GetTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(pt.MetadataPersistenceSuiteV2)
	s.TestBase = pt.NewTestBaseWithSQL(GetTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLShardPersistenceSuite(t *testing.T) {
	s := new(pt.ShardPersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(GetTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLExecutionManagerSuite(t *testing.T) {
	s := new(pt.ExecutionManagerSuite)
	s.TestBase = pt.NewTestBaseWithSQL(GetTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLExecutionManagerWithEventsV2(t *testing.T) {
	s := new(pt.ExecutionManagerSuiteForEventsV2)
	s.TestBase = pt.NewTestBaseWithSQL(GetTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLVisibilityPersistenceSuite(t *testing.T) {
	s := new(pt.VisibilityPersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(GetTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLQueuePersistence(t *testing.T) {
	s := new(pt.QueuePersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(GetTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestClusterMetadataPersistence(t *testing.T) {
	s := new(pt.ClusterMetadataManagerSuite)
	s.TestBase = pt.NewTestBaseWithSQL(GetTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}
