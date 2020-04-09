package postgres

import (
	"testing"

	"github.com/stretchr/testify/suite"

	pt "github.com/temporalio/temporal/common/persistence/persistence-tests"
	"github.com/temporalio/temporal/environment"
)

const (
	testUser      = "postgres"
	testPassword  = "temporal"
	testSchemaDir = "schema/postgres"
)

func getTestClusterOption() *pt.TestBaseOptions {
	return &pt.TestBaseOptions{
		SQLDBPluginName: PluginName,
		DBUsername:      testUser,
		DBPassword:      testPassword,
		DBHost:          environment.GetPostgresAddress(),
		DBPort:          environment.GetPostgresPort(),
		SchemaDir:       testSchemaDir,
	}
}

func TestSQLHistoryV2PersistenceSuite(t *testing.T) {
	s := new(pt.HistoryV2PersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLMatchingPersistenceSuite(t *testing.T) {
	s := new(pt.MatchingPersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLMetadataPersistenceSuiteV2(t *testing.T) {
	s := new(pt.MetadataPersistenceSuiteV2)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLShardPersistenceSuite(t *testing.T) {
	s := new(pt.ShardPersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLExecutionManagerSuite(t *testing.T) {
	s := new(pt.ExecutionManagerSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLExecutionManagerWithEventsV2(t *testing.T) {
	s := new(pt.ExecutionManagerSuiteForEventsV2)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestSQLVisibilityPersistenceSuite(t *testing.T) {
	s := new(pt.VisibilityPersistenceSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestClusterMetadataPersistence(t *testing.T) {
	s := new(pt.ClusterMetadataManagerSuite)
	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
	s.TestBase.Setup()
	suite.Run(t, s)
}

// TODO flaky test in buildkite
// https://github.com/temporalio/temporal/issues/2877
/*
FAIL: TestSQLQueuePersistence/TestNamespaceReplicationQueue (0.26s)
        queuePersistenceTest.go:102:
            	Error Trace:	queuePersistenceTest.go:102
            	Error:      	Not equal:
            	            	expected: 99
            	            	actual  : 98
            	Test:       	TestSQLQueuePersistence/TestNamespaceReplicationQueue
*/
//func TestSQLQueuePersistence(t *testing.T) {
//	s := new(pt.QueuePersistenceSuite)
//	s.TestBase = pt.NewTestBaseWithSQL(getTestClusterOption())
//	s.TestBase.Setup()
//	suite.Run(t, s)
//}
