package persistencetests

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestCassandraHistoryV2Persistence(t *testing.T) {
	s := new(HistoryV2PersistenceSuite)
	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCassandraMatchingPersistence(t *testing.T) {
	s := new(MatchingPersistenceSuite)
	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCassandraMetadataPersistenceV2(t *testing.T) {
	s := new(MetadataPersistenceSuiteV2)
	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCassandraShardPersistence(t *testing.T) {
	s := new(ShardPersistenceSuite)
	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCassandraVisibilityPersistence(t *testing.T) {
	s := new(VisibilityPersistenceSuite)
	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCassandraExecutionManager(t *testing.T) {
	s := new(ExecutionManagerSuite)
	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestCassandraExecutionManagerWithEventsV2(t *testing.T) {
	s := new(ExecutionManagerSuiteForEventsV2)
	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestQueuePersistence(t *testing.T) {
	s := new(QueuePersistenceSuite)
	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}

func TestClusterMetadataPersistence(t *testing.T) {
	s := new(ClusterMetadataManagerSuite)
	s.TestBase = NewTestBaseWithCassandra(&TestBaseOptions{})
	s.TestBase.Setup()
	suite.Run(t, s)
}
