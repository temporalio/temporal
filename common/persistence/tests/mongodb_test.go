package tests

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
)

type mongoPersistenceTestCluster struct {
	cfg *config.MongoDB
}

func newMongoPersistenceTestCluster(cfg *config.MongoDB) *mongoPersistenceTestCluster {
	return &mongoPersistenceTestCluster{cfg: cfg}
}

func (c *mongoPersistenceTestCluster) SetupTestDatabase()    {}
func (c *mongoPersistenceTestCluster) TearDownTestDatabase() {}
func (c *mongoPersistenceTestCluster) Config() config.Persistence {
	return config.Persistence{
		DefaultStore:    "test-mongodb",
		VisibilityStore: "test-mongodb",
		DataStores: map[string]config.DataStore{
			"test-mongodb": {
				MongoDB: c.cfg,
			},
		},
	}
}

func TestMongoDBShardStoreSuite(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB ShardStore: %v", err)
	}

	s := NewShardSuite(
		t,
		shardStore,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestMongoDBExecutionMutableStateStoreSuite(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB ShardStore: %v", err)
	}
	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB ExecutionStore: %v", err)
	}

	s := NewExecutionMutableStateSuite(
		t,
		shardStore,
		store,
		serialization.NewSerializer(),
		&persistence.HistoryBranchUtilImpl{},
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestMongoDBExecutionMutableStateTaskStoreSuite(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	shardStore, err := testData.Factory.NewShardStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB ShardStore: %v", err)
	}
	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB ExecutionStore: %v", err)
	}

	s := NewExecutionMutableStateTaskSuite(
		t,
		shardStore,
		store,
		serialization.NewSerializer(),
		testData.Logger,
	)
	suite.Run(t, s)
}

func TestMongoDBHistoryStoreSuite(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	store, err := testData.Factory.NewExecutionStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB ExecutionStore: %v", err)
	}

	s := NewHistoryEventsSuite(t, store, testData.Logger)
	suite.Run(t, s)
}

func TestMongoDBTaskQueueSuite(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB TaskStore: %v", err)
	}

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestMongoDBFairTaskQueueSuite(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	taskQueueStore, err := testData.Factory.NewFairTaskStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB FairTaskStore: %v", err)
	}

	s := NewTaskQueueSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestMongoDBTaskQueueTaskSuite(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB TaskStore: %v", err)
	}

	s := NewTaskQueueTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestMongoDBTaskQueueFairTaskSuite(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	taskQueueStore, err := testData.Factory.NewFairTaskStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB FairTaskStore: %v", err)
	}

	s := NewTaskQueueFairTaskSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestMongoDBTaskQueueUserDataSuite(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	taskQueueStore, err := testData.Factory.NewTaskStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB TaskStore: %v", err)
	}

	s := NewTaskQueueUserDataSuite(t, taskQueueStore, testData.Logger)
	suite.Run(t, s)
}

func TestMongoDBQueueV2(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	q, err := testData.Factory.NewQueueV2()
	if err != nil {
		t.Fatalf("unable to create MongoDB QueueV2: %v", err)
	}

	RunQueueV2TestSuite(t, q)
}

func TestMongoDBNexusEndpointPersistence(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	store, err := testData.Factory.NewNexusEndpointStore()
	if err != nil {
		t.Fatalf("unable to create MongoDB NexusEndpointStore: %v", err)
	}

	tableVersion := atomic.Int64{}
	RunNexusEndpointTestSuite(t, store, &tableVersion)
}

func TestMongoDBVisibilityPersistenceSuite(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	testCluster := newMongoPersistenceTestCluster(testData.Cfg)

	s := &VisibilityPersistenceSuite{
		TestBase:                     persistencetests.NewTestBaseForCluster(testCluster, testData.Logger),
		CustomVisibilityStoreFactory: testData.Factory,
	}
	suite.Run(t, s)
}

func TestMongoDBMetadataPersistenceSuiteV2(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	testCluster := newMongoPersistenceTestCluster(testData.Cfg)

	s := new(persistencetests.MetadataPersistenceSuiteV2)
	s.TestBase = persistencetests.NewTestBaseForCluster(testCluster, testData.Logger)
	s.Setup(nil)

	suite.Run(t, s)
}

func TestMongoDBClusterMetadataPersistence(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	testCluster := newMongoPersistenceTestCluster(testData.Cfg)

	s := new(persistencetests.ClusterMetadataManagerSuite)
	s.TestBase = persistencetests.NewTestBaseForCluster(testCluster, testData.Logger)
	s.Setup(nil)

	suite.Run(t, s)
}

func TestMongoDBQueuePersistence(t *testing.T) {
	testData, tearDown := setUpMongoDBTest(t)
	t.Cleanup(tearDown)

	testCluster := newMongoPersistenceTestCluster(testData.Cfg)

	s := new(persistencetests.QueuePersistenceSuite)
	s.TestBase = persistencetests.NewTestBaseForCluster(testCluster, testData.Logger)
	s.Setup(nil)

	suite.Run(t, s)
}
