package matching

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

type DynamicConfigSubscriptionTestSuite struct {
	suite.Suite
	controller     *gomock.Controller
	dcClient       *dynamicconfig.MemoryClient
	dcCollection   *dynamicconfig.Collection
	matchingClient *matchingservicemock.MockMatchingServiceClient
	engine         *matchingEngineImpl
	logger         log.Logger
}

func TestDynamicConfigSubscriptionSuite(t *testing.T) {
	suite.Run(t, new(DynamicConfigSubscriptionTestSuite))
}

func (s *DynamicConfigSubscriptionTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)

	// Create memory-based dynamic config client
	s.dcClient = dynamicconfig.NewMemoryClient()
	s.dcCollection = dynamicconfig.NewCollection(s.dcClient, s.logger)
	s.dcCollection.Start()

	// Set up mock matching client
	s.matchingClient = matchingservicemock.NewMockMatchingServiceClient(s.controller)
	s.matchingClient.EXPECT().ForceLoadTaskQueuePartition(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&matchingservice.ForceLoadTaskQueuePartitionResponse{}, nil).AnyTimes()

	// Create namespace cache
	_, registry := createMockNamespaceCache(s.controller, namespace.Name(namespaceName))

	// Create config with subscribable dynamic configs
	config := NewConfig(s.dcCollection)
	config.EnableMigration = dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(false)
	config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(100 * time.Millisecond)
	config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(1)

	// Create matching engine
	s.engine = createTestMatchingEngine(s.logger, s.controller, config, s.matchingClient, registry)
	s.engine.Start()
}

func (s *DynamicConfigSubscriptionTestSuite) TearDownTest() {
	s.engine.Stop()
	s.dcCollection.Stop()
	s.controller.Finish()
	// Allow async cleanup to complete
	time.Sleep(100 * time.Millisecond)
}

// createPartitionManagerWithFairnessState creates a partition manager with the specified fairness state
func (s *DynamicConfigSubscriptionTestSuite) createPartitionManagerWithFairnessState(
	fairnessState enumsspb.FairnessState,
) *taskQueuePartitionManagerImpl {
	// Use unique IDs to avoid test interference
	testNamespaceID := uuid.NewString()
	testTaskQueueName := "test-tq-" + uuid.NewString()

	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace", Id: testNamespaceID},
		nil,
		"",
	)

	f, err := tqid.NewTaskQueueFamily(testNamespaceID, testTaskQueueName)
	s.Require().NoError(err)
	partition := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition()

	tqConfig := newTaskQueueConfig(partition.TaskQueue(), s.engine.config, ns.Name())

	userData := &mockUserDataManager{
		data: &persistencespb.VersionedTaskQueueUserData{
			Data: &persistencespb.TaskQueueUserData{
				PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
					int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
						FairnessState: fairnessState,
					},
				},
			},
		},
	}

	pm, err := newTaskQueuePartitionManager(
		s.engine,
		ns,
		partition,
		tqConfig,
		s.logger,
		s.logger,
		metrics.NoopMetricsHandler,
		userData,
	)
	s.Require().NoError(err)

	pm.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = pm.WaitUntilInitialized(ctx)
	s.Require().NoError(err)

	// Brief delay for subscription registration
	time.Sleep(10 * time.Millisecond)

	return pm
}

// TestEnableFairness_StoresValues tests that EnableFairness config changes are stored
func (s *DynamicConfigSubscriptionTestSuite) TestEnableFairness_StoresValues() {
	// Start with autoEnable OFF so base configs are used
	cleanupAutoEnable := s.dcClient.OverrideSetting(dynamicconfig.MatchingAutoEnableV2, false)
	cleanupFairness := s.dcClient.OverrideSetting(dynamicconfig.MatchingEnableFairness, false)
	cleanupNewMatcher := s.dcClient.OverrideSetting(dynamicconfig.MatchingUseNewMatcher, false)
	defer cleanupAutoEnable()
	defer cleanupFairness()
	defer cleanupNewMatcher()

	pm := s.createPartitionManagerWithFairnessState(enumsspb.FAIRNESS_STATE_UNSPECIFIED)
	defer pm.Stop(unloadCauseIdle)

	// Verify initial config
	s.False(pm.config.EnableFairness.Load())
	s.False(pm.config.NewMatcher.Load())

	// Change EnableFairness (overwrite directly to avoid double callback)
	cleanupFairness = s.dcClient.OverrideSetting(dynamicconfig.MatchingEnableFairness, true)

	s.Eventually(func() bool {
		return pm.config.EnableFairness.Load() && pm.config.NewMatcher.Load()
	}, 2*time.Second, 10*time.Millisecond, "EnableFairness and NewMatcher should both be true")
}

// TestNewMatcher_StoresValues tests that NewMatcher config changes are stored
func (s *DynamicConfigSubscriptionTestSuite) TestNewMatcher_StoresValues() {
	cleanupAutoEnable := s.dcClient.OverrideSetting(dynamicconfig.MatchingAutoEnableV2, false)
	cleanupFairness := s.dcClient.OverrideSetting(dynamicconfig.MatchingEnableFairness, false)
	cleanupNewMatcher := s.dcClient.OverrideSetting(dynamicconfig.MatchingUseNewMatcher, false)
	defer cleanupAutoEnable()
	defer cleanupFairness()
	defer cleanupNewMatcher()

	pm := s.createPartitionManagerWithFairnessState(enumsspb.FAIRNESS_STATE_UNSPECIFIED)
	defer pm.Stop(unloadCauseIdle)

	s.False(pm.config.NewMatcher.Load())

	cleanupNewMatcher()
	cleanupNewMatcher = s.dcClient.OverrideSetting(dynamicconfig.MatchingUseNewMatcher, true)

	s.Eventually(func() bool {
		return pm.config.NewMatcher.Load()
	}, 2*time.Second, 10*time.Millisecond, "NewMatcher change should be stored")
}

// TestAutoEnable_StoresValue tests that autoEnable changes are tracked
func (s *DynamicConfigSubscriptionTestSuite) TestAutoEnable_StoresValue() {
	cleanupAutoEnable := s.dcClient.OverrideSetting(dynamicconfig.MatchingAutoEnableV2, true)
	cleanupFairness := s.dcClient.OverrideSetting(dynamicconfig.MatchingEnableFairness, false)
	cleanupNewMatcher := s.dcClient.OverrideSetting(dynamicconfig.MatchingUseNewMatcher, false)
	defer cleanupAutoEnable()
	defer cleanupFairness()
	defer cleanupNewMatcher()

	pm := s.createPartitionManagerWithFairnessState(enumsspb.FAIRNESS_STATE_V2)
	defer pm.Stop(unloadCauseIdle)

	// Verify initial autoEnable is ON
	s.True(pm.config.AutoEnable.Load())

	// Turn autoEnable OFF
	cleanupAutoEnable()
	cleanupAutoEnable = s.dcClient.OverrideSetting(dynamicconfig.MatchingAutoEnableV2, false)

	s.Eventually(func() bool {
		return !pm.config.AutoEnable.Load()
	}, 2*time.Second, 10*time.Millisecond, "autoEnable change should be stored")
}

// TestFairnessOverridesNewMatcher tests that when EnableFairness is true, NewMatcher is always true
func (s *DynamicConfigSubscriptionTestSuite) TestFairnessOverridesNewMatcher() {
	cleanupAutoEnable := s.dcClient.OverrideSetting(dynamicconfig.MatchingAutoEnableV2, false)
	cleanupFairness := s.dcClient.OverrideSetting(dynamicconfig.MatchingEnableFairness, true)
	cleanupNewMatcher := s.dcClient.OverrideSetting(dynamicconfig.MatchingUseNewMatcher, false)
	defer cleanupAutoEnable()
	defer cleanupFairness()
	defer cleanupNewMatcher()

	pm := s.createPartitionManagerWithFairnessState(enumsspb.FAIRNESS_STATE_UNSPECIFIED)
	defer pm.Stop(unloadCauseIdle)

	// NewMatcher should be true because EnableFairness is true
	s.True(pm.config.EnableFairness.Load())
	s.True(pm.config.NewMatcher.Load())

	// Turn off EnableFairness
	cleanupFairness()
	cleanupFairness = s.dcClient.OverrideSetting(dynamicconfig.MatchingEnableFairness, false)

	s.Eventually(func() bool {
		return !pm.config.EnableFairness.Load() && !pm.config.NewMatcher.Load()
	}, 2*time.Second, 10*time.Millisecond, "NewMatcher should reflect its dynamic config value when fairness is disabled")
}

// TestConfigChangesWithAutoEnableON tests that base config changes are stored even when autoEnable is ON
func (s *DynamicConfigSubscriptionTestSuite) TestConfigChangesWithAutoEnableON() {
	cleanupAutoEnable := s.dcClient.OverrideSetting(dynamicconfig.MatchingAutoEnableV2, true)
	cleanupFairness := s.dcClient.OverrideSetting(dynamicconfig.MatchingEnableFairness, false)
	cleanupNewMatcher := s.dcClient.OverrideSetting(dynamicconfig.MatchingUseNewMatcher, false)
	defer cleanupAutoEnable()
	defer cleanupFairness()
	defer cleanupNewMatcher()

	pm := s.createPartitionManagerWithFairnessState(enumsspb.FAIRNESS_STATE_V0)
	defer pm.Stop(unloadCauseIdle)

	s.False(pm.config.EnableFairness.Load())
	s.False(pm.config.NewMatcher.Load())

	// Change base config (should be stored even though autoEnable is ON)
	cleanupNewMatcher()
	cleanupNewMatcher = s.dcClient.OverrideSetting(dynamicconfig.MatchingUseNewMatcher, true)

	s.Eventually(func() bool {
		return pm.config.NewMatcher.Load()
	}, 2*time.Second, 10*time.Millisecond, "NewMatcher change should still be stored when autoEnable is ON")
}
