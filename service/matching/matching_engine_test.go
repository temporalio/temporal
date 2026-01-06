package matching

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/consts"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// static error used in tests
var randomTestError = errors.New("random error")

type (
	matchingEngineSuite struct {
		suite.Suite
		*require.Assertions

		newMatcher               bool
		fairness                 bool
		controller               *gomock.Controller
		mockHistoryClient        *historyservicemock.MockHistoryServiceClient
		mockMatchingClient       *matchingservicemock.MockMatchingServiceClient
		ns                       *namespace.Namespace
		mockNamespaceCache       *namespace.MockRegistry
		mockVisibilityManager    *manager.MockVisibilityManager
		mockHostInfoProvider     *membership.MockHostInfoProvider
		mockServiceResolver      *membership.MockServiceResolver
		hostInfoForResolver      membership.HostInfo
		mockNexusEndpointManager *persistence.MockNexusEndpointManager

		matchingEngine     *matchingEngineImpl
		taskManager        *testTaskManager // points to classicTaskManager or fairTaskManager
		classicTaskManager *testTaskManager
		fairTaskManager    *testTaskManager
		logger             *testlogger.TestLogger
	}
)

const (
	matchingTestNamespace = "matching-test"
)

func createTestMatchingEngine(
	logger log.Logger,
	controller *gomock.Controller,
	config *Config,
	matchingClient matchingservice.MatchingServiceClient,
	namespaceRegistry namespace.Registry,
) *matchingEngineImpl {
	tm := newTestTaskManager(logger)
	ftm := newTestFairTaskManager(logger)
	mockVisibilityManager := manager.NewMockVisibilityManager(controller)
	mockVisibilityManager.EXPECT().Close().AnyTimes()
	mockHistoryClient := historyservicemock.NewMockHistoryServiceClient(controller)
	mockHistoryClient.EXPECT().IsWorkflowTaskValid(gomock.Any(), gomock.Any()).Return(&historyservice.IsWorkflowTaskValidResponse{IsValid: true}, nil).AnyTimes()
	mockHistoryClient.EXPECT().IsActivityTaskValid(gomock.Any(), gomock.Any()).Return(&historyservice.IsActivityTaskValidResponse{IsValid: true}, nil).AnyTimes()
	mockHostInfoProvider := membership.NewMockHostInfoProvider(controller)
	hostInfo := membership.NewHostInfoFromAddress("self")
	mockHostInfoProvider.EXPECT().HostInfo().Return(hostInfo).AnyTimes()
	mockServiceResolver := membership.NewMockServiceResolver(controller)
	mockServiceResolver.EXPECT().Lookup(gomock.Any()).Return(hostInfo, nil).AnyTimes()
	mockServiceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).AnyTimes()
	mockServiceResolver.EXPECT().RemoveListener(gomock.Any()).AnyTimes()
	mockNexusEndpointManager := persistence.NewMockNexusEndpointManager(controller)
	mockNexusEndpointManager.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(&persistence.ListNexusEndpointsResponse{}, nil).AnyTimes()
	return newMatchingEngine(config, tm, ftm, mockHistoryClient, logger, namespaceRegistry, matchingClient, mockVisibilityManager, mockHostInfoProvider, mockServiceResolver, mockNexusEndpointManager)
}

func createMockNamespaceCache(controller *gomock.Controller, nsName namespace.Name) (*namespace.Namespace, *namespace.MockRegistry) {
	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: nsName.String(), Id: uuid.NewString()}, nil, "")
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(ns.Name(), nil).AnyTimes()
	return ns, mockNamespaceCache
}

// TODO(pri): cleanup; delete this
func TestMatchingEngine_Classic_Suite(t *testing.T) {
	suite.Run(t, &matchingEngineSuite{newMatcher: false})
}

func TestMatchingEngine_Pri_Suite(t *testing.T) {
	suite.Run(t, &matchingEngineSuite{newMatcher: true})
}

func TestMatchingEngine_Fair_Suite(t *testing.T) {
	suite.Run(t, &matchingEngineSuite{newMatcher: true, fairness: true})
}

func (s *matchingEngineSuite) SetupSuite() {
}

func (s *matchingEngineSuite) TearDownSuite() {
}

func (s *matchingEngineSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	s.controller = gomock.NewController(s.T())
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockMatchingClient = matchingservicemock.NewMockMatchingServiceClient(s.controller)
	s.mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
		Return(&matchingservice.GetTaskQueueUserDataResponse{}, nil).AnyTimes()
	s.mockMatchingClient.EXPECT().UpdateTaskQueueUserData(gomock.Any(), gomock.Any()).
		Return(&matchingservice.UpdateTaskQueueUserDataResponse{}, nil).AnyTimes()
	s.mockMatchingClient.EXPECT().ReplicateTaskQueueUserData(gomock.Any(), gomock.Any()).
		Return(&matchingservice.ReplicateTaskQueueUserDataResponse{}, nil).AnyTimes()
	s.mockMatchingClient.EXPECT().ForceLoadTaskQueuePartition(gomock.Any(), gomock.Any()).
		Return(&matchingservice.ForceLoadTaskQueuePartitionResponse{WasUnloaded: true}, nil).AnyTimes()

	// create and supply two task managers, but only one is expected to be used at a time since
	// we run tests with fairness enabled in separate suite.
	s.classicTaskManager = newTestTaskManager(s.logger)
	s.fairTaskManager = newTestFairTaskManager(s.logger)
	if s.fairness {
		s.taskManager = s.fairTaskManager
	} else {
		s.taskManager = s.classicTaskManager
	}

	s.ns, s.mockNamespaceCache = createMockNamespaceCache(s.controller, matchingTestNamespace)
	s.mockVisibilityManager = manager.NewMockVisibilityManager(s.controller)
	s.mockVisibilityManager.EXPECT().Close().AnyTimes()
	s.mockHostInfoProvider = membership.NewMockHostInfoProvider(s.controller)
	hostInfo := membership.NewHostInfoFromAddress("self")
	s.hostInfoForResolver = hostInfo
	s.mockHostInfoProvider.EXPECT().HostInfo().Return(hostInfo).AnyTimes()
	s.mockServiceResolver = membership.NewMockServiceResolver(s.controller)
	s.mockServiceResolver.EXPECT().Lookup(gomock.Any()).DoAndReturn(func(string) (membership.HostInfo, error) {
		return s.hostInfoForResolver, nil
	}).AnyTimes()
	s.mockServiceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockServiceResolver.EXPECT().RemoveListener(gomock.Any()).AnyTimes()
	s.mockNexusEndpointManager = persistence.NewMockNexusEndpointManager(s.controller)
	s.mockNexusEndpointManager.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(&persistence.ListNexusEndpointsResponse{}, nil).AnyTimes()

	s.matchingEngine = s.newMatchingEngine(s.newConfig(), s.classicTaskManager, s.fairTaskManager)
	s.matchingEngine.Start()
}

func (s *matchingEngineSuite) newConfig() *Config {
	res := defaultTestConfig()
	if s.fairness {
		useFairness(res)
	} else if s.newMatcher {
		useNewMatcher(res)
	}
	return res
}

func (s *matchingEngineSuite) TearDownTest() {
	s.matchingEngine.Stop()
}

func (s *matchingEngineSuite) newMatchingEngine(
	config *Config,
	taskMgr persistence.TaskManager,
	fairTaskMgr persistence.TaskManager,
) *matchingEngineImpl {
	return newMatchingEngine(config, taskMgr, fairTaskMgr, s.mockHistoryClient, s.logger, s.mockNamespaceCache, s.mockMatchingClient, s.mockVisibilityManager,
		s.mockHostInfoProvider, s.mockServiceResolver, s.mockNexusEndpointManager)
}

func newMatchingEngine(
	config *Config, taskMgr persistence.TaskManager, fairTaskMgr persistence.FairTaskManager,
	mockHistoryClient historyservice.HistoryServiceClient,
	logger log.Logger, mockNamespaceCache namespace.Registry, mockMatchingClient matchingservice.MatchingServiceClient,
	mockVisibilityManager manager.VisibilityManager, mockHostInfoProvider membership.HostInfoProvider,
	mockServiceResolver membership.ServiceResolver, nexusEndpointManager persistence.NexusEndpointManager,
) *matchingEngineImpl {
	return &matchingEngineImpl{
		taskManager:     taskMgr,
		fairTaskManager: fairTaskMgr,
		historyClient:   mockHistoryClient,
		partitions:      make(map[tqid.PartitionKey]taskQueuePartitionManager),
		gaugeMetrics: gaugeMetrics{
			loadedTaskQueueFamilyCount:    make(map[taskQueueCounterKey]int),
			loadedTaskQueueCount:          make(map[taskQueueCounterKey]int),
			loadedTaskQueuePartitionCount: make(map[taskQueueCounterKey]int),
			loadedPhysicalTaskQueueCount:  make(map[taskQueueCounterKey]int),
		},
		queryResults:                  collection.NewSyncMap[string, chan *queryResult](),
		logger:                        logger,
		throttledLogger:               log.ThrottledLogger(logger),
		metricsHandler:                metrics.NoopMetricsHandler,
		matchingRawClient:             mockMatchingClient,
		tokenSerializer:               tasktoken.NewSerializer(),
		config:                        config,
		namespaceRegistry:             mockNamespaceCache,
		hostInfoProvider:              mockHostInfoProvider,
		serviceResolver:               mockServiceResolver,
		membershipChangedCh:           make(chan *membership.ChangedEvent, 1),
		clusterMeta:                   clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(false, true)),
		timeSource:                    clock.NewRealTimeSource(),
		visibilityManager:             mockVisibilityManager,
		nexusEndpointClient:           newEndpointClient(config.NexusEndpointsRefreshInterval, nexusEndpointManager),
		nexusEndpointsOwnershipLostCh: make(chan struct{}),
	}
}

func (s *matchingEngineSuite) newPartitionManager(prtn tqid.Partition, config *Config) taskQueuePartitionManager {
	tqConfig := newTaskQueueConfig(prtn.TaskQueue(), config, matchingTestNamespace)
	logger, _, metricsHandler := s.matchingEngine.loggerAndMetricsForPartition(s.ns, prtn, tqConfig)
	pm, err := newTaskQueuePartitionManager(s.matchingEngine, s.ns, prtn, tqConfig, logger, logger, metricsHandler, &mockUserDataManager{})
	s.Require().NoError(err)
	return pm
}

func (s *matchingEngineSuite) TestPollActivityTaskQueuesEmptyResult() {
	s.PollForTasksEmptyResultTest(context.Background(), enumspb.TASK_QUEUE_TYPE_ACTIVITY)
}

func (s *matchingEngineSuite) TestPollWorkflowTaskQueuesEmptyResult() {
	s.PollForTasksEmptyResultTest(context.Background(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
}

func (s *matchingEngineSuite) TestPollActivityTaskQueuesEmptyResultWithShortContext() {
	shortContextTimeout := returnEmptyTaskTimeBudget + 10*time.Millisecond
	callContext, cancel := context.WithTimeout(context.Background(), shortContextTimeout)
	defer cancel()
	s.PollForTasksEmptyResultTest(callContext, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
}

func (s *matchingEngineSuite) TestPollWorkflowTaskQueuesEmptyResultWithShortContext() {
	shortContextTimeout := returnEmptyTaskTimeBudget + 10*time.Millisecond
	callContext, cancel := context.WithTimeout(context.Background(), shortContextTimeout)
	defer cancel()
	s.PollForTasksEmptyResultTest(callContext, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
}

func (s *matchingEngineSuite) PollForTasksEmptyResultTest(callContext context.Context, taskType enumspb.TaskQueueType) {
	s.matchingEngine.config.RangeSize = 2 // to test that range is not updated without tasks
	if _, ok := callContext.Deadline(); !ok {
		s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Millisecond)
	}

	namespaceID := uuid.NewString()
	tl := "makeToast"
	identity := "selfDrivingToaster"

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	var taskQueueType enumspb.TaskQueueType
	tlID := newUnversionedRootQueueKey(namespaceID, tl, taskType)
	const pollCount = 10
	for i := 0; i < pollCount; i++ {
		if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			pollResp, err := s.matchingEngine.PollActivityTaskQueue(callContext, &matchingservice.PollActivityTaskQueueRequest{
				NamespaceId: namespaceID,
				PollRequest: &workflowservice.PollActivityTaskQueueRequest{
					TaskQueue: taskQueue,
					Identity:  identity,
				},
			}, metrics.NoopMetricsHandler)
			s.NoError(err)
			s.Equal(emptyPollActivityTaskQueueResponse, pollResp)

			taskQueueType = enumspb.TASK_QUEUE_TYPE_ACTIVITY
		} else {
			resp, err := s.matchingEngine.PollWorkflowTaskQueue(callContext, &matchingservice.PollWorkflowTaskQueueRequest{
				NamespaceId: namespaceID,
				PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
					TaskQueue: taskQueue,
					Identity:  identity,
				},
			}, metrics.NoopMetricsHandler)
			s.NoError(err)
			s.Equal(emptyPollWorkflowTaskQueueResponse, resp)

			taskQueueType = enumspb.TASK_QUEUE_TYPE_WORKFLOW
		}
		select {
		case <-callContext.Done():
			s.FailNow("Call context has expired.")
		default:
		}
		// check the poller information
		descResp, err := s.matchingEngine.DescribeTaskQueue(context.Background(), &matchingservice.DescribeTaskQueueRequest{
			NamespaceId: namespaceID,
			DescRequest: &workflowservice.DescribeTaskQueueRequest{
				TaskQueue:              taskQueue,
				TaskQueueType:          taskQueueType,
				IncludeTaskQueueStatus: false,
			},
		})
		s.NoError(err)
		s.Equal(1, len(descResp.DescResponse.Pollers))
		s.Equal(identity, descResp.DescResponse.Pollers[0].GetIdentity())
		s.NotEmpty(descResp.DescResponse.Pollers[0].GetLastAccessTime())
		s.Nil(descResp.DescResponse.GetTaskQueueStatus())
	}
	s.EqualValues(1, s.taskManager.getQueueDataByKey(tlID).RangeID())
}

func (s *matchingEngineSuite) TestOnlyUnloadMatchingInstance() {
	prtn := newRootPartition(
		uuid.NewString(),
		"makeToast",
		enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tqm, _, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), prtn, true, loadCauseUnspecified)
	s.Require().NoError(err)

	tqm2 := s.newPartitionManager(prtn, s.matchingEngine.config)

	// try to unload a different tqMgr instance with the same taskqueue ID
	s.matchingEngine.unloadTaskQueuePartition(tqm2, unloadCauseUnspecified)

	got, _, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), prtn, true, loadCauseUnspecified)
	s.Require().NoError(err)
	s.Require().Same(tqm, got,
		"Unload call with non-matching taskQueuePartitionManager should not cause unload")

	// this time unload the right tqMgr
	s.matchingEngine.unloadTaskQueuePartition(tqm, unloadCauseUnspecified)

	got, _, err = s.matchingEngine.getTaskQueuePartitionManager(context.Background(), prtn, true, loadCauseUnspecified)
	s.Require().NoError(err)
	s.Require().NotSame(tqm, got,
		"Unload call with matching incarnation should have caused unload")
}

func (s *matchingEngineSuite) TestFailAddTaskWithHistoryExhausted() {
	tqName := "testFailAddTaskWithHistoryExhausted"
	historyError := consts.ErrResourceExhaustedBusyWorkflow
	s.testFailAddTaskWithHistoryError(tqName, false, historyError, nil)
}

func (s *matchingEngineSuite) TestFailAddTaskWithHistoryError() {
	s.logger.Expect(testlogger.Error, "dropping task due to non-nonretryable errors")
	historyError := serviceerror.NewInternal("nothing to start")
	tqName := "testFailAddTaskWithHistoryError"
	s.testFailAddTaskWithHistoryError(tqName, true, historyError, nil) // expectedError shall be nil since history drops the task
}

func (s *matchingEngineSuite) testFailAddTaskWithHistoryError(
	tqName string,
	expectSyncMatch bool,
	recordError error,
	expectedError error,
) {
	namespaceID := namespace.ID(uuid.NewString())
	identity := "identity"

	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_STICKY}

	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(1 * time.Second)

	runID := uuid.NewString()
	workflowID := "workflow1"
	execution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}
	scheduledEventID := int64(0)

	addRequest := matchingservice.AddWorkflowTaskRequest{
		NamespaceId:            namespaceID.String(),
		Execution:              execution,
		ScheduledEventId:       scheduledEventID,
		TaskQueue:              stickyTaskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
	}

	pollRequest := matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: stickyTaskQueue,
			Identity:  identity,
		},
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		_, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &pollRequest, metrics.NoopMetricsHandler)
		if err != nil {
			s.logger.Info(err.Error())
		}
		wg.Done()
	}()

	partitionReady := func() bool {
		return len(s.matchingEngine.getTaskQueuePartitions(10)) >= 1
	}
	s.Eventually(partitionReady, 100*time.Millisecond, 10*time.Millisecond)

	recordWorkflowTaskStartedResponse := &historyservice.RecordWorkflowTaskStartedResponse{
		PreviousStartedEventId:     scheduledEventID,
		ScheduledEventId:           scheduledEventID + 1,
		Attempt:                    1,
		StickyExecutionEnabled:     true,
		WorkflowExecutionTaskQueue: &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		History:                    &historypb.History{Events: []*historypb.HistoryEvent{}},
		NextPageToken:              nil,
	}

	s.mockHistoryClient.EXPECT().
		RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *historyservice.RecordWorkflowTaskStartedRequest, _ ...interface{}) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
			s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Millisecond)
			return nil, recordError
		})

	s.mockHistoryClient.EXPECT().
		RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(recordWorkflowTaskStartedResponse, nil).AnyTimes()

	_, syncMatch, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)

	s.Equal(syncMatch, expectSyncMatch)
	if expectedError != nil {
		s.ErrorAs(err, &expectedError)
	} else {
		s.Nil(err)
	}
	wg.Wait()
}

func (s *matchingEngineSuite) TestPollWorkflowTaskQueues() {
	namespaceID := namespace.ID(uuid.NewString())
	tl := "makeToast"
	stickyTl := "makeStickyToast"
	stickyTlKind := enumspb.TASK_QUEUE_KIND_STICKY
	identity := "selfDrivingToaster"

	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: stickyTl, Kind: stickyTlKind}

	s.matchingEngine.config.RangeSize = 2 // to test that range is not updated without tasks
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Millisecond)

	runID := uuid.NewString()
	workflowID := "workflow1"
	workflowType := &commonpb.WorkflowType{
		Name: "workflow",
	}
	execution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}
	scheduledEventID := int64(0)

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordWorkflowTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordWorkflowTaskStartedRequest")
			response := &historyservice.RecordWorkflowTaskStartedResponse{
				WorkflowType:               workflowType,
				PreviousStartedEventId:     scheduledEventID,
				ScheduledEventId:           scheduledEventID + 1,
				Attempt:                    1,
				StickyExecutionEnabled:     true,
				WorkflowExecutionTaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				History:                    &historypb.History{Events: []*historypb.HistoryEvent{}},
				NextPageToken:              nil,
			}
			return response, nil
		}).AnyTimes()

	addRequest := matchingservice.AddWorkflowTaskRequest{
		NamespaceId:            namespaceID.String(),
		Execution:              execution,
		ScheduledEventId:       scheduledEventID,
		TaskQueue:              stickyTaskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
	}

	_, _, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
	// fail due to no sticky worker
	s.ErrorAs(err, new(*serviceerrors.StickyWorkerUnavailable))
	// poll the sticky queue, should get no result
	resp, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: stickyTaskQueue,
			Identity:  identity,
		},
	}, metrics.NoopMetricsHandler)
	s.NoError(err)
	s.Equal(emptyPollWorkflowTaskQueueResponse, resp)

	// add task to sticky queue again, this time it should pass
	_, _, err = s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
	s.NoError(err)

	resp, err = s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: stickyTaskQueue,
			Identity:  identity,
		},
	}, metrics.NoopMetricsHandler)
	s.NoError(err)

	expectedResp := &matchingservice.PollWorkflowTaskQueueResponse{
		TaskToken:              resp.TaskToken,
		WorkflowExecution:      execution,
		WorkflowType:           workflowType,
		PreviousStartedEventId: scheduledEventID,
		StartedEventId:         common.EmptyEventID,
		Attempt:                1,
		NextEventId:            common.EmptyEventID,
		BacklogCountHint:       0,
		StickyExecutionEnabled: true,
		Query:                  nil,
		TransientWorkflowTask:  nil,
		WorkflowExecutionTaskQueue: &taskqueuepb.TaskQueue{
			Name: tl,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		BranchToken:   nil,
		ScheduledTime: nil,
		StartedTime:   nil,
		Queries:       nil,
		History:       &historypb.History{Events: []*historypb.HistoryEvent{}},
		NextPageToken: nil,
	}

	s.Nil(err)
	s.Equal(expectedResp, resp)
}

func (s *matchingEngineSuite) TestPollWorkflowTaskQueues_NamespaceHandover() {
	namespaceID := uuid.NewString()
	taskQueue := &taskqueuepb.TaskQueue{Name: "queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	addRequest := matchingservice.AddWorkflowTaskRequest{
		NamespaceId:            namespaceID,
		Execution:              &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewString()},
		ScheduledEventId:       int64(0),
		TaskQueue:              taskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
	}

	// add multiple workflow tasks, but matching should not keep polling new tasks
	// upon getting namespace handover error when recording start for the first task
	_, _, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
	s.NoError(err)
	_, _, err = s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
	s.NoError(err)

	s.mockHistoryClient.EXPECT().RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, common.ErrNamespaceHandover).Times(1)

	resp, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	s.Nil(resp)
	s.Equal(common.ErrNamespaceHandover.Error(), err.Error())
}

func (s *matchingEngineSuite) TestPollActivityTaskQueues_InternalError() {
	s.logger.Expect(testlogger.Error, "dropping task due to non-nonretryable errors")
	namespaceID := uuid.NewString()
	tl := "queue"
	taskQueue := &taskqueuepb.TaskQueue{Name: "queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	addRequest := matchingservice.AddActivityTaskRequest{
		NamespaceId:            namespaceID,
		Execution:              &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewString()},
		ScheduledEventId:       int64(5),
		TaskQueue:              taskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(0),
	}

	// add an activity task
	_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.NoError(err)
	s.Equal(1, s.taskManager.getTaskCount(newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)))

	// task is dropped with no retry; RecordActivityTaskStarted should only be called once
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewInternal("Internal error")).Times(1)
	resp, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	protoassert.ProtoEqual(s.T(), emptyPollActivityTaskQueueResponse, resp)
	s.NoError(err)
}

func (s *matchingEngineSuite) TestPollActivityTaskQueues_DataLossError() {
	s.logger.Expect(testlogger.Error, "dropping task due to non-nonretryable errors")

	namespaceID := uuid.NewString()
	tl := "queue"
	taskQueue := &taskqueuepb.TaskQueue{Name: "queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	addRequest := matchingservice.AddActivityTaskRequest{
		NamespaceId:            namespaceID,
		Execution:              &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewString()},
		ScheduledEventId:       int64(5),
		TaskQueue:              taskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(0),
	}

	// add an activity task
	_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.NoError(err)
	s.Equal(1, s.taskManager.getTaskCount(newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)))

	// task is dropped with no retry; RecordActivityTaskStarted should only be called once
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewDataLoss("DataLoss Error")).Times(1)

	resp, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	protoassert.ProtoEqual(s.T(), emptyPollActivityTaskQueueResponse, resp)
	s.NoError(err)
}

func (s *matchingEngineSuite) TestPollWorkflowTaskQueues_InternalError() {
	s.logger.Expect(testlogger.Error, "dropping task due to non-nonretryable errors")

	tqName := "queue"
	taskQueue := &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	wfExecution := &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewString()}

	// add a wf task
	s.addWorkflowTask(wfExecution, taskQueue)
	s.Equal(1, s.taskManager.getTaskCount(newUnversionedRootQueueKey(namespaceID, tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)))

	// task is dropped with no retry; RecordWorkflowTaskStarted should only be called once
	s.mockHistoryClient.EXPECT().RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewInternal("internal error")).Times(1)

	resp, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	protoassert.ProtoEqual(s.T(), emptyPollWorkflowTaskQueueResponse, resp)
	s.NoError(err)
}

func (s *matchingEngineSuite) TestPollWorkflowTaskQueues_DataLossError() {
	s.logger.Expect(testlogger.Error, "dropping task due to non-nonretryable errors")

	tqName := "queue"
	taskQueue := &taskqueuepb.TaskQueue{Name: tqName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	wfExecution := &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewString()}

	// add a wf task
	s.addWorkflowTask(wfExecution, taskQueue)
	s.Equal(1, s.taskManager.getTaskCount(newUnversionedRootQueueKey(namespaceID, tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)))

	// task is dropped with no retry; RecordWorkflowTaskStarted should only be called once
	s.mockHistoryClient.EXPECT().RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewDataLoss("DataLoss error")).Times(1)

	resp, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	protoassert.ProtoEqual(s.T(), emptyPollWorkflowTaskQueueResponse, resp)
	s.NoError(err)
}

func (s *matchingEngineSuite) TestPollActivityTaskQueues_NamespaceHandover() {
	namespaceID := uuid.NewString()
	taskQueue := &taskqueuepb.TaskQueue{Name: "queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	addRequest := matchingservice.AddActivityTaskRequest{
		NamespaceId:            namespaceID,
		Execution:              &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewString()},
		ScheduledEventId:       int64(5),
		TaskQueue:              taskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
	}

	// add multiple activity tasks, but matching should not keep polling new tasks
	// upon getting namespace handover error when recording start for the first task
	_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.NoError(err)
	_, _, err = s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.NoError(err)

	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, common.ErrNamespaceHandover).Times(1)
	resp, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	s.Nil(resp)
	s.Equal(common.ErrNamespaceHandover.Error(), err.Error())
}

func (s *matchingEngineSuite) TestAddActivityTasks() {
	s.AddTasksTest(enumspb.TASK_QUEUE_TYPE_ACTIVITY, false)
}

func (s *matchingEngineSuite) TestAddWorkflowTasks() {
	s.AddTasksTest(enumspb.TASK_QUEUE_TYPE_WORKFLOW, false)
}

func (s *matchingEngineSuite) TestAddWorkflowTasksForwarded() {
	s.AddTasksTest(enumspb.TASK_QUEUE_TYPE_WORKFLOW, true)
}

func (s *matchingEngineSuite) TestAddWorkflowAutoEnable() {
	tv := testvars.New(s.T()).WithNamespaceID(s.ns.ID())
	req := &matchingservice.UpdateFairnessStateRequest{
		NamespaceId:   tv.NamespaceID().String(),
		TaskQueue:     tv.TaskQueue().Name,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		FairnessState: enumsspb.FAIRNESS_STATE_V2,
	}
	var didUpdate atomic.Bool
	s.mockMatchingClient.EXPECT().UpdateFairnessState(context.Background(), req).DoAndReturn(
		func(ctx context.Context, req *matchingservice.UpdateFairnessStateRequest, opts ...grpc.CallOption) (*matchingservice.UpdateFairnessStateResponse, error) {
			didUpdate.Store(true)
			return s.matchingEngine.UpdateFairnessState(ctx, req)
		},
	)
	dbq := newUnversionedRootQueueKey(tv.NamespaceID().String(), tv.TaskQueue().Name, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	mgr := s.newPartitionManager(dbq.partition, s.matchingEngine.config)
	cMgr := mgr.(*taskQueuePartitionManagerImpl)
	mgr.GetUserDataManager().(*mockUserDataManager).onChange = cMgr.userDataChanged
	s.matchingEngine.updateTaskQueue(dbq.partition, mgr)
	mgr.Start()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err := mgr.WaitUntilInitialized(ctx)
	s.Require().NoError(err)
	cancel()

	_, _, err = s.matchingEngine.AddWorkflowTask(
		context.Background(),
		&matchingservice.AddWorkflowTaskRequest{
			NamespaceId: tv.NamespaceID().String(),
			Execution:   tv.WorkflowExecution(),
			TaskQueue:   tv.TaskQueue(),
			Priority: &commonpb.Priority{
				PriorityKey: 3,
				FairnessKey: "myFairnessKey",
			},
		},
	)
	// The task may or may not be enqueued before a shutdown, so ignore that error
	if err != errShutdown {
		s.Require().NoError(err)
	}
	s.Eventually(didUpdate.Load, time.Second, time.Millisecond)

	// We check the old partition manager for the change because a new partition created will not reference the same mockUserDataManager.
	data, _, _ := mgr.GetUserDataManager().GetUserData()
	s.Require().Equal(enumsspb.FAIRNESS_STATE_V2, data.GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].FairnessState)
	// At this point the partition manager should be unloaded
	select {
	case <-cMgr.initCtx.Done():
	case <-time.After(time.Second):
		s.Require().Fail("our partition manager was not unloaded")
	}
}

func (s *matchingEngineSuite) TestSkipAutoEnable() {
	if !s.newMatcher && !s.fairness {
		s.T().Skip("We only skip auto enable if new matcher is explicitly enabled already")
	}

	// Explicitly set to zero times in the event this call is added as expected during setup in the future
	s.mockMatchingClient.EXPECT().UpdateFairnessState(context.Background(), nil).DoAndReturn(
		func(ctx context.Context, req *matchingservice.UpdateFairnessStateRequest, opts ...grpc.CallOption) (*matchingservice.UpdateFairnessStateResponse, error) {
			return s.matchingEngine.UpdateFairnessState(ctx, req)
		},
	).Times(0)

	tv := testvars.New(s.T())
	_, _, err := s.matchingEngine.AddWorkflowTask(
		context.Background(),
		&matchingservice.AddWorkflowTaskRequest{
			NamespaceId: tv.NamespaceID().String(),
			Execution:   tv.WorkflowExecution(),
			TaskQueue:   tv.TaskQueue(),
			Priority: &commonpb.Priority{
				PriorityKey: 3,
			},
		},
	)
	s.Require().NoError(err)
}

func (s *matchingEngineSuite) AddTasksTest(taskType enumspb.TaskQueueType, isForwarded bool) {
	s.matchingEngine.config.RangeSize = 300 // override to low number for the test

	namespaceID := s.ns.ID().String()
	tl := "makeToast"
	forwardedFrom := "/_sys/makeToast/1"

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	const taskCount = 111

	runID := uuid.NewString()
	workflowID := "workflow1"
	execution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	for i := int64(0); i < taskCount; i++ {
		scheduledEventID := i * 3
		var err error
		if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			addRequest := matchingservice.AddActivityTaskRequest{
				NamespaceId:            namespaceID,
				Execution:              execution,
				ScheduledEventId:       scheduledEventID,
				TaskQueue:              taskQueue,
				ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			}
			if isForwarded {
				addRequest.ForwardInfo = &taskqueuespb.TaskForwardInfo{SourcePartition: forwardedFrom}
			}
			_, _, err = s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		} else {
			addRequest := matchingservice.AddWorkflowTaskRequest{
				NamespaceId:            namespaceID,
				Execution:              execution,
				ScheduledEventId:       scheduledEventID,
				TaskQueue:              taskQueue,
				ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			}
			if isForwarded {
				addRequest.ForwardInfo = &taskqueuespb.TaskForwardInfo{SourcePartition: forwardedFrom}
			}
			_, _, err = s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
		}

		switch isForwarded {
		case false:
			s.NoError(err)
		case true:
			s.Equal(errRemoteSyncMatchFailed, err)
		}
	}

	switch isForwarded {
	case false:
		s.Equal(taskCount, s.taskManager.getTaskCount(newUnversionedRootQueueKey(namespaceID, tl, taskType)))
	case true:
		s.Equal(0, s.taskManager.getTaskCount(newUnversionedRootQueueKey(namespaceID, tl, taskType)))
	}
}

func (s *matchingEngineSuite) TestAddWorkflowTaskDoesNotLoadSticky() {
	addRequest := matchingservice.AddWorkflowTaskRequest{
		NamespaceId:            uuid.NewString(),
		Execution:              &commonpb.WorkflowExecution{RunId: uuid.NewString(), WorkflowId: "wf1"},
		ScheduledEventId:       0,
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "sticky", Kind: enumspb.TASK_QUEUE_KIND_STICKY},
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
	}
	_, _, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
	s.ErrorAs(err, new(*serviceerrors.StickyWorkerUnavailable))
	// check loaded queues
	s.matchingEngine.partitionsLock.RLock()
	defer s.matchingEngine.partitionsLock.RUnlock()
	s.Equal(0, len(s.matchingEngine.partitions))
}

func (s *matchingEngineSuite) TestQueryWorkflowDoesNotLoadSticky() {
	query := matchingservice.QueryWorkflowRequest{
		NamespaceId: uuid.NewString(),
		TaskQueue:   &taskqueuepb.TaskQueue{Name: "sticky", Kind: enumspb.TASK_QUEUE_KIND_STICKY},
		QueryRequest: &workflowservice.QueryWorkflowRequest{
			Namespace: "ns",
			Execution: &commonpb.WorkflowExecution{RunId: uuid.NewString(), WorkflowId: "wf1"},
			Query:     &querypb.WorkflowQuery{QueryType: "q"},
		},
	}
	_, err := s.matchingEngine.QueryWorkflow(context.Background(), &query)
	s.ErrorAs(err, new(*serviceerrors.StickyWorkerUnavailable))
	// check loaded queues
	s.matchingEngine.partitionsLock.RLock()
	defer s.matchingEngine.partitionsLock.RUnlock()
	s.Equal(0, len(s.matchingEngine.partitions))
}

func (s *matchingEngineSuite) TestAddThenConsumeActivities() {
	if s.newMatcher {
		s.T().Skip("not supported by new matcher; flaky")
	}

	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Millisecond)

	runID := uuid.NewString()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const taskCount = 1000
	// TODO: Understand why publish is low when rangeSize is 3
	const rangeSize = 30

	namespaceID := uuid.NewString()
	tl := "makeToast"
	tlID := newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	for i := int64(0); i < taskCount; i++ {
		scheduledEventID := i * 3
		addRequest := matchingservice.AddActivityTaskRequest{
			NamespaceId:            namespaceID,
			Execution:              workflowExecution,
			ScheduledEventId:       scheduledEventID,
			TaskQueue:              taskQueue,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		}

		_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		s.NoError(err)
	}
	s.EqualValues(taskCount, s.taskManager.getTaskCount(tlID))

	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &commonpb.ActivityType{Name: activityTypeName}
	activityInput := payloads.EncodeString("Activity1 Input")

	identity := "nobody"

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			resp := &historyservice.RecordActivityTaskStartedResponse{
				Attempt: 1,
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduledEventId, 0,
					&commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId: activityID,
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: taskQueue.Name,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
						ActivityType:           activityType,
						Input:                  activityInput,
						ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
						ScheduleToStartTimeout: durationpb.New(50 * time.Second),
						StartToCloseTimeout:    durationpb.New(50 * time.Second),
						HeartbeatTimeout:       durationpb.New(10 * time.Second),
					}),
			}
			resp.StartedTime = timestamp.TimeNowPtrUtc()
			return resp, nil
		}).AnyTimes()

	for i := int64(0); i < taskCount; {
		scheduledEventID := i * 3

		result, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceID,
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{
				TaskQueue: taskQueue,
				Identity:  identity,
			},
		}, metrics.NoopMetricsHandler)

		s.NoError(err)
		s.NotNil(result)
		if len(result.TaskToken) == 0 {
			s.logger.Debug("empty poll returned")
			continue
		}
		s.EqualValues(activityID, result.ActivityId)
		s.EqualValues(activityType, result.ActivityType)
		s.EqualValues(activityInput, result.Input)
		s.EqualValues(workflowExecution, result.WorkflowExecution)
		s.Equal(true, validateTimeRange(result.ScheduledTime.AsTime(), time.Minute))
		s.EqualValues(time.Second*100, result.ScheduleToCloseTimeout.AsDuration())
		s.Equal(true, validateTimeRange(result.StartedTime.AsTime(), time.Minute))
		s.EqualValues(time.Second*50, result.StartToCloseTimeout.AsDuration())
		s.EqualValues(time.Second*10, result.HeartbeatTimeout.AsDuration())
		taskToken := &tokenspb.Task{
			Attempt:          1,
			NamespaceId:      namespaceID,
			WorkflowId:       workflowID,
			RunId:            runID,
			ScheduledEventId: scheduledEventID,
			ActivityId:       activityID,
			ActivityType:     activityTypeName,
		}

		serializedToken, _ := s.matchingEngine.tokenSerializer.Serialize(taskToken)
		s.EqualValues(serializedToken, result.TaskToken)
		i++
	}
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
	expectedRange := int64((taskCount + 1) / rangeSize)
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.LessOrEqual(expectedRange, s.taskManager.getQueueDataByKey(tlID).rangeID)
}

// TODO: this unit test does not seem to belong to matchingEngine, move it to the right place
func (s *matchingEngineSuite) TestSyncMatchActivities() {
	if s.newMatcher {
		s.T().Skip("not supported by new matcher")
	}
	s.logger.Expect(testlogger.Error, "unexpected error dispatching task")

	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope).WithTags(metrics.ServiceNameTag(primitives.MatchingService))

	// Set a short long poll expiration so that we don't have to wait too long for 0 throttling cases
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(2 * time.Second)
	s.matchingEngine.config.MinTaskThrottlingBurstSize = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(0)
	s.matchingEngine.config.RangeSize = 30 // override to low number for the test

	// Overriding the dynamic config so that the rate-limiter has a refresh rate of 0. By default, the rate-limiter has a refresh rate of 1 minute which is too long for this test.
	s.matchingEngine.config.RateLimiterRefreshInterval = 0
	s.matchingEngine.config.AdminNamespaceToPartitionDispatchRate = dynamicconfig.GetFloatPropertyFnFilteredByNamespace(25000)
	s.matchingEngine.config.AdminNamespaceTaskqueueToPartitionDispatchRate = dynamicconfig.GetFloatPropertyFnFilteredByTaskQueue(25000)

	namespaceID := uuid.NewString()
	tl := "makeToast"
	dbq := newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	mgr := s.newPartitionManager(dbq.partition, s.matchingEngine.config)

	// Directly override admin rate limits to simulate dynamic config at rateLimitManager.
	mgr.GetRateLimitManager().SetAdminRateForTesting(25000.0)

	s.matchingEngine.updateTaskQueue(dbq.partition, mgr)
	mgr.Start()

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &commonpb.ActivityType{Name: activityTypeName}
	activityInput := payloads.EncodeString("Activity1 Input")

	identity := "nobody"

	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &historyservice.RecordActivityTaskStartedResponse{
				Attempt: 1,
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduledEventId, 0,
					&commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId: activityID,
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: taskQueue.Name,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
						ActivityType:           activityType,
						Input:                  activityInput,
						ScheduleToStartTimeout: durationpb.New(1 * time.Second),
						ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
						StartToCloseTimeout:    durationpb.New(1 * time.Second),
						HeartbeatTimeout:       durationpb.New(1 * time.Second),
					}),
			}, nil
		}).AnyTimes()

	const taskCount = 10
	runID := uuid.NewString()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}
	pollFunc := func(maxDispatch float64) (*matchingservice.PollActivityTaskQueueResponse, error) {
		return s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceID,
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{
				TaskQueue:         taskQueue,
				Identity:          identity,
				TaskQueueMetadata: &taskqueuepb.TaskQueueMetadata{MaxTasksPerSecond: &wrapperspb.DoubleValue{Value: maxDispatch}},
			},
		}, metrics.NoopMetricsHandler)
	}
	for i := int64(0); i < taskCount; i++ {
		scheduledEventID := i * 3

		var wg sync.WaitGroup
		var result *matchingservice.PollActivityTaskQueueResponse
		var pollErr error
		maxDispatch := defaultTaskDispatchRPS
		if i == taskCount/2 {
			maxDispatch = 0
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, pollErr = pollFunc(maxDispatch)
		}()
		time.Sleep(20 * time.Millisecond) // Necessary for sync match to happen

		addRequest := matchingservice.AddActivityTaskRequest{
			NamespaceId:            namespaceID,
			Execution:              workflowExecution,
			ScheduledEventId:       scheduledEventID,
			TaskQueue:              taskQueue,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		}
		_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		wg.Wait()
		s.NoError(err)
		s.NoError(pollErr)
		s.NotNil(result)

		if len(result.TaskToken) == 0 {
			// when ratelimit is set to zero, poller is expected to return empty result
			// reset ratelimit, poll again and make sure task is returned this time
			s.logger.Debug("empty poll returned")
			s.Equal(float64(0), maxDispatch)
			maxDispatch = defaultTaskDispatchRPS
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, pollErr = pollFunc(maxDispatch)
			}()
			wg.Wait()
			s.NoError(err)
			s.NoError(pollErr)
			s.NotNil(result)
			s.True(len(result.TaskToken) > 0)
		}

		s.EqualValues(activityID, result.ActivityId)
		s.EqualValues(activityType, result.ActivityType)
		s.EqualValues(activityInput, result.Input)
		s.EqualValues(workflowExecution, result.WorkflowExecution)

		taskToken := &tokenspb.Task{
			Attempt:          1,
			NamespaceId:      namespaceID,
			WorkflowId:       workflowID,
			RunId:            runID,
			ScheduledEventId: scheduledEventID,
			ActivityId:       activityID,
			ActivityType:     activityTypeName,
		}
		serializedToken, _ := s.matchingEngine.tokenSerializer.Serialize(taskToken)
		s.EqualValues(serializedToken, result.TaskToken)
	}

	s.EventuallyWithT(func(collect *assert.CollectT) {
		assert.EqualValues(collect, 1, s.taskManager.getCreateTaskCount(dbq)) // Check times zero rps is set = Tasks stored in persistence
		assert.EqualValues(collect, 0, s.taskManager.getTaskCount(dbq))
	}, 2*time.Second, 100*time.Millisecond)

	syncCtr := scope.Snapshot().Counters()["test.sync_throttle_count+namespace="+matchingTestNamespace+",namespace_state=active,operation=TaskQueueMgr,partition=0,service_name=matching,task_type=Activity,taskqueue=makeToast,worker_version=__unversioned__"]
	s.Equal(1, int(syncCtr.Value())) // Check times zero rps is set = throttle counter
	expectedRange := int64((taskCount + 1) / 30)
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.LessOrEqual(expectedRange, s.taskManager.getQueueDataByKey(dbq).rangeID)

	// check the poller information
	tlType := enumspb.TASK_QUEUE_TYPE_ACTIVITY
	descResp, err := s.matchingEngine.DescribeTaskQueue(context.Background(), &matchingservice.DescribeTaskQueueRequest{
		NamespaceId: namespaceID,
		DescRequest: &workflowservice.DescribeTaskQueueRequest{
			TaskQueue:              taskQueue,
			TaskQueueType:          tlType,
			IncludeTaskQueueStatus: true,
		},
	})
	s.NoError(err)
	s.Equal(1, len(descResp.DescResponse.Pollers))
	s.Equal(identity, descResp.DescResponse.Pollers[0].GetIdentity())
	s.NotEmpty(descResp.DescResponse.Pollers[0].GetLastAccessTime())
	s.Equal(defaultTaskDispatchRPS, descResp.DescResponse.Pollers[0].GetRatePerSecond())
	s.NotNil(descResp.DescResponse.GetTaskQueueStatus())
	numPartitions := float64(s.matchingEngine.config.NumTaskqueueWritePartitions("", "", tlType))
	//nolint:staticcheck // checking deprecated field
	s.GreaterOrEqual(descResp.DescResponse.GetTaskQueueStatus().GetRatePerSecond()*numPartitions,
		(defaultTaskDispatchRPS - 1))
}

func (s *matchingEngineSuite) TestRateLimiterAcrossVersionedQueues() {
	/*
		1. Start a versioned poller with maxTasksPerSecond = defaultTaskDispatchRPS
		2. Start another versioned poller, polling a different version, maxTasksPerSecond = 0
		3. Add tasks to both these versioned queues and notice no dispatch of tasks.
		4. Restart the pollers with maxTasksPerSecond = defaultTaskDispatchRPS
		5. Verify that both the pollers have received tasks.
	*/

	if s.newMatcher {
		s.T().Skip("not supported by new matcher")
	}
	s.logger.Expect(testlogger.Error, "unexpected error dispatching task")

	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope).WithTags(metrics.ServiceNameTag(primitives.MatchingService))

	// Set a short long poll expiration so that the pollers don't wait too long for tasks
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(5 * time.Second)
	s.matchingEngine.config.MinTaskThrottlingBurstSize = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(0)
	// Disable deployment versions since a nil DeploymentClient is used in unit tests
	s.matchingEngine.config.EnableDeploymentVersions = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)

	// Overriding the dynamic config so that the rate-limiter has a refresh rate of 0. By default, the rate-limiter has a refresh rate of 1 minute which is too long for this test.
	s.matchingEngine.config.RateLimiterRefreshInterval = 0

	tl := "makeToast"
	dbq := newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	runID := uuid.NewString()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}
	deploymentName := "test-deployment"

	mgr := s.newPartitionManager(dbq.partition, s.matchingEngine.config)
	tqPTM, ok := mgr.(*taskQueuePartitionManagerImpl)
	s.True(ok)

	s.matchingEngine.updateTaskQueue(dbq.partition, mgr)
	mgr.Start()

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &commonpb.ActivityType{Name: activityTypeName}
	activityInput := payloads.EncodeString("Activity1 Input")

	identity := "nobody"

	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &historyservice.RecordActivityTaskStartedResponse{
				Attempt: 1,
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduledEventId, 0,
					&commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId: activityID,
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: taskQueue.Name,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
						ActivityType:           activityType,
						Input:                  activityInput,
						ScheduleToStartTimeout: durationpb.New(1 * time.Second),
						ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
						StartToCloseTimeout:    durationpb.New(1 * time.Second),
						HeartbeatTimeout:       durationpb.New(1 * time.Second),
					}),
			}, nil
		}).Times(2)

	pollFunc := func(maxDispatch float64, buildID string) (*matchingservice.PollActivityTaskQueueResponse, error) {
		return s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceID,
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{
				TaskQueue:         taskQueue,
				Identity:          identity,
				TaskQueueMetadata: &taskqueuepb.TaskQueueMetadata{MaxTasksPerSecond: &wrapperspb.DoubleValue{Value: maxDispatch}},
				DeploymentOptions: &deploymentpb.WorkerDeploymentOptions{
					DeploymentName:       deploymentName,
					BuildId:              buildID,
					WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
				},
			},
		}, metrics.NoopMetricsHandler)
	}

	const taskCount = 2
	resultChan := make(chan *matchingservice.PollActivityTaskQueueResponse)

	for i := int64(0); i < taskCount; i++ {
		maxDispatch := defaultTaskDispatchRPS
		if i == 1 {
			maxDispatch = 0 // second poller overrides the dispatch rate to 0
		}
		go func() {
			result, _ := pollFunc(float64(maxDispatch), strconv.FormatInt(int64(i), 10))
			resultChan <- result
		}()

		//nolint:forbidigo
		time.Sleep(10 * time.Millisecond) // Delay to allow the second poller coming in a little later.
	}

	// Update user data of the task queue so that the activity tasks generated are not treated as independent activities.
	// Independent activity tasks are those if the task queue the task is scheduled on is not part of the workflow's pinned
	// deployment.
	updateOptions := UserDataUpdateOptions{Source: "SyncDeploymentUserData"}
	_, err := tqPTM.GetUserDataManager().UpdateUserData(context.Background(), updateOptions, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {

		newData := &persistencespb.TaskQueueUserData{
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_ACTIVITY): {
					DeploymentData: &persistencespb.DeploymentData{
						Versions: []*deploymentspb.DeploymentVersionData{
							{
								Version: &deploymentspb.WorkerDeploymentVersion{
									DeploymentName: deploymentName,
									BuildId:        strconv.FormatInt(int64(0), 10),
								},
								RoutingUpdateTime: timestamppb.Now(),
								CurrentSinceTime:  timestamppb.Now(),
							},
							{
								Version: &deploymentspb.WorkerDeploymentVersion{
									DeploymentName: deploymentName,
									BuildId:        strconv.FormatInt(int64(1), 10),
								},
								RoutingUpdateTime: timestamppb.Now(),
								RampingSinceTime:  timestamppb.Now(),
							},
						},
					},
				},
			},
		}
		return newData, true, nil
	})
	s.NoError(err)

	for i := int64(0); i < taskCount; i++ {
		scheduledEventID := i * 3
		addRequest := matchingservice.AddActivityTaskRequest{
			NamespaceId:            namespaceID,
			Execution:              workflowExecution,
			ScheduledEventId:       scheduledEventID,
			TaskQueue:              taskQueue,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			VersionDirective: &taskqueuespb.TaskVersionDirective{
				Behavior: enumspb.VERSIONING_BEHAVIOR_PINNED,
				DeploymentVersion: &deploymentspb.WorkerDeploymentVersion{
					DeploymentName: deploymentName,
					BuildId:        strconv.FormatInt(int64(i), 10),
				},
			},
		}

		_, _, err = s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		s.NoError(err)
	}

	// Verifying that both the pollers don't receive any tasks since the overall dispatch rate has been set to 0
	for i := int64(0); i < taskCount; i++ {
		receivedResult := <-resultChan
		s.Nil(receivedResult.TaskToken)
	}

	// Restart the pollers with maxTasksPerSecond = defaultTaskDispatchRPS so that they can receive tasks
	maxDispatch := float64(defaultTaskDispatchRPS)
	for i := int64(0); i < taskCount; i++ {
		go func() {
			result, _ := pollFunc(maxDispatch, strconv.FormatInt(int64(i), 10))
			resultChan <- result
		}()
	}

	// Verifying that both the pollers receive the tasks which were added previously
	for i := int64(0); i < taskCount; i++ {
		receivedResult := <-resultChan

		s.NotNil(receivedResult)
		s.NotNil(receivedResult.TaskToken)
	}
}

func (s *matchingEngineSuite) TestConcurrentPublishConsumeActivities() {
	if s.newMatcher {
		s.T().Skip("test is flaky with new matcher")
	}
	dispatchLimitFn := func(int, int64) float64 {
		return defaultTaskDispatchRPS
	}
	const workerCount = 20
	const taskCount = 100
	throttleCt := s.concurrentPublishConsumeActivities(workerCount, taskCount, dispatchLimitFn)
	s.Zero(throttleCt)
}

func (s *matchingEngineSuite) TestConcurrentPublishConsumeActivitiesWithZeroDispatch() {
	s.T().Skip("Racy - times out ~50% of the time running locally with --race")
	// Set a short long poll expiration so that we don't have to wait too long for 0 throttling cases
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(20 * time.Millisecond)
	dispatchLimitFn := func(wc int, tc int64) float64 {
		if tc%50 == 0 && wc%5 == 0 { // Gets triggered atleast 20 times
			return 0
		}
		return defaultTaskDispatchRPS
	}
	const workerCount = 20
	const taskCount = 100
	throttleCt := s.concurrentPublishConsumeActivities(workerCount, taskCount, dispatchLimitFn)
	s.logger.Info("Number of tasks throttled", tag.Number(throttleCt))
	// atleast once from 0 dispatch poll, and until TTL is hit at which time throttle limit is reset
	// hard to predict exactly how many times, since the atomic.Value load might not have updated.
	s.GreaterOrEqual(throttleCt, 1)
}

func (s *matchingEngineSuite) concurrentPublishConsumeActivities(
	workerCount int,
	taskCount int64,
	dispatchLimitFn func(int, int64) float64,
) int64 {
	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope).WithTags(metrics.ServiceNameTag(primitives.MatchingService))
	s.matchingEngine.config.MinTaskThrottlingBurstSize = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(0)

	runID := uuid.NewString()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const rangeSize = 3
	var scheduledEventID int64 = 123
	namespaceID := uuid.NewString()
	tl := "makeToast"
	dbq := newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	mgr := s.newPartitionManager(dbq.partition, s.matchingEngine.config)
	s.matchingEngine.updateTaskQueue(dbq.partition, mgr)
	mgr.Start()

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	var wg sync.WaitGroup
	wg.Add(2 * workerCount)

	for p := 0; p < workerCount; p++ {
		go func() {
			defer wg.Done()
			for i := int64(0); i < taskCount; i++ {
				addRequest := matchingservice.AddActivityTaskRequest{
					NamespaceId:            namespaceID,
					Execution:              workflowExecution,
					ScheduledEventId:       scheduledEventID,
					TaskQueue:              taskQueue,
					ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
				}

				_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
				if err != nil {
					s.logger.Info("Failure in AddActivityTask", tag.Error(err))
					i--
				}
			}
		}()
	}

	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &commonpb.ActivityType{Name: activityTypeName}
	activityInput := payloads.EncodeString("Activity1 Input")
	activityHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{"tracing": payload.EncodeString("tracing data")},
	}

	identity := "nobody"

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &historyservice.RecordActivityTaskStartedResponse{
				Attempt: 1,
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduledEventId, 0,
					&commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId: activityID,
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: taskQueue.Name,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
						ActivityType:           activityType,
						Input:                  activityInput,
						Header:                 activityHeader,
						ScheduleToStartTimeout: durationpb.New(1 * time.Second),
						ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
						StartToCloseTimeout:    durationpb.New(1 * time.Second),
						HeartbeatTimeout:       durationpb.New(1 * time.Second),
					}),
			}, nil
		}).AnyTimes()

	for p := 0; p < workerCount; p++ {
		go func(wNum int) {
			defer wg.Done()
			for i := int64(0); i < taskCount; {
				maxDispatch := dispatchLimitFn(wNum, i)
				result, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
					NamespaceId: namespaceID,
					PollRequest: &workflowservice.PollActivityTaskQueueRequest{
						TaskQueue:         taskQueue,
						Identity:          identity,
						TaskQueueMetadata: &taskqueuepb.TaskQueueMetadata{MaxTasksPerSecond: &wrapperspb.DoubleValue{Value: maxDispatch}},
					},
				}, metrics.NoopMetricsHandler)
				s.NoError(err)
				s.NotNil(result)
				if len(result.TaskToken) == 0 {
					s.logger.Debug("empty poll returned")
					continue
				}
				s.EqualValues(activityID, result.ActivityId)
				s.EqualValues(activityType, result.ActivityType)
				s.EqualValues(activityInput, result.Input)
				s.EqualValues(activityHeader, result.Header)
				s.EqualValues(workflowExecution, result.WorkflowExecution)
				taskToken := &tokenspb.Task{
					Attempt:          1,
					NamespaceId:      namespaceID,
					WorkflowId:       workflowID,
					RunId:            runID,
					ScheduledEventId: scheduledEventID,
					ActivityId:       activityID,
					ActivityType:     activityTypeName,
				}
				resultToken, err := s.matchingEngine.tokenSerializer.Deserialize(result.TaskToken)
				s.NoError(err)
				protoassert.ProtoEqual(s.T(), taskToken, resultToken)
				i++
			}
		}(p)
	}
	wg.Wait()
	totalTasks := int(taskCount) * workerCount
	persisted := s.taskManager.getCreateTaskCount(dbq)
	s.True(persisted < totalTasks)
	expectedRange := int64((persisted + 1) / rangeSize)
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.LessOrEqual(expectedRange, s.taskManager.getQueueDataByKey(dbq).rangeID)
	s.EqualValues(0, s.taskManager.getTaskCount(dbq))

	syncCtr := scope.Snapshot().Counters()["test.sync_throttle_count+namespace="+matchingTestNamespace+",operation=TaskQueueMgr,taskqueue=makeToast"]
	bufCtr := scope.Snapshot().Counters()["test.buffer_throttle_count+namespace="+matchingTestNamespace+",operation=TaskQueueMgr,taskqueue=makeToast"]
	total := int64(0)
	if syncCtr != nil {
		total += syncCtr.Value()
	}
	if bufCtr != nil {
		total += bufCtr.Value()
	}
	return total
}

func (s *matchingEngineSuite) TestConcurrentPublishConsumeWorkflowTasks() {
	if s.newMatcher {
		s.T().Skip("not supported by new matcher; flaky")
	}

	runID := uuid.NewString()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const workerCount = 20
	const taskCount = 100
	const rangeSize = 5
	var scheduledEventID int64 = 123
	var startedEventID int64 = 1412

	namespaceID := uuid.NewString()
	tl := "makeToast"
	tlID := newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	var wg sync.WaitGroup
	wg.Add(2 * workerCount)
	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; i++ {
				addRequest := matchingservice.AddWorkflowTaskRequest{
					NamespaceId:            namespaceID,
					Execution:              workflowExecution,
					ScheduledEventId:       scheduledEventID,
					TaskQueue:              taskQueue,
					ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
				}

				_, _, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}
	workflowTypeName := "workflowType1"
	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}

	identity := "nobody"

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordWorkflowTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordWorkflowTaskStartedRequest")
			return &historyservice.RecordWorkflowTaskStartedResponse{
				PreviousStartedEventId: startedEventID,
				StartedEventId:         startedEventID,
				ScheduledEventId:       scheduledEventID,
				WorkflowType:           workflowType,
				Attempt:                1,
				History:                &historypb.History{Events: []*historypb.HistoryEvent{}},
				NextPageToken:          nil,
			}, nil
		}).AnyTimes()

	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; {
				result, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
					NamespaceId: namespaceID,
					PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
						TaskQueue: taskQueue,
						Identity:  identity,
					},
				}, metrics.NoopMetricsHandler)
				if err != nil {
					panic(err)
				}
				s.NotNil(result)
				if len(result.TaskToken) == 0 {
					s.logger.Debug("empty poll returned")
					continue
				}
				s.EqualValues(workflowExecution, result.WorkflowExecution)
				s.EqualValues(workflowType, result.WorkflowType)
				s.EqualValues(startedEventID, result.StartedEventId)
				s.EqualValues(workflowExecution, result.WorkflowExecution)
				taskToken := &tokenspb.Task{
					Attempt:          1,
					NamespaceId:      namespaceID,
					WorkflowId:       workflowID,
					RunId:            runID,
					ScheduledEventId: scheduledEventID,
					StartedEventId:   startedEventID,
				}
				resultToken, err := s.matchingEngine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}
				protoassert.ProtoEqual(s.T(), taskToken, resultToken)
				i++
			}
			wg.Done()
		}()
	}
	wg.Wait()
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
	totalTasks := taskCount * workerCount
	persisted := s.taskManager.getCreateTaskCount(tlID)
	s.True(persisted < totalTasks)
	expectedRange := int64((persisted + 1) / rangeSize)
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.LessOrEqual(expectedRange, s.taskManager.getQueueDataByKey(tlID).rangeID)
}

func (s *matchingEngineSuite) TestPollWithExpiredContext() {
	identity := "nobody"
	namespaceID := namespace.ID(uuid.NewString())
	tl := "makeToast"

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	// Try with cancelled context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	_, err := s.matchingEngine.PollActivityTaskQueue(ctx, &matchingservice.PollActivityTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  identity,
		},
	}, metrics.NoopMetricsHandler)

	s.Equal(ctx.Err(), err)

	// Try with expired context
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := s.matchingEngine.PollActivityTaskQueue(ctx, &matchingservice.PollActivityTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  identity,
		},
	}, metrics.NoopMetricsHandler)
	s.Nil(err)
	s.Equal(emptyPollActivityTaskQueueResponse, resp)
}

func (s *matchingEngineSuite) TestForceUnloadTaskQueue() {
	s.logger.Expect(testlogger.Error, "unexpected error dispatching task", tag.Error(errTaskQueueClosed))

	ctx := context.Background()
	namespaceID := uuid.NewString()
	identity := "nobody"

	// We unload a sticky queue so that we can verify the unload took effect by
	// attempting to add a task to it
	stickyQueue := &taskqueuepb.TaskQueue{Name: "sticky-queue", Kind: enumspb.TASK_QUEUE_KIND_STICKY}

	addTaskRequest := matchingservice.AddWorkflowTaskRequest{
		NamespaceId:      namespaceID,
		Execution:        &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewString()},
		ScheduledEventId: int64(0),
		TaskQueue:        stickyQueue,
	}

	// Poll to create the queue
	pollResp, err := s.matchingEngine.PollWorkflowTaskQueue(ctx, &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: stickyQueue,
			Identity:  identity,
		}},
		metrics.NoopMetricsHandler)
	s.Nil(err)
	s.NotNil(pollResp)

	// Sanity check: adding a task should succeed with the queue loaded
	_, _, err = s.matchingEngine.AddWorkflowTask(ctx, &addTaskRequest)
	s.NoError(err)

	// Force unload the sticky queue
	unloadResp, err := s.matchingEngine.ForceUnloadTaskQueuePartition(ctx, &matchingservice.ForceUnloadTaskQueuePartitionRequest{
		NamespaceId: namespaceID,
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
			TaskQueue:     stickyQueue.Name,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		},
	})
	s.NoError(err)
	s.NotNil(unloadResp)
	s.True(unloadResp.WasLoaded)

	// Adding a task should now fast fail
	_, _, err = s.matchingEngine.AddWorkflowTask(ctx, &addTaskRequest)
	s.ErrorAs(err, new(*serviceerrors.StickyWorkerUnavailable))
}

func (s *matchingEngineSuite) TestMultipleEnginesActivitiesRangeStealing() {
	if s.newMatcher {
		s.T().Skip("test is flaky with new matcher")
	}
	runID := uuid.NewString()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const engineCount = 2
	const taskCount = 400
	const iterations = 2
	const rangeSize = 10
	var scheduledEventID int64 = 123

	namespaceID := uuid.NewString()
	tl := "makeToast"
	tlID := newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	engines := make([]*matchingEngineImpl, engineCount)
	for p := 0; p < engineCount; p++ {
		e := s.newMatchingEngine(s.newConfig(), s.classicTaskManager, s.fairTaskManager)
		e.config.RangeSize = rangeSize
		engines[p] = e
		e.Start()
	}

	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; i++ {
				addRequest := matchingservice.AddActivityTaskRequest{
					NamespaceId:            namespaceID,
					Execution:              workflowExecution,
					ScheduledEventId:       scheduledEventID,
					TaskQueue:              taskQueue,
					ScheduleToStartTimeout: timestamp.DurationFromSeconds(600),
				}
				// scheduledEventID is the key for the workflow task, so needs a different
				// scheduledEventID for each task for deduplication logic below
				scheduledEventID++

				_, _, err := engine.AddActivityTask(context.Background(), &addRequest)
				if err != nil {
					if _, ok := err.(*persistence.ConditionFailedError); ok {
						i-- // retry adding
					} else {
						panic(fmt.Sprintf("errType=%T, err=%v", err, err))
					}
				}
			}
		}
	}

	s.EqualValues(iterations*engineCount*taskCount, s.taskManager.getCreateTaskCount(tlID))

	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &commonpb.ActivityType{Name: activityTypeName}
	activityInput := payloads.EncodeString("Activity1 Input")

	identity := "nobody"

	startedTasks := make(map[int64]struct{})

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordActivityTaskStartedResponse, error) {
			if _, ok := startedTasks[taskRequest.GetScheduledEventId()]; ok {
				s.logger.Debug("From error function Mock Received DUPLICATED RecordActivityTaskStartedRequest", tag.NewInt64("scheduled-event-id", taskRequest.GetScheduledEventId()))
				return nil, serviceerror.NewNotFound("already started")
			}

			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest", tag.NewInt64("scheduled-event-id", taskRequest.GetScheduledEventId()))
			startedTasks[taskRequest.GetScheduledEventId()] = struct{}{}
			return &historyservice.RecordActivityTaskStartedResponse{
				Attempt: 1,
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduledEventId, 0,
					&commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId: activityID,
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: taskQueue.Name,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
						ActivityType:           activityType,
						Input:                  activityInput,
						ScheduleToStartTimeout: durationpb.New(600 * time.Second),
						ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
						StartToCloseTimeout:    durationpb.New(1 * time.Second),
						HeartbeatTimeout:       durationpb.New(1 * time.Second),
					}),
			}, nil
		}).AnyTimes()
	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; /* incremented explicitly to skip empty polls */ {
				result, err := engine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
					NamespaceId: namespaceID,
					PollRequest: &workflowservice.PollActivityTaskQueueRequest{
						TaskQueue: taskQueue,
						Identity:  identity,
					},
				}, metrics.NoopMetricsHandler)
				if err != nil {
					panic(err)
				}
				s.NotNil(result)
				if len(result.TaskToken) == 0 {
					s.logger.Debug("empty poll returned")
					continue
				}
				s.EqualValues(activityID, result.ActivityId)
				s.EqualValues(activityType, result.ActivityType)
				s.EqualValues(activityInput, result.Input)
				s.EqualValues(workflowExecution, result.WorkflowExecution)
				taskToken := &tokenspb.Task{
					Attempt:      1,
					NamespaceId:  namespaceID,
					WorkflowId:   workflowID,
					RunId:        runID,
					ActivityId:   activityID,
					ActivityType: activityTypeName,
				}
				resultToken, err := engine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}

				// we don't know the expected scheduledEventID for the task polled, so just set it to the result
				taskToken.ScheduledEventId = resultToken.ScheduledEventId
				protoassert.ProtoEqual(s.T(), taskToken, resultToken)
				i++
			}
		}
	}

	for _, e := range engines {
		e.Stop()
	}

	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
	totalTasks := taskCount * engineCount * iterations
	persisted := s.taskManager.getCreateTaskCount(tlID)
	// No sync matching as all messages are published first
	s.EqualValues(totalTasks, persisted)
	expectedRange := int64((persisted + 1) / rangeSize)
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.LessOrEqual(expectedRange, s.taskManager.getQueueDataByKey(tlID).rangeID)
}

func (s *matchingEngineSuite) TestMultipleEnginesWorkflowTasksRangeStealing() {
	if s.newMatcher {
		s.T().Skip("test is flaky with new matcher")
	}
	runID := uuid.NewString()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const engineCount = 2
	const taskCount = 400
	const iterations = 2
	const rangeSize = 10
	var scheduledEventID int64 = 123

	namespaceID := uuid.NewString()
	tl := "makeToast"
	tlID := newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	engines := make([]*matchingEngineImpl, engineCount)
	for p := 0; p < engineCount; p++ {
		e := s.newMatchingEngine(s.newConfig(), s.classicTaskManager, s.fairTaskManager)
		e.config.RangeSize = rangeSize
		engines[p] = e
		e.Start()
	}

	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; i++ {
				addRequest := matchingservice.AddWorkflowTaskRequest{
					NamespaceId:            namespaceID,
					Execution:              workflowExecution,
					ScheduledEventId:       scheduledEventID,
					TaskQueue:              taskQueue,
					ScheduleToStartTimeout: timestamp.DurationFromSeconds(600),
				}
				// scheduledEventID is the key for the workflow task, so needs a different
				// scheduledEventID for each task for deduplication logic below
				scheduledEventID++

				_, _, err := engine.AddWorkflowTask(context.Background(), &addRequest)
				if err != nil {
					if _, ok := err.(*persistence.ConditionFailedError); ok {
						i-- // retry adding
					} else {
						panic(fmt.Sprintf("errType=%T, err=%v", err, err))
					}
				}
			}
		}
	}
	workflowTypeName := "workflowType1"
	workflowType := &commonpb.WorkflowType{Name: workflowTypeName}

	identity := "nobody"
	var startedEventID int64 = 1412

	startedTasks := make(map[int64]struct{})

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordWorkflowTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
			if _, ok := startedTasks[taskRequest.GetScheduledEventId()]; ok {
				s.logger.Debug("From error function Mock Received DUPLICATED RecordWorkflowTaskStartedRequest", tag.NewInt64("scheduled-event-id", taskRequest.GetScheduledEventId()))
				return nil, serviceerrors.NewTaskAlreadyStarted("Workflow")
			}

			s.logger.Debug("Mock Received RecordWorkflowTaskStartedRequest", tag.NewInt64("scheduled-event-id", taskRequest.GetScheduledEventId()))
			startedTasks[taskRequest.GetScheduledEventId()] = struct{}{}
			return &historyservice.RecordWorkflowTaskStartedResponse{
				PreviousStartedEventId: startedEventID,
				StartedEventId:         startedEventID,
				ScheduledEventId:       taskRequest.GetScheduledEventId(),
				WorkflowType:           workflowType,
				Attempt:                1,
				History:                &historypb.History{Events: []*historypb.HistoryEvent{}},
				NextPageToken:          nil,
			}, nil
		}).AnyTimes()

	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; /* incremented explicitly to skip empty polls */ {
				result, err := engine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
					NamespaceId: namespaceID,
					PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
						TaskQueue: taskQueue,
						Identity:  identity,
					},
				}, metrics.NoopMetricsHandler)
				if err != nil {
					panic(err)
				}
				s.NotNil(result)
				if len(result.TaskToken) == 0 {
					s.logger.Debug("empty poll returned")
					continue
				}
				s.EqualValues(workflowExecution, result.WorkflowExecution)
				s.EqualValues(workflowType, result.WorkflowType)
				s.EqualValues(startedEventID, result.StartedEventId)
				s.EqualValues(workflowExecution, result.WorkflowExecution)
				taskToken := &tokenspb.Task{
					Attempt:        1,
					NamespaceId:    namespaceID,
					WorkflowId:     workflowID,
					RunId:          runID,
					StartedEventId: startedEventID,
				}
				resultToken, err := engine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}

				// we don't know the expected scheduledEventID for the task polled, so just set it to the result
				taskToken.ScheduledEventId = resultToken.ScheduledEventId
				protoassert.ProtoEqual(s.T(), taskToken, resultToken)
				i++
			}
		}
	}

	for _, e := range engines {
		e.Stop()
	}

	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
	totalTasks := taskCount * engineCount * iterations
	persisted := s.taskManager.getCreateTaskCount(tlID)
	// No sync matching as all messages are published first
	s.EqualValues(totalTasks, persisted)
	expectedRange := int64((persisted + 1) / rangeSize)
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.LessOrEqual(expectedRange, s.taskManager.getQueueDataByKey(tlID).rangeID)
}

func (s *matchingEngineSuite) TestAddTaskAfterStartFailure() {
	if s.newMatcher {
		s.T().Skip("not supported by new matcher")
	}

	// test default is 100ms, but make it longer for this test so it's not flaky
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Second)

	namespaceID := uuid.NewString()
	tl := "makeToast"
	dbq := newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	_, _, err := s.matchingEngine.AddActivityTask(context.Background(),
		&matchingservice.AddActivityTaskRequest{
			NamespaceId:      namespaceID,
			Execution:        &commonpb.WorkflowExecution{RunId: uuid.NewString(), WorkflowId: "workflow1"},
			ScheduledEventId: int64(0),
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: tl,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		})
	s.NoError(err)
	s.EqualValues(1, s.taskManager.getTaskCount(dbq))

	task1, _, err := s.matchingEngine.pollTask(context.Background(), dbq.partition, &pollMetadata{})
	s.NoError(err)

	task1.finish(serviceerror.NewInternal("test error"), true)
	s.EqualValues(1, s.taskManager.getTaskCount(dbq))

	task2, _, err := s.matchingEngine.pollTask(context.Background(), dbq.partition, &pollMetadata{})
	s.NoError(err)
	protoassert.ProtoEqual(s.T(), task1.event.Data, task2.event.Data)
	s.NotEqual(task1.event.GetTaskId(), task2.event.GetTaskId(), "IDs should not match")

	task2.finish(nil, true)
	s.EqualValues(0, s.taskManager.getTaskCount(dbq))
}

// TODO: should be moved to backlog_manager_test
func (s *matchingEngineSuite) TestTaskQueueManagerGetTaskBatch() {
	if s.newMatcher {
		s.T().Skip("not supported by new matcher")
	}

	runID := uuid.NewString()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	namespaceID := uuid.NewString()
	tl := "makeToast"
	dbq := newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	const taskCount = 1200
	const rangeSize = 10
	s.matchingEngine.config.RangeSize = rangeSize

	// add taskCount tasks
	for i := int64(0); i < taskCount; i++ {
		scheduledEventID := i * 3
		addRequest := matchingservice.AddActivityTaskRequest{
			NamespaceId:            namespaceID,
			Execution:              workflowExecution,
			ScheduledEventId:       scheduledEventID,
			TaskQueue:              taskQueue,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		}

		_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		s.NoError(err)
	}

	tlMgr := s.getPhysicalTaskQueueManagerImplFromKey(dbq)
	s.EqualValues(taskCount, s.taskManager.getTaskCount(dbq))

	// wait until all tasks are read by the task pump and enqueued into the in-memory buffer
	// at the end of this step, ackManager readLevel will also be equal to the buffer size
	blm := tlMgr.backlogMgr.(*backlogManagerImpl)
	expectedBufSize := min(cap(blm.taskReader.taskBuffer), taskCount)
	s.Eventually(func() bool { return len(blm.taskReader.taskBuffer) == expectedBufSize },
		time.Second, 5*time.Millisecond)

	// unload the queue and stop all goroutines that read / write tasks in the background
	// remainder of this test works with the in-memory buffer
	tlMgr.UnloadFromPartitionManager(unloadCauseUnspecified)

	// setReadLevel should NEVER be called without updating ackManager.outstandingTasks
	// This is only for unit test purpose
	blm.taskAckManager.setReadLevel(blm.getDB().GetMaxReadLevel(0))
	batch, err := blm.taskReader.getTaskBatch(context.Background())
	s.Nil(err)
	s.EqualValues(0, len(batch.tasks))
	s.EqualValues(blm.getDB().GetMaxReadLevel(0), batch.readLevel)
	s.True(batch.isReadBatchDone)

	blm.taskAckManager.setReadLevel(0)
	batch, err = blm.taskReader.getTaskBatch(context.Background())
	s.Nil(err)
	s.EqualValues(rangeSize, len(batch.tasks))
	s.EqualValues(rangeSize, batch.readLevel)
	s.True(batch.isReadBatchDone)

	s.setupRecordActivityTaskStartedMock(tl)

	// reset the ackManager readLevel to the buffer size and consume
	// the in-memory tasks by calling Poll API - assert ackMgr state
	// at the end
	blm.taskAckManager.setReadLevel(int64(expectedBufSize))

	// complete rangeSize events
	for i := int64(0); i < rangeSize; i++ {
		identity := "nobody"
		result, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceID,
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{
				TaskQueue: taskQueue,
				Identity:  identity,
			},
		}, metrics.NoopMetricsHandler)

		s.NoError(err)
		s.NotNil(result)
		s.NotEqual(emptyPollActivityTaskQueueResponse, result)
		if len(result.TaskToken) == 0 {
			s.logger.Debug("empty poll returned")
			continue
		}
	}
	s.EqualValues(taskCount-rangeSize, s.taskManager.getTaskCount(dbq))
	batch, err = blm.taskReader.getTaskBatch(context.Background())
	s.Nil(err)
	s.True(0 < len(batch.tasks) && len(batch.tasks) <= rangeSize)
	s.True(batch.isReadBatchDone)
}

func (s *matchingEngineSuite) TestTaskQueueManager_CyclingBehavior() {
	config := s.newConfig()
	dbq := newUnversionedRootQueueKey(uuid.NewString(), "makeToast", enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	for i := 0; i < 4; i++ {
		prevGetTasksCount := s.taskManager.getGetTasksCount(dbq)

		mgr := s.newPartitionManager(dbq.partition, config)
		mgr.Start()

		// tlMgr.taskWriter startup is async so give it time to complete
		time.Sleep(100 * time.Millisecond)
		mgr.Stop(unloadCauseUnspecified)

		s.LessOrEqual(s.taskManager.getGetTasksCount(dbq)-prevGetTasksCount, 1)
	}
}

// TODO: should be moved to backlog_manager_test
func (s *matchingEngineSuite) TestTaskExpiryAndCompletion() {
	if s.newMatcher {
		s.T().Skip("not supported by new matcher")
	}

	runID := uuid.NewString()
	workflowID := uuid.NewString()
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	namespaceID := uuid.NewString()
	tl := "task-expiry-completion-tl0"
	dbq := newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	const taskCount = 20 // must be multiple of 4
	const rangeSize = 10
	s.matchingEngine.config.RangeSize = rangeSize
	s.matchingEngine.config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(2)

	testCases := []struct {
		maxTimeBtwnDeletes time.Duration
	}{
		{time.Minute},     // test taskGC deleting due to size threshold
		{time.Nanosecond}, // test taskGC deleting due to time condition
	}

	for _, tc := range testCases {
		for i := int64(0); i < taskCount; i++ {
			scheduledEventID := i * 3
			addRequest := matchingservice.AddActivityTaskRequest{
				NamespaceId:            namespaceID,
				Execution:              workflowExecution,
				ScheduledEventId:       scheduledEventID,
				TaskQueue:              taskQueue,
				ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			}
			switch i % 4 {
			case 0:
				// simulates creating a task whose scheduledToStartTimeout is already expired
				addRequest.ScheduleToStartTimeout = timestamp.DurationFromSeconds(-5)
			case 2:
				// simulates creating a task which will time out in the buffer
				addRequest.ScheduleToStartTimeout = durationpb.New(250 * time.Millisecond)
			}
			_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
			s.NoError(err)
		}

		tlMgr := s.getPhysicalTaskQueueManagerImplFromKey(dbq)
		s.EqualValues(taskCount, s.taskManager.getTaskCount(dbq))
		blm := tlMgr.backlogMgr.(*backlogManagerImpl)

		// wait until all tasks are loaded by into in-memory buffers by task queue manager
		// the buffer size should be one less than expected because dispatcher will dequeue the head
		// 1/4 should be thrown out because they are expired before they hit the buffer
		s.Eventually(func() bool { return len(blm.taskReader.taskBuffer) >= (3*taskCount/4 - 1) },
			time.Second, 5*time.Millisecond)

		// ensure the 1/4 of tasks with small ScheduleToStartTimeout will be expired when they come out of the buffer
		time.Sleep(300 * time.Millisecond)

		maxTimeBetweenTaskDeletes = tc.maxTimeBtwnDeletes

		s.setupRecordActivityTaskStartedMock(tl)

		pollReq := &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceID,
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{TaskQueue: taskQueue, Identity: "test"},
		}

		remaining := taskCount
		for i := 0; i < 2; i++ {
			// verify that (1) expired tasks are not returned in poll result (2) taskCleaner deletes tasks correctly
			for i := int64(0); i < taskCount/4; i++ {
				result, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), pollReq, metrics.NoopMetricsHandler)
				s.NoError(err)
				s.NotNil(result)
				s.NotEqual(result, emptyPollActivityTaskQueueResponse)
			}
			remaining -= taskCount / 2
			// since every other task is expired, we expect half the tasks to be deleted
			// after poll consumed 1/4th of what is available.
			// however, the gc is best-effort and might not run exactly when we want it to.
			// various thread interleavings between the two task reader threads and this one
			// might leave the gc behind by up to 3 tasks, or ahead by up to 1.
			delta := remaining - s.taskManager.getTaskCount(dbq)
			s.Truef(-3 <= delta && delta <= 1, "remaining %d, getTaskCount %d", remaining, s.taskManager.getTaskCount(dbq))
		}
		// ensure full gc for the next case (twice in case one doesn't get the gc lock)
		blm.taskGC.RunNow(blm.taskAckManager.getAckLevel())
		blm.taskGC.RunNow(blm.taskAckManager.getAckLevel())
	}
}

func (s *matchingEngineSuite) TestGetVersioningData() {
	namespaceID := namespace.ID(uuid.NewString())
	tq := "tupac"

	// Ensure we can fetch without first needing to set anything
	res, err := s.matchingEngine.GetWorkerBuildIdCompatibility(context.Background(), &matchingservice.GetWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceID.String(),
		Request: &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespaceID.String(),
			TaskQueue: tq,
			MaxSets:   0,
		},
	})
	s.NoError(err)
	s.NotNil(res)

	// Set a long list of versions
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("%d", i)
		res, err := s.matchingEngine.UpdateWorkerBuildIdCompatibility(context.Background(), &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
			NamespaceId: namespaceID.String(),
			TaskQueue:   tq,
			Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest_{
				ApplyPublicRequest: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest{
					Request: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
						Namespace: namespaceID.String(),
						TaskQueue: tq,
						Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
							AddNewBuildIdInNewDefaultSet: id,
						},
					},
				},
			},
		})
		s.NoError(err)
		s.NotNil(res)
	}
	// Make a long compat-versions chain
	for i := 0; i < 80; i++ {
		id := fmt.Sprintf("9.%d", i)
		prevCompat := fmt.Sprintf("9.%d", i-1)
		if i == 0 {
			prevCompat = "9"
		}
		res, err := s.matchingEngine.UpdateWorkerBuildIdCompatibility(context.Background(), &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
			NamespaceId: namespaceID.String(),
			TaskQueue:   tq,
			Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest_{
				ApplyPublicRequest: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest{
					Request: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
						Namespace: namespaceID.String(),
						TaskQueue: tq,
						Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleBuildId{
							AddNewCompatibleBuildId: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewCompatibleVersion{
								NewBuildId:                id,
								ExistingCompatibleBuildId: prevCompat,
								MakeSetDefault:            false,
							},
						},
					},
				},
			},
		})
		s.NoError(err)
		s.NotNil(res)
	}

	// Ensure they all exist
	res, err = s.matchingEngine.GetWorkerBuildIdCompatibility(context.Background(), &matchingservice.GetWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceID.String(),
		Request: &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespaceID.String(),
			TaskQueue: tq,
			MaxSets:   0,
		},
	})
	s.NoError(err)
	majorSets := res.GetResponse().GetMajorVersionSets()
	curDefault := majorSets[len(majorSets)-1]
	s.NotNil(curDefault)
	s.Equal("9", curDefault.GetBuildIds()[0])
	lastNode := curDefault.GetBuildIds()[len(curDefault.GetBuildIds())-1]
	s.Equal("9.79", lastNode)
	s.Equal("0", majorSets[0].GetBuildIds()[0])

	// Ensure depth limiting works
	res, err = s.matchingEngine.GetWorkerBuildIdCompatibility(context.Background(), &matchingservice.GetWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceID.String(),
		Request: &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespaceID.String(),
			TaskQueue: tq,
			MaxSets:   1,
		},
	})
	s.NoError(err)
	majorSets = res.GetResponse().GetMajorVersionSets()
	curDefault = majorSets[len(majorSets)-1]
	s.Equal("9", curDefault.GetBuildIds()[0])
	lastNode = curDefault.GetBuildIds()[len(curDefault.GetBuildIds())-1]
	s.Equal("9.79", lastNode)
	s.Equal(1, len(majorSets))

	res, err = s.matchingEngine.GetWorkerBuildIdCompatibility(context.Background(), &matchingservice.GetWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceID.String(),
		Request: &workflowservice.GetWorkerBuildIdCompatibilityRequest{
			Namespace: namespaceID.String(),
			TaskQueue: tq,
			MaxSets:   5,
		},
	})
	s.NoError(err)
	majorSets = res.GetResponse().GetMajorVersionSets()
	s.Equal("5", majorSets[0].GetBuildIds()[0])
}

func (s *matchingEngineSuite) TestGetTaskQueueUserData_NoData() {
	namespaceID := namespace.ID(uuid.NewString())
	tq := "tupac"

	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID.String(),
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	s.Nil(res.UserData.GetData())
}

func (s *matchingEngineSuite) TestGetTaskQueueUserData_ReturnsData() {
	namespaceID := namespace.ID(uuid.NewString())
	tq := "tupac"

	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 123456}},
	}
	s.NoError(s.classicTaskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID.String(),
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				tq: &persistence.SingleTaskQueueUserDataUpdate{
					UserData: userData,
				},
			},
		}))
	userData.Version++

	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID.String(),
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	protoassert.ProtoEqual(s.T(), res.UserData, userData)
}

func (s *matchingEngineSuite) TestGetTaskQueueUserData_ReturnsEmpty() {
	namespaceID := namespace.ID(uuid.NewString())
	tq := "tupac"

	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 123456}},
	}
	s.NoError(s.classicTaskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID.String(),
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				tq: &persistence.SingleTaskQueueUserDataUpdate{
					UserData: userData,
				},
			},
		}))
	userData.Version++

	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID.String(),
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: userData.Version,
	})
	s.NoError(err)
	s.Nil(res.UserData.GetData())
}

func (s *matchingEngineSuite) TestGetTaskQueueUserData_LongPoll_Expires() {
	namespaceID := namespace.ID(uuid.NewString())
	tq := "tupac"

	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 123456}},
	}
	s.NoError(s.classicTaskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID.String(),
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				tq: &persistence.SingleTaskQueueUserDataUpdate{
					UserData: userData,
				},
			},
		}))
	userData.Version++

	// GetTaskQueueUserData will try to return 5s with a min of 1s before the deadline, so this will block 1s
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	res, err := s.matchingEngine.GetTaskQueueUserData(ctx, &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID.String(),
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: userData.Version,
		WaitNewData:              true,
	})
	s.NoError(err)
	s.Nil(res.UserData.GetData())
	elapsed := time.Since(start)
	s.Greater(elapsed, 900*time.Millisecond)
}

func (s *matchingEngineSuite) TestGetTaskQueueUserData_LongPoll_WakesUp_FromNothing() {
	namespaceID := namespace.ID(uuid.NewString())
	tq := "tupac"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		time.Sleep(200 * time.Millisecond)

		_, err := s.matchingEngine.UpdateWorkerBuildIdCompatibility(context.Background(), &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
			NamespaceId: namespaceID.String(),
			TaskQueue:   tq,
			Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest_{
				ApplyPublicRequest: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest{
					Request: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
						Namespace: namespaceID.String(),
						TaskQueue: tq,
						Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
							AddNewBuildIdInNewDefaultSet: "v1",
						},
					},
				},
			},
		})
		s.NoError(err)
	}()

	res, err := s.matchingEngine.GetTaskQueueUserData(ctx, &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID.String(),
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0, // must be zero to start
		WaitNewData:              true,
	})
	s.NoError(err)
	s.NotNil(res.UserData.Data.VersioningData)
}

func (s *matchingEngineSuite) TestGetTaskQueueUserData_LongPoll_WakesUp_From2to3() {
	namespaceID := namespace.ID(uuid.NewString())
	tq := "tupac"

	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 123456}},
	}
	s.NoError(s.classicTaskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID.String(),
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				tq: &persistence.SingleTaskQueueUserDataUpdate{
					UserData: userData,
				},
			},
		}))
	userData.Version++

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		time.Sleep(200 * time.Millisecond)

		_, err := s.matchingEngine.UpdateWorkerBuildIdCompatibility(context.Background(), &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
			NamespaceId: namespaceID.String(),
			TaskQueue:   tq,
			Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest_{
				ApplyPublicRequest: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest{
					Request: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
						Namespace: namespaceID.String(),
						TaskQueue: tq,
						Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{
							AddNewBuildIdInNewDefaultSet: "v1",
						},
					},
				},
			},
		})
		s.NoError(err)
	}()

	res, err := s.matchingEngine.GetTaskQueueUserData(ctx, &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID.String(),
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: userData.Version,
		WaitNewData:              true,
	})
	s.NoError(err)
	s.True(hlc.Greater(res.UserData.Data.Clock, userData.Data.Clock))
	s.NotNil(res.UserData.Data.VersioningData)
}

func (s *matchingEngineSuite) TestGetTaskQueueUserData_LongPoll_Closes() {
	namespaceID := namespace.ID(uuid.NewString())
	tq := "tupac"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		time.Sleep(200 * time.Millisecond)
		_, _ = s.matchingEngine.ForceUnloadTaskQueuePartition(context.Background(), &matchingservice.ForceUnloadTaskQueuePartitionRequest{
			NamespaceId: namespaceID.String(),
			TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
				TaskQueue:     tq,
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			},
		})
	}()

	res, err := s.matchingEngine.GetTaskQueueUserData(ctx, &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:   namespaceID.String(),
		TaskQueue:     tq,
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		WaitNewData:   true,
	})
	s.NoError(err)
	s.Nil(res.UserData)
}

func (s *matchingEngineSuite) TestUpdateUserData_FailsOnKnownVersionMismatch() {
	namespaceID := namespace.ID(uuid.NewString())
	tq := "tupac"

	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 123456}},
	}

	err := s.classicTaskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID.String(),
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				tq: &persistence.SingleTaskQueueUserDataUpdate{
					UserData: userData,
				},
			},
		})
	s.NoError(err)

	_, err = s.matchingEngine.UpdateWorkerBuildIdCompatibility(context.Background(), &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceID.String(),
		TaskQueue:   tq,
		Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_RemoveBuildIds_{
			RemoveBuildIds: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_RemoveBuildIds{
				KnownUserDataVersion: 1,
			},
		},
	})
	var failedPreconditionError *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionError)
}

func (s *matchingEngineSuite) TestUnknownBuildId_Match() {
	namespaceID := uuid.NewString()
	tq := "makeToast"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.mockMatchingClient.EXPECT().UpdateWorkerBuildIdCompatibility(gomock.Any(), &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceID,
		TaskQueue:   tq,
		Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_PersistUnknownBuildId{
			PersistUnknownBuildId: "unknown",
		},
	}).Return(&matchingservice.UpdateWorkerBuildIdCompatibilityResponse{}, nil).AnyTimes() // might get called again on dispatch from spooled

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		_, _, err := s.matchingEngine.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
			NamespaceId:            namespaceID,
			Execution:              &commonpb.WorkflowExecution{RunId: "run", WorkflowId: "wf"},
			ScheduledEventId:       123,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			// do not set ForwardedSource, allow to go to db
			VersionDirective: worker_versioning.MakeBuildIdDirective("unknown"),
		})
		s.NoError(err)
		wg.Done()
	}()

	go func() {
		prtn := newRootPartition(namespaceID, tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		task, _, err := s.matchingEngine.pollTask(ctx, prtn, &pollMetadata{
			workerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
				BuildId:       "unknown",
				UseVersioning: true,
			},
		})
		s.NoError(err)
		s.Equal("wf", task.event.Data.WorkflowId)
		s.Equal(int64(123), task.event.Data.ScheduledEventId)
		task.finish(nil, true)
		wg.Done()
	}()

	wg.Wait()
}

func (s *matchingEngineSuite) TestDemotedMatch() {
	namespaceID := uuid.NewString()
	tq := "makeToast"
	build0 := "build0"
	build1 := "build1"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// add build0 as the overall default
	clk := hlc.Zero(1)
	userData := &persistencespb.TaskQueueUserData{
		Clock: clk,
		VersioningData: &persistencespb.VersioningData{
			VersionSets: []*persistencespb.CompatibleVersionSet{
				{
					SetIds: []string{hashBuildId(build0)},
					BuildIds: []*persistencespb.BuildId{
						mkBuildId(build0, clk),
					},
					BecameDefaultTimestamp: clk,
				},
			},
		},
	}

	err := s.classicTaskManager.UpdateTaskQueueUserData(ctx, &persistence.UpdateTaskQueueUserDataRequest{
		NamespaceID: namespaceID,
		Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
			tq: &persistence.SingleTaskQueueUserDataUpdate{
				UserData: &persistencespb.VersionedTaskQueueUserData{
					Data:    userData,
					Version: 34,
				},
			},
		},
	})
	s.Assert().NoError(err)

	// add a task for build0, will get spooled in its set
	_, _, err = s.matchingEngine.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
		NamespaceId:      namespaceID,
		Execution:        &commonpb.WorkflowExecution{RunId: "run", WorkflowId: "wf"},
		ScheduledEventId: 123,
		TaskQueue:        &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		VersionDirective: worker_versioning.MakeBuildIdDirective(build0),
	})
	s.NoError(err)
	// allow taskReader to finish starting dispatch loop so that we can unload tqms cleanly
	time.Sleep(10 * time.Millisecond)

	// unload base and versioned tqMgr. note: unload the partition manager unloads both
	prtn := newRootPartition(namespaceID, tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	baseTqm, _, err := s.matchingEngine.getTaskQueuePartitionManager(ctx, prtn, false, loadCauseUnspecified)
	s.NoError(err)
	s.NotNil(baseTqm)
	s.matchingEngine.unloadTaskQueuePartition(baseTqm, unloadCauseUnspecified)
	// wait for taskReader goroutines to exit
	baseTqm.(*taskQueuePartitionManagerImpl).userDataManager.(*userDataManagerImpl).goroGroup.Wait()

	// both are now unloaded. change versioning data to merge unknown into another set.
	userData = &persistencespb.TaskQueueUserData{
		Clock: clk,
		VersioningData: &persistencespb.VersioningData{
			VersionSets: []*persistencespb.CompatibleVersionSet{
				{
					// make build0 set the demoted one to test demoted set loading.
					// it works the other way too but doesn't test anything new.
					SetIds: []string{hashBuildId(build1), hashBuildId(build0)},
					BuildIds: []*persistencespb.BuildId{
						mkBuildId(build0, clk),
						mkBuildId(build1, clk),
					},
					BecameDefaultTimestamp: clk,
				},
			},
		},
	}

	err = s.classicTaskManager.UpdateTaskQueueUserData(ctx, &persistence.UpdateTaskQueueUserDataRequest{
		NamespaceID: namespaceID,
		Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
			tq: &persistence.SingleTaskQueueUserDataUpdate{
				UserData: &persistencespb.VersionedTaskQueueUserData{
					Data:    userData,
					Version: 34,
				},
			},
		},
	})
	s.NoError(err)

	// now poll for the task
	task, _, err := s.matchingEngine.pollTask(ctx, prtn, &pollMetadata{
		workerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			BuildId:       build1,
			UseVersioning: true,
		},
	})
	s.Require().NoError(err)
	s.Equal("wf", task.event.Data.WorkflowId)
	s.Equal(int64(123), task.event.Data.ScheduledEventId)
	task.finish(nil, true)
}

func (s *matchingEngineSuite) TestUnloadOnMembershipChange() {
	// need to create a new engine for this test to customize mockServiceResolver
	s.mockServiceResolver = membership.NewMockServiceResolver(s.controller)
	s.mockServiceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockServiceResolver.EXPECT().RemoveListener(gomock.Any()).AnyTimes()

	self := s.mockHostInfoProvider.HostInfo()
	other := membership.NewHostInfoFromAddress("other")

	config := s.newConfig()
	config.MembershipUnloadDelay = dynamicconfig.GetDurationPropertyFn(10 * time.Millisecond)
	e := s.newMatchingEngine(config, s.classicTaskManager, s.fairTaskManager)
	e.Start()
	defer e.Stop()

	p1, err := tqid.NormalPartitionFromRpcName("makeToast", uuid.NewString(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.NoError(err)
	p2, err := tqid.NormalPartitionFromRpcName("makeToast", uuid.NewString(), enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.NoError(err)

	_, _, err = e.getTaskQueuePartitionManager(context.Background(), p1, true, loadCauseUnspecified)
	s.NoError(err)
	_, _, err = e.getTaskQueuePartitionManager(context.Background(), p2, true, loadCauseUnspecified)
	s.NoError(err)

	s.Equal(2, len(e.getTaskQueuePartitions(1000)))

	s.mockServiceResolver.EXPECT().Lookup(nexusEndpointsTablePartitionRoutingKey).Return(self, nil).AnyTimes()

	// signal membership changed and give time for loop to wake up
	s.mockServiceResolver.EXPECT().Lookup(p1.RoutingKey()).Return(self, nil)
	s.mockServiceResolver.EXPECT().Lookup(p2.RoutingKey()).Return(self, nil)
	e.membershipChangedCh <- nil
	time.Sleep(50 * time.Millisecond)
	s.Equal(2, len(e.getTaskQueuePartitions(1000)), "nothing should be unloaded yet")

	// signal again but p2 doesn't belong to us anymore
	s.mockServiceResolver.EXPECT().Lookup(p1.RoutingKey()).Return(self, nil)
	s.mockServiceResolver.EXPECT().Lookup(p2.RoutingKey()).Return(other, nil).Times(2)
	e.membershipChangedCh <- nil
	s.Eventually(func() bool {
		return len(e.getTaskQueuePartitions(1000)) == 1
	}, 100*time.Millisecond, 10*time.Millisecond, "p2 should have been unloaded")

	isLoaded := func(p tqid.Partition) bool {
		tqm, _, err := e.getTaskQueuePartitionManager(context.Background(), p, false, loadCauseUnspecified)
		s.NoError(err)
		return tqm != nil
	}
	s.True(isLoaded(p1))
	s.False(isLoaded(p2))
}

func (s *matchingEngineSuite) TaskQueueMetricValidator(capture *metricstest.Capture, familyCounterLength int, familyCounter float64, queueCounterLength int, queueCounter float64, queuePartitionCounterLength int, queuePartitionCounter float64) {
	// checks the metrics according to the values passed in the parameters
	snapshot := capture.Snapshot()
	familyCounterRecordings := snapshot[metrics.LoadedTaskQueueFamilyGauge.Name()]
	s.Len(familyCounterRecordings, familyCounterLength)
	s.Equal(familyCounter, familyCounterRecordings[familyCounterLength-1].Value.(float64))

	queueCounterRecordings := snapshot[metrics.LoadedTaskQueueGauge.Name()]
	s.Len(queueCounterRecordings, queueCounterLength)
	s.Equal(queueCounter, queueCounterRecordings[queueCounterLength-1].Value.(float64))

	queuePartitionCounterRecordings := snapshot[metrics.LoadedTaskQueuePartitionGauge.Name()]
	s.Len(queuePartitionCounterRecordings, queuePartitionCounterLength)
	s.Equal(queuePartitionCounter, queuePartitionCounterRecordings[queuePartitionCounterLength-1].Value.(float64))
}

func (s *matchingEngineSuite) PhysicalQueueMetricValidator(capture *metricstest.Capture, physicalTaskQueueLength int, physicalTaskQueueCounter float64) {
	// checks the metrics according to the values passed in the parameters
	snapshot := capture.Snapshot()
	physicalTaskQueueRecordings := snapshot[metrics.LoadedPhysicalTaskQueueGauge.Name()]
	s.Len(physicalTaskQueueRecordings, physicalTaskQueueLength)
	s.Equal(physicalTaskQueueCounter, physicalTaskQueueRecordings[physicalTaskQueueLength-1].Value.(float64))
}

func (s *matchingEngineSuite) TestUpdateTaskQueuePartitionGauge_RootPartitionWorkflowType() {
	// for getting snapshots of metrics
	s.matchingEngine.metricsHandler = metricstest.NewCaptureHandler()
	captureHandler, ok := s.matchingEngine.metricsHandler.(*metricstest.CaptureHandler)
	s.True(ok)
	capture := captureHandler.StartCapture()

	rootPrtn := newRootPartition(uuid.NewString(), "MetricTester", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	_, _, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), rootPrtn, true, loadCauseUnspecified)
	s.Require().NoError(err)

	s.TaskQueueMetricValidator(capture, 1, 1, 1, 1, 1, 1)

	// Calling the update gauge function should increase each of the metrics to 2
	// since we are dealing with a root partition
	s.matchingEngine.updateTaskQueuePartitionGauge(s.ns, rootPrtn, 1)
	s.TaskQueueMetricValidator(capture, 2, 2, 2, 2, 2, 2)
}

func (s *matchingEngineSuite) TestUpdateTaskQueuePartitionGauge_RootPartitionActivityType() {
	// for getting snapshots of metrics
	s.matchingEngine.metricsHandler = metricstest.NewCaptureHandler()
	captureHandler, ok := s.matchingEngine.metricsHandler.(*metricstest.CaptureHandler)
	s.True(ok)
	capture := captureHandler.StartCapture()

	rootPrtn := newRootPartition(uuid.NewString(), "MetricTester", enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	_, _, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), rootPrtn, true, loadCauseUnspecified)
	s.Require().NoError(err)

	// Creation of a new root partition, having an activity task queue, should not have
	// increased FamilyCounter. Other metrics should be 1.
	s.TaskQueueMetricValidator(capture, 1, 0, 1, 1, 1, 1)

	// Calling the update gauge function should increase each of the metrics to 2
	// since we are dealing with a root partition
	s.matchingEngine.updateTaskQueuePartitionGauge(s.ns, rootPrtn, 1)
	s.TaskQueueMetricValidator(capture, 2, 0, 2, 2, 2, 2)
}

func (s *matchingEngineSuite) TestUpdateTaskQueuePartitionGauge_NonRootPartition() {
	s.matchingEngine.metricsHandler = metricstest.NewCaptureHandler()
	captureHandler, ok := s.matchingEngine.metricsHandler.(*metricstest.CaptureHandler)
	s.True(ok)
	capture := captureHandler.StartCapture()

	nonRootPrtn := newTestTaskQueue(uuid.NewString(), "MetricTester", enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(31)
	_, _, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), nonRootPrtn, true, loadCauseUnspecified)
	s.Require().NoError(err)

	// Creation of a non-root partition should only increase the Queue Partition counter
	s.TaskQueueMetricValidator(capture, 1, 0, 1, 0, 1, 1)

	// Calling the update gauge function should increase each of the metrics to 2
	// since we are dealing with a root partition
	s.matchingEngine.updateTaskQueuePartitionGauge(s.ns, nonRootPrtn, 1)
	s.TaskQueueMetricValidator(capture, 2, 0, 2, 0, 2, 2)
}

func (s *matchingEngineSuite) TestUpdatePhysicalTaskQueueGauge_UnVersioned() {
	s.matchingEngine.metricsHandler = metricstest.NewCaptureHandler()
	captureHandler, ok := s.matchingEngine.metricsHandler.(*metricstest.CaptureHandler)
	s.True(ok)
	capture := captureHandler.StartCapture()

	prtn := newRootPartition(
		uuid.NewString(),
		"MetricTester",
		enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tqm, _, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), prtn, true, loadCauseUnspecified)
	s.Require().NoError(err)

	// Creating a TaskQueuePartitionManager results in creating a PhysicalTaskQueueManager which should increase
	// the size of the map to 1 and it's counter to 1.
	s.PhysicalQueueMetricValidator(capture, 1, 1)

	tlmImpl := s.getPhysicalTaskQueueManagerImpl(tqm)

	s.matchingEngine.updatePhysicalTaskQueueGauge(s.ns, prtn, tlmImpl.queue.version, 1)

	s.PhysicalQueueMetricValidator(capture, 2, 2)

	// Validating if versioned has been set right for the respective parameters
	physicalTaskQueueParameters := taskQueueCounterKey{
		namespaceID:   prtn.NamespaceId(),
		taskType:      prtn.TaskType(),
		partitionType: prtn.Kind(),
		versioned:     "unversioned",
	}
	assert.Equal(s.T(), s.matchingEngine.gaugeMetrics.loadedPhysicalTaskQueueCount[physicalTaskQueueParameters], 2)

}

func (s *matchingEngineSuite) TestUpdatePhysicalTaskQueueGauge_VersionSet() {
	s.matchingEngine.metricsHandler = metricstest.NewCaptureHandler()
	captureHandler, ok := s.matchingEngine.metricsHandler.(*metricstest.CaptureHandler)
	s.True(ok)
	capture := captureHandler.StartCapture()

	namespaceID := uuid.NewString()
	versionSet := uuid.NewString()
	tl := "MetricTester"
	rootPrtn := newTestTaskQueue(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY).RootPartition()
	dbq := VersionSetQueueKey(rootPrtn, versionSet)
	tqm, _, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), dbq.Partition(), true, loadCauseUnspecified)
	s.Require().NoError(err)

	// Creating a TaskQueuePartitionManager results in creating a PhysicalTaskQueueManager which should increase
	// the size of the map to 1 and it's counter to 1.
	s.PhysicalQueueMetricValidator(capture, 1, 1)

	vqtpm, err := tqm.(*taskQueuePartitionManagerImpl).getVersionedQueueNoWait(
		versionSet,
		"",
		nil,
		true,
	)
	s.Require().NoError(err)

	// Creating a VersionedQueue results in increasing the size of the map to 2, due to 2 entries now,
	// with it's counter to 1.
	s.PhysicalQueueMetricValidator(capture, 2, 1)
	s.matchingEngine.updatePhysicalTaskQueueGauge(s.ns, rootPrtn, vqtpm.(*physicalTaskQueueManagerImpl).queue.version, 1)
	s.PhysicalQueueMetricValidator(capture, 3, 2)

	// Validating if versioned has been set right for the specific parameters
	physicalTaskQueueParameters := taskQueueCounterKey{
		namespaceID:   dbq.Partition().NamespaceId(),
		taskType:      dbq.Partition().TaskType(),
		partitionType: dbq.Partition().Kind(),
		versioned:     "versionSet",
	}
	assert.Equal(s.T(), s.matchingEngine.gaugeMetrics.loadedPhysicalTaskQueueCount[physicalTaskQueueParameters], 2)
}

func (s *matchingEngineSuite) TestUpdatePhysicalTaskQueueGauge_BuildID() {
	s.matchingEngine.metricsHandler = metricstest.NewCaptureHandler()
	captureHandler, ok := s.matchingEngine.metricsHandler.(*metricstest.CaptureHandler)
	s.True(ok)
	capture := captureHandler.StartCapture()

	namespaceID := uuid.NewString()
	buildID := uuid.NewString()
	rootPrtn := newTestTaskQueue(namespaceID, "MetricTester", enumspb.TASK_QUEUE_TYPE_ACTIVITY).RootPartition()
	dbq := BuildIdQueueKey(rootPrtn, buildID)
	tqm, _, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), dbq.Partition(), true, loadCauseUnspecified)
	s.Require().NoError(err)

	// Creating a TaskQueuePartitionManager results in creating a PhysicalTaskQueueManager which should increase
	// the size of the map to 1 and it's counter to 1.
	s.PhysicalQueueMetricValidator(capture, 1, 1)

	vqtpm, err := tqm.(*taskQueuePartitionManagerImpl).getVersionedQueueNoWait(
		"",
		buildID,
		nil,
		true,
	)
	s.Require().NoError(err)

	// Creating a VersionedQueue results in increasing the size of the map to 2, due to 2 entries now,
	// with it's counter to 1.
	s.PhysicalQueueMetricValidator(capture, 2, 1)
	s.matchingEngine.updatePhysicalTaskQueueGauge(s.ns, rootPrtn, vqtpm.(*physicalTaskQueueManagerImpl).queue.version, 1)
	s.PhysicalQueueMetricValidator(capture, 3, 2)

	// Validating if versioned has been set right for the specific parameters
	physicalTaskQueueParameters := taskQueueCounterKey{
		namespaceID:   dbq.Partition().NamespaceId(),
		taskType:      dbq.Partition().TaskType(),
		partitionType: dbq.Partition().Kind(),
		versioned:     "buildId",
	}
	assert.Equal(s.T(), s.matchingEngine.gaugeMetrics.loadedPhysicalTaskQueueCount[physicalTaskQueueParameters], 2)

}

// generateWorkflowExecution makes a sample workflowExecution and WorkflowType for the required tests
func (s *matchingEngineSuite) generateWorkflowExecution() (*commonpb.WorkflowType, *commonpb.WorkflowExecution) {
	runID := uuid.NewString()
	workflowID := "workflow1"
	workflowType := &commonpb.WorkflowType{
		Name: "workflow",
	}
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}
	return workflowType, workflowExecution
}

func (s *matchingEngineSuite) mockHistoryWhilePolling(workflowType *commonpb.WorkflowType) {
	s.mockHistoryClient.EXPECT().RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordWorkflowTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
			return &historyservice.RecordWorkflowTaskStartedResponse{
				PreviousStartedEventId: 1,
				StartedEventId:         1,
				ScheduledEventId:       1,
				WorkflowType:           workflowType,
				Attempt:                1,
				History:                &historypb.History{Events: []*historypb.HistoryEvent{}},
				NextPageToken:          nil,
			}, nil
		}).AnyTimes()
}

func (s *matchingEngineSuite) createTQAndPTQForBacklogTests() (*taskqueuepb.TaskQueue, *PhysicalTaskQueueKey) {
	s.matchingEngine.config.RangeSize = 10
	tq := "approximateBacklogCounter"
	ptq := newUnversionedRootQueueKey(namespaceID, tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	taskQueue := &taskqueuepb.TaskQueue{
		Name: tq,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	return taskQueue, ptq
}

func (s *matchingEngineSuite) addWorkflowTask(
	workflowExecution *commonpb.WorkflowExecution,
	taskQueue *taskqueuepb.TaskQueue,
) {
	s.EventuallyWithT(func(c *assert.CollectT) {
		addRequest := matchingservice.AddWorkflowTaskRequest{
			NamespaceId:            namespaceID,
			Execution:              workflowExecution,
			ScheduledEventId:       1,
			TaskQueue:              taskQueue,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		}
		_, _, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
		require.NoError(c, err)
	}, 10*time.Second, time.Millisecond, "failed to add workflow task")
}

func (s *matchingEngineSuite) createPollWorkflowTaskRequestAndPoll(taskQueue *taskqueuepb.TaskQueue) {
	s.EventuallyWithT(func(c *assert.CollectT) {
		result, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
			NamespaceId: namespaceID,
			PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: taskQueue,
				Identity:  "nobody",
			},
		}, metrics.NoopMetricsHandler)
		require.NoError(c, err) // DB could have failed while fetching tasks; try again
		require.NotEmpty(c, result.TaskToken)
		require.NotZero(c, result.Attempt)
	}, 3*time.Second, time.Millisecond, "failed to poll workflow task")
}

// addWorkflowTasks adds taskCount number of tasks sequentially
func (s *matchingEngineSuite) addWorkflowTasks(
	taskCount int, taskQueue *taskqueuepb.TaskQueue, workflowExecution *commonpb.WorkflowExecution,
) {
	for range taskCount {
		s.addWorkflowTask(workflowExecution, taskQueue)
	}
}

// addWorkflowTasksConcurrent adds taskCount number of tasks for each numWorker concurrently
func (s *matchingEngineSuite) addWorkflowTasksConcurrently(
	wg *sync.WaitGroup, numWorkers int, taskCount int,
	taskQueue *taskqueuepb.TaskQueue, workflowExecution *commonpb.WorkflowExecution,
) {
	for range numWorkers {
		wg.Add(1)
		go func() {
			for range taskCount {
				s.addWorkflowTask(workflowExecution, taskQueue)
			}
			wg.Done()
		}()
	}
}

// pollWorkflowTasks polls tasks sequentially
func (s *matchingEngineSuite) pollWorkflowTasks(
	workflowType *commonpb.WorkflowType, taskCount int,
	ptq *PhysicalTaskQueueKey, taskQueue *taskqueuepb.TaskQueue,
) {
	s.mockHistoryWhilePolling(workflowType)
	tasksPolled := 0
	for range taskCount {
		s.createPollWorkflowTaskRequestAndPoll(taskQueue)
		tasksPolled += 1

		// relax ApproximateBacklogCount for fairness impl
		if !s.fairness {
			// PartitionManager could have been unloaded; fetch the latest copy
			pgMgr := s.getPhysicalTaskQueueManagerImplFromKey(ptq)
			s.LessOrEqual(int64(taskCount-tasksPolled), totalApproximateBacklogCount(pgMgr.backlogMgr))
		}
	}
}

// pollWorkflowTasksConcurrently polls tasks using numWorkers concurrently
func (s *matchingEngineSuite) pollWorkflowTasksConcurrently(
	wg *sync.WaitGroup, workflowType *commonpb.WorkflowType, numPollers int, taskCount int,
	ptq *PhysicalTaskQueueKey, taskQueue *taskqueuepb.TaskQueue,
) {
	s.mockHistoryWhilePolling(workflowType)
	for range numPollers {
		wg.Add(1)
		go func() {
			for range taskCount {
				s.createPollWorkflowTaskRequestAndPoll(taskQueue)
			}
			wg.Done()
		}()
	}
}

func (s *matchingEngineSuite) getTaskQueuePartitionManagerImpl(ptq *PhysicalTaskQueueKey) *taskQueuePartitionManagerImpl {
	return s.matchingEngine.partitions[ptq.Partition().Key()].(*taskQueuePartitionManagerImpl)
}

// getPhysicalTaskQueueManagerImpl extracts the physicalTaskQueueManagerImpl for the given taskQueuePartitionManager
func (s *matchingEngineSuite) getPhysicalTaskQueueManagerImpl(mgr taskQueuePartitionManager) *physicalTaskQueueManagerImpl {
	defaultQ, err := mgr.(*taskQueuePartitionManagerImpl).defaultQueueFuture.GetIfReady()
	s.Require().NoError(err)
	return defaultQ.(*physicalTaskQueueManagerImpl)
}

func (s *matchingEngineSuite) getPhysicalTaskQueueManagerImplFromKey(ptq *PhysicalTaskQueueKey) *physicalTaskQueueManagerImpl {
	return s.getPhysicalTaskQueueManagerImpl(s.getTaskQueuePartitionManagerImpl(ptq))
}

func (s *matchingEngineSuite) addConsumeAllWorkflowTasksNonConcurrently(taskCount int) {
	workflowType, workflowExecution := s.generateWorkflowExecution()
	taskQueue, ptq := s.createTQAndPTQForBacklogTests()

	s.addWorkflowTasks(taskCount, taskQueue, workflowExecution)
	s.Equal(taskCount, s.taskManager.getCreateTaskCount(ptq))
	s.Equal(taskCount, s.taskManager.getTaskCount(ptq))

	pgMgr := s.getPhysicalTaskQueueManagerImplFromKey(ptq)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err := pgMgr.WaitUntilInitialized(ctx)
	s.Require().NoError(err)
	cancel()
	backlogCount := totalApproximateBacklogCount(pgMgr.backlogMgr)
	if s.fairness {
		// Relax this condition for fairBacklogManager: it can sometimes reset backlog count on
		// read, making it more accurate in theory, but breaking this test's assumptions.
		s.InDelta(taskCount, backlogCount, 2)
	} else {
		s.EqualValues(taskCount, backlogCount)
	}

	s.pollWorkflowTasks(workflowType, taskCount, ptq, taskQueue)

	s.LessOrEqual(int64(0), totalApproximateBacklogCount(pgMgr.backlogMgr))
}

func (s *matchingEngineSuite) TestAddConsumeWorkflowTasksNoDBErrors() {
	s.addConsumeAllWorkflowTasksNonConcurrently(200)
}

func (s *matchingEngineSuite) TestAddConsumeWorkflowTasksDBErrors() {
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.logger.Expect(testlogger.Error, "unexpected error dispatching task")
	s.taskManager.addFault("CreateTasks", "ConditionFailed", 0.1)
	s.taskManager.addFault("GetTasks", "Unavailable", 0.1)

	s.addConsumeAllWorkflowTasksNonConcurrently(200)
}

func (s *matchingEngineSuite) resetBacklogCounter(numWorkers int, taskCount int, rangeSize int) {
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(1 * time.Millisecond)
	s.matchingEngine.config.UpdateAckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(100 * time.Millisecond)

	workflowType, workflowExecution := s.generateWorkflowExecution()
	taskQueue, ptq := s.createTQAndPTQForBacklogTests()
	s.matchingEngine.config.RangeSize = int64(rangeSize)

	s.addWorkflowTasks(taskCount*numWorkers, taskQueue, workflowExecution)

	// TaskID of the first task to be added
	minTaskID, done := s.taskManager.minTaskID(ptq)
	s.True(done)

	partitionManager, _, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), ptq.Partition(), false, loadCauseTask)
	s.NoError(err)
	pqMgr := s.getPhysicalTaskQueueManagerImplFromKey(ptq)

	s.EqualValues(taskCount*numWorkers, s.taskManager.getTaskCount(ptq))

	// Check the maxReadLevel with the value of task stored in db
	maxTaskId, ok := s.taskManager.maxTaskID(ptq)
	s.True(ok)
	s.EqualValues(maxTaskId, pqMgr.backlogMgr.getDB().GetMaxReadLevel(0))

	// validate the approximateBacklogCounter
	s.EqualValues(taskCount*numWorkers, totalApproximateBacklogCount(pqMgr.backlogMgr))

	// Unload the PQM
	s.matchingEngine.unloadTaskQueuePartition(partitionManager, unloadCauseForce)

	// Simulate a TTL'ed task in Cassandra by removing it from the DB
	// Remove the task from testTaskManager but not from db/AckManager

	// Stop the backlogManager so that we TTL and the taskReader does not catch this
	request := &persistence.CompleteTasksLessThanRequest{
		NamespaceID:        namespaceID,
		TaskQueueName:      taskQueue.Name,
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		ExclusiveMaxTaskID: minTaskID + 1,
		Limit:              100,
	}
	_, err = s.taskManager.CompleteTasksLessThan(context.Background(), request)
	s.NoError(err)
	s.EqualValues((taskCount*numWorkers)-1, s.taskManager.getTaskCount(ptq))

	// Add pollers which shall also load the fresher version of tqMgr
	s.pollWorkflowTasks(workflowType, (taskCount*numWorkers)-1, ptq, taskQueue)

	// Update pgMgr to have the latest pgMgr
	pqMgr = s.getPhysicalTaskQueueManagerImplFromKey(ptq)

	// Overwrite the maxReadLevel since it could have increased if the previous taskWriter was
	// stopped (which would not result in resetting).
	pqMgr.backlogMgr.getDB().setMaxReadLevelForTesting(subqueueZero, maxTaskId)

	s.EqualValues(0, s.taskManager.getTaskCount(ptq))
	s.EventuallyWithT(func(collect *assert.CollectT) {
		require.Equal(collect, int64(0), totalApproximateBacklogCount(pqMgr.backlogMgr))
	}, 4*time.Second, 10*time.Millisecond, "backlog counter should have been reset")
}

// TestResettingBacklogCounter tests the scenario where approximateBacklogCounter over-counts and resets it accordingly
func (s *matchingEngineSuite) TestResetBacklogCounterNoDBErrors() {
	if s.newMatcher {
		s.T().Skip("not supported by new matcher; flaky")
	}

	s.resetBacklogCounter(2, 2, 2)
}

func (s *matchingEngineSuite) TestResetBacklogCounterDBErrors() {
	if s.newMatcher {
		s.T().Skip("test is flaky with new matcher")
	}
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.logger.Expect(testlogger.Error, "unexpected error dispatching task")
	s.taskManager.addFault("CreateTasks", "ConditionFailed", 0.1)
	s.taskManager.addFault("GetTasks", "Unavailable", 0.1)

	s.resetBacklogCounter(2, 2, 2)
}

func (s *matchingEngineSuite) TestMoreTasksResetBacklogCounterNoDBErrors() {
	if s.newMatcher {
		s.T().Skip("test is flaky with new matcher")
	}
	s.resetBacklogCounter(10, 20, 2)
}

func (s *matchingEngineSuite) TestMoreTasksResetBacklogCounterDBErrors() {
	if s.newMatcher {
		s.T().Skip("test is flaky with new matcher")
	}
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.logger.Expect(testlogger.Error, "unexpected error dispatching task")
	s.taskManager.addFault("CreateTasks", "ConditionFailed", 0.1)
	s.taskManager.addFault("GetTasks", "Unavailable", 0.1)

	s.resetBacklogCounter(10, 50, 5)
}

// Concurrent tests for testing approximateBacklogCounter

func (s *matchingEngineSuite) concurrentPublishAndConsumeValidateBacklogCounter(
	numWorkers, tasksToAdd, tasksToPoll int,
) {
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Millisecond)
	s.matchingEngine.config.UpdateAckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(1 * time.Millisecond)

	var wg sync.WaitGroup
	workflowType, workflowExecution := s.generateWorkflowExecution()
	taskQueue, ptq := s.createTQAndPTQForBacklogTests()
	s.addWorkflowTasksConcurrently(&wg, numWorkers, tasksToAdd, taskQueue, workflowExecution)
	s.pollWorkflowTasksConcurrently(&wg, workflowType, numWorkers, tasksToPoll, ptq, taskQueue)
	wg.Wait()

	ptqMgr := s.getPhysicalTaskQueueManagerImplFromKey(ptq)
	dbTasks := int64(s.taskManager.getTaskCount(ptq))
	backlogCount := totalApproximateBacklogCount(ptqMgr.backlogMgr)
	if s.fairness {
		// Relax this condition for fairBacklogManager: it can sometimes reset backlog count on
		// read, making it more accurate in theory, but breaking this test's assumptions.
		s.InDelta(dbTasks, backlogCount, 2)
	} else {
		s.LessOrEqual(dbTasks, backlogCount)
	}
}

func (s *matchingEngineSuite) TestConcurrentAddWorkflowTasksNoDBErrors() {
	s.concurrentPublishAndConsumeValidateBacklogCounter(150, 100, 0)
}

func (s *matchingEngineSuite) TestConcurrentAddWorkflowTasksDBErrors() {
	s.T().Skip("Skipping this as the backlog counter could under-count. Fix requires making " +
		"UpdateState an atomic operation.")
	s.taskManager.addFault("CreateTasks", "ConditionFailed", 0.1)
	s.taskManager.addFault("GetTasks", "Unavailable", 0.1)

	s.concurrentPublishAndConsumeValidateBacklogCounter(150, 100, 0)
}

func (s *matchingEngineSuite) TestConcurrentAdd_PollWorkflowTasksNoDBErrors() {
	if s.newMatcher {
		s.T().Skip("test is flaky with new matcher")
	}
	s.concurrentPublishAndConsumeValidateBacklogCounter(20, 100, 100)
}

func (s *matchingEngineSuite) TestConcurrentAdd_PollWorkflowTasksDBErrors() {
	s.T().Skip("Skipping this as the backlog counter could under-count. Fix requires making " +
		"UpdateState an atomic operation.")
	s.taskManager.addFault("CreateTasks", "ConditionFailed", 0.1)
	s.taskManager.addFault("GetTasks", "Unavailable", 0.1)

	s.concurrentPublishAndConsumeValidateBacklogCounter(20, 100, 100)
}

func (s *matchingEngineSuite) TestLesserNumberOfPollersThanTasksNoDBErrors() {
	s.concurrentPublishAndConsumeValidateBacklogCounter(1, 500, 200)
}

func (s *matchingEngineSuite) TestLesserNumberOfPollersThanTasksDBErrors() {
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.logger.Expect(testlogger.Error, "unexpected error dispatching task")
	s.taskManager.addFault("CreateTasks", "ConditionFailed", 0.1)
	s.taskManager.addFault("GetTasks", "Unavailable", 0.1)

	s.concurrentPublishAndConsumeValidateBacklogCounter(1, 500, 200)
}

func (s *matchingEngineSuite) TestMultipleWorkersLesserNumberOfPollersThanTasksNoDBErrors() {
	s.concurrentPublishAndConsumeValidateBacklogCounter(5, 500, 200)
}

func (s *matchingEngineSuite) TestMultipleWorkersLesserNumberOfPollersThanTasksDBErrors() {
	s.T().Skip("Skipping this as the backlog counter could under-count. Fix requires making " +
		"UpdateState an atomic operation.")
	s.taskManager.addFault("CreateTasks", "ConditionFailed", 0.1)
	s.taskManager.addFault("GetTasks", "Unavailable", 0.1)

	s.concurrentPublishAndConsumeValidateBacklogCounter(5, 500, 200)
}

func (s *matchingEngineSuite) TestOldestBacklogAge() {
	firstAge := durationpb.New(100 * time.Second)
	secondAge := durationpb.New(1 * time.Millisecond)
	s.Same(firstAge, oldestBacklogAge(firstAge, secondAge))

	thirdAge := durationpb.New(5 * time.Minute)
	s.Same(thirdAge, oldestBacklogAge(firstAge, thirdAge))
	s.Same(thirdAge, oldestBacklogAge(secondAge, thirdAge))
}

func (s *matchingEngineSuite) TestCheckNexusEndpointsOwnership() {
	isOwner, _, err := s.matchingEngine.checkNexusEndpointsOwnership()
	s.NoError(err)
	s.True(isOwner)
	s.hostInfoForResolver = membership.NewHostInfoFromAddress("other")
	isOwner, _, err = s.matchingEngine.checkNexusEndpointsOwnership()
	s.NoError(err)
	s.False(isOwner)
}

func (s *matchingEngineSuite) TestNotifyNexusEndpointsOwnershipLost() {
	ch := s.matchingEngine.nexusEndpointsOwnershipLostCh
	s.matchingEngine.notifyNexusEndpointsOwnershipChange()
	select {
	case <-ch:
		s.Fail("expected nexusEndpointsOwnershipLost channel to not have been closed")
	default:
	}
	s.hostInfoForResolver = membership.NewHostInfoFromAddress("other")
	s.matchingEngine.notifyNexusEndpointsOwnershipChange()
	<-ch
	// If the channel is unblocked the test passed.
}

func (s *matchingEngineSuite) TestPollActivityTaskQueueWithRateLimiterError() {
	mockRateLimiter := quotas.NewMockRequestRateLimiter(s.controller)
	rateLimiterErr := serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "rate limit exceeded")
	s.matchingEngine.rateLimiter = mockRateLimiter

	namespaceID := uuid.NewString()
	tl := "queue"
	taskQueue := &taskqueuepb.TaskQueue{Name: "queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	addRequest := matchingservice.AddActivityTaskRequest{
		NamespaceId:      namespaceID,
		Execution:        &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewString()},
		ScheduledEventId: int64(5),
		TaskQueue:        taskQueue,
	}

	_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.NoError(err)
	s.Equal(1, s.taskManager.getTaskCount(newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)))

	mockRateLimiter.EXPECT().
		Wait(gomock.Any(), gomock.Any()).
		Return(rateLimiterErr).Times(1)
	s.matchingEngine.rateLimiter = mockRateLimiter

	_, err = s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	s.ErrorIs(err, rateLimiterErr)
}

func (s *matchingEngineSuite) TestPollWorkflowTaskQueueWithRateLimiterError() {
	mockRateLimiter := quotas.NewMockRequestRateLimiter(s.controller)
	rateLimiterErr := serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, "rate limit exceeded")

	namespaceID := uuid.NewString()
	tl := "queue"
	taskQueue := &taskqueuepb.TaskQueue{Name: "queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	addRequest := matchingservice.AddWorkflowTaskRequest{
		NamespaceId:      namespaceID,
		Execution:        &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewString()},
		ScheduledEventId: int64(5),
		TaskQueue:        taskQueue,
	}

	_, _, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
	s.NoError(err)
	s.Equal(1, s.taskManager.getTaskCount(newUnversionedRootQueueKey(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_WORKFLOW)))

	mockRateLimiter.EXPECT().
		Wait(gomock.Any(), gomock.Any()).
		Return(rateLimiterErr).Times(1)
	s.matchingEngine.rateLimiter = mockRateLimiter

	_, err = s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	s.ErrorIs(err, rateLimiterErr)
}

func (s *matchingEngineSuite) TestDispatchNexusTask_ValidateTimeoutBuffer() {
	const ctxTimeout = 2 * time.Second
	var defaultTimeoutBuffer = nexusoperations.MinDispatchTaskTimeout.Get(dynamicconfig.NewNoopCollection())("my-nsid")

	type testCase struct {
		name      string
		sleepTime time.Duration
		assertion func(t *testing.T, response *matchingservice.DispatchNexusTaskResponse, err error)
	}

	testCases := []testCase{
		{
			name:      "deadline_exceeded_immediately",
			sleepTime: ctxTimeout - defaultTimeoutBuffer,
			assertion: func(t *testing.T, response *matchingservice.DispatchNexusTaskResponse, err error) {
				require.NoError(t, err)
				require.NotNil(t, response.GetRequestTimeout())
			},
		},
		{
			name:      "deadline_exceeded_awaiting_local_dispatch",
			sleepTime: ctxTimeout - defaultTimeoutBuffer - time.Nanosecond,
			assertion: func(t *testing.T, response *matchingservice.DispatchNexusTaskResponse, err error) {
				require.NoError(t, err)
				require.NotNil(t, response.GetRequestTimeout())
			},
		},
		{
			name:      "deadline_not_exceeded_on_forwarding",
			sleepTime: ctxTimeout - defaultTimeoutBuffer - time.Nanosecond,
			assertion: func(t *testing.T, response *matchingservice.DispatchNexusTaskResponse, err error) {
				require.NoError(t, err)
				require.NotNil(t, response.GetResponse())
			},
		},
	}

	testFn := func(t *testing.T, tc testCase) {
		synctest.Test(t, func(t *testing.T) {
			nexusRequest := &matchingservice.DispatchNexusTaskRequest{
				NamespaceId: "my-nsid",
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: "my-tq",
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				Request: &nexuspb.Request{
					Header: map[string]string{
						"request-timeout": "2s",
					},
				},
			}

			mockPartitionManager := NewMocktaskQueuePartitionManager(s.controller)
			mockPartitionManager.EXPECT().WaitUntilInitialized(gomock.Any()).Return(nil).AnyTimes()
			mockPartitionManager.EXPECT().Stop(gomock.Any()).AnyTimes()

			//nolint:forbidigo // We're safe to use the sleep here since we're using synctest to control the timing
			mockPartitionManager.EXPECT().DispatchNexusTask(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, taskId string, request *matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error) {
					time.Sleep(tc.sleepTime)
					synctest.Wait()

					if err := ctx.Err(); err != nil {
						return nil, err
					}

					if tc.name == "deadline_exceeded_awaiting_local_dispatch" {
						return nil, nil
					}

					return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
						Response: &nexuspb.Response{},
					}}, nil
				},
			).Times(1)

			partition := newRootPartition("my-nsid", "my-tq", enumspb.TASK_QUEUE_TYPE_NEXUS)
			s.matchingEngine.partitions[partition.Key()] = mockPartitionManager
			s.matchingEngine.nexusResults = collection.NewSyncMap[string, chan *nexusResult]()

			ctx, cancel := context.WithTimeout(context.Background(), ctxTimeout)
			defer cancel()

			resp, err := s.matchingEngine.DispatchNexusTask(ctx, nexusRequest)

			tc.assertion(t, resp, err)
		})
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			testFn(t, tc)
		})
	}
}

// Following are tests for SyncDeploymentUserData API when it uses the new deployment data format.

// TestSyncDeploymentUserData_NewDeploymentDataRemovesOldVersions verifies that when a new routing config is set for a deployment using the latest deployment data format,
// versions, under the same deployment, that had synced using the old deployment data format are removed.
func (s *matchingEngineSuite) TestSyncDeploymentUserData_NewDeploymentDataRemovesOldVersions() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()

	// Seed user data with old-format versions across two deployments: foo.A and bar.B
	t1 := timestamp.TimePtr(time.Now().Add(-time.Hour))
	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data: &persistencespb.TaskQueueUserData{
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
					DeploymentData: &persistencespb.DeploymentData{
						Versions: []*deploymentspb.DeploymentVersionData{
							{Version: &deploymentspb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: "A"}, RoutingUpdateTime: t1, CurrentSinceTime: t1},
							{Version: &deploymentspb.WorkerDeploymentVersion{DeploymentName: "bar", BuildId: "B"}, RoutingUpdateTime: t1, CurrentSinceTime: t1},
						},
					},
				},
			},
		},
	}

	// Using the lower level UpdateTaskQueueUserData to set the user data for multiple versions at once.
	s.NoError(s.classicTaskManager.UpdateTaskQueueUserData(context.Background(), &persistence.UpdateTaskQueueUserDataRequest{
		NamespaceID: namespaceID,
		Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
			tq: {UserData: userData},
		},
	}))
	userData.Version++

	// Sync new-format routing config for deployment "foo" with a newer revision to trigger cleanup
	rc := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: "C"},
		CurrentVersionChangedTime: timestamppb.Now(),
		RevisionNumber:            1,
	}
	_, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:         namespaceID,
		TaskQueue:           tq,
		DeploymentName:      "foo",
		TaskQueueTypes:      []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		UpdateRoutingConfig: rc,
	})
	s.NoError(err)

	// Fetch and verify that old-format versions under the same deployment (foo) are removed, others remain
	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	//nolint:staticcheck // SA1019 deprecated versions will clean up later
	got := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetVersions()
	s.Require().Len(got, 1)
	s.Equal("bar", got[0].GetVersion().GetDeploymentName())
	s.Equal("B", got[0].GetVersion().GetBuildId())
}

func (s *matchingEngineSuite) TestSyncDeploymentUserData_RoutingConfigRevisionGating() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()

	rc1 := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: "rev1"},
		RevisionNumber:            1,
		CurrentVersionChangedTime: timestamppb.Now(),
	}
	resp, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:         namespaceID,
		TaskQueue:           tq,
		DeploymentName:      "foo",
		TaskQueueTypes:      []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpdateRoutingConfig: rc1,
	})
	s.NoError(err)
	s.True(resp.GetRoutingConfigChanged())

	// Attempt to sync lower revision (0) — should be ignored
	rc0 := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: "rev0"},
		RevisionNumber:            0,
		CurrentVersionChangedTime: timestamppb.Now(),
	}
	resp, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:         namespaceID,
		TaskQueue:           tq,
		DeploymentName:      "foo",
		TaskQueueTypes:      []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpdateRoutingConfig: rc0,
	})
	s.NoError(err)
	s.False(resp.GetRoutingConfigChanged())

	// Verify still rev 1
	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	gotRC := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()["foo"].GetRoutingConfig()
	s.Equal(int64(1), gotRC.GetRevisionNumber())
	s.Equal("rev1", gotRC.GetCurrentDeploymentVersion().GetBuildId())

	// Now sync higher revision (2) — should be applied
	rc2 := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: "foo", BuildId: "rev2"},
		RevisionNumber:            2,
		CurrentVersionChangedTime: timestamppb.Now(),
	}
	resp, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:         namespaceID,
		TaskQueue:           tq,
		DeploymentName:      "foo",
		TaskQueueTypes:      []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpdateRoutingConfig: rc2,
	})
	s.NoError(err)
	s.True(resp.GetRoutingConfigChanged())

	res, err = s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	gotRC = res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()["foo"].GetRoutingConfig()
	s.Equal(int64(2), gotRC.GetRevisionNumber())
	s.Equal("rev2", gotRC.GetCurrentDeploymentVersion().GetBuildId())
}

func (s *matchingEngineSuite) TestSyncDeploymentUserData_UpsertAndForgetVersions_WithoutRoutingConfig() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()
	deploymentName := "foo"

	// Upsert versions without providing RoutingConfig
	upserts1 := map[string]*deploymentspb.WorkerDeploymentVersionData{
		"b1": {},
		"b2": {},
	}
	resp, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:        namespaceID,
		TaskQueue:          tq,
		DeploymentName:     deploymentName,
		TaskQueueTypes:     []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: upserts1,
		// No UpdateRoutingConfig provided
	})
	s.NoError(err)
	s.False(resp.GetRoutingConfigChanged())

	// Verify both b1 and b2 exist
	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	versions := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()[deploymentName].GetVersions()
	s.Contains(versions, "b1")
	s.Contains(versions, "b2")
	s.Len(versions, 2)

	// Forget b1 and upsert b3, still without RoutingConfig
	upserts2 := map[string]*deploymentspb.WorkerDeploymentVersionData{
		"b3": {},
	}
	resp, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:        namespaceID,
		TaskQueue:          tq,
		DeploymentName:     deploymentName,
		TaskQueueTypes:     []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: upserts2,
		ForgetVersions:     []string{"b1"},
		// No UpdateRoutingConfig provided
	})
	s.NoError(err)
	s.False(resp.GetRoutingConfigChanged())

	// Verify b1 removed, b2 remains, b3 added
	res, err = s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	versions = res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()[deploymentName].GetVersions()
	s.NotContains(versions, "b1")
	s.Contains(versions, "b2")
	s.Contains(versions, "b3")
	s.Len(versions, 2)
}

//nolint:staticcheck // SA1019 deprecated versions will clean up later
func (s *matchingEngineSuite) TestSyncDeploymentUserData_UnsetOldFormatRamp_ClearsNewFormatRamp() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()
	deploymentName := "foo"

	// New-format ramp for deployment "foo"
	rc := &deploymentpb.RoutingConfig{
		RampingDeploymentVersion:            &deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: "b1"},
		RampingVersionPercentage:            25,
		RampingVersionPercentageChangedTime: timestamppb.Now(),
		RevisionNumber:                      1,
	}
	resp, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:         namespaceID,
		TaskQueue:           tq,
		DeploymentName:      deploymentName,
		TaskQueueTypes:      []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpdateRoutingConfig: rc,
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"b1": {
				Status: enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
			},
		},
	})
	s.NoError(err)
	s.True(resp.GetRoutingConfigChanged())

	// Verify ramp present in new-format data
	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	rcGot := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()[deploymentName].GetRoutingConfig()
	s.NotNil(rcGot.GetRampingDeploymentVersion())
	s.Equal("b1", rcGot.GetRampingDeploymentVersion().GetBuildId())
	s.InDelta(float32(25), rcGot.GetRampingVersionPercentage(), 0.0000)

	// Now unset ramp via old-format unversioned ramp (Version=nil, RampingSinceTime=nil)
	unsetVD := &deploymentspb.DeploymentVersionData{
		// Version: nil (unversioned)
		// RampingSinceTime: nil indicates unset
	}
	resp, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
			UpdateVersionData: unsetVD,
		},
	})
	s.NoError(err)

	// Verify unversioned ramp cleared and new-format ramp cleared
	res, err = s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	depData := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData()
	// old-format unversioned ramp should be nil
	s.Nil(depData.GetUnversionedRampData())
	// new-format ramp entries should be cleared
	rcGot = depData.GetDeploymentsData()[deploymentName].GetRoutingConfig()
	s.Nil(rcGot.GetRampingDeploymentVersion())
	s.InDelta(float32(0), rcGot.GetRampingVersionPercentage(), 0.0000)
	s.Nil(rcGot.GetRampingVersionPercentageChangedTime())
	s.Nil(rcGot.GetRampingVersionChangedTime())
}

//nolint:staticcheck // SA1019 deprecated versions will clean up later
func (s *matchingEngineSuite) TestSyncDeploymentUserData_OldFormatSetSameCurrent_ClearsNewFormatCurrent() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()
	deploymentName := "foo"

	// set current=v1
	rc := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: "v1"},
		CurrentVersionChangedTime: timestamppb.Now(),
		RevisionNumber:            1,
	}
	resp, err := s.matchingEngine.SyncDeploymentUserData(
		context.Background(),
		&matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:         namespaceID,
			TaskQueue:           tq,
			DeploymentName:      deploymentName,
			TaskQueueTypes:      []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			UpdateRoutingConfig: rc,
		},
	)
	s.NoError(err)
	s.True(resp.GetRoutingConfigChanged())

	// Verify current=v1 present in new-format
	res, err := s.matchingEngine.GetTaskQueueUserData(
		context.Background(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              namespaceID,
			TaskQueue:                tq,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
		},
	)
	s.NoError(err)
	rcGot := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].
		GetDeploymentData().GetDeploymentsData()[deploymentName].GetRoutingConfig()
	s.Equal("v1", rcGot.GetCurrentDeploymentVersion().GetBuildId())

	// Old-format: set SAME version (v1) as current -> should clear new-format current
	vdCurrent := &deploymentspb.DeploymentVersionData{
		Version:           &deploymentspb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: "v1"},
		CurrentSinceTime:  timestamppb.Now(),
		RoutingUpdateTime: timestamppb.Now(),
	}
	_, err = s.matchingEngine.SyncDeploymentUserData(
		context.Background(),
		&matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    namespaceID,
			TaskQueue:      tq,
			DeploymentName: deploymentName,
			TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
				UpdateVersionData: vdCurrent,
			},
		},
	)
	s.NoError(err)

	// Verify new-format current cleared
	res, err = s.matchingEngine.GetTaskQueueUserData(
		context.Background(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              namespaceID,
			TaskQueue:                tq,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
		},
	)
	s.NoError(err)
	rcGot = res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].
		GetDeploymentData().GetDeploymentsData()[deploymentName].GetRoutingConfig()
	s.Nil(rcGot.GetCurrentDeploymentVersion())
}

//nolint:staticcheck // SA1019 deprecated versions will clean up later
func (s *matchingEngineSuite) TestSyncDeploymentUserData_ForgetNewFormat_RemovesOldFormatMembership() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()
	deploymentName := "foo"
	buildID := "b1"

	// Upsert b1 using the old format
	_, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
			UpdateVersionData: &deploymentspb.DeploymentVersionData{
				Version:           &deploymentspb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: buildID},
				RoutingUpdateTime: timestamppb.Now(),
			},
		},
	})
	s.NoError(err)

	// Upsert b1 using the new format
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			buildID: {},
		},
	})
	s.NoError(err)

	// Forget via new-format
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:         namespaceID,
		TaskQueue:           tq,
		DeploymentName:      deploymentName,
		TaskQueueTypes:      []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		ForgetVersions:      []string{buildID},
		UpdateRoutingConfig: nil,
	})
	s.NoError(err)

	// Verify removal from both formats
	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	depData := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData()
	// new-format membership
	_, exists := depData.GetDeploymentsData()[deploymentName].GetVersions()[buildID]
	s.False(exists)
	// old-format slice
	for _, dv := range depData.GetVersions() {
		s.NotEqual(buildID, dv.GetVersion().GetBuildId())
	}
	// and the deployment entry should be removed from DeploymentsData as versions map is now empty
	_, deploymentExists := depData.GetDeploymentsData()[deploymentName]
	s.False(deploymentExists)
}

//nolint:staticcheck // SA1019 deprecated versions will clean up later
func (s *matchingEngineSuite) TestSyncDeploymentUserData_ForgetOldFormat_RemovesNewFormatMembership() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()
	deploymentName := "foo"
	buildID := "b1"

	// Upsert b1 using the old format
	_, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
			UpdateVersionData: &deploymentspb.DeploymentVersionData{
				Version:           &deploymentspb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: buildID},
				RoutingUpdateTime: timestamppb.Now(),
			},
		},
	})
	s.NoError(err)

	// Upsert b1 using the new format (membership map)
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			buildID: {},
		},
	})
	s.NoError(err)

	// Forget via old-format
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		Operation: &matchingservice.SyncDeploymentUserDataRequest_ForgetVersion{
			ForgetVersion: &deploymentspb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: buildID},
		},
	})
	s.NoError(err)

	// Verify removal from both formats
	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	depData := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData()
	// new-format membership map
	_, exists := depData.GetDeploymentsData()[deploymentName].GetVersions()[buildID]
	s.False(exists)
	// old-format slice
	for _, dv := range depData.GetVersions() {
		s.NotEqual(buildID, dv.GetVersion().GetBuildId())
	}
	// the deployment entry should be removed from DeploymentsData as versions map is now empty
	_, deploymentExists := depData.GetDeploymentsData()[deploymentName]
	s.False(deploymentExists)
}

func (s *matchingEngineSuite) setupRecordActivityTaskStartedMock(tlName string) {
	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &commonpb.ActivityType{Name: activityTypeName}
	activityInput := payloads.EncodeString("Activity1 Input")

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &historyservice.RecordActivityTaskStartedResponse{
				Attempt: 1,
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduledEventId, 0,
					&commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId: activityID,
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: tlName,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
						ActivityType:           activityType,
						Input:                  activityInput,
						ScheduleToCloseTimeout: durationpb.New(100 * time.Second),
						ScheduleToStartTimeout: durationpb.New(50 * time.Second),
						StartToCloseTimeout:    durationpb.New(50 * time.Second),
						HeartbeatTimeout:       durationpb.New(10 * time.Second),
					}),
			}, nil
		}).AnyTimes()
}
func newActivityTaskScheduledEvent(eventID int64, workflowTaskCompletedEventID int64,
	scheduleAttributes *commandpb.ScheduleActivityTaskCommandAttributes,
) *historypb.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
		ActivityId:                   scheduleAttributes.ActivityId,
		ActivityType:                 scheduleAttributes.ActivityType,
		TaskQueue:                    scheduleAttributes.TaskQueue,
		Input:                        scheduleAttributes.Input,
		Header:                       scheduleAttributes.Header,
		ScheduleToCloseTimeout:       scheduleAttributes.ScheduleToCloseTimeout,
		ScheduleToStartTimeout:       scheduleAttributes.ScheduleToStartTimeout,
		StartToCloseTimeout:          scheduleAttributes.StartToCloseTimeout,
		HeartbeatTimeout:             scheduleAttributes.HeartbeatTimeout,
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
	}}
	return historyEvent
}

func (s *matchingEngineSuite) TestSyncDeploymentUserData_UpdateRoutingConfig_MigratesOldVersionsToNewFormat() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()
	depFoo := "foo"
	depBar := "bar"

	// Add old-format versions: foo:A, foo:B, bar:C
	addOldFormatVersion := func(dep, build string) {
		_, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
			NamespaceId:    namespaceID,
			TaskQueue:      tq,
			DeploymentName: dep,
			TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
			Operation: &matchingservice.SyncDeploymentUserDataRequest_UpdateVersionData{
				UpdateVersionData: &deploymentspb.DeploymentVersionData{
					Version:           &deploymentspb.WorkerDeploymentVersion{DeploymentName: dep, BuildId: build},
					RoutingUpdateTime: timestamppb.Now(),
				},
			},
		})
		s.NoError(err)
	}
	addOldFormatVersion(depFoo, "A")
	addOldFormatVersion(depFoo, "B")
	addOldFormatVersion(depBar, "C")

	// Apply new-format routing config for foo with higher revision to trigger migration
	rc := &deploymentpb.RoutingConfig{
		CurrentDeploymentVersion:  &deploymentpb.WorkerDeploymentVersion{DeploymentName: depFoo, BuildId: "A"},
		CurrentVersionChangedTime: timestamppb.Now(),
		RevisionNumber:            1,
	}
	_, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:         namespaceID,
		TaskQueue:           tq,
		DeploymentName:      depFoo,
		TaskQueueTypes:      []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpdateRoutingConfig: rc,
	})
	s.NoError(err)

	// Verify: old-format slice retains only bar:C; foo:A & foo:B migrated
	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	depData := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData()

	// Old-format slice check
	var oldSliceBuilds []string
	//nolint:staticcheck // SA1019 deprecated versions will clean up later
	for _, dv := range depData.GetVersions() {
		if dv.GetVersion().GetDeploymentName() == depBar {
			oldSliceBuilds = append(oldSliceBuilds, dv.GetVersion().GetBuildId())
		}
		// Ensure none for foo remain
		s.NotEqual(depFoo, dv.GetVersion().GetDeploymentName())
	}
	s.ElementsMatch([]string{"C"}, oldSliceBuilds)

	// New-format membership map for foo should contain A & B
	fooVersions := depData.GetDeploymentsData()[depFoo].GetVersions()
	_, hasA := fooVersions["A"]
	_, hasB := fooVersions["B"]
	s.True(hasA)
	s.True(hasB)
	// And not C
	_, hasC := fooVersions["C"]
	s.False(hasC)
}

func (s *matchingEngineSuite) TestSyncDeploymentUserData_VersionDataRevisionGating() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()
	deploymentName := "foo"
	buildID := "v1"

	// Upsert version data with revision 1
	versionData1 := &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: 1,
		Status:         enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}
	_, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			buildID: versionData1,
		},
	})
	s.NoError(err)

	// Verify version data is stored
	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	versions := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()[deploymentName].GetVersions()
	s.Require().Contains(versions, buildID)
	s.Equal(int64(1), versions[buildID].GetRevisionNumber())
	s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, versions[buildID].GetStatus())

	// Attempt to upsert with lower revision (0) — should be ignored
	versionData0 := &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: 0,
		Status:         enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
	}
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			buildID: versionData0,
		},
	})
	s.NoError(err)

	// Verify revision still 1 and status unchanged
	res, err = s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	versions = res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()[deploymentName].GetVersions()
	s.Equal(int64(1), versions[buildID].GetRevisionNumber())
	s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, versions[buildID].GetStatus())

	// Upsert with equal revision (1) — should be accepted
	versionData1Equal := &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: 1,
		Status:         enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING,
	}
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			buildID: versionData1Equal,
		},
	})
	s.NoError(err)

	// Verify status changed even though revision is equal
	res, err = s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	versions = res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()[deploymentName].GetVersions()
	s.Equal(int64(1), versions[buildID].GetRevisionNumber())
	s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_RAMPING, versions[buildID].GetStatus())

	// Upsert with higher revision (2) — should be accepted
	versionData2 := &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: 2,
		Status:         enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
	}
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			buildID: versionData2,
		},
	})
	s.NoError(err)

	// Verify revision updated
	res, err = s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	versions = res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()[deploymentName].GetVersions()
	s.Equal(int64(2), versions[buildID].GetRevisionNumber())
	s.Equal(enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT, versions[buildID].GetStatus())
}

//nolint:staticcheck // SA1019 deprecated versions will clean up later
func (s *matchingEngineSuite) TestSyncDeploymentUserData_DeletedVersionRemovesOldFormat() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()
	deploymentName := "foo"
	buildID := "v1"

	// Add old-format version
	t1 := timestamppb.Now()
	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data: &persistencespb.TaskQueueUserData{
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
					DeploymentData: &persistencespb.DeploymentData{
						Versions: []*deploymentspb.DeploymentVersionData{
							{
								Version:           &deploymentspb.WorkerDeploymentVersion{DeploymentName: deploymentName, BuildId: buildID},
								RoutingUpdateTime: t1,
								CurrentSinceTime:  t1,
							},
						},
					},
				},
			},
		},
	}

	s.NoError(s.classicTaskManager.UpdateTaskQueueUserData(context.Background(), &persistence.UpdateTaskQueueUserDataRequest{
		NamespaceID: namespaceID,
		Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
			tq: {UserData: userData},
		},
	}))

	// Verify old-format version exists
	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	//nolint:staticcheck // SA1019 deprecated versions will clean up later
	oldVersions := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetVersions()
	s.Require().Len(oldVersions, 1)
	s.Equal(buildID, oldVersions[0].GetVersion().GetBuildId())

	// Mark version as deleted in new format (with recent update time so it doesn't get cleaned up)
	deletedVersionData := &deploymentspb.WorkerDeploymentVersionData{
		RevisionNumber: 1,
		Deleted:        true,
		UpdateTime:     timestamppb.Now(), // Set recent update time to prevent cleanup
	}
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			buildID: deletedVersionData,
		},
	})
	s.NoError(err)

	// Verify old-format version is removed
	res, err = s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	//nolint:staticcheck // SA1019 deprecated versions will clean up later
	oldVersions = res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetVersions()
	s.Empty(oldVersions)

	// Verify deleted version still exists in new format
	versions := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()[deploymentName].GetVersions()
	s.Require().Contains(versions, buildID)
	s.True(versions[buildID].GetDeleted())
}

func (s *matchingEngineSuite) TestSyncDeploymentUserData_CleanupOldDeletedVersions() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()
	deploymentName := "foo"

	// First, insert versions with recent update times
	recentTime := timestamppb.New(time.Now().Add(-6 * 24 * time.Hour)) // 6 days old (should remain)

	versionsData := map[string]*deploymentspb.WorkerDeploymentVersionData{
		"to-be-old-build": {
			RevisionNumber: 1,
			Deleted:        true,
			UpdateTime:     recentTime, // Start with recent time
		},
		"recent-build": {
			RevisionNumber: 1,
			Deleted:        true,
			UpdateTime:     recentTime,
		},
		"active-build": {
			RevisionNumber: 1,
			Status:         enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT,
			UpdateTime:     recentTime,
		},
	}

	// Insert all versions
	_, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:        namespaceID,
		TaskQueue:          tq,
		DeploymentName:     deploymentName,
		TaskQueueTypes:     []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: versionsData,
	})
	s.NoError(err)

	// Verify all versions are present
	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	versions := res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()[deploymentName].GetVersions()
	s.Len(versions, 3)
	s.Contains(versions, "to-be-old-build")
	s.Contains(versions, "recent-build")
	s.Contains(versions, "active-build")

	// Now update "to-be-old-build" to have an old timestamp (8 days)
	oldTime := timestamppb.New(time.Now().Add(-8 * 24 * time.Hour))
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"to-be-old-build": {
				RevisionNumber: 2, // Higher revision to allow update
				Deleted:        true,
				UpdateTime:     oldTime,
			},
		},
	})
	s.NoError(err)

	// Trigger cleanup by syncing again (cleanupOldDeletedVersions is called during each sync)
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		// Empty sync to trigger cleanup
	})
	s.NoError(err)

	// Verify only old deleted version is removed
	res, err = s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID,
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	versions = res.GetUserData().GetData().GetPerType()[int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW)].GetDeploymentData().GetDeploymentsData()[deploymentName].GetVersions()
	s.Len(versions, 2)
	s.NotContains(versions, "to-be-old-build") // Old deleted version should be removed
	s.Contains(versions, "recent-build")       // Recent deleted version should remain
	s.Contains(versions, "active-build")       // Active version should remain
	s.True(versions["recent-build"].GetDeleted())
	s.False(versions["active-build"].GetDeleted())
}

func (s *matchingEngineSuite) TestSyncDeploymentUserData_UpsertedVersionsData() {
	tv := testvars.New(s.T())
	namespaceID := tv.NamespaceID().String()
	tq := tv.TaskQueue().GetName()
	deploymentName := "foo"

	// Test single upsert
	_, err := s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"v1": {RevisionNumber: 1},
		},
	})
	s.NoError(err)

	// Test multiple upserts
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"v2": {RevisionNumber: 1},
			"v3": {RevisionNumber: 1},
		},
	})
	s.NoError(err)

	// Test upsert with one stale version (should not be included in response)
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		UpsertVersionsData: map[string]*deploymentspb.WorkerDeploymentVersionData{
			"v1": {RevisionNumber: 0}, // Stale, should be rejected
			"v4": {RevisionNumber: 1}, // New, should be accepted
		},
	})
	s.NoError(err)

	// Test no upserts
	_, err = s.matchingEngine.SyncDeploymentUserData(context.Background(), &matchingservice.SyncDeploymentUserDataRequest{
		NamespaceId:    namespaceID,
		TaskQueue:      tq,
		DeploymentName: deploymentName,
		TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW},
	})
	s.NoError(err)
}

func newHistoryEvent(eventID int64, eventType enumspb.EventType) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventTime: timestamppb.New(time.Now().UTC()),
		EventType: eventType,
	}
}

var _ persistence.TaskManager = (*testTaskManager)(nil)     // Asserts that interface is indeed implemented
var _ persistence.FairTaskManager = (*testTaskManager)(nil) // Asserts that interface is indeed implemented

type testTaskManager struct {
	sync.Mutex
	queues   map[dbTaskQueueKey]*testQueueData
	logger   log.Logger
	fairness bool

	// TODO: run some tests with this and without
	updateMetadataOnCreateTasks bool

	faultInjection map[string]float32 // "op:error" -> fraction of time
	delayInjection time.Duration
}

type dbTaskQueueKey struct {
	persistenceName string
	namespaceID     string
	taskType        enumspb.TaskQueueType
}

func newTestTaskManager(logger log.Logger) *testTaskManager {
	return &testTaskManager{
		queues:                      make(map[dbTaskQueueKey]*testQueueData),
		logger:                      logger,
		updateMetadataOnCreateTasks: true,
	}
}

func newTestFairTaskManager(logger log.Logger) *testTaskManager {
	m := newTestTaskManager(logger)
	m.fairness = true
	return m
}

func (m *testTaskManager) GetName() string {
	return "test"
}

func (m *testTaskManager) Close() {
}

func (m *testTaskManager) getQueueDataByKey(dbq *PhysicalTaskQueueKey) *testQueueData {
	return m.getQueueData(dbq.PersistenceName(), dbq.NamespaceId(), dbq.TaskType())
}

func (m *testTaskManager) getQueueData(name, namespaceID string, taskType enumspb.TaskQueueType) *testQueueData {
	key := dbTaskQueueKey{persistenceName: name, namespaceID: namespaceID, taskType: taskType}
	m.Lock()
	defer m.Unlock()
	if queue, ok := m.queues[key]; ok {
		return queue
	}
	queue := newTestQueueData()
	m.queues[key] = queue
	return queue
}

func newUnversionedRootQueueKey(namespaceID string, name string, taskType enumspb.TaskQueueType) *PhysicalTaskQueueKey {
	return UnversionedQueueKey(newTestTaskQueue(namespaceID, name, taskType).RootPartition())
}

func newRootPartition(namespaceID string, name string, taskType enumspb.TaskQueueType) *tqid.NormalPartition {
	return newTestTaskQueue(namespaceID, name, taskType).RootPartition()
}

func newTestTaskQueue(namespaceID string, name string, taskType enumspb.TaskQueueType) *tqid.TaskQueue {
	result, err := tqid.NewTaskQueueFamily(namespaceID, name)
	if err != nil {
		panic(fmt.Sprintf("newTaskQueueID failed with error %v", err))
	}
	return result.TaskQueue(taskType)
}

type testQueueData struct {
	sync.Mutex
	rangeID  int64
	info     *persistencespb.TaskQueueInfo
	tasks    treemap.Map
	userData *persistencespb.VersionedTaskQueueUserData

	createTaskCount  int
	getTasksCount    int
	getUserDataCount int
	updateCount      int
}

func newTestQueueData() *testQueueData {
	return &testQueueData{tasks: *newFairLevelTreeMap()}
}

func (q *testQueueData) String() string {
	var out strings.Builder
	q.Lock()
	defer q.Unlock()
	fmt.Fprintf(&out, "<task queue ")
	fmt.Fprintf(&out, "rangeID: %d ", q.rangeID)
	fmt.Fprintf(&out, "info: %s ", q.info.String())
	fmt.Fprintf(&out, "tasks: %d>", q.tasks.Size())
	return out.String()
}

func (q *testQueueData) RangeID() int64 {
	q.Lock()
	defer q.Unlock()
	return q.rangeID
}

func (m *testTaskManager) CreateTaskQueue(
	_ context.Context,
	request *persistence.CreateTaskQueueRequest,
) (*persistence.CreateTaskQueueResponse, error) {
	tli := request.TaskQueueInfo
	tlm := m.getQueueData(tli.Name, tli.NamespaceId, tli.TaskType)

	m.delay()
	defer m.delay()

	tlm.Lock()
	defer tlm.Unlock()

	if tlm.rangeID != 0 {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task queue: name=%v, type=%v", tli.Name, tli.TaskType),
		}
	}
	tlm.rangeID = request.RangeID
	tlm.info = common.CloneProto(tli)
	return &persistence.CreateTaskQueueResponse{}, nil
}

func (m *testTaskManager) UpdateTaskQueue(
	_ context.Context,
	request *persistence.UpdateTaskQueueRequest,
) (*persistence.UpdateTaskQueueResponse, error) {
	tli := request.TaskQueueInfo
	tlm := m.getQueueData(tli.Name, tli.NamespaceId, tli.TaskType)

	m.delay()
	defer m.delay()

	tlm.Lock()
	defer tlm.Unlock()

	tlm.updateCount++

	if tlm.rangeID != request.PrevRangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task queue: name=%v, type=%v", tli.Name, tli.TaskType),
		}
	}
	tlm.rangeID = request.RangeID
	tlm.info = common.CloneProto(tli)
	return &persistence.UpdateTaskQueueResponse{}, nil
}

func (m *testTaskManager) GetTaskQueue(
	_ context.Context,
	request *persistence.GetTaskQueueRequest,
) (*persistence.GetTaskQueueResponse, error) {
	tlm := m.getQueueData(request.TaskQueue, request.NamespaceID, request.TaskType)
	tlm.Lock()
	defer tlm.Unlock()

	if tlm.rangeID == 0 {
		return nil, serviceerror.NewNotFound("task queue not found")
	}
	return &persistence.GetTaskQueueResponse{
		RangeID:       tlm.rangeID,
		TaskQueueInfo: common.CloneProto(tlm.info),
	}, nil
}

// minTaskID returns the minimum value of the TaskID present in testTaskManager
func (m *testTaskManager) minTaskID(dbq *PhysicalTaskQueueKey) (int64, bool) {
	tlm := m.getQueueDataByKey(dbq)
	tlm.Lock()
	defer tlm.Unlock()
	minKey, _ := tlm.tasks.Min()
	key, ok := minKey.(fairLevel)
	return key.id, ok
}

// maxTaskID returns the maximum value of the TaskID present in testTaskManager
func (m *testTaskManager) maxTaskID(dbq *PhysicalTaskQueueKey) (int64, bool) {
	tlm := m.getQueueDataByKey(dbq)
	tlm.Lock()
	defer tlm.Unlock()
	maxKey, _ := tlm.tasks.Max()
	key, ok := maxKey.(fairLevel)
	return key.id, ok
}

func (m *testTaskManager) CompleteTasksLessThan(
	_ context.Context,
	request *persistence.CompleteTasksLessThanRequest,
) (int, error) {
	if m.fairness && request.ExclusiveMaxPass < 1 {
		return 0, serviceerror.NewInternal("invalid CompleteTasksLessThan request on fair queue")
	} else if !m.fairness && request.ExclusiveMaxPass != 0 {
		return 0, serviceerror.NewInternal("invalid CompleteTasksLessThan request on queue")
	}

	m.delay()
	defer m.delay()

	tlm := m.getQueueData(request.TaskQueueName, request.NamespaceID, request.TaskType)
	tlm.Lock()
	defer tlm.Unlock()
	keys := tlm.tasks.Keys()
	for _, key := range keys {
		level := key.(fairLevel)
		if m.fairness {
			if level.less(fairLevel{pass: request.ExclusiveMaxPass, id: request.ExclusiveMaxTaskID}) {
				tlm.tasks.Remove(level)
			}
		} else {
			if level.id < request.ExclusiveMaxTaskID {
				tlm.tasks.Remove(level)
			}
		}
	}
	return persistence.UnknownNumRowsAffected, nil
}

func (m *testTaskManager) ListTaskQueue(
	_ context.Context,
	_ *persistence.ListTaskQueueRequest,
) (*persistence.ListTaskQueueResponse, error) {
	return nil, fmt.Errorf("unsupported operation")
}

func (m *testTaskManager) DeleteTaskQueue(
	_ context.Context,
	request *persistence.DeleteTaskQueueRequest,
) error {
	m.Lock()
	defer m.Unlock()
	key := dbTaskQueueKey{persistenceName: request.TaskQueue.TaskQueueName, namespaceID: request.TaskQueue.NamespaceID, taskType: request.TaskQueue.TaskQueueType}
	delete(m.queues, key)
	return nil
}

func (m *testTaskManager) delay() {
	if m.delayInjection > 0 && rand.Int31n(128) >= 13 {
		time.Sleep(time.Duration(rand.Float32() * float32(m.delayInjection))) // nolint:forbidigo
	}
}

// all calls to addFault should be done before starting to call methods on testTaskManager
func (m *testTaskManager) addFault(method, err string, fraction float32) {
	if m.faultInjection == nil {
		m.faultInjection = make(map[string]float32)
	}
	m.faultInjection[method+":"+err] = fraction
}

func (m *testTaskManager) fault(method, err string) bool {
	return rand.Float32() < m.faultInjection[method+":"+err]
}

func (m *testTaskManager) CreateTasks(
	_ context.Context,
	request *persistence.CreateTasksRequest,
) (*persistence.CreateTasksResponse, error) {
	namespaceID := request.TaskQueueInfo.Data.GetNamespaceId()
	taskQueue := request.TaskQueueInfo.Data.Name
	taskType := request.TaskQueueInfo.Data.TaskType
	rangeID := request.TaskQueueInfo.RangeID

	m.delay()
	defer m.delay()

	if m.fault("CreateTasks", "ConditionFailed") {
		return nil, &persistence.ConditionFailedError{Msg: "Fake ConditionFailedError"}
	} else if m.fault("CreateTasks", "Unavailable") {
		return nil, serviceerror.NewUnavailable("Fake Unavailable")
	}

	tlm := m.getQueueData(taskQueue, namespaceID, taskType)
	tlm.Lock()
	defer tlm.Unlock()

	if tlm.rangeID != rangeID {
		m.logger.Debug("testTaskManager.CreateTask ConditionFailedError",
			tag.ShardRangeID(rangeID), tag.ShardRangeID(tlm.rangeID))
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("CreateTask failed, range id mismatch. TaskQueue: %v, taskQueueType: %v, rangeID: %v, db rangeID: %v",
				taskQueue, taskType, rangeID, tlm.rangeID),
		}
	}

	// First validate the entire batch
	for _, task := range request.Tasks {
		level := fairLevelFromAllocatedTask(task)
		m.logger.Debug("testTaskManager.CreateTask", tag.ShardRangeID(rangeID), tag.TaskKey(level), tag.Value(task.Data))

		if task.GetTaskId() <= 0 {
			panic(fmt.Errorf("invalid taskID=%v", task.GetTaskId()))
		}
		if m.fairness && task.TaskPass == 0 {
			return nil, serviceerror.NewInternal("invalid fair queue task missing pass number")
		} else if !m.fairness && task.TaskPass != 0 {
			return nil, serviceerror.NewInternal("invalid non-fair queue task with pass number")
		}

		if _, ok := tlm.tasks.Get(level); ok {
			panic(fmt.Sprintf("Duplicated TaskID %v", level))
		}
	}

	// Then insert all tasks if no errors
	for _, task := range request.Tasks {
		tlm.tasks.Put(fairLevelFromAllocatedTask(task), common.CloneProto(task))
		tlm.createTaskCount++
	}

	resp := &persistence.CreateTasksResponse{}
	if m.updateMetadataOnCreateTasks {
		tlm.info = common.CloneProto(request.TaskQueueInfo.Data)
		resp.UpdatedMetadata = true
	}
	return resp, nil
}

func (m *testTaskManager) GetTasks(
	_ context.Context,
	request *persistence.GetTasksRequest,
) (*persistence.GetTasksResponse, error) {
	m.logger.Debug("testTaskManager.GetTasks", tag.Value(request))

	if m.fairness && (request.InclusiveMinPass < 1 || request.ExclusiveMaxTaskID != math.MaxInt64) {
		return nil, serviceerror.NewInternal("invalid GetTasks request on fair queue")
	} else if !m.fairness && request.InclusiveMinPass != 0 {
		return nil, serviceerror.NewInternal("invalid GetTasks request on queue")
	}

	m.delay()
	defer m.delay()

	if m.fault("GetTasks", "Unavailable") {
		return nil, serviceerror.NewUnavailablef("GetTasks operation failed")
	}

	tlm := m.getQueueData(request.TaskQueue, request.NamespaceID, request.TaskType)
	tlm.Lock()
	defer tlm.Unlock()
	var tasks []*persistencespb.AllocatedTaskInfo

	it := tlm.tasks.Iterator()
	for it.Next() && len(tasks) < request.PageSize {
		level := it.Key().(fairLevel)
		if m.fairness {
			if level.less(fairLevel{pass: request.InclusiveMinPass, id: request.InclusiveMinTaskID}) {
				continue
			}
		} else {
			if level.id < request.InclusiveMinTaskID {
				continue
			}
			if level.id >= request.ExclusiveMaxTaskID {
				break
			}
		}
		tasks = append(tasks, it.Value().(*persistencespb.AllocatedTaskInfo))
	}
	tlm.getTasksCount++
	return &persistence.GetTasksResponse{Tasks: tasks}, nil
}

// getTaskCount returns number of tasks in a task queue
func (m *testTaskManager) getTaskCount(q *PhysicalTaskQueueKey) int {
	tlm := m.getQueueDataByKey(q)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.tasks.Size()
}

// getCreateTaskCount returns how many times CreateTask was called
func (m *testTaskManager) getCreateTaskCount(q *PhysicalTaskQueueKey) int {
	tlm := m.getQueueDataByKey(q)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.createTaskCount
}

// getGetTasksCount returns how many times GetTasks was called
func (m *testTaskManager) getGetTasksCount(q *PhysicalTaskQueueKey) int {
	tlm := m.getQueueDataByKey(q)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.getTasksCount
}

// getGetUserDataCount returns how many times GetUserData was called
func (m *testTaskManager) getGetUserDataCount(q *PhysicalTaskQueueKey) int {
	tlm := m.getQueueDataByKey(q)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.getUserDataCount
}

// getUpdateCount returns how many times UpdateTaskQueue was called
func (m *testTaskManager) getUpdateCount(q *PhysicalTaskQueueKey) int {
	tlm := m.getQueueDataByKey(q)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.updateCount
}

func (m *testTaskManager) String() string {
	m.Lock()
	defer m.Unlock()
	var out strings.Builder
	fmt.Fprintf(&out, "testTaskManager: %d queues:\n", len(m.queues))
	for k, q := range m.queues {
		fmt.Fprintf(&out, "  %s %s: %s\n", k.persistenceName, k.taskType, q)
	}
	return out.String()
}

// GetTaskQueueData implements persistence.TaskManager
func (m *testTaskManager) GetTaskQueueUserData(_ context.Context, request *persistence.GetTaskQueueUserDataRequest) (*persistence.GetTaskQueueUserDataResponse, error) {
	if m.fairness {
		panic("userdata calls should not to go fair task manager")
	}
	tlm := m.getQueueData(request.TaskQueue, request.NamespaceID, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	tlm.Lock()
	defer tlm.Unlock()
	tlm.getUserDataCount++
	return &persistence.GetTaskQueueUserDataResponse{
		UserData: tlm.userData,
	}, nil
}

// UpdateTaskQueueUserData implements persistence.TaskManager
func (m *testTaskManager) UpdateTaskQueueUserData(_ context.Context, request *persistence.UpdateTaskQueueUserDataRequest) error {
	if m.fairness {
		panic("userdata calls should not to go fair task manager")
	}
	for tq, update := range request.Updates {
		tlm := m.getQueueData(tq, request.NamespaceID, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		tlm.Lock()
		newData := common.CloneProto(update.UserData)
		newData.Version++
		tlm.userData = newData
		tlm.Unlock()
	}
	return nil
}

// ListTaskQueueUserDataEntries implements persistence.TaskManager
func (*testTaskManager) ListTaskQueueUserDataEntries(context.Context, *persistence.ListTaskQueueUserDataEntriesRequest) (*persistence.ListTaskQueueUserDataEntriesResponse, error) {
	// No need to implement this for unit tests
	panic("unimplemented")
}

// GetTaskQueuesByBuildId implements persistence.TaskManager
func (*testTaskManager) GetTaskQueuesByBuildId(context.Context, *persistence.GetTaskQueuesByBuildIdRequest) ([]string, error) {
	// No need to implement this for unit tests
	panic("unimplemented")
}

// CountTaskQueuesByBuildId implements persistence.TaskManager
func (*testTaskManager) CountTaskQueuesByBuildId(context.Context, *persistence.CountTaskQueuesByBuildIdRequest) (int, error) {
	// This is only used to validate that the build ID to task queue mapping is enforced (at the time of writing), report 0.
	return 0, nil
}

func validateTimeRange(t time.Time, expectedDuration time.Duration) bool {
	currentTime := time.Now().UTC()
	diff := time.Duration(currentTime.UnixNano() - t.UnixNano())
	if diff > expectedDuration {
		fmt.Printf("Current time: %v, Application time: %v, Difference: %v \n", currentTime, t, diff)
		return false
	}
	return true
}

func defaultTestConfig() *Config {
	config := NewConfig(dynamicconfig.NewNoopCollection())
	config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(100 * time.Millisecond)
	config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(1)
	config.AutoEnableV2 = dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true)
	return config
}

type (
	dynamicRateBurstWrapper struct {
		quotas.MutableRateBurst
		*quotas.RateLimiterImpl
	}
)

func (d *dynamicRateBurstWrapper) SetRPS(rps float64) {
	d.MutableRateBurst.SetRPS(rps)
	d.RateLimiterImpl.SetRPS(rps)
}

func (d *dynamicRateBurstWrapper) SetBurst(burst int) {
	d.MutableRateBurst.SetBurst(burst)
	d.RateLimiterImpl.SetBurst(burst)
}

func (d *dynamicRateBurstWrapper) Rate() float64 {
	return d.RateLimiterImpl.Rate()
}

func (d *dynamicRateBurstWrapper) Burst() int {
	return d.RateLimiterImpl.Burst()
}

// TODO(pri): cleanup; delete this
func useNewMatcher(config *Config) {
	config.NewMatcherSub = staticTrueChange
}

func useFairness(config *Config) {
	config.EnableFairnessSub = staticTrueChange
}

func staticTrueChange(_, _ string, _ enumspb.TaskQueueType, _ func(dynamicconfig.GradualChange[bool])) (dynamicconfig.GradualChange[bool], func()) {
	return dynamicconfig.StaticGradualChange(true), func() {}
}
