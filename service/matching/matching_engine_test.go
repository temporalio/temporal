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

package matching

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/worker_versioning"

	"github.com/emirpasic/gods/maps/treemap"
	godsutils "github.com/emirpasic/gods/utils"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"go.temporal.io/server/common/cluster/clustertest"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tqid"
)

type (
	matchingEngineSuite struct {
		suite.Suite
		*require.Assertions
		controller            *gomock.Controller
		mockHistoryClient     *historyservicemock.MockHistoryServiceClient
		mockMatchingClient    *matchingservicemock.MockMatchingServiceClient
		ns                    *namespace.Namespace
		mockNamespaceCache    *namespace.MockRegistry
		mockVisibilityManager *manager.MockVisibilityManager
		mockHostInfoProvider  *membership.MockHostInfoProvider
		mockServiceResolver   *membership.MockServiceResolver
		hostInfoForResolver   membership.HostInfo

		matchingEngine *matchingEngineImpl
		taskManager    *testTaskManager
		logger         log.Logger
		sync.Mutex
	}
)

const (
	matchingTestNamespace = "matching-test"
)

func createTestMatchingEngine(
	controller *gomock.Controller,
	config *Config,
	matchingClient matchingservice.MatchingServiceClient,
	namespaceRegistry namespace.Registry,
) *matchingEngineImpl {
	logger := log.NewTestLogger()
	tm := newTestTaskManager(logger)
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
	return newMatchingEngine(config, tm, mockHistoryClient, logger, namespaceRegistry, matchingClient, mockVisibilityManager, mockHostInfoProvider, mockServiceResolver)
}

func createMockNamespaceCache(controller *gomock.Controller, nsName namespace.Name) (*namespace.Namespace, *namespace.MockRegistry) {
	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: nsName.String()}, nil, "")
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(ns.Name(), nil).AnyTimes()
	return ns, mockNamespaceCache
}

func TestMatchingEngineSuite(t *testing.T) {
	s := new(matchingEngineSuite)
	suite.Run(t, s)
}

func (s *matchingEngineSuite) SetupSuite() {
}

func (s *matchingEngineSuite) TearDownSuite() {
}

func (s *matchingEngineSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.logger = log.NewTestLogger()
	s.Lock()
	defer s.Unlock()
	s.controller = gomock.NewController(s.T())
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockMatchingClient = matchingservicemock.NewMockMatchingServiceClient(s.controller)
	s.mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
		Return(&matchingservice.GetTaskQueueUserDataResponse{}, nil).AnyTimes()
	s.mockMatchingClient.EXPECT().UpdateTaskQueueUserData(gomock.Any(), gomock.Any()).
		Return(&matchingservice.UpdateTaskQueueUserDataResponse{}, nil).AnyTimes()
	s.mockMatchingClient.EXPECT().ReplicateTaskQueueUserData(gomock.Any(), gomock.Any()).
		Return(&matchingservice.ReplicateTaskQueueUserDataResponse{}, nil).AnyTimes()
	s.taskManager = newTestTaskManager(s.logger)
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

	s.matchingEngine = s.newMatchingEngine(defaultTestConfig(), s.taskManager)
	s.matchingEngine.Start()
}

func (s *matchingEngineSuite) TearDownTest() {
	s.matchingEngine.Stop()
	s.controller.Finish()
}

func (s *matchingEngineSuite) newMatchingEngine(
	config *Config, taskMgr persistence.TaskManager,
) *matchingEngineImpl {
	return newMatchingEngine(config, taskMgr, s.mockHistoryClient, s.logger, s.mockNamespaceCache, s.mockMatchingClient, s.mockVisibilityManager,
		s.mockHostInfoProvider, s.mockServiceResolver)
}

func newMatchingEngine(
	config *Config, taskMgr persistence.TaskManager, mockHistoryClient historyservice.HistoryServiceClient,
	logger log.Logger, mockNamespaceCache namespace.Registry, mockMatchingClient matchingservice.MatchingServiceClient,
	mockVisibilityManager manager.VisibilityManager, mockHostInfoProvider membership.HostInfoProvider, mockServiceResolver membership.ServiceResolver,
) *matchingEngineImpl {
	return &matchingEngineImpl{
		taskManager:   taskMgr,
		historyClient: mockHistoryClient,
		partitions:    make(map[tqid.PartitionKey]taskQueuePartitionManager),
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
		tokenSerializer:               common.NewProtoTaskTokenSerializer(),
		config:                        config,
		namespaceRegistry:             mockNamespaceCache,
		hostInfoProvider:              mockHostInfoProvider,
		serviceResolver:               mockServiceResolver,
		membershipChangedCh:           make(chan *membership.ChangedEvent, 1),
		clusterMeta:                   clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(false, true)),
		timeSource:                    clock.NewRealTimeSource(),
		visibilityManager:             mockVisibilityManager,
		nexusEndpointsOwnershipLostCh: make(chan struct{}),
	}
}

func (s *matchingEngineSuite) newPartitionManager(prtn tqid.Partition, config *Config) taskQueuePartitionManager {
	tqConfig := newTaskQueueConfig(prtn.TaskQueue(), config, matchingTestNamespace)

	pm, err := newTaskQueuePartitionManager(s.matchingEngine, s.ns, prtn, tqConfig, &mockUserDataManager{})
	s.Require().NoError(err)
	return pm
}

func (s *matchingEngineSuite) TestAckManager() {
	backlogMgr := newBacklogMgr(s.controller, false)
	m := newAckManager(backlogMgr)

	m.setAckLevel(100)
	s.EqualValues(100, m.getAckLevel())
	s.EqualValues(100, m.getReadLevel())
	const t1 = 200
	const t2 = 220
	const t3 = 320
	const t4 = 340
	const t5 = 360
	const t6 = 380

	m.addTask(t1)
	// Increment the backlog so that we don't under-count
	// this happens since we decrease the counter on completion of a task
	backlogMgr.db.updateApproximateBacklogCount(1)
	s.EqualValues(100, m.getAckLevel())
	s.EqualValues(t1, m.getReadLevel())

	m.addTask(t2)
	backlogMgr.db.updateApproximateBacklogCount(1)
	s.EqualValues(100, m.getAckLevel())
	s.EqualValues(t2, m.getReadLevel())

	m.completeTask(t2)
	s.EqualValues(100, m.getAckLevel())
	s.EqualValues(t2, m.getReadLevel())

	m.completeTask(t1)
	s.EqualValues(t2, m.getAckLevel())
	s.EqualValues(t2, m.getReadLevel())

	m.setAckLevel(300)
	s.EqualValues(300, m.getAckLevel())
	s.EqualValues(300, m.getReadLevel())

	m.addTask(t3)
	backlogMgr.db.updateApproximateBacklogCount(1)
	s.EqualValues(300, m.getAckLevel())
	s.EqualValues(t3, m.getReadLevel())

	m.addTask(t4)
	backlogMgr.db.updateApproximateBacklogCount(1)
	s.EqualValues(300, m.getAckLevel())
	s.EqualValues(t4, m.getReadLevel())

	m.completeTask(t3)
	s.EqualValues(t3, m.getAckLevel())
	s.EqualValues(t4, m.getReadLevel())

	m.completeTask(t4)
	s.EqualValues(t4, m.getAckLevel())
	s.EqualValues(t4, m.getReadLevel())

	m.setReadLevel(t5)
	s.EqualValues(t5, m.getReadLevel())

	m.setAckLevel(t5)
	m.setReadLevelAfterGap(t6)
	s.EqualValues(t6, m.getReadLevel())
	s.EqualValues(t6, m.getAckLevel())
}

func (s *matchingEngineSuite) TestAckManager_Sort() {
	backlogMgr := newBacklogMgr(s.controller, false)
	m := newAckManager(backlogMgr)

	const t0 = 100
	m.setAckLevel(t0)
	s.EqualValues(t0, m.getAckLevel())
	s.EqualValues(t0, m.getReadLevel())
	const t1 = 200
	const t2 = 220
	const t3 = 320
	const t4 = 340
	const t5 = 360

	m.addTask(t1)
	m.addTask(t2)
	m.addTask(t3)
	m.addTask(t4)
	m.addTask(t5)

	// Increment the backlog so that we don't under-count
	// this happens since we decrease the counter on completion of a task
	backlogMgr.db.updateApproximateBacklogCount(5)

	m.completeTask(t2)
	s.EqualValues(t0, m.getAckLevel())

	m.completeTask(t1)
	s.EqualValues(t2, m.getAckLevel())

	m.completeTask(t5)
	s.EqualValues(t2, m.getAckLevel())

	m.completeTask(t4)
	s.EqualValues(t2, m.getAckLevel())

	m.completeTask(t3)
	s.EqualValues(t5, m.getAckLevel())
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

func (s *matchingEngineSuite) TestOnlyUnloadMatchingInstance() {
	prtn := newRootPartition(
		uuid.New(),
		"makeToast",
		enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tqm, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), prtn, true, loadCauseUnspecified)
	s.Require().NoError(err)

	tqm2 := s.newPartitionManager(prtn, s.matchingEngine.config)

	// try to unload a different tqm instance with the same taskqueue ID
	s.matchingEngine.unloadTaskQueuePartition(tqm2, unloadCauseUnspecified)

	got, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), prtn, true, loadCauseUnspecified)
	s.Require().NoError(err)
	s.Require().Same(tqm, got,
		"Unload call with non-matching taskQueuePartitionManager should not cause unload")

	// this time unload the right tqm
	s.matchingEngine.unloadTaskQueuePartition(tqm, unloadCauseUnspecified)

	got, err = s.matchingEngine.getTaskQueuePartitionManager(context.Background(), prtn, true, loadCauseUnspecified)
	s.Require().NoError(err)
	s.Require().NotSame(tqm, got,
		"Unload call with matching incarnation should have caused unload")
}

func (s *matchingEngineSuite) TestPollWorkflowTaskQueues() {
	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	stickyTl := "makeStickyToast"
	stickyTlKind := enumspb.TASK_QUEUE_KIND_STICKY
	identity := "selfDrivingToaster"

	stickyTaskQueue := &taskqueuepb.TaskQueue{Name: stickyTl, Kind: stickyTlKind}

	s.matchingEngine.config.RangeSize = 2 // to test that range is not updated without tasks
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Millisecond)

	runID := uuid.NewRandom().String()
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

func (s *matchingEngineSuite) PollForTasksEmptyResultTest(callContext context.Context, taskType enumspb.TaskQueueType) {
	s.matchingEngine.config.RangeSize = 2 // to test that range is not updated without tasks
	if _, ok := callContext.Deadline(); !ok {
		s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Millisecond)
	}

	namespaceId := uuid.New()
	tl := "makeToast"
	identity := "selfDrivingToaster"

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	var taskQueueType enumspb.TaskQueueType
	tlID := newUnversionedRootQueueKey(namespaceId, tl, taskType)
	const pollCount = 10
	for i := 0; i < pollCount; i++ {
		if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			pollResp, err := s.matchingEngine.PollActivityTaskQueue(callContext, &matchingservice.PollActivityTaskQueueRequest{
				NamespaceId: namespaceId,
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
				NamespaceId: namespaceId,
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
			NamespaceId: namespaceId,
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
	s.EqualValues(1, s.taskManager.getQueueManager(tlID).RangeID())
}

func (s *matchingEngineSuite) TestPollWorkflowTaskQueues_NamespaceHandover() {
	namespaceId := uuid.New()
	taskQueue := &taskqueuepb.TaskQueue{Name: "queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	addRequest := matchingservice.AddWorkflowTaskRequest{
		NamespaceId:            namespaceId,
		Execution:              &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewRandom().String()},
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
		NamespaceId: namespaceId,
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	s.Nil(resp)
	s.Equal(common.ErrNamespaceHandover.Error(), err.Error())
}

func (s *matchingEngineSuite) TestPollActivityTaskQueues_NamespaceHandover() {
	namespaceId := uuid.New()
	taskQueue := &taskqueuepb.TaskQueue{Name: "queue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	addRequest := matchingservice.AddActivityTaskRequest{
		NamespaceId:            namespaceId,
		Execution:              &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewRandom().String()},
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
		NamespaceId: namespaceId,
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

func (s *matchingEngineSuite) AddTasksTest(taskType enumspb.TaskQueueType, isForwarded bool) {
	s.matchingEngine.config.RangeSize = 300 // override to low number for the test

	namespaceId := uuid.New()
	tl := "makeToast"
	forwardedFrom := "/_sys/makeToast/1"

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	const taskCount = 111

	runID := uuid.New()
	workflowID := "workflow1"
	execution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	for i := int64(0); i < taskCount; i++ {
		scheduledEventID := i * 3
		var err error
		if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			addRequest := matchingservice.AddActivityTaskRequest{
				NamespaceId:            namespaceId,
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
				NamespaceId:            namespaceId,
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
		s.EqualValues(taskCount, s.taskManager.getTaskCount(newUnversionedRootQueueKey(namespaceId, tl, taskType)))
	case true:
		s.EqualValues(0, s.taskManager.getTaskCount(newUnversionedRootQueueKey(namespaceId, tl, taskType)))
	}
}

func (s *matchingEngineSuite) TestAddWorkflowTaskDoesNotLoadSticky() {
	addRequest := matchingservice.AddWorkflowTaskRequest{
		NamespaceId:            uuid.New(),
		Execution:              &commonpb.WorkflowExecution{RunId: uuid.New(), WorkflowId: "wf1"},
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
		NamespaceId: uuid.New(),
		TaskQueue:   &taskqueuepb.TaskQueue{Name: "sticky", Kind: enumspb.TASK_QUEUE_KIND_STICKY},
		QueryRequest: &workflowservice.QueryWorkflowRequest{
			Namespace: "ns",
			Execution: &commonpb.WorkflowExecution{RunId: uuid.New(), WorkflowId: "wf1"},
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
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Millisecond)

	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const taskCount = 1000
	const initialRangeID = 102
	// TODO: Understand why publish is low when rangeSize is 3
	const rangeSize = 30

	namespaceId := uuid.New()
	tl := "makeToast"
	tlID := newUnversionedRootQueueKey(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.taskManager.getQueueManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	for i := int64(0); i < taskCount; i++ {
		scheduledEventID := i * 3
		addRequest := matchingservice.AddActivityTaskRequest{
			NamespaceId:            namespaceId,
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
			NamespaceId: namespaceId,
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
			NamespaceId:      namespaceId,
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
	expectedRange := int64(initialRangeID + taskCount/rangeSize)
	if taskCount%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getQueueManager(tlID).rangeID)
}

// TODO: this unit test does not seem to belong to matchingEngine, move it to the right place
func (s *matchingEngineSuite) TestSyncMatchActivities() {
	// Set a short long poll expiration so that we don't have to wait too long for 0 throttling cases
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(2 * time.Second)

	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const taskCount = 10
	const initialRangeID = 102
	// TODO: Understand why publish is low when rangeSize is 3
	const rangeSize = 30

	namespaceId := uuid.New()
	tl := "makeToast"
	dbq := newUnversionedRootQueueKey(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test
	// So we can get snapshots
	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope).WithTags(metrics.ServiceNameTag(primitives.MatchingService))

	var err error
	s.taskManager.getQueueManager(dbq).rangeID = initialRangeID
	mgr := s.newPartitionManager(dbq.partition, s.matchingEngine.config)

	mgrImpl, ok := mgr.(*taskQueuePartitionManagerImpl).defaultQueue.(*physicalTaskQueueManagerImpl)
	s.True(ok)

	mgrImpl.matcher.config.MinTaskThrottlingBurstSize = func() int { return 0 }
	mgrImpl.matcher.rateLimiter = quotas.NewRateLimiter(
		defaultTaskDispatchRPS,
		defaultTaskDispatchRPS,
	)
	mgrImpl.matcher.dynamicRateBurst = &dynamicRateBurstWrapper{
		MutableRateBurst: quotas.NewMutableRateBurst(
			defaultTaskDispatchRPS,
			defaultTaskDispatchRPS,
		),
		RateLimiterImpl: mgrImpl.matcher.rateLimiter.(*quotas.RateLimiterImpl),
	}
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
						ScheduleToStartTimeout: durationpb.New(1 * time.Second),
						ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
						StartToCloseTimeout:    durationpb.New(1 * time.Second),
						HeartbeatTimeout:       durationpb.New(1 * time.Second),
					}),
			}, nil
		}).AnyTimes()

	pollFunc := func(maxDispatch float64) (*matchingservice.PollActivityTaskQueueResponse, error) {
		return s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceId,
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
			NamespaceId:            namespaceId,
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
			NamespaceId:      namespaceId,
			WorkflowId:       workflowID,
			RunId:            runID,
			ScheduledEventId: scheduledEventID,
			ActivityId:       activityID,
			ActivityType:     activityTypeName,
		}

		serializedToken, _ := s.matchingEngine.tokenSerializer.Serialize(taskToken)
		// s.EqualValues(scheduledEventID, result.Task)

		s.EqualValues(serializedToken, result.TaskToken)
	}

	time.Sleep(20 * time.Millisecond) // So any buffer tasks from 0 rps get picked up
	snap := scope.Snapshot()
	syncCtr := snap.Counters()["test.sync_throttle_count+namespace="+matchingTestNamespace+",operation=TaskQueueMgr,service_name=matching,task_type=Activity,taskqueue=makeToast,worker-build-id=_unversioned_"]
	s.Equal(1, int(syncCtr.Value()))                        // Check times zero rps is set = throttle counter
	s.EqualValues(1, s.taskManager.getCreateTaskCount(dbq)) // Check times zero rps is set = Tasks stored in persistence
	s.EqualValues(0, s.taskManager.getTaskCount(dbq))
	expectedRange := int64(initialRangeID + taskCount/rangeSize)
	if taskCount%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getQueueManager(dbq).rangeID)

	// check the poller information
	tlType := enumspb.TASK_QUEUE_TYPE_ACTIVITY
	descResp, err := s.matchingEngine.DescribeTaskQueue(context.Background(), &matchingservice.DescribeTaskQueueRequest{
		NamespaceId: namespaceId,
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
	s.True(descResp.DescResponse.GetTaskQueueStatus().GetRatePerSecond()*numPartitions >= (defaultTaskDispatchRPS - 1))
}

func (s *matchingEngineSuite) TestConcurrentPublishConsumeActivities() {
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
	s.True(throttleCt >= 1)
}

func (s *matchingEngineSuite) concurrentPublishConsumeActivities(
	workerCount int,
	taskCount int64,
	dispatchLimitFn func(int, int64) float64,
) int64 {
	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope).WithTags(metrics.ServiceNameTag(primitives.MatchingService))
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const initialRangeID = 0
	const rangeSize = 3
	var scheduledEventID int64 = 123
	namespaceId := uuid.New()
	tl := "makeToast"
	dbq := newUnversionedRootQueueKey(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	s.taskManager.getQueueManager(dbq).rangeID = initialRangeID
	mgr := s.newPartitionManager(dbq.partition, s.matchingEngine.config)

	mgrImpl, ok := mgr.(*taskQueuePartitionManagerImpl).defaultQueue.(*physicalTaskQueueManagerImpl)
	s.Assert().True(ok)

	mgrImpl.matcher.config.MinTaskThrottlingBurstSize = func() int { return 0 }
	mgrImpl.matcher.rateLimiter = quotas.NewRateLimiter(
		defaultTaskDispatchRPS,
		defaultTaskDispatchRPS,
	)
	mgrImpl.matcher.dynamicRateBurst = &dynamicRateBurstWrapper{
		MutableRateBurst: quotas.NewMutableRateBurst(
			defaultTaskDispatchRPS,
			defaultTaskDispatchRPS,
		),
		RateLimiterImpl: mgrImpl.matcher.rateLimiter.(*quotas.RateLimiterImpl),
	}
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
					NamespaceId:            namespaceId,
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
					NamespaceId: namespaceId,
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
					NamespaceId:      namespaceId,
					WorkflowId:       workflowID,
					RunId:            runID,
					ScheduledEventId: scheduledEventID,
					ActivityId:       activityID,
					ActivityType:     activityTypeName,
				}
				resultToken, err := s.matchingEngine.tokenSerializer.Deserialize(result.TaskToken)
				s.NoError(err)

				s.EqualValues(taskToken, resultToken, fmt.Sprintf("%v!=%v", taskToken, resultToken))
				i++
			}
		}(p)
	}
	wg.Wait()
	totalTasks := int(taskCount) * workerCount
	persisted := s.taskManager.getCreateTaskCount(dbq)
	s.True(persisted < totalTasks)
	expectedRange := int64(initialRangeID + persisted/rangeSize)
	if persisted%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getQueueManager(dbq).rangeID)
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
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const workerCount = 20
	const taskCount = 100
	const initialRangeID = 0
	const rangeSize = 5
	var scheduledEventID int64 = 123
	var startedEventID int64 = 1412

	namespaceId := uuid.New()
	tl := "makeToast"
	tlID := newUnversionedRootQueueKey(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.taskManager.getQueueManager(tlID).rangeID = initialRangeID
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
					NamespaceId:            namespaceId,
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
					NamespaceId: namespaceId,
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
					NamespaceId:      namespaceId,
					WorkflowId:       workflowID,
					RunId:            runID,
					ScheduledEventId: scheduledEventID,
					StartedEventId:   startedEventID,
				}
				resultToken, err := s.matchingEngine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}

				s.EqualValues(taskToken, resultToken, fmt.Sprintf("%v!=%v", taskToken, resultToken))
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
	expectedRange := int64(initialRangeID + persisted/rangeSize)
	if persisted%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getQueueManager(tlID).rangeID)
}

func (s *matchingEngineSuite) TestPollWithExpiredContext() {
	identity := "nobody"
	namespaceID := namespace.ID(uuid.New())
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

func (s *matchingEngineSuite) TestMultipleEnginesActivitiesRangeStealing() {
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const engineCount = 2
	const taskCount = 400
	const iterations = 2
	const initialRangeID = 0
	const rangeSize = 10
	var scheduledEventID int64 = 123

	namespaceId := uuid.New()
	tl := "makeToast"
	tlID := newUnversionedRootQueueKey(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.taskManager.getQueueManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	engines := make([]*matchingEngineImpl, engineCount)
	for p := 0; p < engineCount; p++ {
		e := s.newMatchingEngine(defaultTestConfig(), s.taskManager)
		e.config.RangeSize = rangeSize
		engines[p] = e
		e.Start()
	}

	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; i++ {
				addRequest := matchingservice.AddActivityTaskRequest{
					NamespaceId:            namespaceId,
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
					NamespaceId: namespaceId,
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
					NamespaceId:  namespaceId,
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
				s.EqualValues(taskToken, resultToken, fmt.Sprintf("%v!=%v", taskToken, resultToken))
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
	expectedRange := int64(initialRangeID + persisted/rangeSize)
	if persisted%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getQueueManager(tlID).rangeID)
}

func (s *matchingEngineSuite) TestMultipleEnginesWorkflowTasksRangeStealing() {
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const engineCount = 2
	const taskCount = 400
	const iterations = 2
	const initialRangeID = 0
	const rangeSize = 10
	var scheduledEventID int64 = 123

	namespaceId := uuid.New()
	tl := "makeToast"
	tlID := newUnversionedRootQueueKey(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.taskManager.getQueueManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	engines := make([]*matchingEngineImpl, engineCount)
	for p := 0; p < engineCount; p++ {
		e := s.newMatchingEngine(defaultTestConfig(), s.taskManager)
		e.config.RangeSize = rangeSize
		engines[p] = e
		e.Start()
	}

	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; i++ {
				addRequest := matchingservice.AddWorkflowTaskRequest{
					NamespaceId:            namespaceId,
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
					NamespaceId: namespaceId,
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
					NamespaceId:    namespaceId,
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
				s.EqualValues(taskToken, resultToken, fmt.Sprintf("%v!=%v", taskToken, resultToken))
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
	expectedRange := int64(initialRangeID + persisted/rangeSize)
	if persisted%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getQueueManager(tlID).rangeID)
}

func (s *matchingEngineSuite) TestAddTaskAfterStartFailure() {
	// test default is 100ms, but make it longer for this test so it's not flaky
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Second)

	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	namespaceId := uuid.New()
	tl := "makeToast"
	dbq := newUnversionedRootQueueKey(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	scheduledEventID := int64(0)
	addRequest := matchingservice.AddActivityTaskRequest{
		NamespaceId:            namespaceId,
		Execution:              workflowExecution,
		ScheduledEventId:       scheduledEventID,
		TaskQueue:              taskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
	}

	_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.NoError(err)
	s.EqualValues(1, s.taskManager.getTaskCount(dbq))

	task, _, err := s.matchingEngine.pollTask(context.Background(), dbq.partition, &pollMetadata{})
	s.NoError(err)

	task.finish(errors.New("test error"))
	s.EqualValues(1, s.taskManager.getTaskCount(dbq))
	task2, _, err := s.matchingEngine.pollTask(context.Background(), dbq.partition, &pollMetadata{})
	s.NoError(err)
	s.NotNil(task2)

	s.NotEqual(task.event.GetTaskId(), task2.event.GetTaskId())
	s.Equal(task.event.Data.GetWorkflowId(), task2.event.Data.GetWorkflowId())
	s.Equal(task.event.Data.GetRunId(), task2.event.Data.GetRunId())
	s.Equal(task.event.Data.GetScheduledEventId(), task2.event.Data.GetScheduledEventId())

	task2.finish(nil)
	s.EqualValues(0, s.taskManager.getTaskCount(dbq))
}

// TODO: should be moved to backlog_manager_test
func (s *matchingEngineSuite) TestTaskQueueManagerGetTaskBatch() {
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	namespaceId := uuid.New()
	tl := "makeToast"
	dbq := newUnversionedRootQueueKey(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

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
			NamespaceId:            namespaceId,
			Execution:              workflowExecution,
			ScheduledEventId:       scheduledEventID,
			TaskQueue:              taskQueue,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		}

		_, _, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		s.NoError(err)
	}

	tlMgr, ok := s.matchingEngine.partitions[dbq.Partition().Key()].(*taskQueuePartitionManagerImpl).defaultQueue.(*physicalTaskQueueManagerImpl)
	s.True(ok, "taskQueueManger doesn't implement taskQueuePartitionManager interface")
	s.EqualValues(taskCount, s.taskManager.getTaskCount(dbq))

	// wait until all tasks are read by the task pump and enqueued into the in-memory buffer
	// at the end of this step, ackManager readLevel will also be equal to the buffer size
	expectedBufSize := min(cap(tlMgr.backlogMgr.taskReader.taskBuffer), taskCount)
	s.True(s.awaitCondition(func() bool { return len(tlMgr.backlogMgr.taskReader.taskBuffer) == expectedBufSize }, time.Second))

	// unload the queue and stop all goroutines that read / write tasks in the background
	// remainder of this test works with the in-memory buffer
	tlMgr.UnloadFromPartitionManager(unloadCauseUnspecified)

	// setReadLevel should NEVER be called without updating ackManager.outstandingTasks
	// This is only for unit test purpose
	tlMgr.backlogMgr.taskAckManager.setReadLevel(tlMgr.backlogMgr.db.GetMaxReadLevel())
	batch, err := tlMgr.backlogMgr.taskReader.getTaskBatch(context.Background())
	s.Nil(err)
	s.EqualValues(0, len(batch.tasks))
	s.EqualValues(tlMgr.backlogMgr.db.GetMaxReadLevel(), batch.readLevel)
	s.True(batch.isReadBatchDone)

	tlMgr.backlogMgr.taskAckManager.setReadLevel(0)
	batch, err = tlMgr.backlogMgr.taskReader.getTaskBatch(context.Background())
	s.Nil(err)
	s.EqualValues(rangeSize, len(batch.tasks))
	s.EqualValues(rangeSize, batch.readLevel)
	s.True(batch.isReadBatchDone)

	s.setupRecordActivityTaskStartedMock(tl)

	// reset the ackManager readLevel to the buffer size and consume
	// the in-memory tasks by calling Poll API - assert ackMgr state
	// at the end
	tlMgr.backlogMgr.taskAckManager.setReadLevel(int64(expectedBufSize))

	// complete rangeSize events
	for i := int64(0); i < rangeSize; i++ {
		identity := "nobody"
		result, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceId,
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
	batch, err = tlMgr.backlogMgr.taskReader.getTaskBatch(context.Background())
	s.Nil(err)
	s.True(0 < len(batch.tasks) && len(batch.tasks) <= rangeSize)
	s.True(batch.isReadBatchDone)
}

// TODO: should be moved to backlog_manager_test
func (s *matchingEngineSuite) TestTaskQueueManagerGetTaskBatch_ReadBatchDone() {
	namespaceId := uuid.New()
	tl := "makeToast"
	prtn := newRootPartition(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	const rangeSize = 10
	const maxReadLevel = int64(120)
	config := defaultTestConfig()
	config.RangeSize = rangeSize
	pm := s.newPartitionManager(prtn, config)

	tlMgr, ok := pm.(*taskQueuePartitionManagerImpl).defaultQueue.(*physicalTaskQueueManagerImpl)
	s.True(ok)

	tlMgr.Start()

	// tlMgr.taskWriter startup is async so give it time to complete, otherwise
	// the following few lines get clobbered as part of the taskWriter.Start()
	time.Sleep(100 * time.Millisecond)

	tlMgr.backlogMgr.taskAckManager.setReadLevel(0)
	tlMgr.backlogMgr.db.SetMaxReadLevel(maxReadLevel)
	batch, err := tlMgr.backlogMgr.taskReader.getTaskBatch(context.Background())
	s.Empty(batch.tasks)
	s.Equal(int64(rangeSize*10), batch.readLevel)
	s.False(batch.isReadBatchDone)
	s.NoError(err)

	tlMgr.backlogMgr.taskAckManager.setReadLevel(batch.readLevel)
	batch, err = tlMgr.backlogMgr.taskReader.getTaskBatch(context.Background())
	s.Empty(batch.tasks)
	s.Equal(maxReadLevel, batch.readLevel)
	s.True(batch.isReadBatchDone)
	s.NoError(err)
}

func (s *matchingEngineSuite) TestTaskQueueManager_CyclingBehavior() {
	namespaceId := uuid.New()
	tl := "makeToast"
	dbq := newUnversionedRootQueueKey(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	config := defaultTestConfig()

	for i := 0; i < 4; i++ {
		prevGetTasksCount := s.taskManager.getGetTasksCount(dbq)

		mgr := s.newPartitionManager(dbq.partition, config)

		mgr.Start()
		// tlMgr.taskWriter startup is async so give it time to complete
		time.Sleep(100 * time.Millisecond)
		mgr.(*taskQueuePartitionManagerImpl).unloadFromEngine(unloadCauseUnspecified)

		getTasksCount := s.taskManager.getGetTasksCount(dbq) - prevGetTasksCount
		s.LessOrEqual(getTasksCount, 1)
	}
}

// TODO: should be moved to backlog_manager_test
func (s *matchingEngineSuite) TestTaskExpiryAndCompletion() {
	runID := uuid.NewRandom().String()
	workflowID := uuid.New()
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	namespaceId := uuid.New()
	tl := "task-expiry-completion-tl0"
	dbq := newUnversionedRootQueueKey(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

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
				NamespaceId:            namespaceId,
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

		tlMgr, ok := s.matchingEngine.partitions[dbq.Partition().Key()].(*taskQueuePartitionManagerImpl).defaultQueue.(*physicalTaskQueueManagerImpl)
		s.True(ok, "failed to load task queue")
		s.EqualValues(taskCount, s.taskManager.getTaskCount(dbq))

		// wait until all tasks are loaded by into in-memory buffers by task queue manager
		// the buffer size should be one less than expected because dispatcher will dequeue the head
		// 1/4 should be thrown out because they are expired before they hit the buffer
		s.True(s.awaitCondition(func() bool { return len(tlMgr.backlogMgr.taskReader.taskBuffer) >= (3*taskCount/4 - 1) }, time.Second))

		// ensure the 1/4 of tasks with small ScheduleToStartTimeout will be expired when they come out of the buffer
		time.Sleep(300 * time.Millisecond)

		maxTimeBetweenTaskDeletes = tc.maxTimeBtwnDeletes

		s.setupRecordActivityTaskStartedMock(tl)

		pollReq := &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceId,
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
		tlMgr.backlogMgr.taskGC.RunNow(context.Background(), tlMgr.backlogMgr.taskAckManager.getAckLevel())
		tlMgr.backlogMgr.taskGC.RunNow(context.Background(), tlMgr.backlogMgr.taskAckManager.getAckLevel())
	}
}

func (s *matchingEngineSuite) TestGetVersioningData() {
	namespaceID := namespace.ID(uuid.New())
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
	namespaceID := namespace.ID(uuid.New())
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
	namespaceID := namespace.ID(uuid.New())
	tq := "tupac"

	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 123456}},
	}
	s.NoError(s.taskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID.String(),
			TaskQueue:   tq,
			UserData:    userData,
		}))
	userData.Version++

	res, err := s.matchingEngine.GetTaskQueueUserData(context.Background(), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId:              namespaceID.String(),
		TaskQueue:                tq,
		TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		LastKnownUserDataVersion: 0,
	})
	s.NoError(err)
	s.Equal(res.UserData, userData)
}

func (s *matchingEngineSuite) TestGetTaskQueueUserData_ReturnsEmpty() {
	namespaceID := namespace.ID(uuid.New())
	tq := "tupac"

	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 123456}},
	}
	s.NoError(s.taskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID.String(),
			TaskQueue:   tq,
			UserData:    userData,
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
	namespaceID := namespace.ID(uuid.New())
	tq := "tupac"

	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 123456}},
	}
	s.NoError(s.taskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID.String(),
			TaskQueue:   tq,
			UserData:    userData,
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
	namespaceID := namespace.ID(uuid.New())
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
	namespaceID := namespace.ID(uuid.New())
	tq := "tupac"

	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 123456}},
	}
	s.NoError(s.taskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID.String(),
			TaskQueue:   tq,
			UserData:    userData,
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
	namespaceID := namespace.ID(uuid.New())
	tq := "tupac"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		time.Sleep(200 * time.Millisecond)
		_, _ = s.matchingEngine.ForceUnloadTaskQueue(context.Background(), &matchingservice.ForceUnloadTaskQueueRequest{
			NamespaceId:   namespaceID.String(),
			TaskQueue:     tq,
			TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
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
	namespaceID := namespace.ID(uuid.New())
	tq := "tupac"

	userData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 123456}},
	}
	err := s.taskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID.String(),
			TaskQueue:   tq,
			UserData:    userData,
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
	namespaceId := uuid.New()
	tq := "makeToast"

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.mockMatchingClient.EXPECT().UpdateWorkerBuildIdCompatibility(gomock.Any(), &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceId,
		TaskQueue:   tq,
		Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_PersistUnknownBuildId{
			PersistUnknownBuildId: "unknown",
		},
	}).Return(&matchingservice.UpdateWorkerBuildIdCompatibilityResponse{}, nil).AnyTimes() // might get called again on dispatch from spooled

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		_, _, err := s.matchingEngine.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
			NamespaceId:            namespaceId,
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
		prtn := newRootPartition(namespaceId, tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		task, _, err := s.matchingEngine.pollTask(ctx, prtn, &pollMetadata{
			workerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
				BuildId:       "unknown",
				UseVersioning: true,
			},
		})
		s.NoError(err)
		s.Equal("wf", task.event.Data.WorkflowId)
		s.Equal(int64(123), task.event.Data.ScheduledEventId)
		task.finish(nil)
		wg.Done()
	}()

	wg.Wait()
}

func (s *matchingEngineSuite) TestDemotedMatch() {
	namespaceId := uuid.New()
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
	err := s.taskManager.UpdateTaskQueueUserData(ctx, &persistence.UpdateTaskQueueUserDataRequest{
		NamespaceID: namespaceId,
		TaskQueue:   tq,
		UserData: &persistencespb.VersionedTaskQueueUserData{
			Data:    userData,
			Version: 34,
		},
	})
	s.Assert().NoError(err)

	// add a task for build0, will get spooled in its set
	_, _, err = s.matchingEngine.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
		NamespaceId:      namespaceId,
		Execution:        &commonpb.WorkflowExecution{RunId: "run", WorkflowId: "wf"},
		ScheduledEventId: 123,
		TaskQueue:        &taskqueuepb.TaskQueue{Name: tq, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		VersionDirective: worker_versioning.MakeBuildIdDirective(build0),
	})
	s.NoError(err)
	// allow taskReader to finish starting dispatch loop so that we can unload tqms cleanly
	time.Sleep(10 * time.Millisecond)

	// unload base and versioned tqm. note: unload the partition manager unloads both
	prtn := newRootPartition(namespaceId, tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	baseTqm, err := s.matchingEngine.getTaskQueuePartitionManager(ctx, prtn, false, loadCauseUnspecified)
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
	err = s.taskManager.UpdateTaskQueueUserData(ctx, &persistence.UpdateTaskQueueUserDataRequest{
		NamespaceID: namespaceId,
		TaskQueue:   tq,
		UserData: &persistencespb.VersionedTaskQueueUserData{
			Data:    userData,
			Version: 34,
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
	task.finish(nil)
}

func (s *matchingEngineSuite) TestUnloadOnMembershipChange() {
	// need to create a new engine for this test to customize mockServiceResolver
	s.mockServiceResolver = membership.NewMockServiceResolver(s.controller)
	s.mockServiceResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockServiceResolver.EXPECT().RemoveListener(gomock.Any()).AnyTimes()

	self := s.mockHostInfoProvider.HostInfo()
	other := membership.NewHostInfoFromAddress("other")

	config := defaultTestConfig()
	config.MembershipUnloadDelay = dynamicconfig.GetDurationPropertyFn(10 * time.Millisecond)
	e := s.newMatchingEngine(config, s.taskManager)
	e.Start()
	defer e.Stop()

	p1, err := tqid.NormalPartitionFromRpcName("makeToast", uuid.New(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.NoError(err)
	p2, err := tqid.NormalPartitionFromRpcName("makeToast", uuid.New(), enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.NoError(err)

	_, err = e.getTaskQueuePartitionManager(context.Background(), p1, true, loadCauseUnspecified)
	s.NoError(err)
	_, err = e.getTaskQueuePartitionManager(context.Background(), p2, true, loadCauseUnspecified)
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
		tqm, err := e.getTaskQueuePartitionManager(context.Background(), p, false, loadCauseUnspecified)
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

	prtn := newRootPartition(
		uuid.New(),
		"MetricTester",
		enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	tqm, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), prtn, true, loadCauseUnspecified)
	s.Require().NoError(err)

	s.TaskQueueMetricValidator(capture, 1, 1, 1, 1, 1, 1)

	// Calling the update gauge function should increase each of the metrics to 2
	// since we are dealing with a root partition
	tqmImpl, ok := tqm.(*taskQueuePartitionManagerImpl)
	s.True(ok)
	s.matchingEngine.updateTaskQueuePartitionGauge(tqmImpl, 1)
	s.TaskQueueMetricValidator(capture, 2, 2, 2, 2, 2, 2)

}

func (s *matchingEngineSuite) TestUpdateTaskQueuePartitionGauge_RootPartitionActivityType() {
	// for getting snapshots of metrics
	s.matchingEngine.metricsHandler = metricstest.NewCaptureHandler()
	captureHandler, ok := s.matchingEngine.metricsHandler.(*metricstest.CaptureHandler)
	s.True(ok)
	capture := captureHandler.StartCapture()

	prtn := newRootPartition(
		uuid.New(),
		"MetricTester",
		enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tqm, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), prtn, true, loadCauseUnspecified)
	s.Require().NoError(err)

	// Creation of a new root partition, having an activity task queue, should not have
	// increased FamilyCounter. Other metrics should be 1.
	s.TaskQueueMetricValidator(capture, 1, 0, 1, 1, 1, 1)

	// Calling the update gauge function should increase each of the metrics to 2
	// since we are dealing with a root partition
	tqmImpl, ok := tqm.(*taskQueuePartitionManagerImpl)
	s.True(ok)
	s.matchingEngine.updateTaskQueuePartitionGauge(tqmImpl, 1)
	s.TaskQueueMetricValidator(capture, 2, 0, 2, 2, 2, 2)

}

func (s *matchingEngineSuite) TestUpdateTaskQueuePartitionGauge_NonRootPartition() {
	s.matchingEngine.metricsHandler = metricstest.NewCaptureHandler()
	captureHandler, ok := s.matchingEngine.metricsHandler.(*metricstest.CaptureHandler)
	s.True(ok)
	capture := captureHandler.StartCapture()

	NonRootPrtn := newTestTaskQueue(
		uuid.New(),
		"MetricTester",
		enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(31)
	tqm, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), NonRootPrtn, true, loadCauseUnspecified)
	s.Require().NoError(err)

	// Creation of a non-root partition should only increase the Queue Partition counter
	s.TaskQueueMetricValidator(capture, 1, 0, 1, 0, 1, 1)

	// Calling the update gauge function should increase each of the metrics to 2
	// since we are dealing with a root partition
	tqmImpl, ok := tqm.(*taskQueuePartitionManagerImpl)
	s.True(ok)
	s.matchingEngine.updateTaskQueuePartitionGauge(tqmImpl, 1)
	s.TaskQueueMetricValidator(capture, 2, 0, 2, 0, 2, 2)

}

func (s *matchingEngineSuite) TestUpdatePhysicalTaskQueueGauge_UnVersioned() {
	s.matchingEngine.metricsHandler = metricstest.NewCaptureHandler()
	captureHandler, ok := s.matchingEngine.metricsHandler.(*metricstest.CaptureHandler)
	s.True(ok)
	capture := captureHandler.StartCapture()

	prtn := newRootPartition(
		uuid.New(),
		"MetricTester",
		enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tqm, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), prtn, true, loadCauseUnspecified)
	s.Require().NoError(err)

	// Creating a TaskQueuePartitionManager results in creating a PhysicalTaskQueueManager which should increase
	// the size of the map to 1 and it's counter to 1.
	s.PhysicalQueueMetricValidator(capture, 1, 1)

	tlmImpl, ok := tqm.(*taskQueuePartitionManagerImpl).defaultQueue.(*physicalTaskQueueManagerImpl)
	s.True(ok)

	s.matchingEngine.updatePhysicalTaskQueueGauge(tlmImpl, 1)

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

	namespaceId := uuid.New()
	versionSet := uuid.New()
	tl := "MetricTester"
	dbq := VersionSetQueueKey(newTestTaskQueue(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY).RootPartition(), versionSet)
	tqm, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), dbq.Partition(), true, loadCauseUnspecified)
	s.Require().NoError(err)

	// Creating a TaskQueuePartitionManager results in creating a PhysicalTaskQueueManager which should increase
	// the size of the map to 1 and it's counter to 1.
	s.PhysicalQueueMetricValidator(capture, 1, 1)

	Vqtpm, err := tqm.(*taskQueuePartitionManagerImpl).getVersionedQueueNoWait(versionSet, "", true)
	s.Require().NoError(err)

	// Creating a VersionedQueue results in increasing the size of the map to 2, due to 2 entries now,
	// with it's counter to 1.
	s.PhysicalQueueMetricValidator(capture, 2, 1)
	s.matchingEngine.updatePhysicalTaskQueueGauge(Vqtpm.(*physicalTaskQueueManagerImpl), 1)
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

	namespaceId := uuid.New()
	buildID := uuid.New()
	tl := "MetricTester"
	dbq := BuildIdQueueKey(newTestTaskQueue(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY).RootPartition(), buildID)
	tqm, err := s.matchingEngine.getTaskQueuePartitionManager(context.Background(), dbq.Partition(), true, loadCauseUnspecified)
	s.Require().NoError(err)

	// Creating a TaskQueuePartitionManager results in creating a PhysicalTaskQueueManager which should increase
	// the size of the map to 1 and it's counter to 1.
	s.PhysicalQueueMetricValidator(capture, 1, 1)

	Vqtpm, err := tqm.(*taskQueuePartitionManagerImpl).getVersionedQueueNoWait("", buildID, true)
	s.Require().NoError(err)

	// Creating a VersionedQueue results in increasing the size of the map to 2, due to 2 entries now,
	// with it's counter to 1.
	s.PhysicalQueueMetricValidator(capture, 2, 1)
	s.matchingEngine.updatePhysicalTaskQueueGauge(Vqtpm.(*physicalTaskQueueManagerImpl), 1)
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
	runID := uuid.NewRandom().String()
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
	tq := "approximateBacklogCounter"
	ptq := newUnversionedRootQueueKey(namespaceId, tq, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.taskManager.getQueueManager(ptq).rangeID = 1
	s.matchingEngine.config.RangeSize = 10

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tq,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	return taskQueue, ptq
}

func (s *matchingEngineSuite) addWorkflowTask(workflowExecution *commonpb.WorkflowExecution, taskQueue *taskqueuepb.TaskQueue) {
	var doneAdding bool
	for !doneAdding {
		addRequest := matchingservice.AddWorkflowTaskRequest{
			NamespaceId:            namespaceId,
			Execution:              workflowExecution,
			ScheduledEventId:       1,
			TaskQueue:              taskQueue,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		}
		_, _, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
		if err != nil {
			continue
		}
		s.NoError(err)
		doneAdding = true
	}
}

func (s *matchingEngineSuite) createPollWorkflowTaskRequestAndPoll(taskQueue *taskqueuepb.TaskQueue) {
	var donePolling bool
	for !donePolling {
		result, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
			NamespaceId: namespaceId,
			PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
				TaskQueue: taskQueue,
				Identity:  "nobody",
			},
		}, metrics.NoopMetricsHandler)
		if len(result.TaskToken) == 0 || result.GetAttempt() == 0 {
			continue
		}
		if err != nil {
			// DB could have failed while fetching tasks; try again
			continue
		}
		s.NoError(err)
		donePolling = true
	}
}

// addWorkflowTasks adds taskCount number of tasks for each numWorker
func (s *matchingEngineSuite) addWorkflowTasks(concurrently bool, numWorkers int, taskCount int,
	taskQueue *taskqueuepb.TaskQueue, workflowExecution *commonpb.WorkflowExecution, wg *sync.WaitGroup) {
	if concurrently {
		for p := 0; p < numWorkers; p++ {
			go func() {
				for i := 0; i < taskCount; i++ {
					s.addWorkflowTask(workflowExecution, taskQueue)
				}
				wg.Done()
			}()
		}
	} else {
		// Add tasks sequentially
		for p := 0; p < numWorkers; p++ {
			for i := 0; i < taskCount; i++ {
				s.addWorkflowTask(workflowExecution, taskQueue)
			}
		}
	}
}

// pollWorkflowTasks polls tasks using numWorkers
func (s *matchingEngineSuite) pollWorkflowTasks(concurrently bool, workflowType *commonpb.WorkflowType, numPollers int, taskCount int,
	ptq *PhysicalTaskQueueKey, taskQueue *taskqueuepb.TaskQueue, wg *sync.WaitGroup) {
	s.mockHistoryWhilePolling(workflowType)
	if concurrently {
		for p := 0; p < numPollers; p++ {
			go func() {
				for i := 0; i < taskCount; i++ {
					s.createPollWorkflowTaskRequestAndPoll(taskQueue)
				}
				wg.Done()
			}()
		}
	} else {
		tasksPolled := 0
		for i := 0; i < numPollers*taskCount; i++ {
			s.createPollWorkflowTaskRequestAndPoll(taskQueue)
			tasksPolled += 1

			// PartitionManager could have been unloaded; fetch the latest copy
			pgMgr := s.getPhysicalTaskQueueManagerImpl(ptq)
			s.LessOrEqual(int64(taskCount-tasksPolled), pgMgr.backlogMgr.db.getApproximateBacklogCount())
		}
	}
}

// getPhysicalTaskQueueManagerImpl extracts the physicalTaskQueueManagerImpl for the given PhysicalTaskQueueKey
func (s *matchingEngineSuite) getPhysicalTaskQueueManagerImpl(ptq *PhysicalTaskQueueKey) *physicalTaskQueueManagerImpl {
	pgMgr, ok := s.matchingEngine.partitions[ptq.Partition().Key()].(*taskQueuePartitionManagerImpl).defaultQueue.(*physicalTaskQueueManagerImpl)
	s.True(ok, "taskQueueManger doesn't implement taskQueuePartitionManager interface")
	return pgMgr
}

func (s *matchingEngineSuite) addConsumeAllWorkflowTasksNonConcurrently(taskCount int, numWorkers int, numPollers int) {
	workflowType, workflowExecution := s.generateWorkflowExecution()
	taskQueue, ptq := s.createTQAndPTQForBacklogTests()

	s.addWorkflowTasks(false, numWorkers, taskCount, taskQueue, workflowExecution, nil)
	s.EqualValues(taskCount*numWorkers, s.taskManager.getCreateTaskCount(ptq))
	s.EqualValues(taskCount*numWorkers, s.taskManager.getTaskCount(ptq))

	// Extract the pgMgr for validating approximateBacklogCounter
	pgMgr := s.getPhysicalTaskQueueManagerImpl(ptq)
	s.EqualValues(int64(taskCount*numWorkers), pgMgr.backlogMgr.db.getApproximateBacklogCount())

	s.pollWorkflowTasks(false, workflowType, numPollers, taskCount, ptq, taskQueue, nil)

	s.LessOrEqual(int64(0), pgMgr.backlogMgr.db.getApproximateBacklogCount())
}

func (s *matchingEngineSuite) TestAddConsumeWorkflowTasksNoDBErrors() {
	s.addConsumeAllWorkflowTasksNonConcurrently(100, 1, 1)
}

func (s *matchingEngineSuite) TestAddConsumeWorkflowTasksDBErrors() {
	s.taskManager.dbConditionalFailedError = true
	s.addConsumeAllWorkflowTasksNonConcurrently(100, 1, 1)
}

func (s *matchingEngineSuite) TestMultipleWorkersAddConsumeWorkflowTasksNoDBErrors() {
	s.addConsumeAllWorkflowTasksNonConcurrently(100, 5, 5)
}

func (s *matchingEngineSuite) TestMultipleWorkersAddConsumeWorkflowTasksDBErrors() {
	s.taskManager.dbConditionalFailedError = true
	s.addConsumeAllWorkflowTasksNonConcurrently(100, 5, 5)
}

func (s *matchingEngineSuite) resetBacklogCounter(numWorkers int, taskCount int, rangeSize int) {
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(1 * time.Millisecond)
	s.matchingEngine.config.UpdateAckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(100 * time.Millisecond)

	workflowType, workflowExecution := s.generateWorkflowExecution()
	taskQueue, ptq := s.createTQAndPTQForBacklogTests()
	s.matchingEngine.config.RangeSize = int64(rangeSize)

	s.addWorkflowTasks(false, numWorkers, taskCount, taskQueue, workflowExecution, nil)

	// TaskID of the first task to be added
	minTaskID, done := s.taskManager.minTaskID(ptq)
	s.True(done)

	partitionManager, err := s.matchingEngine.getTaskQueuePartitionManagerNoWait(ptq.Partition(), false, loadCauseTask)
	s.NoError(err)
	pgMgr := s.getPhysicalTaskQueueManagerImpl(ptq)

	s.EqualValues(taskCount*numWorkers, s.taskManager.getTaskCount(ptq))

	// Check the maxReadLevel with the value of task stored in db
	maxTaskId, ok := s.taskManager.maxTaskID(ptq)
	s.True(ok)
	s.EqualValues(maxTaskId, pgMgr.backlogMgr.db.maxReadLevel.Load())

	// validate the approximateBacklogCounter
	s.EqualValues(taskCount*numWorkers, pgMgr.backlogMgr.db.getApproximateBacklogCount())

	// Unload the PQM
	s.matchingEngine.unloadTaskQueuePartition(partitionManager, unloadCauseForce)

	// Simulate a TTL'ed task in Cassandra by removing it from the DB
	// Remove the task from testTaskManager but not from db/AckManager

	// Stop the backlogManager so that we TTL and the taskReader does not catch this
	request := &persistence.CompleteTasksLessThanRequest{
		NamespaceID:        namespaceId,
		TaskQueueName:      taskQueue.Name,
		TaskType:           enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		ExclusiveMaxTaskID: minTaskID + 1,
		Limit:              100,
	}
	_, err = s.taskManager.CompleteTasksLessThan(context.Background(), request)
	s.NoError(err)
	s.EqualValues((taskCount*numWorkers)-1, s.taskManager.getTaskCount(ptq))

	// Add pollers which shall also load the fresher version of tqm
	s.pollWorkflowTasks(false, workflowType, 1, (taskCount*numWorkers)-1, ptq, taskQueue, nil)

	// Update pgMgr to have the latest pgMgr
	pgMgr = s.getPhysicalTaskQueueManagerImpl(ptq)

	// Overwrite the maxReadLevel since it could have increased if the previous taskWriter was stopped (which would not result in resetting);
	// This should never be called and is only being done here for test purposes
	pgMgr.backlogMgr.db.SetMaxReadLevel(maxTaskId)

	s.EqualValues(0, s.taskManager.getTaskCount(ptq))
	s.Eventually(func() bool {
		return int64(0) == pgMgr.backlogMgr.db.getApproximateBacklogCount()
	}, 3*time.Second, 10*time.Millisecond, "backlog counter should have been reset")

	s.EqualValues(int64(0), pgMgr.backlogMgr.db.getApproximateBacklogCount())
}

// TestResettingBacklogCounter tests the scenario where approximateBacklogCounter over-counts and resets it accordingly
func (s *matchingEngineSuite) TestResetBacklogCounterNoDBErrors() {
	s.resetBacklogCounter(2, 2, 2)
}

func (s *matchingEngineSuite) TestResetBacklogCounterDBErrors() {
	s.taskManager.dbConditionalFailedError = true
	s.resetBacklogCounter(2, 2, 2)
}

func (s *matchingEngineSuite) TestMoreTasksResetBacklogCounterNoDBErrors() {
	s.resetBacklogCounter(10, 20, 2)
}

func (s *matchingEngineSuite) TestMoreTasksResetBacklogCounterDBErrors() {
	s.taskManager.dbConditionalFailedError = true
	s.resetBacklogCounter(10, 50, 5)
}

// Concurrent tests for testing approximateBacklogCounter

func (s *matchingEngineSuite) concurrentPublishAndConsumeValidateBacklogCounter(numWorkers int, tasksToAdd int, tasksToPoll int) {
	workflowType, workflowExecution := s.generateWorkflowExecution()
	taskQueue, ptq := s.createTQAndPTQForBacklogTests()

	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(10 * time.Millisecond)
	s.matchingEngine.config.UpdateAckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueue(1 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(2 * numWorkers)

	s.addWorkflowTasks(true, numWorkers, tasksToAdd, taskQueue, workflowExecution, &wg)
	s.pollWorkflowTasks(true, workflowType, numWorkers, tasksToPoll, ptq, taskQueue, &wg)

	wg.Wait()

	pgMgr := s.getPhysicalTaskQueueManagerImpl(ptq)

	// force GC to make sure all the acked tasks are cleaned up before validating the count
	pgMgr.backlogMgr.taskGC.RunNow(context.Background(), pgMgr.backlogMgr.taskAckManager.getAckLevel())
	s.LessOrEqual(int64(s.taskManager.getTaskCount(ptq)), pgMgr.backlogMgr.db.getApproximateBacklogCount())
}

func (s *matchingEngineSuite) TestConcurrentAddWorkflowTasksNoDBErrors() {
	s.concurrentPublishAndConsumeValidateBacklogCounter(150, 100, 0)
}

func (s *matchingEngineSuite) TestConcurrentAddWorkflowTasksDBErrors() {
	s.T().Skip("Skipping this as the backlog counter could under-count. Fix requires making " +
		"UpdateState an atomic operation.")
	s.taskManager.dbConditionalFailedError = true
	s.concurrentPublishAndConsumeValidateBacklogCounter(150, 100, 0)
}

func (s *matchingEngineSuite) TestConcurrentAdd_PollWorkflowTasksNoDBErrors() {
	s.concurrentPublishAndConsumeValidateBacklogCounter(20, 100, 100)
}

func (s *matchingEngineSuite) TestConcurrentAdd_PollWorkflowTasksDBErrors() {
	s.T().Skip("Skipping this as the backlog counter could under-count. Fix requires making " +
		"UpdateState an atomic operation.")
	s.taskManager.dbConditionalFailedError = true
	s.concurrentPublishAndConsumeValidateBacklogCounter(20, 100, 100)
}

func (s *matchingEngineSuite) TestLesserNumberOfPollersThanTasksNoDBErrors() {
	s.concurrentPublishAndConsumeValidateBacklogCounter(1, 500, 200)
}

func (s *matchingEngineSuite) TestLesserNumberOfPollersThanTasksDBErrors() {
	s.taskManager.dbConditionalFailedError = true
	s.concurrentPublishAndConsumeValidateBacklogCounter(1, 500, 200)
}

func (s *matchingEngineSuite) TestMultipleWorkersLesserNumberOfPollersThanTasksNoDBErrors() {
	s.concurrentPublishAndConsumeValidateBacklogCounter(5, 500, 200)
}

func (s *matchingEngineSuite) TestMultipleWorkersLesserNumberOfPollersThanTasksDBErrors() {
	s.T().Skip("Skipping this as the backlog counter could under-count. Fix requires making " +
		"UpdateState an atomic operation.")
	s.taskManager.dbConditionalFailedError = true
	s.concurrentPublishAndConsumeValidateBacklogCounter(5, 500, 200)
}

func (s *matchingEngineSuite) TestLargerBacklogAge() {
	firstAge := durationpb.New(100 * time.Second)
	secondAge := durationpb.New(1 * time.Millisecond)
	s.Same(firstAge, largerBacklogAge(firstAge, secondAge))

	thirdAge := durationpb.New(5 * time.Minute)
	s.Same(thirdAge, largerBacklogAge(firstAge, thirdAge))
	s.Same(thirdAge, largerBacklogAge(secondAge, thirdAge))
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
	s.matchingEngine.notifyIfNexusEndpointsOwnershipLost()
	select {
	case <-ch:
		s.Fail("expected nexusEndpointsOwnershipLost channel to not have been closed")
	default:
	}
	s.hostInfoForResolver = membership.NewHostInfoFromAddress("other")
	s.matchingEngine.notifyIfNexusEndpointsOwnershipLost()
	<-ch
	// If the channel is unblocked the test passed.
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

func (s *matchingEngineSuite) awaitCondition(cond func() bool, timeout time.Duration) bool {
	expiry := time.Now().UTC().Add(timeout)
	for !cond() {
		time.Sleep(time.Millisecond * 5)
		if time.Now().UTC().After(expiry) {
			return false
		}
	}
	return true
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

func newHistoryEvent(eventID int64, eventType enumspb.EventType) *historypb.HistoryEvent {
	historyEvent := &historypb.HistoryEvent{
		EventId:   eventID,
		EventTime: timestamppb.New(time.Now().UTC()),
		EventType: eventType,
	}

	return historyEvent
}

var _ persistence.TaskManager = (*testTaskManager)(nil) // Asserts that interface is indeed implemented

type testTaskManager struct {
	sync.Mutex
	queues                   map[dbTaskQueueKey]*testPhysicalTaskQueueManager
	logger                   log.Logger
	dbConditionalFailedError bool
	dbServiceError           bool
}

type dbTaskQueueKey struct {
	partitionKey tqid.PartitionKey
	versionSet   string
	buildId      string
}

func getKey(dbq *PhysicalTaskQueueKey) dbTaskQueueKey {
	return dbTaskQueueKey{dbq.partition.Key(), dbq.versionSet, dbq.buildId}
}

func newTestTaskManager(logger log.Logger) *testTaskManager {
	return &testTaskManager{queues: make(map[dbTaskQueueKey]*testPhysicalTaskQueueManager), logger: logger}
}

func (m *testTaskManager) GetName() string {
	return "test"
}

func (m *testTaskManager) Close() {
}

func (m *testTaskManager) getQueueManager(queue *PhysicalTaskQueueKey) *testPhysicalTaskQueueManager {
	key := getKey(queue)
	m.Lock()
	defer m.Unlock()
	result, ok := m.queues[key]
	if ok {
		return result
	}
	result = newTestTaskQueueManager()
	m.queues[key] = result
	return result
}

func newUnversionedRootQueueKey(namespaceId string, name string, taskType enumspb.TaskQueueType) *PhysicalTaskQueueKey {
	return UnversionedQueueKey(newTestTaskQueue(namespaceId, name, taskType).RootPartition())
}

func newRootPartition(namespaceId string, name string, taskType enumspb.TaskQueueType) *tqid.NormalPartition {
	return newTestTaskQueue(namespaceId, name, taskType).RootPartition()
}

func newTestTaskQueue(namespaceId string, name string, taskType enumspb.TaskQueueType) *tqid.TaskQueue {
	result, err := tqid.NewTaskQueueFamily(namespaceId, name)
	if err != nil {
		panic(fmt.Sprintf("newTaskQueueID failed with error %v", err))
	}
	return result.TaskQueue(taskType)
}

type testPhysicalTaskQueueManager struct {
	sync.Mutex
	queue                   *PhysicalTaskQueueKey
	rangeID                 int64
	ackLevel                int64
	ApproximateBacklogCount int64
	createTaskCount         int
	getTasksCount           int
	getUserDataCount        int
	updateCount             int
	tasks                   *treemap.Map
	userData                *persistencespb.VersionedTaskQueueUserData
}

func (m *testPhysicalTaskQueueManager) RangeID() int64 {
	m.Lock()
	defer m.Unlock()
	return m.rangeID
}

func newTestTaskQueueManager() *testPhysicalTaskQueueManager {
	return &testPhysicalTaskQueueManager{tasks: treemap.NewWith(godsutils.Int64Comparator)}
}

func (m *testTaskManager) CreateTaskQueue(
	_ context.Context,
	request *persistence.CreateTaskQueueRequest,
) (*persistence.CreateTaskQueueResponse, error) {
	tli := request.TaskQueueInfo
	dbq, err := ParsePhysicalTaskQueueKey(tli.Name, tli.NamespaceId, tli.TaskType)
	if err != nil {
		return nil, err
	}
	tlm := m.getQueueManager(dbq)
	tlm.Lock()
	defer tlm.Unlock()

	if tlm.rangeID != 0 {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task queue: name=%v, type=%v", tli.Name, tli.TaskType),
		}
	}

	tlm.rangeID = request.RangeID
	tlm.ackLevel = tli.AckLevel
	return &persistence.CreateTaskQueueResponse{}, nil
}

// UpdateTaskQueue provides a mock function with given fields: request
func (m *testTaskManager) UpdateTaskQueue(
	_ context.Context,
	request *persistence.UpdateTaskQueueRequest,
) (*persistence.UpdateTaskQueueResponse, error) {
	tli := request.TaskQueueInfo
	dbq, err := ParsePhysicalTaskQueueKey(tli.Name, tli.NamespaceId, tli.TaskType)
	if err != nil {
		return nil, err
	}
	tlm := m.getQueueManager(dbq)
	tlm.Lock()
	defer tlm.Unlock()
	tlm.updateCount++

	if tlm.rangeID != request.PrevRangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task queue: name=%v, type=%v", tli.Name, tli.TaskType),
		}
	}
	tlm.ackLevel = tli.AckLevel
	tlm.ApproximateBacklogCount = tli.ApproximateBacklogCount
	tlm.rangeID = request.RangeID
	return &persistence.UpdateTaskQueueResponse{}, nil
}

func (m *testTaskManager) GetTaskQueue(
	_ context.Context,
	request *persistence.GetTaskQueueRequest,
) (*persistence.GetTaskQueueResponse, error) {
	dbq, err := ParsePhysicalTaskQueueKey(request.TaskQueue, request.NamespaceID, request.TaskType)
	if err != nil {
		return nil, err
	}
	tlm := m.getQueueManager(dbq)
	tlm.Lock()
	defer tlm.Unlock()

	if tlm.rangeID == 0 {
		return nil, serviceerror.NewNotFound("task queue not found")
	}
	return &persistence.GetTaskQueueResponse{
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId:             request.NamespaceID,
			Name:                    request.TaskQueue,
			TaskType:                request.TaskType,
			Kind:                    enumspb.TASK_QUEUE_KIND_NORMAL,
			AckLevel:                tlm.ackLevel,
			ExpiryTime:              nil,
			LastUpdateTime:          timestamp.TimeNowPtrUtc(),
			ApproximateBacklogCount: tlm.ApproximateBacklogCount,
		},
		RangeID: tlm.rangeID,
	}, nil
}

// minTaskID returns the minimum value of the TaskID present in testTaskManager
func (m *testTaskManager) minTaskID(dbq *PhysicalTaskQueueKey) (int64, bool) {
	tlm := m.getQueueManager(dbq)
	tlm.Lock()
	defer tlm.Unlock()
	minKey, _ := tlm.tasks.Min()
	key, ok := minKey.(int64)
	return key, ok
}

// maxTaskID returns the maximum value of the TaskID present in testTaskManager
func (m *testTaskManager) maxTaskID(dbq *PhysicalTaskQueueKey) (int64, bool) {
	tlm := m.getQueueManager(dbq)
	tlm.Lock()
	defer tlm.Unlock()
	maxKey, _ := tlm.tasks.Max()
	key, ok := maxKey.(int64)
	return key, ok
}

func (m *testTaskManager) CompleteTasksLessThan(
	_ context.Context,
	request *persistence.CompleteTasksLessThanRequest,
) (int, error) {
	dbq, err := ParsePhysicalTaskQueueKey(request.TaskQueueName, request.NamespaceID, request.TaskType)
	if err != nil {
		return 0, err
	}
	tlm := m.getQueueManager(dbq)
	tlm.Lock()
	defer tlm.Unlock()
	keys := tlm.tasks.Keys()
	for _, key := range keys {
		id := key.(int64)
		if id < request.ExclusiveMaxTaskID {
			tlm.tasks.Remove(id)
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
	q := newUnversionedRootQueueKey(request.TaskQueue.NamespaceID, request.TaskQueue.TaskQueueName, request.TaskQueue.TaskQueueType)
	delete(m.queues, getKey(q))
	return nil
}

// generateErrorRandomly states if a taskManager's operation should return an error or not
func (m *testTaskManager) generateErrorRandomly() bool {
	if m.dbConditionalFailedError {
		threshold := 10

		// Generate a random number between 0 and 99
		randomNumber := rand.Intn(100)
		if randomNumber < threshold {
			return true
		}
	}
	return false
}

// CreateTask provides a mock function with given fields: request
func (m *testTaskManager) CreateTasks(
	_ context.Context,
	request *persistence.CreateTasksRequest,
) (*persistence.CreateTasksResponse, error) {
	namespaceId := request.TaskQueueInfo.Data.GetNamespaceId()
	taskQueue := request.TaskQueueInfo.Data.Name
	taskType := request.TaskQueueInfo.Data.TaskType
	rangeID := request.TaskQueueInfo.RangeID

	// Randomly returns a ConditionFailedError
	if m.generateErrorRandomly() {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task. TaskQueue: %v, taskQueueType: %v, rangeID: %v, db rangeID: %v",
				taskQueue, taskType, rangeID, rangeID),
		}
	}

	if m.dbServiceError {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("CreateTasks operation failed during serialization. Error : %v", errors.New("failure")))
	}

	dbq, err := ParsePhysicalTaskQueueKey(taskQueue, namespaceId, taskType)
	if err != nil {
		return nil, err
	}
	tlm := m.getQueueManager(dbq)
	tlm.Lock()
	defer tlm.Unlock()

	// First validate the entire batch
	for _, task := range request.Tasks {
		m.logger.Debug("testTaskManager.CreateTask", tag.TaskID(task.GetTaskId()), tag.ShardRangeID(rangeID))
		if task.GetTaskId() <= 0 {
			panic(fmt.Errorf("invalid taskID=%v", task.GetTaskId()))
		}

		if tlm.rangeID != rangeID {
			m.logger.Debug("testTaskManager.CreateTask ConditionFailedError",
				tag.TaskID(task.GetTaskId()), tag.ShardRangeID(rangeID), tag.ShardRangeID(tlm.rangeID))

			return nil, &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("testTaskManager.CreateTask failed. TaskQueue: %v, taskQueueType: %v, rangeID: %v, db rangeID: %v",
					taskQueue, taskType, rangeID, tlm.rangeID),
			}
		}
		_, ok := tlm.tasks.Get(task.GetTaskId())
		if ok {
			panic(fmt.Sprintf("Duplicated TaskID %v", task.GetTaskId()))
		}
	}

	// Then insert all tasks if no errors
	for _, task := range request.Tasks {
		tlm.tasks.Put(task.GetTaskId(), &persistencespb.AllocatedTaskInfo{
			Data:   task.Data,
			TaskId: task.GetTaskId(),
		})
		tlm.createTaskCount++
		tlm.ApproximateBacklogCount++
	}

	return &persistence.CreateTasksResponse{}, nil
}

// GetTasks provides a mock function with given fields: request
func (m *testTaskManager) GetTasks(
	_ context.Context,
	request *persistence.GetTasksRequest,
) (*persistence.GetTasksResponse, error) {
	m.logger.Debug("testTaskManager.GetTasks", tag.MinLevel(request.InclusiveMinTaskID), tag.MaxLevel(request.ExclusiveMaxTaskID))

	if m.generateErrorRandomly() {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetTasks operation failed"))
	}

	dbq, err := ParsePhysicalTaskQueueKey(request.TaskQueue, request.NamespaceID, request.TaskType)

	if err != nil {
		return nil, err
	}
	tlm := m.getQueueManager(dbq)
	tlm.Lock()
	defer tlm.Unlock()
	var tasks []*persistencespb.AllocatedTaskInfo

	it := tlm.tasks.Iterator()
	for it.Next() {
		taskID := it.Key().(int64)
		if taskID < request.InclusiveMinTaskID {
			continue
		}
		if taskID >= request.ExclusiveMaxTaskID {
			break
		}
		tasks = append(tasks, it.Value().(*persistencespb.AllocatedTaskInfo))
	}
	tlm.getTasksCount++
	return &persistence.GetTasksResponse{
		Tasks: tasks,
	}, nil
}

// getTaskCount returns number of tasks in a task queue
func (m *testTaskManager) getTaskCount(q *PhysicalTaskQueueKey) int {
	tlm := m.getQueueManager(q)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.tasks.Size()
}

// getCreateTaskCount returns how many times CreateTask was called
func (m *testTaskManager) getCreateTaskCount(q *PhysicalTaskQueueKey) int {
	tlm := m.getQueueManager(q)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.createTaskCount
}

// getGetTasksCount returns how many times GetTasks was called
func (m *testTaskManager) getGetTasksCount(q *PhysicalTaskQueueKey) int {
	tlm := m.getQueueManager(q)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.getTasksCount
}

// getGetUserDataCount returns how many times GetUserData was called
func (m *testTaskManager) getGetUserDataCount(q *PhysicalTaskQueueKey) int {
	tlm := m.getQueueManager(q)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.getUserDataCount
}

// getUpdateCount returns how many times UpdateTaskQueue was called
func (m *testTaskManager) getUpdateCount(q *PhysicalTaskQueueKey) int {
	tlm := m.getQueueManager(q)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.updateCount
}

func (m *testTaskManager) String() string {
	m.Lock()
	defer m.Unlock()
	var result string
	for _, q := range m.queues {
		q.Lock()
		if q.queue.TaskType() == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			result += "Activity"
		} else {
			result += "Workflow"
		}
		result += " task queue " + q.queue.PersistenceName()
		result += "\n"
		result += fmt.Sprintf("AckLevel=%v\n", q.ackLevel)
		result += fmt.Sprintf("CreateTaskCount=%v\n", q.createTaskCount)
		result += fmt.Sprintf("RangeID=%v\n", q.rangeID)
		result += "Tasks=\n"
		for _, t := range q.tasks.Values() {
			result += fmt.Sprintf("%v\n", t)
		}
		q.Unlock()
	}
	return result
}

// GetTaskQueueData implements persistence.TaskManager
func (m *testTaskManager) GetTaskQueueUserData(_ context.Context, request *persistence.GetTaskQueueUserDataRequest) (*persistence.GetTaskQueueUserDataResponse, error) {
	dbq, err := ParsePhysicalTaskQueueKey(request.TaskQueue, request.NamespaceID, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	tlm := m.getQueueManager(dbq)
	tlm.Lock()
	defer tlm.Unlock()
	tlm.getUserDataCount++
	return &persistence.GetTaskQueueUserDataResponse{
		UserData: tlm.userData,
	}, nil
}

// UpdateTaskQueueUserData implements persistence.TaskManager
func (m *testTaskManager) UpdateTaskQueueUserData(_ context.Context, request *persistence.UpdateTaskQueueUserDataRequest) error {
	dbq, err := ParsePhysicalTaskQueueKey(request.TaskQueue, request.NamespaceID, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return err
	}
	tlm := m.getQueueManager(dbq)
	tlm.Lock()
	defer tlm.Unlock()
	newData := common.CloneProto(request.UserData)
	newData.Version++
	tlm.userData = newData
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
