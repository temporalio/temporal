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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/api/taskqueue/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
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
	"go.temporal.io/server/common/util"
)

type (
	matchingEngineSuite struct {
		suite.Suite
		controller            *gomock.Controller
		mockHistoryClient     *historyservicemock.MockHistoryServiceClient
		mockMatchingClient    *matchingservicemock.MockMatchingServiceClient
		mockNamespaceCache    *namespace.MockRegistry
		mockVisibilityManager *manager.MockVisibilityManager

		matchingEngine *matchingEngineImpl
		taskManager    *testTaskManager
		logger         log.Logger
		sync.Mutex
	}
)

const (
	matchingTestNamespace = "matching-test"
)

func TestMatchingEngineSuite(t *testing.T) {
	s := new(matchingEngineSuite)
	suite.Run(t, s)
}

func (s *matchingEngineSuite) SetupSuite() {
}

func (s *matchingEngineSuite) TearDownSuite() {
}

func (s *matchingEngineSuite) SetupTest() {
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
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)
	ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: matchingTestNamespace}, nil, "")
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(ns.Name(), nil).AnyTimes()
	s.mockVisibilityManager = manager.NewMockVisibilityManager(s.controller)
	s.mockVisibilityManager.EXPECT().Close().AnyTimes()

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
	return newMatchingEngine(config, taskMgr, s.mockHistoryClient, s.logger, s.mockNamespaceCache, s.mockMatchingClient, s.mockVisibilityManager)
}

func newMatchingEngine(
	config *Config, taskMgr persistence.TaskManager, mockHistoryClient historyservice.HistoryServiceClient,
	logger log.Logger, mockNamespaceCache namespace.Registry, mockMatchingClient matchingservice.MatchingServiceClient,
	mockVisibilityManager manager.VisibilityManager,
) *matchingEngineImpl {
	return &matchingEngineImpl{
		taskManager:       taskMgr,
		historyClient:     mockHistoryClient,
		taskQueues:        make(map[taskQueueID]taskQueueManager),
		taskQueueCount:    make(map[taskQueueCounterKey]int),
		logger:            logger,
		throttledLogger:   log.ThrottledLogger(logger),
		metricsHandler:    metrics.NoopMetricsHandler,
		matchingClient:    mockMatchingClient,
		tokenSerializer:   common.NewProtoTaskTokenSerializer(),
		config:            config,
		namespaceRegistry: mockNamespaceCache,
		clusterMeta:       cluster.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(false, true)),
		timeSource:        clock.NewRealTimeSource(),
		visibilityManager: mockVisibilityManager,
	}
}

func (s *matchingEngineSuite) TestAckManager() {
	m := newAckManager(s.logger)
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
	s.EqualValues(100, m.getAckLevel())
	s.EqualValues(t1, m.getReadLevel())

	m.addTask(t2)
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
	s.EqualValues(300, m.getAckLevel())
	s.EqualValues(t3, m.getReadLevel())

	m.addTask(t4)
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
	m := newAckManager(s.logger)
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
	queueID := newTestTaskQueueID(
		namespace.ID(uuid.New()),
		"makeToast",
		enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tqm, err := s.matchingEngine.getTaskQueueManager(
		context.Background(),
		queueID,
		normalStickyInfo,
		true)
	s.Require().NoError(err)

	tqm2, err := newTaskQueueManager(
		s.matchingEngine,
		queueID, // same queueID as above
		normalStickyInfo,
		s.matchingEngine.config,
	)
	s.Require().NoError(err)

	// try to unload a different tqm instance with the same taskqueue ID
	s.matchingEngine.unloadTaskQueue(tqm2)

	got, err := s.matchingEngine.getTaskQueueManager(
		context.Background(), queueID, normalStickyInfo, true)
	s.Require().NoError(err)
	s.Require().Same(tqm, got,
		"Unload call with non-matching taskQueueManager should not cause unload")

	// this time unload the right tqm
	s.matchingEngine.unloadTaskQueue(tqm)

	got, err = s.matchingEngine.getTaskQueueManager(
		context.Background(), queueID, normalStickyInfo, true)
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
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(10 * time.Millisecond)

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

	_, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
	// fail due to no sticky worker
	s.Error(err)
	s.ErrorContains(err, "sticky worker unavailable")
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
	_, err = s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
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
	}

	s.Nil(err)
	s.Equal(expectedResp, resp)
}

func (s *matchingEngineSuite) PollForTasksEmptyResultTest(callContext context.Context, taskType enumspb.TaskQueueType) {
	s.matchingEngine.config.RangeSize = 2 // to test that range is not updated without tasks
	if _, ok := callContext.Deadline(); !ok {
		s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(10 * time.Millisecond)
	}

	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	identity := "selfDrivingToaster"

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	var taskQueueType enumspb.TaskQueueType
	tlID := newTestTaskQueueID(namespaceID, tl, taskType)
	const pollCount = 10
	for i := 0; i < pollCount; i++ {
		if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			pollResp, err := s.matchingEngine.PollActivityTaskQueue(callContext, &matchingservice.PollActivityTaskQueueRequest{
				NamespaceId: namespaceID.String(),
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
				NamespaceId: namespaceID.String(),
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
			NamespaceId: namespaceID.String(),
			DescRequest: &workflowservice.DescribeTaskQueueRequest{
				TaskQueue:              taskQueue,
				TaskQueueType:          taskQueueType,
				IncludeTaskQueueStatus: false,
			},
		})
		s.NoError(err)
		s.Equal(1, len(descResp.Pollers))
		s.Equal(identity, descResp.Pollers[0].GetIdentity())
		s.NotEmpty(descResp.Pollers[0].GetLastAccessTime())
		s.Nil(descResp.GetTaskQueueStatus())
	}
	s.EqualValues(1, s.taskManager.getTaskQueueManager(tlID).RangeID())
}

func (s *matchingEngineSuite) TestPollWorkflowTaskQueues_NamespaceHandover() {
	namespaceID := namespace.ID(uuid.New())
	taskQueue := &taskqueuepb.TaskQueue{Name: "taskQueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	addRequest := matchingservice.AddWorkflowTaskRequest{
		NamespaceId:            namespaceID.String(),
		Execution:              &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewRandom().String()},
		ScheduledEventId:       int64(0),
		TaskQueue:              taskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
	}

	// add multiple workflow tasks, but matching should not keep polling new tasks
	// upon getting namespace handover error when recording start for the first task
	_, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
	s.NoError(err)
	_, err = s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
	s.NoError(err)

	s.mockHistoryClient.EXPECT().RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, common.ErrNamespaceHandover).Times(1)
	resp, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	s.Nil(resp)
	s.Equal(common.ErrNamespaceHandover.Error(), err.Error())
}

func (s *matchingEngineSuite) TestPollActivityTaskQueues_NamespaceHandover() {
	namespaceID := namespace.ID(uuid.New())
	taskQueue := &taskqueuepb.TaskQueue{Name: "taskQueue", Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	addRequest := matchingservice.AddActivityTaskRequest{
		NamespaceId:            namespaceID.String(),
		Execution:              &commonpb.WorkflowExecution{WorkflowId: "workflowID", RunId: uuid.NewRandom().String()},
		ScheduledEventId:       int64(5),
		TaskQueue:              taskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
	}

	// add multiple activity tasks, but matching should not keep polling new tasks
	// upon getting namespace handover error when recording start for the first task
	_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.NoError(err)
	_, err = s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.NoError(err)

	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, common.ErrNamespaceHandover).Times(1)
	resp, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		PollRequest: &workflowservice.PollActivityTaskQueueRequest{
			TaskQueue: taskQueue,
			Identity:  "identity",
		},
	}, metrics.NoopMetricsHandler)
	s.Nil(resp)
	s.Equal(common.ErrNamespaceHandover.Error(), err.Error())
}

func (s *matchingEngineSuite) TestPollWorkflowTask_UserDataDisabled() {
	s.matchingEngine.config.LoadUserData = dynamicconfig.GetBoolPropertyFnFilteredByTaskQueueInfo(false)
	taskQueue := s.T().Name()

	resp, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
		NamespaceId: "asdf",
		PollRequest: &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: "asdf",
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			Identity: "identity",
			WorkerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
				BuildId:       "some_build_id",
				UseVersioning: true,
			},
		},
	}, metrics.NoopMetricsHandler)
	s.Error(err)
	s.Nil(resp)
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
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

	namespaceID := namespace.ID(uuid.New())
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
				NamespaceId:            namespaceID.String(),
				Execution:              execution,
				ScheduledEventId:       scheduledEventID,
				TaskQueue:              taskQueue,
				ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			}
			if isForwarded {
				addRequest.ForwardedSource = forwardedFrom
			}
			_, err = s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		} else {
			addRequest := matchingservice.AddWorkflowTaskRequest{
				NamespaceId:            namespaceID.String(),
				Execution:              execution,
				ScheduledEventId:       scheduledEventID,
				TaskQueue:              taskQueue,
				ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			}
			if isForwarded {
				addRequest.ForwardedSource = forwardedFrom
			}
			_, err = s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
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
		s.EqualValues(taskCount, s.taskManager.getTaskCount(newTestTaskQueueID(namespaceID, tl, taskType)))
	case true:
		s.EqualValues(0, s.taskManager.getTaskCount(newTestTaskQueueID(namespaceID, tl, taskType)))
	}
}

func (s *matchingEngineSuite) TestTaskWriterShutdown() {
	s.matchingEngine.config.RangeSize = 300 // override to low number for the test

	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	execution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tlm, err := s.matchingEngine.getTaskQueueManager(context.Background(), tlID, normalStickyInfo, true)
	s.Nil(err)

	addRequest := matchingservice.AddActivityTaskRequest{
		NamespaceId:            namespaceID.String(),
		Execution:              execution,
		TaskQueue:              taskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
	}

	// stop the task writer explicitly
	tlmImpl := tlm.(*taskQueueManagerImpl)
	tlmImpl.taskWriter.Stop()

	// now attempt to add a task
	scheduledEventID := int64(5)
	addRequest.ScheduledEventId = scheduledEventID
	_, err = s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.Error(err)
}

func (s *matchingEngineSuite) TestAddThenConsumeActivities() {
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(10 * time.Millisecond)

	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const taskCount = 1000
	const initialRangeID = 102
	// TODO: Understand why publish is low when rangeSize is 3
	const rangeSize = 30

	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	for i := int64(0); i < taskCount; i++ {
		scheduledEventID := i * 3
		addRequest := matchingservice.AddActivityTaskRequest{
			NamespaceId:            namespaceID.String(),
			Execution:              workflowExecution,
			ScheduledEventId:       scheduledEventID,
			TaskQueue:              taskQueue,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		}

		_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
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
						ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						ScheduleToStartTimeout: timestamp.DurationPtr(50 * time.Second),
						StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
						HeartbeatTimeout:       timestamp.DurationPtr(10 * time.Second),
					}),
			}
			resp.StartedTime = timestamp.TimeNowPtrUtc()
			return resp, nil
		}).AnyTimes()

	for i := int64(0); i < taskCount; {
		scheduledEventID := i * 3

		result, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceID.String(),
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
		s.Equal(true, validateTimeRange(*result.ScheduledTime, time.Minute))
		s.EqualValues(time.Second*100, *result.ScheduleToCloseTimeout)
		s.Equal(true, validateTimeRange(*result.StartedTime, time.Minute))
		s.EqualValues(time.Second*50, *result.StartToCloseTimeout)
		s.EqualValues(time.Second*10, *result.HeartbeatTimeout)
		taskToken := &tokenspb.Task{
			Attempt:          1,
			NamespaceId:      namespaceID.String(),
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
	s.True(expectedRange <= s.taskManager.getTaskQueueManager(tlID).rangeID)
}

func (s *matchingEngineSuite) TestSyncMatchActivities() {
	// Set a short long poll expiration so that we don't have to wait too long for 0 throttling cases
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(2 * time.Second)

	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const taskCount = 10
	const initialRangeID = 102
	// TODO: Understand why publish is low when rangeSize is 3
	const rangeSize = 30

	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test
	// So we can get snapshots
	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope).WithTags(metrics.ServiceNameTag(primitives.MatchingService))

	var err error
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
	mgr, err := newTaskQueueManager(s.matchingEngine, tlID, normalStickyInfo, s.matchingEngine.config)
	s.NoError(err)

	mgrImpl, ok := mgr.(*taskQueueManagerImpl)
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
	s.matchingEngine.updateTaskQueue(tlID, mgr)

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
						ScheduleToStartTimeout: timestamp.DurationPtr(1 * time.Second),
						ScheduleToCloseTimeout: timestamp.DurationPtr(2 * time.Second),
						StartToCloseTimeout:    timestamp.DurationPtr(1 * time.Second),
						HeartbeatTimeout:       timestamp.DurationPtr(1 * time.Second),
					}),
			}, nil
		}).AnyTimes()

	pollFunc := func(maxDispatch float64) (*matchingservice.PollActivityTaskQueueResponse, error) {
		return s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceID.String(),
			PollRequest: &workflowservice.PollActivityTaskQueueRequest{
				TaskQueue:         taskQueue,
				Identity:          identity,
				TaskQueueMetadata: &taskqueuepb.TaskQueueMetadata{MaxTasksPerSecond: &types.DoubleValue{Value: maxDispatch}},
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
			NamespaceId:            namespaceID.String(),
			Execution:              workflowExecution,
			ScheduledEventId:       scheduledEventID,
			TaskQueue:              taskQueue,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		}
		_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
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
			NamespaceId:      namespaceID.String(),
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
	syncCtr := snap.Counters()["test.sync_throttle_count+namespace="+matchingTestNamespace+",operation=TaskQueueMgr,service_name=matching,task_type=Activity,taskqueue=makeToast"]
	s.Equal(1, int(syncCtr.Value()))                         // Check times zero rps is set = throttle counter
	s.EqualValues(1, s.taskManager.getCreateTaskCount(tlID)) // Check times zero rps is set = Tasks stored in persistence
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
	expectedRange := int64(initialRangeID + taskCount/rangeSize)
	if taskCount%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getTaskQueueManager(tlID).rangeID)

	// check the poller information
	tlType := enumspb.TASK_QUEUE_TYPE_ACTIVITY
	descResp, err := s.matchingEngine.DescribeTaskQueue(context.Background(), &matchingservice.DescribeTaskQueueRequest{
		NamespaceId: namespaceID.String(),
		DescRequest: &workflowservice.DescribeTaskQueueRequest{
			TaskQueue:              taskQueue,
			TaskQueueType:          tlType,
			IncludeTaskQueueStatus: true,
		},
	})
	s.NoError(err)
	s.Equal(1, len(descResp.Pollers))
	s.Equal(identity, descResp.Pollers[0].GetIdentity())
	s.NotEmpty(descResp.Pollers[0].GetLastAccessTime())
	s.Equal(defaultTaskDispatchRPS, descResp.Pollers[0].GetRatePerSecond())
	s.NotNil(descResp.GetTaskQueueStatus())
	numPartitions := float64(s.matchingEngine.config.NumTaskqueueWritePartitions("", "", tlType))
	s.True(descResp.GetTaskQueueStatus().GetRatePerSecond()*numPartitions >= (defaultTaskDispatchRPS - 1))
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
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(20 * time.Millisecond)
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
	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
	var err error
	mgr, err := newTaskQueueManager(s.matchingEngine, tlID, normalStickyInfo, s.matchingEngine.config)
	s.NoError(err)

	mgrImpl := mgr.(*taskQueueManagerImpl)
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
	s.matchingEngine.updateTaskQueue(tlID, mgr)
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
					NamespaceId:            namespaceID.String(),
					Execution:              workflowExecution,
					ScheduledEventId:       scheduledEventID,
					TaskQueue:              taskQueue,
					ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
				}

				_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
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
						ScheduleToStartTimeout: timestamp.DurationPtr(1 * time.Second),
						ScheduleToCloseTimeout: timestamp.DurationPtr(2 * time.Second),
						StartToCloseTimeout:    timestamp.DurationPtr(1 * time.Second),
						HeartbeatTimeout:       timestamp.DurationPtr(1 * time.Second),
					}),
			}, nil
		}).AnyTimes()

	for p := 0; p < workerCount; p++ {
		go func(wNum int) {
			defer wg.Done()
			for i := int64(0); i < taskCount; {
				maxDispatch := dispatchLimitFn(wNum, i)
				result, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
					NamespaceId: namespaceID.String(),
					PollRequest: &workflowservice.PollActivityTaskQueueRequest{
						TaskQueue:         taskQueue,
						Identity:          identity,
						TaskQueueMetadata: &taskqueuepb.TaskQueueMetadata{MaxTasksPerSecond: &types.DoubleValue{Value: maxDispatch}},
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
					NamespaceId:      namespaceID.String(),
					WorkflowId:       workflowID,
					RunId:            runID,
					ScheduledEventId: scheduledEventID,
					ActivityId:       activityID,
					ActivityType:     activityTypeName,
				}
				resultToken, err := s.matchingEngine.tokenSerializer.Deserialize(result.TaskToken)
				s.NoError(err)

				// taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
				// s.EqualValues(taskToken, result.Task, fmt.Sprintf("%v!=%v", string(taskToken)))
				s.EqualValues(taskToken, resultToken, fmt.Sprintf("%v!=%v", taskToken, resultToken))
				i++
			}
		}(p)
	}
	wg.Wait()
	totalTasks := int(taskCount) * workerCount
	persisted := s.taskManager.getCreateTaskCount(tlID)
	s.True(persisted < totalTasks)
	expectedRange := int64(initialRangeID + persisted/rangeSize)
	if persisted%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getTaskQueueManager(tlID).rangeID)
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))

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

	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
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
					NamespaceId:            namespaceID.String(),
					Execution:              workflowExecution,
					ScheduledEventId:       scheduledEventID,
					TaskQueue:              taskQueue,
					ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
				}

				_, err := s.matchingEngine.AddWorkflowTask(context.Background(), &addRequest)
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
			}, nil
		}).AnyTimes()
	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; {
				result, err := s.matchingEngine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
					NamespaceId: namespaceID.String(),
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
					NamespaceId:      namespaceID.String(),
					WorkflowId:       workflowID,
					RunId:            runID,
					ScheduledEventId: scheduledEventID,
					StartedEventId:   startedEventID,
				}
				resultToken, err := s.matchingEngine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}

				// taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
				// s.EqualValues(taskToken, result.Task, fmt.Sprintf("%v!=%v", string(taskToken)))
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
	s.True(expectedRange <= s.taskManager.getTaskQueueManager(tlID).rangeID)
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

	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
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
					NamespaceId:            namespaceID.String(),
					Execution:              workflowExecution,
					ScheduledEventId:       scheduledEventID,
					TaskQueue:              taskQueue,
					ScheduleToStartTimeout: timestamp.DurationFromSeconds(600),
				}

				_, err := engine.AddActivityTask(context.Background(), &addRequest)
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

	startedTasks := make(map[int64]bool)

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordActivityTaskStartedResponse, error) {
			if _, ok := startedTasks[taskRequest.TaskId]; ok {
				s.logger.Debug("From error function Mock Received DUPLICATED RecordActivityTaskStartedRequest", tag.TaskID(taskRequest.TaskId))
				return nil, serviceerror.NewNotFound("already started")
			}
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest", tag.TaskID(taskRequest.TaskId))

			startedTasks[taskRequest.TaskId] = true
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
						ScheduleToStartTimeout: timestamp.DurationPtr(600 * time.Second),
						ScheduleToCloseTimeout: timestamp.DurationPtr(2 * time.Second),
						StartToCloseTimeout:    timestamp.DurationPtr(1 * time.Second),
						HeartbeatTimeout:       timestamp.DurationPtr(1 * time.Second),
					}),
			}, nil
		}).AnyTimes()
	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; /* incremented explicitly to skip empty polls */ {
				result, err := engine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
					NamespaceId: namespaceID.String(),
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
					Attempt:          1,
					NamespaceId:      namespaceID.String(),
					WorkflowId:       workflowID,
					RunId:            runID,
					ScheduledEventId: scheduledEventID,
					ActivityId:       activityID,
					ActivityType:     activityTypeName,
				}
				resultToken, err := engine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}
				// taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
				// s.EqualValues(taskToken, result.Task, fmt.Sprintf("%v!=%v", string(taskToken)))
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
	s.True(expectedRange <= s.taskManager.getTaskQueueManager(tlID).rangeID)
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

	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
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
					NamespaceId:            namespaceID.String(),
					Execution:              workflowExecution,
					ScheduledEventId:       scheduledEventID,
					TaskQueue:              taskQueue,
					ScheduleToStartTimeout: timestamp.DurationFromSeconds(600),
				}

				_, err := engine.AddWorkflowTask(context.Background(), &addRequest)
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

	startedTasks := make(map[int64]bool)

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordWorkflowTaskStarted(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordWorkflowTaskStartedRequest, arg2 ...interface{}) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
			if _, ok := startedTasks[taskRequest.TaskId]; ok {
				s.logger.Debug("From error function Mock Received DUPLICATED RecordWorkflowTaskStartedRequest", tag.TaskID(taskRequest.TaskId))
				return nil, serviceerrors.NewTaskAlreadyStarted("Workflow")
			}
			s.logger.Debug("Mock Received RecordWorkflowTaskStartedRequest", tag.TaskID(taskRequest.TaskId))
			s.logger.Debug("Mock Received RecordWorkflowTaskStartedRequest")
			startedTasks[taskRequest.TaskId] = true
			return &historyservice.RecordWorkflowTaskStartedResponse{
				PreviousStartedEventId: startedEventID,
				StartedEventId:         startedEventID,
				ScheduledEventId:       scheduledEventID,
				WorkflowType:           workflowType,
				Attempt:                1,
			}, nil
		}).AnyTimes()
	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; /* incremented explicitly to skip empty polls */ {
				result, err := engine.PollWorkflowTaskQueue(context.Background(), &matchingservice.PollWorkflowTaskQueueRequest{
					NamespaceId: namespaceID.String(),
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
					NamespaceId:      namespaceID.String(),
					WorkflowId:       workflowID,
					RunId:            runID,
					ScheduledEventId: scheduledEventID,
					StartedEventId:   startedEventID,
				}
				resultToken, err := engine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}

				// taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
				// s.EqualValues(taskToken, result.Task, fmt.Sprintf("%v!=%v", string(taskToken)))
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
	s.True(expectedRange <= s.taskManager.getTaskQueueManager(tlID).rangeID)
}

func (s *matchingEngineSuite) TestAddTaskAfterStartFailure() {
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	scheduledEventID := int64(0)
	addRequest := matchingservice.AddActivityTaskRequest{
		NamespaceId:            namespaceID.String(),
		Execution:              workflowExecution,
		ScheduledEventId:       scheduledEventID,
		TaskQueue:              taskQueue,
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
	}

	_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.NoError(err)
	s.EqualValues(1, s.taskManager.getTaskCount(tlID))

	ctx, err := s.matchingEngine.getTask(context.Background(), tlID, normalStickyInfo, &pollMetadata{})
	s.NoError(err)

	ctx.finish(errors.New("test error"))
	s.EqualValues(1, s.taskManager.getTaskCount(tlID))
	ctx2, err := s.matchingEngine.getTask(context.Background(), tlID, normalStickyInfo, &pollMetadata{})
	s.NoError(err)

	s.NotEqual(ctx.event.GetTaskId(), ctx2.event.GetTaskId())
	s.Equal(ctx.event.Data.GetWorkflowId(), ctx2.event.Data.GetWorkflowId())
	s.Equal(ctx.event.Data.GetRunId(), ctx2.event.Data.GetRunId())
	s.Equal(ctx.event.Data.GetScheduledEventId(), ctx2.event.Data.GetScheduledEventId())

	ctx2.finish(nil)
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
}

func (s *matchingEngineSuite) TestTaskQueueManagerGetTaskBatch() {
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

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
			NamespaceId:            namespaceID.String(),
			Execution:              workflowExecution,
			ScheduledEventId:       scheduledEventID,
			TaskQueue:              taskQueue,
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		}

		_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		s.NoError(err)
	}

	tlMgr, ok := s.matchingEngine.taskQueues[*tlID].(*taskQueueManagerImpl)
	s.True(ok, "taskQueueManger doesn't implement taskQueueManager interface")
	s.EqualValues(taskCount, s.taskManager.getTaskCount(tlID))

	// wait until all tasks are read by the task pump and enqueued into the in-memory buffer
	// at the end of this step, ackManager readLevel will also be equal to the buffer size
	expectedBufSize := util.Min(cap(tlMgr.taskReader.taskBuffer), taskCount)
	s.True(s.awaitCondition(func() bool { return len(tlMgr.taskReader.taskBuffer) == expectedBufSize }, time.Second))

	// stop all goroutines that read / write tasks in the background
	// remainder of this test works with the in-memory buffer
	tlMgr.Stop()

	// setReadLevel should NEVER be called without updating ackManager.outstandingTasks
	// This is only for unit test purpose
	tlMgr.taskAckManager.setReadLevel(tlMgr.taskWriter.GetMaxReadLevel())
	batch, err := tlMgr.taskReader.getTaskBatch(context.Background())
	s.Nil(err)
	s.EqualValues(0, len(batch.tasks))
	s.EqualValues(tlMgr.taskWriter.GetMaxReadLevel(), batch.readLevel)
	s.True(batch.isReadBatchDone)

	tlMgr.taskAckManager.setReadLevel(0)
	batch, err = tlMgr.taskReader.getTaskBatch(context.Background())
	s.Nil(err)
	s.EqualValues(rangeSize, len(batch.tasks))
	s.EqualValues(rangeSize, batch.readLevel)
	s.True(batch.isReadBatchDone)

	s.setupRecordActivityTaskStartedMock(tl)

	// reset the ackManager readLevel to the buffer size and consume
	// the in-memory tasks by calling Poll API - assert ackMgr state
	// at the end
	tlMgr.taskAckManager.setReadLevel(int64(expectedBufSize))

	// complete rangeSize events
	for i := int64(0); i < rangeSize; i++ {
		identity := "nobody"
		result, err := s.matchingEngine.PollActivityTaskQueue(context.Background(), &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceID.String(),
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
	s.EqualValues(taskCount-rangeSize, s.taskManager.getTaskCount(tlID))
	batch, err = tlMgr.taskReader.getTaskBatch(context.Background())
	s.Nil(err)
	s.True(0 < len(batch.tasks) && len(batch.tasks) <= rangeSize)
	s.True(batch.isReadBatchDone)
}

func (s *matchingEngineSuite) TestTaskQueueManagerGetTaskBatch_ReadBatchDone() {
	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	const rangeSize = 10
	const maxReadLevel = int64(120)
	config := defaultTestConfig()
	config.RangeSize = rangeSize
	tlMgr0, err := newTaskQueueManager(s.matchingEngine, tlID, normalStickyInfo, config)
	s.NoError(err)

	tlMgr, ok := tlMgr0.(*taskQueueManagerImpl)
	s.True(ok)

	tlMgr.Start()

	// tlMgr.taskWriter startup is async so give it time to complete, otherwise
	// the following few lines get clobbered as part of the taskWriter.Start()
	time.Sleep(100 * time.Millisecond)

	tlMgr.taskAckManager.setReadLevel(0)
	atomic.StoreInt64(&tlMgr.taskWriter.maxReadLevel, maxReadLevel)
	batch, err := tlMgr.taskReader.getTaskBatch(context.Background())
	s.Empty(batch.tasks)
	s.Equal(int64(rangeSize*10), batch.readLevel)
	s.False(batch.isReadBatchDone)
	s.NoError(err)

	tlMgr.taskAckManager.setReadLevel(batch.readLevel)
	batch, err = tlMgr.taskReader.getTaskBatch(context.Background())
	s.Empty(batch.tasks)
	s.Equal(maxReadLevel, batch.readLevel)
	s.True(batch.isReadBatchDone)
	s.NoError(err)
}

func (s *matchingEngineSuite) TestTaskQueueManager_CyclingBehavior() {
	namespaceID := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	config := defaultTestConfig()

	for i := 0; i < 4; i++ {
		prevGetTasksCount := s.taskManager.getGetTasksCount(tlID)

		tlMgr, err := newTaskQueueManager(s.matchingEngine, tlID, normalStickyInfo, config)
		s.NoError(err)

		tlMgr.Start()
		// tlMgr.taskWriter startup is async so give it time to complete
		time.Sleep(100 * time.Millisecond)
		tlMgr.Stop()

		getTasksCount := s.taskManager.getGetTasksCount(tlID) - prevGetTasksCount
		s.LessOrEqual(getTasksCount, 1)
	}
}

func (s *matchingEngineSuite) TestTaskExpiryAndCompletion() {
	runID := uuid.NewRandom().String()
	workflowID := uuid.New()
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	namespaceID := namespace.ID(uuid.New())
	tl := "task-expiry-completion-tl0"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	taskQueue := &taskqueuepb.TaskQueue{
		Name: tl,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	const taskCount = 20 // must be multiple of 4
	const rangeSize = 10
	s.matchingEngine.config.RangeSize = rangeSize
	s.matchingEngine.config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskQueueInfo(2)

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
				NamespaceId:            namespaceID.String(),
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
				addRequest.ScheduleToStartTimeout = timestamp.DurationPtr(250 * time.Millisecond)
			}
			_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
			s.NoError(err)
		}

		tlMgr, ok := s.matchingEngine.taskQueues[*tlID].(*taskQueueManagerImpl)
		s.True(ok, "failed to load task queue")
		s.EqualValues(taskCount, s.taskManager.getTaskCount(tlID))

		// wait until all tasks are loaded by into in-memory buffers by task queue manager
		// the buffer size should be one less than expected because dispatcher will dequeue the head
		// 1/4 should be thrown out because they are expired before they hit the buffer
		s.True(s.awaitCondition(func() bool { return len(tlMgr.taskReader.taskBuffer) >= (3*taskCount/4 - 1) }, time.Second))

		// ensure the 1/4 of tasks with small ScheduleToStartTimeout will be expired when they come out of the buffer
		time.Sleep(300 * time.Millisecond)

		maxTimeBetweenTaskDeletes = tc.maxTimeBtwnDeletes

		s.setupRecordActivityTaskStartedMock(tl)

		pollReq := &matchingservice.PollActivityTaskQueueRequest{
			NamespaceId: namespaceID.String(),
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
			delta := remaining - s.taskManager.getTaskCount(tlID)
			s.Truef(-3 <= delta && delta <= 1, "remaining %d, getTaskCount %d", remaining, s.taskManager.getTaskCount(tlID))
		}
		// ensure full gc for the next case (twice in case one doesn't get the gc lock)
		tlMgr.taskGC.RunNow(context.Background(), tlMgr.taskAckManager.getAckLevel())
		tlMgr.taskGC.RunNow(context.Background(), tlMgr.taskAckManager.getAckLevel())
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
	s.False(res.TaskQueueHasUserData)
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
	s.True(res.TaskQueueHasUserData)
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
	s.True(res.TaskQueueHasUserData)
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
	s.True(res.TaskQueueHasUserData)
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
		time.Sleep(500 * time.Millisecond)

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
	s.True(res.TaskQueueHasUserData)
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
		time.Sleep(500 * time.Millisecond)

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
	s.True(res.TaskQueueHasUserData)
	s.True(hybrid_logical_clock.Greater(*res.UserData.Data.Clock, *userData.Data.Clock))
	s.NotNil(res.UserData.Data.VersioningData)
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

func (s *matchingEngineSuite) TestAddWorkflowTask_ForVersionedWorkflows_SilentlyDroppedWhenDisablingLoadingUserData() {
	namespaceId := uuid.New()
	tq := taskqueuepb.TaskQueue{
		Name: "test",
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	s.matchingEngine.config.LoadUserData = dynamicconfig.GetBoolPropertyFnFilteredByTaskQueueInfo(false)

	_, err := s.matchingEngine.AddWorkflowTask(context.Background(), &matchingservice.AddWorkflowTaskRequest{
		NamespaceId: namespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "test",
			RunId:      uuid.New(),
		},
		TaskQueue:        &tq,
		ScheduledEventId: 7,
		Source:           enums.TASK_SOURCE_HISTORY,
		VersionDirective: &taskqueue.TaskVersionDirective{
			Value: &taskqueue.TaskVersionDirective_UseDefault{UseDefault: &types.Empty{}},
		},
	})
	s.Require().NoError(err)
}

func (s *matchingEngineSuite) TestAddActivityTask_ForVersionedWorkflows_SilentlyDroppedWhenDisablingLoadingUserData() {
	namespaceId := uuid.New()
	tq := taskqueuepb.TaskQueue{
		Name: "test",
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	s.matchingEngine.config.LoadUserData = dynamicconfig.GetBoolPropertyFnFilteredByTaskQueueInfo(false)

	_, err := s.matchingEngine.AddActivityTask(context.Background(), &matchingservice.AddActivityTaskRequest{
		NamespaceId: namespaceId,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "test",
			RunId:      uuid.New(),
		},
		TaskQueue:        &tq,
		ScheduledEventId: 7,
		Source:           enums.TASK_SOURCE_HISTORY,
		VersionDirective: &taskqueue.TaskVersionDirective{
			Value: &taskqueue.TaskVersionDirective_UseDefault{UseDefault: &types.Empty{}},
		},
	})
	s.Require().NoError(err)
}

func (s *matchingEngineSuite) TestUnknownBuildId_Poll() {
	namespaceId := namespace.ID(uuid.New())
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_WORKFLOW)

	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := s.matchingEngine.getTask(ctx, tlID, normalStickyInfo, &pollMetadata{
		workerVersionCapabilities: &commonpb.WorkerVersionCapabilities{
			BuildId:       "unknown",
			UseVersioning: true,
		},
	})
	s.Error(err) // deadline exceeded or canceled

	unknownCtr := scope.Snapshot().Counters()["test.unknown_build_polls+namespace="+matchingTestNamespace+",operation=TaskQueueMgr,task_type=Workflow,taskqueue=makeToast"]
	s.Equal(int64(1), unknownCtr.Value())
}

func (s *matchingEngineSuite) TestUnknownBuildId_Add() {
	namespaceId := namespace.ID(uuid.New())
	tl := "makeToast"

	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	s.mockMatchingClient.EXPECT().UpdateWorkerBuildIdCompatibility(gomock.Any(), &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceId.String(),
		TaskQueue:   tl,
		Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_PersistUnknownBuildId{
			PersistUnknownBuildId: "unknown",
		},
	}).Return(&matchingservice.UpdateWorkerBuildIdCompatibilityResponse{}, nil)

	_, err := s.matchingEngine.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
		NamespaceId:            namespaceId.String(),
		Execution:              &commonpb.WorkflowExecution{RunId: "run", WorkflowId: "wf"},
		ScheduledEventId:       0,
		TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
		ForwardedSource:        "somewhere", // force sync match only
		VersionDirective: &taskqueue.TaskVersionDirective{
			Value: &taskqueue.TaskVersionDirective_BuildId{
				BuildId: "unknown",
			},
		},
	})
	s.ErrorIs(err, errRemoteSyncMatchFailed)

	unknownCtr := scope.Snapshot().Counters()["test.unknown_build_tasks+namespace="+matchingTestNamespace+",operation=TaskQueueMgr,task_type=Workflow,taskqueue=makeToast"]
	s.Equal(int64(1), unknownCtr.Value())
}

func (s *matchingEngineSuite) TestUnknownBuildId_Match() {
	namespaceId := namespace.ID(uuid.New())
	tl := "makeToast"

	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsHandler = metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	s.mockMatchingClient.EXPECT().UpdateWorkerBuildIdCompatibility(gomock.Any(), &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
		NamespaceId: namespaceId.String(),
		TaskQueue:   tl,
		Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_PersistUnknownBuildId{
			PersistUnknownBuildId: "unknown",
		},
	}).Return(&matchingservice.UpdateWorkerBuildIdCompatibilityResponse{}, nil).AnyTimes() // might get called again on dispatch from spooled

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		_, err := s.matchingEngine.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
			NamespaceId:            namespaceId.String(),
			Execution:              &commonpb.WorkflowExecution{RunId: "run", WorkflowId: "wf"},
			ScheduledEventId:       123,
			TaskQueue:              &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			ScheduleToStartTimeout: timestamp.DurationFromSeconds(100),
			// do not set ForwardedSource, allow to go to db
			VersionDirective: &taskqueue.TaskVersionDirective{
				Value: &taskqueue.TaskVersionDirective_BuildId{
					BuildId: "unknown",
				},
			},
		})
		s.NoError(err)
		wg.Done()
	}()

	go func() {
		tlID := newTestTaskQueueID(namespaceId, tl, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		task, err := s.matchingEngine.getTask(ctx, tlID, normalStickyInfo, &pollMetadata{
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
						ScheduleToCloseTimeout: timestamp.DurationPtr(100 * time.Second),
						ScheduleToStartTimeout: timestamp.DurationPtr(50 * time.Second),
						StartToCloseTimeout:    timestamp.DurationPtr(50 * time.Second),
						HeartbeatTimeout:       timestamp.DurationPtr(10 * time.Second),
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
		EventTime: timestamp.TimePtr(time.Now().UTC()),
		EventType: eventType,
	}

	return historyEvent
}

var _ persistence.TaskManager = (*testTaskManager)(nil) // Asserts that interface is indeed implemented

type testTaskManager struct {
	sync.Mutex
	taskQueues map[taskQueueID]*testTaskQueueManager
	logger     log.Logger
}

func newTestTaskManager(logger log.Logger) *testTaskManager {
	return &testTaskManager{taskQueues: make(map[taskQueueID]*testTaskQueueManager), logger: logger}
}

func (m *testTaskManager) GetName() string {
	return "test"
}

func (m *testTaskManager) Close() {
}

func (m *testTaskManager) getTaskQueueManager(id *taskQueueID) *testTaskQueueManager {
	m.Lock()
	defer m.Unlock()
	result, ok := m.taskQueues[*id]
	if ok {
		return result
	}
	result = newTestTaskQueueManager()
	m.taskQueues[*id] = result
	return result
}

type testTaskQueueManager struct {
	sync.Mutex
	rangeID          int64
	ackLevel         int64
	createTaskCount  int
	getTasksCount    int
	getUserDataCount int
	updateCount      int
	tasks            *treemap.Map
	userData         *persistencespb.VersionedTaskQueueUserData
}

func (m *testTaskQueueManager) RangeID() int64 {
	m.Lock()
	defer m.Unlock()
	return m.rangeID
}

func Int64Comparator(a, b interface{}) int {
	aAsserted := a.(int64)
	bAsserted := b.(int64)
	switch {
	case aAsserted > bAsserted:
		return 1
	case aAsserted < bAsserted:
		return -1
	default:
		return 0
	}
}

func newTestTaskQueueManager() *testTaskQueueManager {
	return &testTaskQueueManager{tasks: treemap.NewWith(Int64Comparator)}
}

func newTestTaskQueueID(namespaceID namespace.ID, name string, taskType enumspb.TaskQueueType) *taskQueueID {
	result, err := newTaskQueueID(namespaceID, name, taskType)
	if err != nil {
		panic(fmt.Sprintf("newTaskQueueID failed with error %v", err))
	}
	return result
}

func (m *testTaskManager) CreateTaskQueue(
	_ context.Context,
	request *persistence.CreateTaskQueueRequest,
) (*persistence.CreateTaskQueueResponse, error) {
	tli := request.TaskQueueInfo
	tlm := m.getTaskQueueManager(newTestTaskQueueID(namespace.ID(tli.GetNamespaceId()), tli.Name, tli.TaskType))
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
	tlm := m.getTaskQueueManager(newTestTaskQueueID(namespace.ID(tli.GetNamespaceId()), tli.Name, tli.TaskType))
	tlm.Lock()
	defer tlm.Unlock()
	tlm.updateCount++

	if tlm.rangeID != request.PrevRangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task queue: name=%v, type=%v", tli.Name, tli.TaskType),
		}
	}
	tlm.ackLevel = tli.AckLevel
	tlm.rangeID = request.RangeID
	return &persistence.UpdateTaskQueueResponse{}, nil
}

func (m *testTaskManager) GetTaskQueue(
	_ context.Context,
	request *persistence.GetTaskQueueRequest,
) (*persistence.GetTaskQueueResponse, error) {
	tlm := m.getTaskQueueManager(newTestTaskQueueID(namespace.ID(request.NamespaceID), request.TaskQueue, request.TaskType))
	tlm.Lock()
	defer tlm.Unlock()

	if tlm.rangeID == 0 {
		return nil, serviceerror.NewNotFound("task queue not found")
	}
	return &persistence.GetTaskQueueResponse{
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId:    request.NamespaceID,
			Name:           request.TaskQueue,
			TaskType:       request.TaskType,
			Kind:           enumspb.TASK_QUEUE_KIND_NORMAL,
			AckLevel:       tlm.ackLevel,
			ExpiryTime:     nil,
			LastUpdateTime: timestamp.TimeNowPtrUtc(),
		},
		RangeID: tlm.rangeID,
	}, nil
}

// CompleteTask provides a mock function with given fields: request
func (m *testTaskManager) CompleteTask(
	_ context.Context,
	request *persistence.CompleteTaskRequest,
) error {
	m.logger.Debug("CompleteTask", tag.TaskID(request.TaskID), tag.Name(request.TaskQueue.TaskQueueName), tag.WorkflowTaskQueueType(request.TaskQueue.TaskQueueType))
	if request.TaskID <= 0 {
		panic(fmt.Errorf("invalid taskID=%v", request.TaskID))
	}

	tli := request.TaskQueue
	tlm := m.getTaskQueueManager(newTestTaskQueueID(namespace.ID(tli.NamespaceID), tli.TaskQueueName, tli.TaskQueueType))

	tlm.Lock()
	defer tlm.Unlock()

	tlm.tasks.Remove(request.TaskID)
	return nil
}

func (m *testTaskManager) CompleteTasksLessThan(
	_ context.Context,
	request *persistence.CompleteTasksLessThanRequest,
) (int, error) {
	tlm := m.getTaskQueueManager(newTestTaskQueueID(namespace.ID(request.NamespaceID), request.TaskQueueName, request.TaskType))
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
	key := newTestTaskQueueID(namespace.ID(request.TaskQueue.NamespaceID), request.TaskQueue.TaskQueueName, request.TaskQueue.TaskQueueType)
	delete(m.taskQueues, *key)
	return nil
}

// CreateTask provides a mock function with given fields: request
func (m *testTaskManager) CreateTasks(
	_ context.Context,
	request *persistence.CreateTasksRequest,
) (*persistence.CreateTasksResponse, error) {
	namespaceID := namespace.ID(request.TaskQueueInfo.Data.GetNamespaceId())
	taskQueue := request.TaskQueueInfo.Data.Name
	taskType := request.TaskQueueInfo.Data.TaskType
	rangeID := request.TaskQueueInfo.RangeID

	tlm := m.getTaskQueueManager(newTestTaskQueueID(namespaceID, taskQueue, taskType))
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
	}

	return &persistence.CreateTasksResponse{}, nil
}

// GetTasks provides a mock function with given fields: request
func (m *testTaskManager) GetTasks(
	_ context.Context,
	request *persistence.GetTasksRequest,
) (*persistence.GetTasksResponse, error) {
	m.logger.Debug("testTaskManager.GetTasks", tag.MinLevel(request.InclusiveMinTaskID), tag.MaxLevel(request.ExclusiveMaxTaskID))

	tlm := m.getTaskQueueManager(newTestTaskQueueID(namespace.ID(request.NamespaceID), request.TaskQueue, request.TaskType))
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
func (m *testTaskManager) getTaskCount(taskQueue *taskQueueID) int {
	tlm := m.getTaskQueueManager(taskQueue)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.tasks.Size()
}

// getCreateTaskCount returns how many times CreateTask was called
func (m *testTaskManager) getCreateTaskCount(taskQueue *taskQueueID) int {
	tlm := m.getTaskQueueManager(taskQueue)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.createTaskCount
}

// getGetTasksCount returns how many times GetTasks was called
func (m *testTaskManager) getGetTasksCount(taskQueue *taskQueueID) int {
	tlm := m.getTaskQueueManager(taskQueue)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.getTasksCount
}

// getGetUserDataCount returns how many times GetUserData was called
func (m *testTaskManager) getGetUserDataCount(taskQueue *taskQueueID) int {
	tlm := m.getTaskQueueManager(taskQueue)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.getUserDataCount
}

// getUpdateCount returns how many times UpdateTaskQueue was called
func (m *testTaskManager) getUpdateCount(taskQueue *taskQueueID) int {
	tlm := m.getTaskQueueManager(taskQueue)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.updateCount
}

func (m *testTaskManager) String() string {
	m.Lock()
	defer m.Unlock()
	var result string
	for id, tl := range m.taskQueues {
		tl.Lock()
		if id.taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			result += "Activity"
		} else {
			result += "Workflow"
		}
		result += " task queue " + id.FullName()
		result += "\n"
		result += fmt.Sprintf("AckLevel=%v\n", tl.ackLevel)
		result += fmt.Sprintf("CreateTaskCount=%v\n", tl.createTaskCount)
		result += fmt.Sprintf("RangeID=%v\n", tl.rangeID)
		result += "Tasks=\n"
		for _, t := range tl.tasks.Values() {
			result += fmt.Sprintf("%v\n", t)
		}
		tl.Unlock()
	}
	return result
}

// GetTaskQueueData implements persistence.TaskManager
func (m *testTaskManager) GetTaskQueueUserData(_ context.Context, request *persistence.GetTaskQueueUserDataRequest) (*persistence.GetTaskQueueUserDataResponse, error) {
	tlm := m.getTaskQueueManager(newTestTaskQueueID(namespace.ID(request.NamespaceID), request.TaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	tlm.Lock()
	defer tlm.Unlock()
	tlm.getUserDataCount++
	return &persistence.GetTaskQueueUserDataResponse{
		UserData: tlm.userData,
	}, nil
}

// UpdateTaskQueueUserData implements persistence.TaskManager
func (m *testTaskManager) UpdateTaskQueueUserData(_ context.Context, request *persistence.UpdateTaskQueueUserDataRequest) error {
	tlm := m.getTaskQueueManager(newTestTaskQueueID(namespace.ID(request.NamespaceID), request.TaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	tlm.Lock()
	defer tlm.Unlock()
	newData := *request.UserData
	newData.Version++
	tlm.userData = &newData
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
	// This is only used to validate that the build id to task queue mapping is enforced (at the time of writing), report 0.
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
	config := NewConfig(dynamicconfig.NewNoopCollection(), false, false)
	config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(100 * time.Millisecond)
	config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskQueueInfo(1)
	return config
}

type (
	dynamicRateBurstWrapper struct {
		quotas.MutableRateBurst
		*quotas.RateLimiterImpl
	}
)

func (d *dynamicRateBurstWrapper) SetRate(rate float64) {
	d.MutableRateBurst.SetRate(rate)
	d.RateLimiterImpl.SetRate(rate)
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
