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
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/gogo/protobuf/types"
	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/temporal-proto/common/v1"
	decisionpb "go.temporal.io/temporal-proto/decision/v1"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	historypb "go.temporal.io/temporal-proto/history/v1"
	"go.temporal.io/temporal-proto/serviceerror"
	taskqueuepb "go.temporal.io/temporal-proto/taskqueue/v1"
	"go.temporal.io/temporal-proto/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type (
	matchingEngineSuite struct {
		suite.Suite
		controller         *gomock.Controller
		mockHistoryClient  *historyservicemock.MockHistoryServiceClient
		mockNamespaceCache *cache.MockNamespaceCache

		matchingEngine       *matchingEngineImpl
		taskManager          *testTaskManager
		mockExecutionManager *mocks.ExecutionManager
		logger               log.Logger
		handlerContext       *handlerContext
		sync.Mutex
	}
)

const (
	_minBurst             = 10000
	matchingTestNamespace = "matching-test"
	matchingTestTaskQueue = "matching-test-taskqueue"
)

func TestMatchingEngineSuite(t *testing.T) {
	s := new(matchingEngineSuite)
	suite.Run(t, s)
}

func (s *matchingEngineSuite) SetupSuite() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	http.Handle("/test/tasks", http.HandlerFunc(s.TasksHandler))
}

// Renders content of taskManager and matchingEngine when called at http://localhost:6060/test/tasks
// Uncomment HTTP server initialization in SetupSuite method to enable.

func (s *matchingEngineSuite) TasksHandler(w http.ResponseWriter, r *http.Request) {
	s.Lock()
	defer s.Unlock()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprint(w, fmt.Sprintf("%v\n", s.taskManager))
	fmt.Fprint(w, fmt.Sprintf("%v\n", s.matchingEngine))
}

func (s *matchingEngineSuite) TearDownSuite() {
}

func (s *matchingEngineSuite) SetupTest() {
	s.Lock()
	defer s.Unlock()
	s.mockExecutionManager = &mocks.ExecutionManager{}
	s.controller = gomock.NewController(s.T())
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.taskManager = newTestTaskManager(s.logger)
	s.mockNamespaceCache = cache.NewMockNamespaceCache(s.controller)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(cache.CreateNamespaceCacheEntry(matchingTestNamespace), nil).AnyTimes()
	s.handlerContext = newHandlerContext(
		context.Background(),
		matchingTestNamespace,
		&taskqueuepb.TaskQueue{matchingTestTaskQueue, enumspb.TASK_QUEUE_KIND_NORMAL},
		metrics.NewClient(tally.NoopScope, metrics.Matching),
		metrics.MatchingTaskQueueMgrScope,
	)

	s.matchingEngine = s.newMatchingEngine(defaultTestConfig(), s.taskManager)
	s.matchingEngine.Start()
}

func (s *matchingEngineSuite) TearDownTest() {
	s.mockExecutionManager.AssertExpectations(s.T())
	s.matchingEngine.Stop()
	s.controller.Finish()
}

func (s *matchingEngineSuite) newMatchingEngine(
	config *Config, taskMgr persistence.TaskManager,
) *matchingEngineImpl {
	return newMatchingEngine(config, taskMgr, s.mockHistoryClient, s.logger, s.mockNamespaceCache)
}

func newMatchingEngine(
	config *Config, taskMgr persistence.TaskManager, mockHistoryClient history.Client,
	logger log.Logger, mockNamespaceCache cache.NamespaceCache,
) *matchingEngineImpl {
	return &matchingEngineImpl{
		taskManager:     taskMgr,
		historyService:  mockHistoryClient,
		taskQueues:      make(map[taskQueueID]taskQueueManager),
		logger:          logger,
		metricsClient:   metrics.NewClient(tally.NoopScope, metrics.Matching),
		tokenSerializer: common.NewProtoTaskTokenSerializer(),
		config:          config,
		namespaceCache:  mockNamespaceCache,
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
}

func (s *matchingEngineSuite) TestPollForActivityTasksEmptyResult() {
	s.PollForTasksEmptyResultTest(context.Background(), enumspb.TASK_QUEUE_TYPE_ACTIVITY)
}

func (s *matchingEngineSuite) TestPollForDecisionTasksEmptyResult() {
	s.PollForTasksEmptyResultTest(context.Background(), enumspb.TASK_QUEUE_TYPE_DECISION)
}

func (s *matchingEngineSuite) TestPollForActivityTasksEmptyResultWithShortContext() {
	shortContextTimeout := returnEmptyTaskTimeBudget + 10*time.Millisecond
	callContext, cancel := context.WithTimeout(context.Background(), shortContextTimeout)
	defer cancel()
	s.PollForTasksEmptyResultTest(callContext, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
}

func (s *matchingEngineSuite) TestPollForDecisionTasksEmptyResultWithShortContext() {
	shortContextTimeout := returnEmptyTaskTimeBudget + 10*time.Millisecond
	callContext, cancel := context.WithTimeout(context.Background(), shortContextTimeout)
	defer cancel()
	s.PollForTasksEmptyResultTest(callContext, enumspb.TASK_QUEUE_TYPE_DECISION)
}

func (s *matchingEngineSuite) TestPollForDecisionTasks() {
	s.PollForDecisionTasksResultTest()
}

func (s *matchingEngineSuite) PollForDecisionTasksResultTest() {
	namespaceID := uuid.NewRandom().String()
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
	scheduleID := int64(0)

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordDecisionTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordDecisionTaskStartedRequest) (*historyservice.RecordDecisionTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordDecisionTaskStartedRequest")
			response := &historyservice.RecordDecisionTaskStartedResponse{
				WorkflowType:               workflowType,
				PreviousStartedEventId:     scheduleID,
				ScheduledEventId:           scheduleID + 1,
				Attempt:                    0,
				StickyExecutionEnabled:     true,
				WorkflowExecutionTaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			}
			return response, nil
		}).AnyTimes()

	addRequest := matchingservice.AddDecisionTaskRequest{
		NamespaceId:                   namespaceID,
		Execution:                     execution,
		ScheduleId:                    scheduleID,
		TaskQueue:                     stickyTaskQueue,
		ScheduleToStartTimeoutSeconds: 1,
	}

	_, err := s.matchingEngine.AddDecisionTask(s.handlerContext, &addRequest)
	s.NoError(err)

	resp, err := s.matchingEngine.PollForDecisionTask(s.handlerContext, &matchingservice.PollForDecisionTaskRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollForDecisionTaskRequest{
			TaskQueue: stickyTaskQueue,
			Identity:  identity},
	})

	expectedResp := &matchingservice.PollForDecisionTaskResponse{
		TaskToken:              resp.TaskToken,
		WorkflowExecution:      execution,
		WorkflowType:           workflowType,
		PreviousStartedEventId: scheduleID,
		StartedEventId:         0, // TODO should be common.EmptyEventID
		Attempt:                0,
		NextEventId:            0, // TODO should be common.EmptyEventID
		BacklogCountHint:       1,
		StickyExecutionEnabled: true,
		Query:                  nil,
		DecisionInfo:           nil,
		WorkflowExecutionTaskQueue: &taskqueuepb.TaskQueue{
			Name: tl,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		EventStoreVersion:  0,
		BranchToken:        nil,
		ScheduledTimestamp: 0,
		StartedTimestamp:   0,
		Queries:            nil,
	}

	s.Nil(err)
	s.Equal(expectedResp, resp)
}

func (s *matchingEngineSuite) PollForTasksEmptyResultTest(callContext context.Context, taskType enumspb.TaskQueueType) {
	s.matchingEngine.config.RangeSize = 2 // to test that range is not updated without tasks
	if _, ok := callContext.Deadline(); !ok {
		s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(10 * time.Millisecond)
	}

	namespaceID := uuid.New()
	tl := "makeToast"
	identity := "selfDrivingToaster"

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	var taskQueueType enumspb.TaskQueueType
	tlID := newTestTaskQueueID(namespaceID, tl, taskType)
	s.handlerContext.Context = callContext
	const pollCount = 10
	for i := 0; i < pollCount; i++ {
		if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			pollResp, err := s.matchingEngine.PollForActivityTask(s.handlerContext, &matchingservice.PollForActivityTaskRequest{
				NamespaceId: namespaceID,
				PollRequest: &workflowservice.PollForActivityTaskRequest{
					TaskQueue: taskQueue,
					Identity:  identity,
				},
			})
			s.NoError(err)
			s.Equal(emptyPollForActivityTaskResponse, pollResp)

			taskQueueType = enumspb.TASK_QUEUE_TYPE_ACTIVITY
		} else {
			resp, err := s.matchingEngine.PollForDecisionTask(s.handlerContext, &matchingservice.PollForDecisionTaskRequest{
				NamespaceId: namespaceID,
				PollRequest: &workflowservice.PollForDecisionTaskRequest{
					TaskQueue: taskQueue,
					Identity:  identity},
			})
			s.NoError(err)
			s.Equal(emptyPollForDecisionTaskResponse, resp)

			taskQueueType = enumspb.TASK_QUEUE_TYPE_DECISION
		}
		select {
		case <-callContext.Done():
			s.FailNow("Call context has expired.")
		default:
		}
		// check the poller information
		s.handlerContext.Context = context.Background()
		descResp, err := s.matchingEngine.DescribeTaskQueue(s.handlerContext, &matchingservice.DescribeTaskQueueRequest{
			NamespaceId: namespaceID,
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
	s.EqualValues(1, s.taskManager.taskQueues[*tlID].rangeID)
}

func (s *matchingEngineSuite) TestAddActivityTasks() {
	s.AddTasksTest(enumspb.TASK_QUEUE_TYPE_ACTIVITY, false)
}

func (s *matchingEngineSuite) TestAddDecisionTasks() {
	s.AddTasksTest(enumspb.TASK_QUEUE_TYPE_DECISION, false)
}

func (s *matchingEngineSuite) TestAddDecisionTasksForwarded() {
	s.AddTasksTest(enumspb.TASK_QUEUE_TYPE_DECISION, true)
}

func (s *matchingEngineSuite) AddTasksTest(taskType enumspb.TaskQueueType, isForwarded bool) {
	s.matchingEngine.config.RangeSize = 300 // override to low number for the test

	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"
	forwardedFrom := "/__temporal_sys/makeToast/1"

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	const taskCount = 111

	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	execution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3
		var err error
		if taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			addRequest := matchingservice.AddActivityTaskRequest{
				SourceNamespaceId:             namespaceID,
				NamespaceId:                   namespaceID,
				Execution:                     execution,
				ScheduleId:                    scheduleID,
				TaskQueue:                     taskQueue,
				ScheduleToStartTimeoutSeconds: 1,
			}
			if isForwarded {
				addRequest.ForwardedFrom = forwardedFrom
			}
			_, err = s.matchingEngine.AddActivityTask(s.handlerContext, &addRequest)
		} else {
			addRequest := matchingservice.AddDecisionTaskRequest{
				NamespaceId:                   namespaceID,
				Execution:                     execution,
				ScheduleId:                    scheduleID,
				TaskQueue:                     taskQueue,
				ScheduleToStartTimeoutSeconds: 1,
			}
			if isForwarded {
				addRequest.ForwardedFrom = forwardedFrom
			}
			_, err = s.matchingEngine.AddDecisionTask(s.handlerContext, &addRequest)
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

	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	execution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tlKind := enumspb.TASK_QUEUE_KIND_NORMAL
	tlm, err := s.matchingEngine.getTaskQueueManager(tlID, tlKind)
	s.Nil(err)

	addRequest := matchingservice.AddActivityTaskRequest{
		SourceNamespaceId:             namespaceID,
		NamespaceId:                   namespaceID,
		Execution:                     execution,
		TaskQueue:                     taskQueue,
		ScheduleToStartTimeoutSeconds: 1,
	}

	// stop the task writer explicitly
	tlmImpl := tlm.(*taskQueueManagerImpl)
	tlmImpl.taskWriter.Stop()

	// now attempt to add a task
	scheduleID := int64(5)
	addRequest.ScheduleId = scheduleID
	_, err = s.matchingEngine.AddActivityTask(s.handlerContext, &addRequest)
	s.Error(err)

	// test race
	tlmImpl.taskWriter.stopped = 0
	_, err = s.matchingEngine.AddActivityTask(s.handlerContext, &addRequest)
	s.Error(err)
	tlmImpl.taskWriter.stopped = 1 // reset it back to old value
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

	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3
		addRequest := matchingservice.AddActivityTaskRequest{
			SourceNamespaceId:             namespaceID,
			NamespaceId:                   namespaceID,
			Execution:                     workflowExecution,
			ScheduleId:                    scheduleID,
			TaskQueue:                     taskQueue,
			ScheduleToStartTimeoutSeconds: 1,
		}

		_, err := s.matchingEngine.AddActivityTask(s.handlerContext, &addRequest)
		s.NoError(err)
	}
	s.EqualValues(taskCount, s.taskManager.getTaskCount(tlID))

	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &commonpb.ActivityType{Name: activityTypeName}
	activityInput := payloads.EncodeString("Activity1 Input")

	identity := "nobody"

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest) (*historyservice.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			resp := &historyservice.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduleId, 0,
					&decisionpb.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    activityID,
						TaskQueue:                     &taskqueuepb.TaskQueue{Name: taskQueue.Name},
						ActivityType:                  activityType,
						Input:                         activityInput,
						ScheduleToCloseTimeoutSeconds: 100,
						ScheduleToStartTimeoutSeconds: 50,
						StartToCloseTimeoutSeconds:    50,
						HeartbeatTimeoutSeconds:       10,
					}),
			}
			resp.StartedTimestamp = time.Now().UnixNano()
			return resp, nil
		}).AnyTimes()

	for i := int64(0); i < taskCount; {
		scheduleID := i * 3

		result, err := s.matchingEngine.PollForActivityTask(s.handlerContext, &matchingservice.PollForActivityTaskRequest{
			NamespaceId: namespaceID,
			PollRequest: &workflowservice.PollForActivityTaskRequest{
				TaskQueue: taskQueue,
				Identity:  identity},
		})

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
		s.Equal(true, validateTimeRange(time.Unix(0, result.ScheduledTimestamp), time.Minute))
		s.Equal(int32(100), result.ScheduleToCloseTimeoutSeconds)
		s.Equal(true, validateTimeRange(time.Unix(0, result.StartedTimestamp), time.Minute))
		s.Equal(int32(50), result.StartToCloseTimeoutSeconds)
		s.Equal(int32(10), result.HeartbeatTimeoutSeconds)
		taskToken := &tokenspb.Task{
			NamespaceId:  namespaceID,
			WorkflowId:   workflowID,
			RunId:        runID,
			ScheduleId:   scheduleID,
			ActivityId:   activityID,
			ActivityType: activityTypeName,
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
	// Set a short long poll expiration so we don't have to wait too long for 0 throttling cases
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(50 * time.Millisecond)

	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const taskCount = 10
	const initialRangeID = 102
	// TODO: Understand why publish is low when rangeSize is 3
	const rangeSize = 30

	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tlKind := enumspb.TASK_QUEUE_KIND_NORMAL
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test
	// So we can get snapshots
	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsClient = metrics.NewClient(scope, metrics.Matching)

	dispatchTTL := time.Nanosecond
	dPtr := _defaultTaskDispatchRPS

	mgr, err := newTaskQueueManager(s.matchingEngine, tlID, tlKind, s.matchingEngine.config)
	s.NoError(err)

	mgrImpl, ok := mgr.(*taskQueueManagerImpl)
	s.True(ok)

	mgrImpl.matcher.limiter = quotas.NewRateLimiter(&dPtr, dispatchTTL, _minBurst)
	s.matchingEngine.updateTaskQueue(tlID, mgr)
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
	s.NoError(mgr.Start())

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &commonpb.ActivityType{Name: activityTypeName}
	activityInput := payloads.EncodeString("Activity1 Input")

	identity := "nobody"

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest) (*historyservice.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &historyservice.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduleId, 0,
					&decisionpb.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    activityID,
						TaskQueue:                     &taskqueuepb.TaskQueue{Name: taskQueue.Name},
						ActivityType:                  activityType,
						Input:                         activityInput,
						ScheduleToStartTimeoutSeconds: 1,
						ScheduleToCloseTimeoutSeconds: 2,
						StartToCloseTimeoutSeconds:    1,
						HeartbeatTimeoutSeconds:       1,
					}),
			}, nil
		}).AnyTimes()

	pollFunc := func(maxDispatch float64) (*matchingservice.PollForActivityTaskResponse, error) {
		return s.matchingEngine.PollForActivityTask(s.handlerContext, &matchingservice.PollForActivityTaskRequest{
			NamespaceId: namespaceID,
			PollRequest: &workflowservice.PollForActivityTaskRequest{
				TaskQueue:         taskQueue,
				Identity:          identity,
				TaskQueueMetadata: &taskqueuepb.TaskQueueMetadata{MaxTasksPerSecond: &types.DoubleValue{Value: maxDispatch}},
			},
		})
	}

	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3

		var wg sync.WaitGroup
		var result *matchingservice.PollForActivityTaskResponse
		var pollErr error
		maxDispatch := _defaultTaskDispatchRPS
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
			SourceNamespaceId:             namespaceID,
			NamespaceId:                   namespaceID,
			Execution:                     workflowExecution,
			ScheduleId:                    scheduleID,
			TaskQueue:                     taskQueue,
			ScheduleToStartTimeoutSeconds: 1,
		}
		_, err := s.matchingEngine.AddActivityTask(s.handlerContext, &addRequest)
		wg.Wait()
		s.NoError(err)
		s.NoError(pollErr)
		s.NotNil(result)

		if len(result.TaskToken) == 0 {
			// when ratelimit is set to zero, poller is expected to return empty result
			// reset ratelimit, poll again and make sure task is returned this time
			s.logger.Debug("empty poll returned")
			s.Equal(float64(0), maxDispatch)
			maxDispatch = _defaultTaskDispatchRPS
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
			NamespaceId:  namespaceID,
			WorkflowId:   workflowID,
			RunId:        runID,
			ScheduleId:   scheduleID,
			ActivityId:   activityID,
			ActivityType: activityTypeName,
		}

		serializedToken, _ := s.matchingEngine.tokenSerializer.Serialize(taskToken)
		// s.EqualValues(scheduleID, result.Task)

		s.EqualValues(serializedToken, result.TaskToken)
	}

	time.Sleep(20 * time.Millisecond) // So any buffer tasks from 0 rps get picked up
	syncCtr := scope.Snapshot().Counters()["test.sync_throttle_count_per_tl+namespace="+matchingTestNamespace+",operation=TaskQueueMgr,taskqueue=makeToast"]
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
	descResp, err := s.matchingEngine.DescribeTaskQueue(s.handlerContext, &matchingservice.DescribeTaskQueueRequest{
		NamespaceId: namespaceID,
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
	s.Equal(_defaultTaskDispatchRPS, descResp.Pollers[0].GetRatePerSecond())
	s.NotNil(descResp.GetTaskQueueStatus())
	s.True(descResp.GetTaskQueueStatus().GetRatePerSecond() >= (_defaultTaskDispatchRPS - 1))
}

func (s *matchingEngineSuite) TestConcurrentPublishConsumeActivities() {
	dispatchLimitFn := func(int, int64) float64 {
		return _defaultTaskDispatchRPS
	}
	const workerCount = 20
	const taskCount = 100
	throttleCt := s.concurrentPublishConsumeActivities(workerCount, taskCount, dispatchLimitFn)
	s.Zero(throttleCt)
}

func (s *matchingEngineSuite) TestConcurrentPublishConsumeActivitiesWithZeroDispatch() {
	// Set a short long poll expiration so we don't have to wait too long for 0 throttling cases
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(20 * time.Millisecond)
	dispatchLimitFn := func(wc int, tc int64) float64 {
		if tc%50 == 0 && wc%5 == 0 { // Gets triggered atleast 20 times
			return 0
		}
		return _defaultTaskDispatchRPS
	}
	const workerCount = 20
	const taskCount = 100
	s.matchingEngine.metricsClient = metrics.NewClient(tally.NewTestScope("test", nil), metrics.Matching)
	throttleCt := s.concurrentPublishConsumeActivities(workerCount, taskCount, dispatchLimitFn)
	s.logger.Info("Number of tasks throttled", tag.Number(throttleCt))
	// atleast once from 0 dispatch poll, and until TTL is hit at which time throttle limit is reset
	// hard to predict exactly how many times, since the atomic.Value load might not have updated.
	s.True(throttleCt >= 1)
}

func (s *matchingEngineSuite) concurrentPublishConsumeActivities(
	workerCount int, taskCount int64, dispatchLimitFn func(int, int64) float64) int64 {
	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsClient = metrics.NewClient(scope, metrics.Matching)
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const initialRangeID = 0
	const rangeSize = 3
	var scheduleID int64 = 123
	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tlKind := enumspb.TASK_QUEUE_KIND_NORMAL
	dispatchTTL := time.Nanosecond
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test
	dPtr := _defaultTaskDispatchRPS

	mgr, err := newTaskQueueManager(s.matchingEngine, tlID, tlKind, s.matchingEngine.config)
	s.NoError(err)

	mgrImpl := mgr.(*taskQueueManagerImpl)
	mgrImpl.matcher.limiter = quotas.NewRateLimiter(&dPtr, dispatchTTL, _minBurst)
	s.matchingEngine.updateTaskQueue(tlID, mgr)
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
	s.NoError(mgr.Start())

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}
	var wg sync.WaitGroup
	wg.Add(2 * workerCount)

	for p := 0; p < workerCount; p++ {
		go func() {
			defer wg.Done()
			for i := int64(0); i < taskCount; i++ {
				addRequest := matchingservice.AddActivityTaskRequest{
					SourceNamespaceId:             namespaceID,
					NamespaceId:                   namespaceID,
					Execution:                     workflowExecution,
					ScheduleId:                    scheduleID,
					TaskQueue:                     taskQueue,
					ScheduleToStartTimeoutSeconds: 1,
				}

				_, err := s.matchingEngine.AddActivityTask(s.handlerContext, &addRequest)
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
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest) (*historyservice.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &historyservice.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduleId, 0,
					&decisionpb.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    activityID,
						TaskQueue:                     &taskqueuepb.TaskQueue{Name: taskQueue.Name},
						ActivityType:                  activityType,
						Input:                         activityInput,
						Header:                        activityHeader,
						ScheduleToStartTimeoutSeconds: 1,
						ScheduleToCloseTimeoutSeconds: 2,
						StartToCloseTimeoutSeconds:    1,
						HeartbeatTimeoutSeconds:       1,
					}),
			}, nil
		}).AnyTimes()

	for p := 0; p < workerCount; p++ {
		go func(wNum int) {
			defer wg.Done()
			for i := int64(0); i < taskCount; {
				maxDispatch := dispatchLimitFn(wNum, i)
				result, err := s.matchingEngine.PollForActivityTask(s.handlerContext, &matchingservice.PollForActivityTaskRequest{
					NamespaceId: namespaceID,
					PollRequest: &workflowservice.PollForActivityTaskRequest{
						TaskQueue:         taskQueue,
						Identity:          identity,
						TaskQueueMetadata: &taskqueuepb.TaskQueueMetadata{MaxTasksPerSecond: &types.DoubleValue{Value: maxDispatch}},
					},
				})
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
					NamespaceId:  namespaceID,
					WorkflowId:   workflowID,
					RunId:        runID,
					ScheduleId:   scheduleID,
					ActivityId:   activityID,
					ActivityType: activityTypeName,
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

	syncCtr := scope.Snapshot().Counters()["test.sync_throttle_count_per_tl+namespace="+matchingTestNamespace+",operation=TaskQueueMgr,taskqueue=makeToast"]
	bufCtr := scope.Snapshot().Counters()["test.buffer_throttle_count_per_tl+namespace="+matchingTestNamespace+",operation=TaskQueueMgr,taskqueue=makeToast"]
	total := int64(0)
	if syncCtr != nil {
		total += syncCtr.Value()
	}
	if bufCtr != nil {
		total += bufCtr.Value()
	}
	return total
}

func (s *matchingEngineSuite) TestConcurrentPublishConsumeDecisions() {
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const workerCount = 20
	const taskCount = 100
	const initialRangeID = 0
	const rangeSize = 5
	var scheduleID int64 = 123
	var startedEventID int64 = 1412

	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_DECISION)
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	var wg sync.WaitGroup
	wg.Add(2 * workerCount)

	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; i++ {
				addRequest := matchingservice.AddDecisionTaskRequest{
					NamespaceId:                   namespaceID,
					Execution:                     workflowExecution,
					ScheduleId:                    scheduleID,
					TaskQueue:                     taskQueue,
					ScheduleToStartTimeoutSeconds: 1,
				}

				_, err := s.matchingEngine.AddDecisionTask(s.handlerContext, &addRequest)
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
	s.mockHistoryClient.EXPECT().RecordDecisionTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordDecisionTaskStartedRequest) (*historyservice.RecordDecisionTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordDecisionTaskStartedRequest")
			return &historyservice.RecordDecisionTaskStartedResponse{
				PreviousStartedEventId: startedEventID,
				StartedEventId:         startedEventID,
				ScheduledEventId:       scheduleID,
				WorkflowType:           workflowType,
			}, nil
		}).AnyTimes()
	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; {
				result, err := s.matchingEngine.PollForDecisionTask(s.handlerContext, &matchingservice.PollForDecisionTaskRequest{
					NamespaceId: namespaceID,
					PollRequest: &workflowservice.PollForDecisionTaskRequest{
						TaskQueue: taskQueue,
						Identity:  identity},
				})
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
					NamespaceId: namespaceID,
					WorkflowId:  workflowID,
					RunId:       runID,
					ScheduleId:  scheduleID,
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
	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	// Try with cancelled context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	s.handlerContext.Context = ctx
	_, err := s.matchingEngine.PollForActivityTask(s.handlerContext, &matchingservice.PollForActivityTaskRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollForActivityTaskRequest{
			TaskQueue: taskQueue,
			Identity:  identity},
	})

	s.Equal(ctx.Err(), err)

	// Try with expired context
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.handlerContext.Context = ctx
	resp, err := s.matchingEngine.PollForActivityTask(s.handlerContext, &matchingservice.PollForActivityTaskRequest{
		NamespaceId: namespaceID,
		PollRequest: &workflowservice.PollForActivityTaskRequest{
			TaskQueue: taskQueue,
			Identity:  identity},
	})
	s.Nil(err)
	s.Equal(emptyPollForActivityTaskResponse, resp)
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
	var scheduleID int64 = 123

	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	var engines []*matchingEngineImpl

	for p := 0; p < engineCount; p++ {
		e := s.newMatchingEngine(defaultTestConfig(), s.taskManager)
		e.config.RangeSize = rangeSize
		engines = append(engines, e)
		e.Start()
	}

	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; i++ {
				addRequest := matchingservice.AddActivityTaskRequest{
					SourceNamespaceId:             namespaceID,
					NamespaceId:                   namespaceID,
					Execution:                     workflowExecution,
					ScheduleId:                    scheduleID,
					TaskQueue:                     taskQueue,
					ScheduleToStartTimeoutSeconds: 600,
				}

				_, err := engine.AddActivityTask(s.handlerContext, &addRequest)
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
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest) (*historyservice.RecordActivityTaskStartedResponse, error) {
			if _, ok := startedTasks[taskRequest.TaskId]; ok {
				s.logger.Debug("From error function Mock Received DUPLICATED RecordActivityTaskStartedRequest", tag.TaskID(taskRequest.TaskId))
				return nil, serviceerror.NewNotFound("already started")
			}
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest", tag.TaskID(taskRequest.TaskId))

			startedTasks[taskRequest.TaskId] = true
			return &historyservice.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduleId, 0,
					&decisionpb.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    activityID,
						TaskQueue:                     &taskqueuepb.TaskQueue{Name: taskQueue.Name},
						ActivityType:                  activityType,
						Input:                         activityInput,
						ScheduleToStartTimeoutSeconds: 600,
						ScheduleToCloseTimeoutSeconds: 2,
						StartToCloseTimeoutSeconds:    1,
						HeartbeatTimeoutSeconds:       1,
					}),
			}, nil
		}).AnyTimes()
	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; /* incremented explicitly to skip empty polls */ {
				result, err := engine.PollForActivityTask(s.handlerContext, &matchingservice.PollForActivityTaskRequest{
					NamespaceId: namespaceID,
					PollRequest: &workflowservice.PollForActivityTaskRequest{
						TaskQueue: taskQueue,
						Identity:  identity},
				})
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
					NamespaceId:  namespaceID,
					WorkflowId:   workflowID,
					RunId:        runID,
					ScheduleId:   scheduleID,
					ActivityId:   activityID,
					ActivityType: activityTypeName,
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

func (s *matchingEngineSuite) TestMultipleEnginesDecisionsRangeStealing() {
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	const engineCount = 2
	const taskCount = 400
	const iterations = 2
	const initialRangeID = 0
	const rangeSize = 10
	var scheduleID int64 = 123

	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_DECISION)
	s.taskManager.getTaskQueueManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	var engines []*matchingEngineImpl

	for p := 0; p < engineCount; p++ {
		e := s.newMatchingEngine(defaultTestConfig(), s.taskManager)
		e.config.RangeSize = rangeSize
		engines = append(engines, e)
		e.Start()
	}

	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; i++ {
				addRequest := matchingservice.AddDecisionTaskRequest{
					NamespaceId:                   namespaceID,
					Execution:                     workflowExecution,
					ScheduleId:                    scheduleID,
					TaskQueue:                     taskQueue,
					ScheduleToStartTimeoutSeconds: 600,
				}

				_, err := engine.AddDecisionTask(s.handlerContext, &addRequest)
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
	s.mockHistoryClient.EXPECT().RecordDecisionTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordDecisionTaskStartedRequest) (*historyservice.RecordDecisionTaskStartedResponse, error) {
			if _, ok := startedTasks[taskRequest.TaskId]; ok {
				s.logger.Debug("From error function Mock Received DUPLICATED RecordDecisionTaskStartedRequest", tag.TaskID(taskRequest.TaskId))
				return nil, serviceerror.NewEventAlreadyStarted("already started")
			}
			s.logger.Debug("Mock Received RecordDecisionTaskStartedRequest", tag.TaskID(taskRequest.TaskId))
			s.logger.Debug("Mock Received RecordDecisionTaskStartedRequest")
			startedTasks[taskRequest.TaskId] = true
			return &historyservice.RecordDecisionTaskStartedResponse{
				PreviousStartedEventId: startedEventID,
				StartedEventId:         startedEventID,
				ScheduledEventId:       scheduleID,
				WorkflowType:           workflowType,
			}, nil
		}).AnyTimes()
	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; /* incremented explicitly to skip empty polls */ {
				result, err := engine.PollForDecisionTask(s.handlerContext, &matchingservice.PollForDecisionTaskRequest{
					NamespaceId: namespaceID,
					PollRequest: &workflowservice.PollForDecisionTaskRequest{
						TaskQueue: taskQueue,
						Identity:  identity},
				})
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
					NamespaceId: namespaceID,
					WorkflowId:  workflowID,
					RunId:       runID,
					ScheduleId:  scheduleID,
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

	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tlKind := enumspb.TASK_QUEUE_KIND_NORMAL

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	scheduleID := int64(0)
	addRequest := matchingservice.AddActivityTaskRequest{
		SourceNamespaceId:             namespaceID,
		NamespaceId:                   namespaceID,
		Execution:                     workflowExecution,
		ScheduleId:                    scheduleID,
		TaskQueue:                     taskQueue,
		ScheduleToStartTimeoutSeconds: 1,
	}

	_, err := s.matchingEngine.AddActivityTask(s.handlerContext, &addRequest)
	s.NoError(err)
	s.EqualValues(1, s.taskManager.getTaskCount(tlID))

	ctx, err := s.matchingEngine.getTask(context.Background(), tlID, nil, tlKind)
	s.NoError(err)

	ctx.finish(errors.New("test error"))
	s.EqualValues(1, s.taskManager.getTaskCount(tlID))
	ctx2, err := s.matchingEngine.getTask(context.Background(), tlID, nil, tlKind)
	s.NoError(err)

	s.NotEqual(ctx.event.GetTaskId(), ctx2.event.GetTaskId())
	s.Equal(ctx.event.Data.GetWorkflowId(), ctx2.event.Data.GetWorkflowId())
	s.Equal(ctx.event.Data.GetRunId(), ctx2.event.Data.GetRunId())
	s.Equal(ctx.event.Data.GetScheduleId(), ctx2.event.Data.GetScheduleId())

	ctx2.finish(nil)
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
}

func (s *matchingEngineSuite) TestTaskQueueManagerGetTaskBatch() {
	runID := uuid.NewRandom().String()
	workflowID := "workflow1"
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	const taskCount = 1200
	const rangeSize = 10
	s.matchingEngine.config.RangeSize = rangeSize

	// add taskCount tasks
	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3
		addRequest := matchingservice.AddActivityTaskRequest{
			SourceNamespaceId:             namespaceID,
			NamespaceId:                   namespaceID,
			Execution:                     workflowExecution,
			ScheduleId:                    scheduleID,
			TaskQueue:                     taskQueue,
			ScheduleToStartTimeoutSeconds: 1,
		}

		_, err := s.matchingEngine.AddActivityTask(s.handlerContext, &addRequest)
		s.NoError(err)
	}

	tlMgr, ok := s.matchingEngine.taskQueues[*tlID].(*taskQueueManagerImpl)
	s.True(ok, "taskQueueManger doesn't implement taskQueueManager interface")
	s.EqualValues(taskCount, s.taskManager.getTaskCount(tlID))

	// wait until all tasks are read by the task pump and enqeued into the in-memory buffer
	// at the end of this step, ackManager readLevel will also be equal to the buffer size
	expectedBufSize := common.MinInt(cap(tlMgr.taskReader.taskBuffer), taskCount)
	s.True(s.awaitCondition(func() bool { return len(tlMgr.taskReader.taskBuffer) == expectedBufSize }, time.Second))

	// stop all goroutines that read / write tasks in the background
	// remainder of this test works with the in-memory buffer
	if !atomic.CompareAndSwapInt32(&tlMgr.stopped, 0, 1) {
		return
	}
	close(tlMgr.shutdownCh)
	tlMgr.taskWriter.Stop()

	// setReadLevel should NEVER be called without updating ackManager.outstandingTasks
	// This is only for unit test purpose
	tlMgr.taskAckManager.setReadLevel(tlMgr.taskWriter.GetMaxReadLevel())
	tasks, readLevel, isReadBatchDone, err := tlMgr.taskReader.getTaskBatch()
	s.Nil(err)
	s.EqualValues(0, len(tasks))
	s.EqualValues(tlMgr.taskWriter.GetMaxReadLevel(), readLevel)
	s.True(isReadBatchDone)

	tlMgr.taskAckManager.setReadLevel(0)
	tasks, readLevel, isReadBatchDone, err = tlMgr.taskReader.getTaskBatch()
	s.Nil(err)
	s.EqualValues(rangeSize, len(tasks))
	s.EqualValues(rangeSize, readLevel)
	s.True(isReadBatchDone)

	s.setupRecordActivityTaskStartedMock(tl)

	// reset the ackManager readLevel to the buffer size and consume
	// the in-memory tasks by calling Poll API - assert ackMgr state
	// at the end
	tlMgr.taskAckManager.setReadLevel(int64(expectedBufSize))

	// complete rangeSize events
	for i := int64(0); i < rangeSize; i++ {
		identity := "nobody"
		result, err := s.matchingEngine.PollForActivityTask(s.handlerContext, &matchingservice.PollForActivityTaskRequest{
			NamespaceId: namespaceID,
			PollRequest: &workflowservice.PollForActivityTaskRequest{
				TaskQueue: taskQueue,
				Identity:  identity},
		})

		s.NoError(err)
		s.NotNil(result)
		if len(result.TaskToken) == 0 {
			s.logger.Debug("empty poll returned")
			continue
		}
	}
	s.EqualValues(taskCount-rangeSize, s.taskManager.getTaskCount(tlID))
	tasks, _, isReadBatchDone, err = tlMgr.taskReader.getTaskBatch()
	s.Nil(err)
	s.True(0 < len(tasks) && len(tasks) <= rangeSize)
	s.True(isReadBatchDone)

	tlMgr.engine.removeTaskQueueManager(tlMgr.taskQueueID)
}

func (s *matchingEngineSuite) TestTaskQueueManagerGetTaskBatch_ReadBatchDone() {
	namespaceID := uuid.NewRandom().String()
	tl := "makeToast"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tlNormal := enumspb.TASK_QUEUE_KIND_NORMAL

	const rangeSize = 10
	const maxReadLevel = int64(120)
	config := defaultTestConfig()
	config.RangeSize = rangeSize
	tlMgr0, err := newTaskQueueManager(s.matchingEngine, tlID, tlNormal, config)
	s.NoError(err)

	tlMgr, ok := tlMgr0.(*taskQueueManagerImpl)
	s.True(ok)

	tlMgr.taskAckManager.setReadLevel(0)
	atomic.StoreInt64(&tlMgr.taskWriter.maxReadLevel, maxReadLevel)
	tasks, readLevel, isReadBatchDone, err := tlMgr.taskReader.getTaskBatch()
	s.Empty(tasks)
	s.Equal(int64(rangeSize*10), readLevel)
	s.False(isReadBatchDone)
	s.NoError(err)

	tlMgr.taskAckManager.setReadLevel(readLevel)
	tasks, readLevel, isReadBatchDone, err = tlMgr.taskReader.getTaskBatch()
	s.Empty(tasks)
	s.Equal(maxReadLevel, readLevel)
	s.True(isReadBatchDone)
	s.NoError(err)
}

func (s *matchingEngineSuite) TestTaskExpiryAndCompletion() {
	runID := uuid.NewRandom().String()
	workflowID := uuid.New()
	workflowExecution := &commonpb.WorkflowExecution{RunId: runID, WorkflowId: workflowID}

	namespaceID := uuid.NewRandom().String()
	tl := "task-expiry-completion-tl0"
	tlID := newTestTaskQueueID(namespaceID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)

	taskQueue := &taskqueuepb.TaskQueue{Name: tl}

	const taskCount = 20
	const rangeSize = 10
	s.matchingEngine.config.RangeSize = rangeSize
	s.matchingEngine.config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskQueueInfo(2)
	// set idle timer check to a really small value to assert that we don't accidentally drop tasks while blocking
	// on enqueuing a task to task buffer
	s.matchingEngine.config.IdleTaskqueueCheckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(time.Microsecond)

	testCases := []struct {
		batchSize          int
		maxTimeBtwnDeletes time.Duration
	}{
		{2, time.Minute},       // test taskGC deleting due to size threshold
		{100, time.Nanosecond}, // test taskGC deleting due to time condition
	}

	for _, tc := range testCases {
		for i := int64(0); i < taskCount; i++ {
			scheduleID := i * 3
			addRequest := matchingservice.AddActivityTaskRequest{
				SourceNamespaceId:             namespaceID,
				NamespaceId:                   namespaceID,
				Execution:                     workflowExecution,
				ScheduleId:                    scheduleID,
				TaskQueue:                     taskQueue,
				ScheduleToStartTimeoutSeconds: 5,
			}
			if i%2 == 0 {
				// simulates creating a task whose scheduledToStartTimeout is already expired
				addRequest.ScheduleToStartTimeoutSeconds = -5
			}
			_, err := s.matchingEngine.AddActivityTask(s.handlerContext, &addRequest)
			s.NoError(err)
		}

		tlMgr, ok := s.matchingEngine.taskQueues[*tlID].(*taskQueueManagerImpl)
		s.True(ok, "failed to load task queue")
		s.EqualValues(taskCount, s.taskManager.getTaskCount(tlID))

		// wait until all tasks are loaded by into in-memory buffers by task queue manager
		// the buffer size should be one less than expected because dispatcher will dequeue the head
		s.True(s.awaitCondition(func() bool { return len(tlMgr.taskReader.taskBuffer) >= (taskCount/2 - 1) }, time.Second))

		maxTimeBetweenTaskDeletes = tc.maxTimeBtwnDeletes
		s.matchingEngine.config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskQueueInfo(tc.batchSize)

		s.setupRecordActivityTaskStartedMock(tl)

		pollReq := &matchingservice.PollForActivityTaskRequest{
			NamespaceId: namespaceID,
			PollRequest: &workflowservice.PollForActivityTaskRequest{TaskQueue: taskQueue, Identity: "test"},
		}

		remaining := taskCount
		for i := 0; i < 2; i++ {
			// verify that (1) expired tasks are not returned in poll result (2) taskCleaner deletes tasks correctly
			for i := int64(0); i < taskCount/4; i++ {
				result, err := s.matchingEngine.PollForActivityTask(s.handlerContext, pollReq)
				s.NoError(err)
				s.NotNil(result)
			}
			remaining -= taskCount / 2
			// since every other task is expired, we expect half the tasks to be deleted
			// after poll consumed 1/4th of what is available
			s.EqualValues(remaining, s.taskManager.getTaskCount(tlID))
		}
	}
}

func (s *matchingEngineSuite) setupRecordActivityTaskStartedMock(tlName string) {
	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &commonpb.ActivityType{Name: activityTypeName}
	activityInput := payloads.EncodeString("Activity1 Input")

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *historyservice.RecordActivityTaskStartedRequest) (*historyservice.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &historyservice.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(taskRequest.ScheduleId, 0,
					&decisionpb.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    activityID,
						TaskQueue:                     &taskqueuepb.TaskQueue{Name: tlName},
						ActivityType:                  activityType,
						Input:                         activityInput,
						ScheduleToCloseTimeoutSeconds: 100,
						ScheduleToStartTimeoutSeconds: 50,
						StartToCloseTimeoutSeconds:    50,
						HeartbeatTimeoutSeconds:       10,
					}),
			}, nil
		}).AnyTimes()
}

func (s *matchingEngineSuite) awaitCondition(cond func() bool, timeout time.Duration) bool {
	expiry := time.Now().Add(timeout)
	for !cond() {
		time.Sleep(time.Millisecond * 5)
		if time.Now().After(expiry) {
			return false
		}
	}
	return true
}

func newActivityTaskScheduledEvent(eventID int64, decisionTaskCompletedEventID int64,
	scheduleAttributes *decisionpb.ScheduleActivityTaskDecisionAttributes) *historypb.HistoryEvent {

	historyEvent := newHistoryEvent(eventID, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
	historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
		ActivityId:                    scheduleAttributes.ActivityId,
		ActivityType:                  scheduleAttributes.ActivityType,
		TaskQueue:                     scheduleAttributes.TaskQueue,
		Input:                         scheduleAttributes.Input,
		Header:                        scheduleAttributes.Header,
		ScheduleToCloseTimeoutSeconds: scheduleAttributes.ScheduleToCloseTimeoutSeconds,
		ScheduleToStartTimeoutSeconds: scheduleAttributes.ScheduleToStartTimeoutSeconds,
		StartToCloseTimeoutSeconds:    scheduleAttributes.StartToCloseTimeoutSeconds,
		HeartbeatTimeoutSeconds:       scheduleAttributes.HeartbeatTimeoutSeconds,
		DecisionTaskCompletedEventId:  decisionTaskCompletedEventID,
	}}
	return historyEvent
}

func newHistoryEvent(eventID int64, eventType enumspb.EventType) *historypb.HistoryEvent {
	historyEvent := &historypb.HistoryEvent{
		EventId:   eventID,
		Timestamp: time.Now().UnixNano(),
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
	return
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
	rangeID         int64
	ackLevel        int64
	createTaskCount int
	tasks           *treemap.Map
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

func newTestTaskQueueID(namespaceID string, name string, taskType enumspb.TaskQueueType) *taskQueueID {
	result, err := newTaskQueueID(namespaceID, name, taskType)
	if err != nil {
		panic(fmt.Sprintf("newTaskQueueID failed with error %v", err))
	}
	return result
}

// LeaseTaskQueue provides a mock function with given fields: request
func (m *testTaskManager) LeaseTaskQueue(request *persistence.LeaseTaskQueueRequest) (*persistence.LeaseTaskQueueResponse, error) {
	tlm := m.getTaskQueueManager(newTestTaskQueueID(request.NamespaceID, request.TaskQueue, request.TaskType))
	tlm.Lock()
	defer tlm.Unlock()
	tlm.rangeID++
	m.logger.Debug("LeaseTaskQueue", tag.ShardRangeID(tlm.rangeID))

	return &persistence.LeaseTaskQueueResponse{
		TaskQueueInfo: &persistence.PersistedTaskQueueInfo{
			Data: &persistenceblobs.TaskQueueInfo{
				AckLevel:    tlm.ackLevel,
				NamespaceId: request.NamespaceID,
				Name:        request.TaskQueue,
				TaskType:    request.TaskType,
				Kind:        request.TaskQueueKind,
			},
			RangeID: tlm.rangeID,
		},
	}, nil
}

// UpdateTaskQueue provides a mock function with given fields: request
func (m *testTaskManager) UpdateTaskQueue(request *persistence.UpdateTaskQueueRequest) (*persistence.UpdateTaskQueueResponse, error) {
	m.logger.Debug("UpdateTaskQueue", tag.TaskQueueInfo(request.TaskQueueInfo), tag.AckLevel(request.TaskQueueInfo.AckLevel))

	tli := request.TaskQueueInfo
	tlm := m.getTaskQueueManager(newTestTaskQueueID(tli.GetNamespaceId(), tli.Name, tli.TaskType))

	tlm.Lock()
	defer tlm.Unlock()
	if tlm.rangeID != request.RangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task queue: name=%v, type=%v", tli.Name, tli.TaskType),
		}
	}
	tlm.ackLevel = tli.AckLevel
	return &persistence.UpdateTaskQueueResponse{}, nil
}

// CompleteTask provides a mock function with given fields: request
func (m *testTaskManager) CompleteTask(request *persistence.CompleteTaskRequest) error {
	m.logger.Debug("CompleteTask", tag.TaskID(request.TaskID), tag.Name(request.TaskQueue.Name), tag.WorkflowTaskQueueType(request.TaskQueue.TaskType))
	if request.TaskID <= 0 {
		panic(fmt.Errorf("Invalid taskID=%v", request.TaskID))
	}

	tli := request.TaskQueue
	tlm := m.getTaskQueueManager(newTestTaskQueueID(tli.NamespaceID, tli.Name, tli.TaskType))

	tlm.Lock()
	defer tlm.Unlock()

	tlm.tasks.Remove(request.TaskID)
	return nil
}

func (m *testTaskManager) CompleteTasksLessThan(request *persistence.CompleteTasksLessThanRequest) (int, error) {
	tlm := m.getTaskQueueManager(newTestTaskQueueID(request.NamespaceID, request.TaskQueueName, request.TaskType))
	tlm.Lock()
	defer tlm.Unlock()
	keys := tlm.tasks.Keys()
	for _, key := range keys {
		id := key.(int64)
		if id <= request.TaskID {
			tlm.tasks.Remove(id)
		}
	}
	return persistence.UnknownNumRowsAffected, nil
}

func (m *testTaskManager) ListTaskQueue(request *persistence.ListTaskQueueRequest) (*persistence.ListTaskQueueResponse, error) {
	return nil, fmt.Errorf("unsupported operation")
}

func (m *testTaskManager) DeleteTaskQueue(request *persistence.DeleteTaskQueueRequest) error {
	m.Lock()
	defer m.Unlock()
	key := newTestTaskQueueID(request.TaskQueue.NamespaceID, request.TaskQueue.Name, request.TaskQueue.TaskType)
	delete(m.taskQueues, *key)
	return nil
}

// CreateTask provides a mock function with given fields: request
func (m *testTaskManager) CreateTasks(request *persistence.CreateTasksRequest) (*persistence.CreateTasksResponse, error) {
	namespaceID := request.TaskQueueInfo.Data.GetNamespaceId()
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
			panic(fmt.Errorf("Invalid taskID=%v", task.GetTaskId()))
		}

		if tlm.rangeID != rangeID {
			m.logger.Debug("testTaskManager.CreateTask ConditionFailedError",
				tag.TaskID(task.GetTaskId()), tag.ShardRangeID(rangeID), tag.ShardRangeID(tlm.rangeID))

			return nil, &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("testTaskManager.CreateTask failed. TaskQueue: %v, taskType: %v, rangeID: %v, db rangeID: %v",
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
		tlm.tasks.Put(task.GetTaskId(), &persistenceblobs.AllocatedTaskInfo{
			Data:   task.Data,
			TaskId: task.GetTaskId(),
		})
		tlm.createTaskCount++
	}

	return &persistence.CreateTasksResponse{}, nil
}

// GetTasks provides a mock function with given fields: request
func (m *testTaskManager) GetTasks(request *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
	if request.MaxReadLevel != nil {
		m.logger.Debug("testTaskManager.GetTasks", tag.ReadLevel(request.ReadLevel), tag.ReadLevel(*request.MaxReadLevel))
	} else {
		m.logger.Debug("testTaskManager.GetTasks", tag.ReadLevel(request.ReadLevel))
	}

	tlm := m.getTaskQueueManager(newTestTaskQueueID(request.NamespaceID, request.TaskQueue, request.TaskType))
	tlm.Lock()
	defer tlm.Unlock()
	var tasks []*persistenceblobs.AllocatedTaskInfo

	it := tlm.tasks.Iterator()
	for it.Next() {
		taskID := it.Key().(int64)
		if taskID <= request.ReadLevel {
			continue
		}
		if taskID > *request.MaxReadLevel {
			break
		}
		tasks = append(tasks, it.Value().(*persistenceblobs.AllocatedTaskInfo))
	}
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

func (m *testTaskManager) String() string {
	m.Lock()
	defer m.Unlock()
	var result string
	for id, tl := range m.taskQueues {
		tl.Lock()
		if id.taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			result += "Activity"
		} else {
			result += "Decision"
		}
		result += " task queue " + id.name
		result += "\n"
		result += fmt.Sprintf("AckLevel=%v\n", tl.ackLevel)
		result += fmt.Sprintf("CreateTaskCount=%v\n", tl.createTaskCount)
		result += fmt.Sprintf("RangeID=%v\n", tl.rangeID)
		result += "Tasks=\n"
		for _, t := range tl.tasks.Values() {
			result += spew.Sdump(t)
			result += "\n"

		}
		tl.Unlock()
	}
	return result
}

func validateTimeRange(t time.Time, expectedDuration time.Duration) bool {
	currentTime := time.Now()
	diff := time.Duration(currentTime.UnixNano() - t.UnixNano())
	if diff > expectedDuration {
		fmt.Printf("Current time: %v, Application time: %v, Difference: %v \n", currentTime, t, diff)
		return false
	}
	return true
}

func defaultTestConfig() *Config {
	config := NewConfig(dynamicconfig.NewNopCollection())
	config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(100 * time.Millisecond)
	config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskQueueInfo(1)
	return config
}
