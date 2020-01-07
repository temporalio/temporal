// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/golang/mock/gomock"

	gohistory "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyservicetest"
	"github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service/dynamicconfig"

	"github.com/davecgh/go-spew/spew"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/cache"
)

type (
	matchingEngineSuite struct {
		suite.Suite
		controller        *gomock.Controller
		mockHistoryClient *historyservicetest.MockClient
		mockDomainCache   *cache.MockDomainCache

		matchingEngine       *matchingEngineImpl
		taskManager          *testTaskManager
		mockExecutionManager *mocks.ExecutionManager
		logger               log.Logger
		callContext          context.Context
		sync.Mutex
	}
)

const _minBurst = 10000

func TestMatchingEngineSuite(t *testing.T) {
	s := new(matchingEngineSuite)
	suite.Run(t, s)
}

func (s *matchingEngineSuite) SetupSuite() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	http.Handle("/test/tasks", http.HandlerFunc(s.TasksHandler))
	// Get pprof HTTP UI at http://localhost:6060/debug/pprof/
	// Add the following
	// import _ "net/http/pprof"
	//go func() {
	//	log.Println(http.ListenAndServe("localhost:6060", nil))
	//}()
	s.callContext = context.Background()
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
	s.mockHistoryClient = historyservicetest.NewMockClient(s.controller)
	s.taskManager = newTestTaskManager(s.logger)
	s.mockDomainCache = cache.NewMockDomainCache(s.controller)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(cache.CreateDomainCacheEntry("domainName"), nil).AnyTimes()

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
	return newMatchingEngine(config, taskMgr, s.mockHistoryClient, s.logger, s.mockDomainCache)
}

func newMatchingEngine(
	config *Config, taskMgr persistence.TaskManager, mockHistoryClient history.Client,
	logger log.Logger, mockDomainCache cache.DomainCache,
) *matchingEngineImpl {
	return &matchingEngineImpl{
		taskManager:     taskMgr,
		historyService:  mockHistoryClient,
		taskLists:       make(map[taskListID]taskListManager),
		logger:          logger,
		metricsClient:   metrics.NewClient(tally.NoopScope, metrics.Matching),
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
		config:          config,
		domainCache:     mockDomainCache,
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
	s.PollForTasksEmptyResultTest(s.callContext, persistence.TaskListTypeActivity)
}

func (s *matchingEngineSuite) TestPollForDecisionTasksEmptyResult() {
	s.PollForTasksEmptyResultTest(s.callContext, persistence.TaskListTypeDecision)
}

func (s *matchingEngineSuite) TestPollForActivityTasksEmptyResultWithShortContext() {
	shortContextTimeout := returnEmptyTaskTimeBudget + 10*time.Millisecond
	callContext, cancel := context.WithTimeout(s.callContext, shortContextTimeout)
	defer cancel()
	s.PollForTasksEmptyResultTest(callContext, persistence.TaskListTypeActivity)
}

func (s *matchingEngineSuite) TestPollForDecisionTasksEmptyResultWithShortContext() {
	shortContextTimeout := returnEmptyTaskTimeBudget + 10*time.Millisecond
	callContext, cancel := context.WithTimeout(s.callContext, shortContextTimeout)
	defer cancel()
	s.PollForTasksEmptyResultTest(callContext, persistence.TaskListTypeDecision)
}

func (s *matchingEngineSuite) TestPollForDecisionTasks() {
	s.PollForDecisionTasksResultTest()
}

func (s *matchingEngineSuite) PollForDecisionTasksResultTest() {

	domainID := "domainId"
	tl := "makeToast"
	stickyTl := "makeStickyToast"
	stickyTlKind := workflow.TaskListKindSticky
	identity := "selfDrivingToaster"

	stickyTaskList := &workflow.TaskList{}
	stickyTaskList.Name = &stickyTl
	stickyTaskList.Kind = &stickyTlKind

	s.matchingEngine.config.RangeSize = 2 // to test that range is not updated without tasks
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(10 * time.Millisecond)

	runID := "run1"
	workflowID := "workflow1"
	workflowType := workflow.WorkflowType{
		Name: common.StringPtr("workflow"),
	}
	execution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}
	scheduleID := int64(0)

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordDecisionTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *gohistory.RecordDecisionTaskStartedRequest) (*gohistory.RecordDecisionTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordDecisionTaskStartedRequest")
			response := &gohistory.RecordDecisionTaskStartedResponse{}
			response.WorkflowType = &workflowType
			response.PreviousStartedEventId = common.Int64Ptr(scheduleID)
			response.ScheduledEventId = common.Int64Ptr(scheduleID + 1)
			response.Attempt = common.Int64Ptr(0)
			response.StickyExecutionEnabled = common.BoolPtr(true)
			response.WorkflowExecutionTaskList = common.TaskListPtr(workflow.TaskList{
				Name: &tl,
				Kind: common.TaskListKindPtr(workflow.TaskListKindNormal),
			})
			return response, nil
		}).AnyTimes()

	addRequest := matching.AddDecisionTaskRequest{
		DomainUUID:                    common.StringPtr(domainID),
		Execution:                     &execution,
		ScheduleId:                    &scheduleID,
		TaskList:                      stickyTaskList,
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
	}

	_, err := s.matchingEngine.AddDecisionTask(context.Background(), &addRequest)
	s.NoError(err)

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

	resp, err := s.matchingEngine.PollForDecisionTask(s.callContext, &matching.PollForDecisionTaskRequest{
		DomainUUID: common.StringPtr(domainID),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: stickyTaskList,
			Identity: &identity},
	})

	expectedResp := &matching.PollForDecisionTaskResponse{
		TaskToken:              resp.TaskToken,
		WorkflowExecution:      &execution,
		WorkflowType:           &workflowType,
		PreviousStartedEventId: common.Int64Ptr(scheduleID),
		Attempt:                common.Int64Ptr(0),
		BacklogCountHint:       common.Int64Ptr(1),
		StickyExecutionEnabled: common.BoolPtr(true),
		WorkflowExecutionTaskList: common.TaskListPtr(workflow.TaskList{
			Name: &tl,
			Kind: common.TaskListKindPtr(workflow.TaskListKindNormal),
		}),
	}

	s.Nil(err)
	s.Equal(expectedResp, resp)
}

func (s *matchingEngineSuite) PollForTasksEmptyResultTest(callContext context.Context, taskType int) {
	s.matchingEngine.config.RangeSize = 2 // to test that range is not updated without tasks
	if _, ok := callContext.Deadline(); !ok {
		s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(10 * time.Millisecond)
	}

	domainID := "domainId"
	tl := "makeToast"
	identity := "selfDrivingToaster"

	taskList := &workflow.TaskList{}
	taskList.Name = &tl
	var taskListType workflow.TaskListType
	tlID := newTestTaskListID(domainID, tl, taskType)
	//const rangeID = 123
	const pollCount = 10
	for i := 0; i < pollCount; i++ {
		if taskType == persistence.TaskListTypeActivity {
			pollResp, err := s.matchingEngine.PollForActivityTask(callContext, &matching.PollForActivityTaskRequest{
				DomainUUID: common.StringPtr(domainID),
				PollRequest: &workflow.PollForActivityTaskRequest{
					TaskList: taskList,
					Identity: &identity,
				},
			})
			s.NoError(err)
			s.Equal(emptyPollForActivityTaskResponse, pollResp)

			taskListType = workflow.TaskListTypeActivity
		} else {
			resp, err := s.matchingEngine.PollForDecisionTask(callContext, &matching.PollForDecisionTaskRequest{
				DomainUUID: common.StringPtr(domainID),
				PollRequest: &workflow.PollForDecisionTaskRequest{
					TaskList: taskList,
					Identity: &identity},
			})
			s.NoError(err)
			s.Equal(emptyPollForDecisionTaskResponse, resp)

			taskListType = workflow.TaskListTypeDecision
		}
		select {
		case <-callContext.Done():
			s.FailNow("Call context has expired.")
		default:
		}
		// check the poller information
		descResp, err := s.matchingEngine.DescribeTaskList(s.callContext, &matching.DescribeTaskListRequest{
			DomainUUID: common.StringPtr(domainID),
			DescRequest: &workflow.DescribeTaskListRequest{
				TaskList:              taskList,
				TaskListType:          &taskListType,
				IncludeTaskListStatus: common.BoolPtr(false),
			},
		})
		s.NoError(err)
		s.Equal(1, len(descResp.Pollers))
		s.Equal(identity, descResp.Pollers[0].GetIdentity())
		s.NotEmpty(descResp.Pollers[0].GetLastAccessTime())
		s.Nil(descResp.GetTaskListStatus())
	}
	s.EqualValues(1, s.taskManager.taskLists[*tlID].rangeID)
}

func (s *matchingEngineSuite) TestAddActivityTasks() {
	s.AddTasksTest(persistence.TaskListTypeActivity)
}

func (s *matchingEngineSuite) TestAddDecisionTasks() {
	s.AddTasksTest(persistence.TaskListTypeDecision)
}

func (s *matchingEngineSuite) AddTasksTest(taskType int) {
	s.matchingEngine.config.RangeSize = 300 // override to low number for the test

	domainID := "domainId"
	tl := "makeToast"

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

	const taskCount = 111

	runID := "run1"
	workflowID := "workflow1"
	execution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3
		var err error
		if taskType == persistence.TaskListTypeActivity {
			addRequest := matching.AddActivityTaskRequest{
				SourceDomainUUID:              common.StringPtr(domainID),
				DomainUUID:                    common.StringPtr(domainID),
				Execution:                     &execution,
				ScheduleId:                    &scheduleID,
				TaskList:                      taskList,
				ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			}

			_, err = s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		} else {
			addRequest := matching.AddDecisionTaskRequest{
				DomainUUID:                    common.StringPtr(domainID),
				Execution:                     &execution,
				ScheduleId:                    &scheduleID,
				TaskList:                      taskList,
				ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			}

			_, err = s.matchingEngine.AddDecisionTask(context.Background(), &addRequest)
		}
		s.NoError(err)
	}
	s.EqualValues(taskCount, s.taskManager.getTaskCount(newTestTaskListID(domainID, tl, taskType)))
}

func (s *matchingEngineSuite) TestTaskWriterShutdown() {
	s.matchingEngine.config.RangeSize = 300 // override to low number for the test

	domainID := "domainId"
	tl := "makeToast"

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

	runID := "run1"
	workflowID := "workflow1"
	execution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeActivity)
	tlKind := common.TaskListKindPtr(workflow.TaskListKindNormal)
	tlm, err := s.matchingEngine.getTaskListManager(tlID, tlKind)
	s.Nil(err)

	addRequest := matching.AddActivityTaskRequest{
		SourceDomainUUID:              common.StringPtr(domainID),
		DomainUUID:                    common.StringPtr(domainID),
		Execution:                     &execution,
		TaskList:                      taskList,
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
	}

	// stop the task writer explicitly
	tlmImpl := tlm.(*taskListManagerImpl)
	tlmImpl.taskWriter.Stop()

	// now attempt to add a task
	scheduleID := int64(5)
	addRequest.ScheduleId = &scheduleID
	_, err = s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.Error(err)

	// test race
	tlmImpl.taskWriter.stopped = 0
	_, err = s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.Error(err)
	tlmImpl.taskWriter.stopped = 1 // reset it back to old value
}

func (s *matchingEngineSuite) TestAddThenConsumeActivities() {
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(10 * time.Millisecond)

	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	const taskCount = 1000
	const initialRangeID = 102
	// TODO: Understand why publish is low when rangeSize is 3
	const rangeSize = 30

	domainID := "domainId"
	tl := "makeToast"
	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeActivity)
	s.taskManager.getTaskListManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3
		addRequest := matching.AddActivityTaskRequest{
			SourceDomainUUID:              common.StringPtr(domainID),
			DomainUUID:                    common.StringPtr(domainID),
			Execution:                     &workflowExecution,
			ScheduleId:                    &scheduleID,
			TaskList:                      taskList,
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		}

		_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		s.NoError(err)
	}
	s.EqualValues(taskCount, s.taskManager.getTaskCount(tlID))

	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")

	identity := "nobody"

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) (*gohistory.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			resp := &gohistory.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(*taskRequest.ScheduleId, 0,
					&workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    &activityID,
						TaskList:                      &workflow.TaskList{Name: taskList.Name},
						ActivityType:                  activityType,
						Input:                         activityInput,
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(50),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(10),
					}),
			}
			resp.StartedTimestamp = common.Int64Ptr(time.Now().UnixNano())
			return resp, nil
		}).AnyTimes()

	for i := int64(0); i < taskCount; {
		scheduleID := i * 3

		result, err := s.matchingEngine.PollForActivityTask(s.callContext, &matching.PollForActivityTaskRequest{
			DomainUUID: common.StringPtr(domainID),
			PollRequest: &workflow.PollForActivityTaskRequest{
				TaskList: taskList,
				Identity: &identity},
		})

		s.NoError(err)
		s.NotNil(result)
		if len(result.TaskToken) == 0 {
			s.logger.Debug(fmt.Sprintf("empty poll returned"))
			continue
		}
		s.EqualValues(activityID, *result.ActivityId)
		s.EqualValues(activityType, result.ActivityType)
		s.EqualValues(activityInput, result.Input)
		s.EqualValues(workflowExecution, *result.WorkflowExecution)
		s.Equal(true, validateTimeRange(time.Unix(0, *result.ScheduledTimestamp), time.Minute))
		s.Equal(int32(100), *result.ScheduleToCloseTimeoutSeconds)
		s.Equal(true, validateTimeRange(time.Unix(0, *result.StartedTimestamp), time.Minute))
		s.Equal(int32(50), *result.StartToCloseTimeoutSeconds)
		s.Equal(int32(10), *result.HeartbeatTimeoutSeconds)
		token := &common.TaskToken{
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			ScheduleID: scheduleID,
		}

		taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
		s.EqualValues(taskToken, result.TaskToken)
		i++
	}
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
	expectedRange := int64(initialRangeID + taskCount/rangeSize)
	if taskCount%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getTaskListManager(tlID).rangeID)
}

func (s *matchingEngineSuite) TestSyncMatchActivities() {
	// Set a short long poll expiration so we don't have to wait too long for 0 throttling cases
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(50 * time.Millisecond)

	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	const taskCount = 10
	const initialRangeID = 102
	// TODO: Understand why publish is low when rangeSize is 3
	const rangeSize = 30

	domainID := "domainId"
	tl := "makeToast"
	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeActivity)
	tlKind := common.TaskListKindPtr(workflow.TaskListKindNormal)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test
	// So we can get snapshots
	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsClient = metrics.NewClient(scope, metrics.Matching)

	dispatchTTL := time.Nanosecond
	dPtr := _defaultTaskDispatchRPS

	mgr, err := newTaskListManager(s.matchingEngine, tlID, tlKind, s.matchingEngine.config)
	s.NoError(err)

	mgrImpl, ok := mgr.(*taskListManagerImpl)
	s.True(ok)

	mgrImpl.matcher.limiter = quotas.NewRateLimiter(&dPtr, dispatchTTL, _minBurst)
	s.matchingEngine.updateTaskList(tlID, mgr)
	s.taskManager.getTaskListManager(tlID).rangeID = initialRangeID
	s.NoError(mgr.Start())

	taskList := &workflow.TaskList{}
	taskList.Name = &tl
	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")

	identity := "nobody"

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) (*gohistory.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &gohistory.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(*taskRequest.ScheduleId, 0,
					&workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    &activityID,
						TaskList:                      &workflow.TaskList{Name: taskList.Name},
						ActivityType:                  activityType,
						Input:                         activityInput,
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(2),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
					}),
			}, nil
		}).AnyTimes()

	pollFunc := func(maxDispatch float64) (*workflow.PollForActivityTaskResponse, error) {
		return s.matchingEngine.PollForActivityTask(s.callContext, &matching.PollForActivityTaskRequest{
			DomainUUID: common.StringPtr(domainID),
			PollRequest: &workflow.PollForActivityTaskRequest{
				TaskList:         taskList,
				Identity:         &identity,
				TaskListMetadata: &workflow.TaskListMetadata{MaxTasksPerSecond: &maxDispatch},
			},
		})
	}

	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3

		var wg sync.WaitGroup
		var result *workflow.PollForActivityTaskResponse
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
		addRequest := matching.AddActivityTaskRequest{
			SourceDomainUUID:              common.StringPtr(domainID),
			DomainUUID:                    common.StringPtr(domainID),
			Execution:                     &workflowExecution,
			ScheduleId:                    &scheduleID,
			TaskList:                      taskList,
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		}
		_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		wg.Wait()
		s.NoError(err)
		s.NoError(pollErr)
		s.NotNil(result)

		if len(result.TaskToken) == 0 {
			// when ratelimit is set to zero, poller is expected to return empty result
			// reset ratelimit, poll again and make sure task is returned this time
			s.logger.Debug(fmt.Sprintf("empty poll returned"))
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

		s.EqualValues(activityID, *result.ActivityId)
		s.EqualValues(activityType, result.ActivityType)
		s.EqualValues(activityInput, result.Input)
		s.EqualValues(workflowExecution, *result.WorkflowExecution)
		token := &common.TaskToken{
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			ScheduleID: scheduleID,
		}

		taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
		//s.EqualValues(scheduleID, result.TaskToken)

		s.EqualValues(string(taskToken), string(result.TaskToken))
	}

	time.Sleep(20 * time.Millisecond) // So any buffer tasks from 0 rps get picked up
	syncCtr := scope.Snapshot().Counters()["test.sync_throttle_count+domain=domainName,operation=TaskListMgr"]
	s.Equal(1, int(syncCtr.Value()))                         // Check times zero rps is set = throttle counter
	s.EqualValues(1, s.taskManager.getCreateTaskCount(tlID)) // Check times zero rps is set = Tasks stored in persistence
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
	expectedRange := int64(initialRangeID + taskCount/rangeSize)
	if taskCount%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getTaskListManager(tlID).rangeID)

	// check the poller information
	tlType := workflow.TaskListTypeActivity
	descResp, err := s.matchingEngine.DescribeTaskList(s.callContext, &matching.DescribeTaskListRequest{
		DomainUUID: common.StringPtr(domainID),
		DescRequest: &workflow.DescribeTaskListRequest{
			TaskList:              taskList,
			TaskListType:          &tlType,
			IncludeTaskListStatus: common.BoolPtr(true),
		},
	})
	s.NoError(err)
	s.Equal(1, len(descResp.Pollers))
	s.Equal(identity, descResp.Pollers[0].GetIdentity())
	s.NotEmpty(descResp.Pollers[0].GetLastAccessTime())
	s.Equal(_defaultTaskDispatchRPS, descResp.Pollers[0].GetRatePerSecond())
	s.NotNil(descResp.GetTaskListStatus())
	s.True(descResp.GetTaskListStatus().GetRatePerSecond() >= (_defaultTaskDispatchRPS - 1))
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
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(20 * time.Millisecond)
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
	s.logger.Info(fmt.Sprintf("Number of tasks throttled: %d", throttleCt))
	// atleast once from 0 dispatch poll, and until TTL is hit at which time throttle limit is reset
	// hard to predict exactly how many times, since the atomic.Value load might not have updated.
	s.True(throttleCt >= 1)
}

func (s *matchingEngineSuite) concurrentPublishConsumeActivities(
	workerCount int, taskCount int64, dispatchLimitFn func(int, int64) float64) int64 {
	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsClient = metrics.NewClient(scope, metrics.Matching)
	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	const initialRangeID = 0
	const rangeSize = 3
	var scheduleID int64 = 123
	domainID := "domainId"
	tl := "makeToast"
	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeActivity)
	tlKind := common.TaskListKindPtr(workflow.TaskListKindNormal)
	dispatchTTL := time.Nanosecond
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test
	dPtr := _defaultTaskDispatchRPS

	mgr, err := newTaskListManager(s.matchingEngine, tlID, tlKind, s.matchingEngine.config)
	s.NoError(err)

	mgrImpl := mgr.(*taskListManagerImpl)
	mgrImpl.matcher.limiter = quotas.NewRateLimiter(&dPtr, dispatchTTL, _minBurst)
	s.matchingEngine.updateTaskList(tlID, mgr)
	s.taskManager.getTaskListManager(tlID).rangeID = initialRangeID
	s.NoError(mgr.Start())

	taskList := &workflow.TaskList{}
	taskList.Name = &tl
	var wg sync.WaitGroup
	wg.Add(2 * workerCount)

	for p := 0; p < workerCount; p++ {
		go func() {
			defer wg.Done()
			for i := int64(0); i < taskCount; i++ {
				addRequest := matching.AddActivityTaskRequest{
					SourceDomainUUID:              common.StringPtr(domainID),
					DomainUUID:                    common.StringPtr(domainID),
					Execution:                     &workflowExecution,
					ScheduleId:                    &scheduleID,
					TaskList:                      taskList,
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
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
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")
	activityHeader := &workflow.Header{
		Fields: map[string][]byte{"tracing": []byte("tracing data")},
	}

	identity := "nobody"

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) (*gohistory.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &gohistory.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(*taskRequest.ScheduleId, 0,
					&workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    &activityID,
						TaskList:                      &workflow.TaskList{Name: taskList.Name},
						ActivityType:                  activityType,
						Input:                         activityInput,
						Header:                        activityHeader,
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(2),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
					}),
			}, nil
		}).AnyTimes()

	for p := 0; p < workerCount; p++ {
		go func(wNum int) {
			defer wg.Done()
			for i := int64(0); i < taskCount; {
				maxDispatch := dispatchLimitFn(wNum, i)
				result, err := s.matchingEngine.PollForActivityTask(s.callContext, &matching.PollForActivityTaskRequest{
					DomainUUID: common.StringPtr(domainID),
					PollRequest: &workflow.PollForActivityTaskRequest{
						TaskList:         taskList,
						Identity:         &identity,
						TaskListMetadata: &workflow.TaskListMetadata{MaxTasksPerSecond: &maxDispatch},
					},
				})
				s.NoError(err)
				s.NotNil(result)
				if len(result.TaskToken) == 0 {
					s.logger.Debug(fmt.Sprintf("empty poll returned"))
					continue
				}
				s.EqualValues(activityID, *result.ActivityId)
				s.EqualValues(activityType, result.ActivityType)
				s.EqualValues(activityInput, result.Input)
				s.EqualValues(activityHeader, result.Header)
				s.EqualValues(workflowExecution, *result.WorkflowExecution)
				token := &common.TaskToken{
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					ScheduleID: scheduleID,
				}
				resultToken, err := s.matchingEngine.tokenSerializer.Deserialize(result.TaskToken)
				s.NoError(err)

				//taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
				//s.EqualValues(taskToken, result.TaskToken, fmt.Sprintf("%v!=%v", string(taskToken)))
				s.EqualValues(token, resultToken, fmt.Sprintf("%v!=%v", token, resultToken))
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
	s.True(expectedRange <= s.taskManager.getTaskListManager(tlID).rangeID)
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))

	syncCtr := scope.Snapshot().Counters()["test.sync_throttle_count+domain=domainName,operation=TaskListMgr"]
	bufCtr := scope.Snapshot().Counters()["test.buffer_throttle_count+domain=domainName,operation=TaskListMgr"]
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
	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	const workerCount = 20
	const taskCount = 100
	const initialRangeID = 0
	const rangeSize = 5
	var scheduleID int64 = 123
	var startedEventID int64 = 1412

	domainID := "domainId"
	tl := "makeToast"
	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeDecision)
	s.taskManager.getTaskListManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

	var wg sync.WaitGroup
	wg.Add(2 * workerCount)

	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; i++ {
				addRequest := matching.AddDecisionTaskRequest{
					DomainUUID:                    common.StringPtr(domainID),
					Execution:                     &workflowExecution,
					ScheduleId:                    &scheduleID,
					TaskList:                      taskList,
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
				}

				_, err := s.matchingEngine.AddDecisionTask(context.Background(), &addRequest)
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}
	workflowTypeName := "workflowType1"
	workflowType := &workflow.WorkflowType{Name: &workflowTypeName}

	identity := "nobody"

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordDecisionTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *gohistory.RecordDecisionTaskStartedRequest) (*gohistory.RecordDecisionTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordDecisionTaskStartedRequest")
			return &gohistory.RecordDecisionTaskStartedResponse{
				PreviousStartedEventId: &startedEventID,
				StartedEventId:         &startedEventID,
				ScheduledEventId:       &scheduleID,
				WorkflowType:           workflowType,
			}, nil
		}).AnyTimes()
	for p := 0; p < workerCount; p++ {
		go func() {
			for i := int64(0); i < taskCount; {
				result, err := s.matchingEngine.PollForDecisionTask(s.callContext, &matching.PollForDecisionTaskRequest{
					DomainUUID: common.StringPtr(domainID),
					PollRequest: &workflow.PollForDecisionTaskRequest{
						TaskList: taskList,
						Identity: &identity},
				})
				if err != nil {
					panic(err)
				}
				s.NotNil(result)
				if len(result.TaskToken) == 0 {
					s.logger.Debug(fmt.Sprintf("empty poll returned"))
					continue
				}
				s.EqualValues(workflowExecution, *result.WorkflowExecution)
				s.EqualValues(workflowType, result.WorkflowType)
				s.EqualValues(startedEventID, *result.StartedEventId)
				s.EqualValues(workflowExecution, *result.WorkflowExecution)
				token := &common.TaskToken{
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					ScheduleID: scheduleID,
				}
				resultToken, err := s.matchingEngine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}

				//taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
				//s.EqualValues(taskToken, result.TaskToken, fmt.Sprintf("%v!=%v", string(taskToken)))
				s.EqualValues(token, resultToken, fmt.Sprintf("%v!=%v", token, resultToken))
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
	s.True(expectedRange <= s.taskManager.getTaskListManager(tlID).rangeID)
}

func (s *matchingEngineSuite) TestPollWithExpiredContext() {
	identity := "nobody"
	domainID := "domainId"
	tl := "makeToast"

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

	// Try with cancelled context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	cancel()
	_, err := s.matchingEngine.PollForActivityTask(ctx, &matching.PollForActivityTaskRequest{
		DomainUUID: common.StringPtr(domainID),
		PollRequest: &workflow.PollForActivityTaskRequest{
			TaskList: taskList,
			Identity: &identity},
	})

	s.Equal(ctx.Err(), err)

	// Try with expired context
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := s.matchingEngine.PollForActivityTask(ctx, &matching.PollForActivityTaskRequest{
		DomainUUID: common.StringPtr(domainID),
		PollRequest: &workflow.PollForActivityTaskRequest{
			TaskList: taskList,
			Identity: &identity},
	})
	s.Nil(err)
	s.Equal(emptyPollForActivityTaskResponse, resp)
}

func (s *matchingEngineSuite) TestMultipleEnginesActivitiesRangeStealing() {
	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	const engineCount = 2
	const taskCount = 400
	const iterations = 2
	const initialRangeID = 0
	const rangeSize = 10
	var scheduleID int64 = 123

	domainID := "domainId"
	tl := "makeToast"
	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeActivity)
	s.taskManager.getTaskListManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

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
				addRequest := matching.AddActivityTaskRequest{
					SourceDomainUUID:              common.StringPtr(domainID),
					DomainUUID:                    common.StringPtr(domainID),
					Execution:                     &workflowExecution,
					ScheduleId:                    &scheduleID,
					TaskList:                      taskList,
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(600),
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
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")

	identity := "nobody"

	startedTasks := make(map[int64]bool)

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) (*gohistory.RecordActivityTaskStartedResponse, error) {
			if _, ok := startedTasks[*taskRequest.TaskId]; ok {
				s.logger.Debug(fmt.Sprintf("From error function Mock Received DUPLICATED RecordActivityTaskStartedRequest for taskID=%v", taskRequest.TaskId))
				return nil, &workflow.EntityNotExistsError{Message: "already started"}
			}
			s.logger.Debug(fmt.Sprintf("Mock Received RecordActivityTaskStartedRequest for taskID=%v", taskRequest.TaskId))

			startedTasks[*taskRequest.TaskId] = true
			return &gohistory.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(*taskRequest.ScheduleId, 0,
					&workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    &activityID,
						TaskList:                      &workflow.TaskList{Name: taskList.Name},
						ActivityType:                  activityType,
						Input:                         activityInput,
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(600),
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(2),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(1),
					}),
			}, nil
		}).AnyTimes()
	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; /* incremented explicitly to skip empty polls */ {
				result, err := engine.PollForActivityTask(s.callContext, &matching.PollForActivityTaskRequest{
					DomainUUID: common.StringPtr(domainID),
					PollRequest: &workflow.PollForActivityTaskRequest{
						TaskList: taskList,
						Identity: &identity},
				})
				if err != nil {
					panic(err)
				}
				s.NotNil(result)
				if len(result.TaskToken) == 0 {
					s.logger.Debug(fmt.Sprintf("empty poll returned"))
					continue
				}
				s.EqualValues(activityID, *result.ActivityId)
				s.EqualValues(activityType, result.ActivityType)
				s.EqualValues(activityInput, result.Input)
				s.EqualValues(workflowExecution, *result.WorkflowExecution)
				token := &common.TaskToken{
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					ScheduleID: scheduleID,
				}
				resultToken, err := engine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}
				//taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
				//s.EqualValues(taskToken, result.TaskToken, fmt.Sprintf("%v!=%v", string(taskToken)))
				s.EqualValues(token, resultToken, fmt.Sprintf("%v!=%v", token, resultToken))
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
	s.True(expectedRange <= s.taskManager.getTaskListManager(tlID).rangeID)

}

func (s *matchingEngineSuite) TestMultipleEnginesDecisionsRangeStealing() {
	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	const engineCount = 2
	const taskCount = 400
	const iterations = 2
	const initialRangeID = 0
	const rangeSize = 10
	var scheduleID int64 = 123

	domainID := "domainId"
	tl := "makeToast"
	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeDecision)
	s.taskManager.getTaskListManager(tlID).rangeID = initialRangeID
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

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
				addRequest := matching.AddDecisionTaskRequest{
					DomainUUID:                    common.StringPtr(domainID),
					Execution:                     &workflowExecution,
					ScheduleId:                    &scheduleID,
					TaskList:                      taskList,
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(600),
				}

				_, err := engine.AddDecisionTask(context.Background(), &addRequest)
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
	workflowType := &workflow.WorkflowType{Name: &workflowTypeName}

	identity := "nobody"
	var startedEventID int64 = 1412

	startedTasks := make(map[int64]bool)

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordDecisionTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *gohistory.RecordDecisionTaskStartedRequest) (*gohistory.RecordDecisionTaskStartedResponse, error) {
			if _, ok := startedTasks[*taskRequest.TaskId]; ok {
				s.logger.Debug(fmt.Sprintf("From error function Mock Received DUPLICATED RecordDecisionTaskStartedRequest for taskID=%v", taskRequest.TaskId))
				return nil, &gohistory.EventAlreadyStartedError{Message: "already started"}
			}
			s.logger.Debug(fmt.Sprintf("Mock Received RecordDecisionTaskStartedRequest for taskID=%v", taskRequest.TaskId))
			s.logger.Debug("Mock Received RecordDecisionTaskStartedRequest")
			startedTasks[*taskRequest.TaskId] = true
			return &gohistory.RecordDecisionTaskStartedResponse{
				PreviousStartedEventId: &startedEventID,
				StartedEventId:         &startedEventID,
				ScheduledEventId:       &scheduleID,
				WorkflowType:           workflowType,
			}, nil
		}).AnyTimes()
	for j := 0; j < iterations; j++ {
		for p := 0; p < engineCount; p++ {
			engine := engines[p]
			for i := int64(0); i < taskCount; /* incremented explicitly to skip empty polls */ {
				result, err := engine.PollForDecisionTask(s.callContext, &matching.PollForDecisionTaskRequest{
					DomainUUID: common.StringPtr(domainID),
					PollRequest: &workflow.PollForDecisionTaskRequest{
						TaskList: taskList,
						Identity: &identity},
				})
				if err != nil {
					panic(err)
				}
				s.NotNil(result)
				if len(result.TaskToken) == 0 {
					s.logger.Debug(fmt.Sprintf("empty poll returned"))
					continue
				}
				s.EqualValues(workflowExecution, *result.WorkflowExecution)
				s.EqualValues(workflowType, result.WorkflowType)
				s.EqualValues(startedEventID, *result.StartedEventId)
				s.EqualValues(workflowExecution, *result.WorkflowExecution)
				token := &common.TaskToken{
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					ScheduleID: scheduleID,
				}
				resultToken, err := engine.tokenSerializer.Deserialize(result.TaskToken)
				if err != nil {
					panic(err)
				}

				//taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
				//s.EqualValues(taskToken, result.TaskToken, fmt.Sprintf("%v!=%v", string(taskToken)))
				s.EqualValues(token, resultToken, fmt.Sprintf("%v!=%v", token, resultToken))
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
	s.True(expectedRange <= s.taskManager.getTaskListManager(tlID).rangeID)

}

func (s *matchingEngineSuite) TestAddTaskAfterStartFailure() {
	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	domainID := "domainId"
	tl := "makeToast"
	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeActivity)
	tlKind := common.TaskListKindPtr(workflow.TaskListKindNormal)

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

	scheduleID := int64(0)
	addRequest := matching.AddActivityTaskRequest{
		SourceDomainUUID:              common.StringPtr(domainID),
		DomainUUID:                    common.StringPtr(domainID),
		Execution:                     &workflowExecution,
		ScheduleId:                    &scheduleID,
		TaskList:                      taskList,
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
	}

	_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
	s.NoError(err)
	s.EqualValues(1, s.taskManager.getTaskCount(tlID))

	ctx, err := s.matchingEngine.getTask(context.Background(), tlID, nil, tlKind)
	s.NoError(err)

	ctx.finish(errors.New("test error"))
	s.EqualValues(1, s.taskManager.getTaskCount(tlID))
	ctx2, err := s.matchingEngine.getTask(context.Background(), tlID, nil, tlKind)
	s.NoError(err)

	s.NotEqual(ctx.event.TaskID, ctx2.event.TaskID)
	s.Equal(ctx.event.WorkflowID, ctx2.event.WorkflowID)
	s.Equal(ctx.event.RunID, ctx2.event.RunID)
	s.Equal(ctx.event.ScheduleID, ctx2.event.ScheduleID)

	ctx2.finish(nil)
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
}

func (s *matchingEngineSuite) TestTaskListManagerGetTaskBatch() {
	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	domainID := "domainId"
	tl := "makeToast"
	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeActivity)

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

	const taskCount = 1200
	const rangeSize = 10
	s.matchingEngine.config.RangeSize = rangeSize

	// add taskCount tasks
	for i := int64(0); i < taskCount; i++ {
		scheduleID := i * 3
		addRequest := matching.AddActivityTaskRequest{
			SourceDomainUUID:              common.StringPtr(domainID),
			DomainUUID:                    common.StringPtr(domainID),
			Execution:                     &workflowExecution,
			ScheduleId:                    &scheduleID,
			TaskList:                      taskList,
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
		}

		_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
		s.NoError(err)
	}

	tlMgr, ok := s.matchingEngine.taskLists[*tlID].(*taskListManagerImpl)
	s.True(ok, "taskListManger doesn't implement taskListManager interface")
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
		result, err := s.matchingEngine.PollForActivityTask(s.callContext, &matching.PollForActivityTaskRequest{
			DomainUUID: common.StringPtr(domainID),
			PollRequest: &workflow.PollForActivityTaskRequest{
				TaskList: taskList,
				Identity: &identity},
		})

		s.NoError(err)
		s.NotNil(result)
		if len(result.TaskToken) == 0 {
			s.logger.Debug(fmt.Sprintf("empty poll returned"))
			continue
		}
	}
	s.EqualValues(taskCount-rangeSize, s.taskManager.getTaskCount(tlID))
	tasks, _, isReadBatchDone, err = tlMgr.taskReader.getTaskBatch()
	s.Nil(err)
	s.True(0 < len(tasks) && len(tasks) <= rangeSize)
	s.True(isReadBatchDone)

	tlMgr.engine.removeTaskListManager(tlMgr.taskListID)
}

func (s *matchingEngineSuite) TestTaskListManagerGetTaskBatch_ReadBatchDone() {
	domainID := "domainId"
	tl := "makeToast"
	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeActivity)
	tlNormal := workflow.TaskListKindNormal

	const rangeSize = 10
	const maxReadLevel = int64(120)
	config := defaultTestConfig()
	config.RangeSize = rangeSize
	tlMgr0, err := newTaskListManager(s.matchingEngine, tlID, &tlNormal, config)
	s.NoError(err)

	tlMgr, ok := tlMgr0.(*taskListManagerImpl)
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
	runID := uuid.New()
	workflowID := uuid.New()
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	domainID := uuid.New()
	tl := "task-expiry-completion-tl0"
	tlID := newTestTaskListID(domainID, tl, persistence.TaskListTypeActivity)

	taskList := &workflow.TaskList{}
	taskList.Name = &tl

	const taskCount = 20
	const rangeSize = 10
	s.matchingEngine.config.RangeSize = rangeSize
	s.matchingEngine.config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskListInfo(2)
	// set idle timer check to a really small value to assert that we don't accidentally drop tasks while blocking
	// on enqueuing a task to task buffer
	s.matchingEngine.config.IdleTasklistCheckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(time.Microsecond)

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
			addRequest := matching.AddActivityTaskRequest{
				SourceDomainUUID:              common.StringPtr(domainID),
				DomainUUID:                    common.StringPtr(domainID),
				Execution:                     &workflowExecution,
				ScheduleId:                    &scheduleID,
				TaskList:                      taskList,
				ScheduleToStartTimeoutSeconds: common.Int32Ptr(5),
			}
			if i%2 == 0 {
				// simulates creating a task whose scheduledToStartTimeout is already expired
				addRequest.ScheduleToStartTimeoutSeconds = common.Int32Ptr(-5)
			}
			_, err := s.matchingEngine.AddActivityTask(context.Background(), &addRequest)
			s.NoError(err)
		}

		tlMgr, ok := s.matchingEngine.taskLists[*tlID].(*taskListManagerImpl)
		s.True(ok, "failed to load task list")
		s.EqualValues(taskCount, s.taskManager.getTaskCount(tlID))

		// wait until all tasks are loaded by into in-memory buffers by task list manager
		// the buffer size should be one less than expected because dispatcher will dequeue the head
		s.True(s.awaitCondition(func() bool { return len(tlMgr.taskReader.taskBuffer) >= (taskCount/2 - 1) }, time.Second))

		maxTimeBetweenTaskDeletes = tc.maxTimeBtwnDeletes
		s.matchingEngine.config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskListInfo(tc.batchSize)

		s.setupRecordActivityTaskStartedMock(tl)

		pollReq := &matching.PollForActivityTaskRequest{
			DomainUUID:  common.StringPtr(domainID),
			PollRequest: &workflow.PollForActivityTaskRequest{TaskList: taskList, Identity: common.StringPtr("test")},
		}

		remaining := taskCount
		for i := 0; i < 2; i++ {
			// verify that (1) expired tasks are not returned in poll result (2) taskCleaner deletes tasks correctly
			for i := int64(0); i < taskCount/4; i++ {
				result, err := s.matchingEngine.PollForActivityTask(s.callContext, pollReq)
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
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")

	// History service is using mock
	s.mockHistoryClient.EXPECT().RecordActivityTaskStarted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) (*gohistory.RecordActivityTaskStartedResponse, error) {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &gohistory.RecordActivityTaskStartedResponse{
				ScheduledEvent: newActivityTaskScheduledEvent(*taskRequest.ScheduleId, 0,
					&workflow.ScheduleActivityTaskDecisionAttributes{
						ActivityId:                    &activityID,
						TaskList:                      &workflow.TaskList{Name: &tlName},
						ActivityType:                  activityType,
						Input:                         activityInput,
						ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
						ScheduleToStartTimeoutSeconds: common.Int32Ptr(50),
						StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
						HeartbeatTimeoutSeconds:       common.Int32Ptr(10),
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
	scheduleAttributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventTypeActivityTaskScheduled)
	attributes := &workflow.ActivityTaskScheduledEventAttributes{}
	attributes.ActivityId = scheduleAttributes.ActivityId
	attributes.ActivityType = scheduleAttributes.ActivityType
	attributes.TaskList = scheduleAttributes.TaskList
	attributes.Input = scheduleAttributes.Input
	attributes.Header = scheduleAttributes.Header
	attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(*scheduleAttributes.ScheduleToCloseTimeoutSeconds)
	attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(*scheduleAttributes.ScheduleToStartTimeoutSeconds)
	attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(*scheduleAttributes.StartToCloseTimeoutSeconds)
	attributes.HeartbeatTimeoutSeconds = common.Int32Ptr(*scheduleAttributes.HeartbeatTimeoutSeconds)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.ActivityTaskScheduledEventAttributes = attributes

	return historyEvent
}

func newHistoryEvent(eventID int64, eventType workflow.EventType) *workflow.HistoryEvent {
	ts := common.Int64Ptr(time.Now().UnixNano())
	historyEvent := &workflow.HistoryEvent{}
	historyEvent.EventId = common.Int64Ptr(eventID)
	historyEvent.Timestamp = ts
	historyEvent.EventType = &eventType

	return historyEvent
}

var _ persistence.TaskManager = (*testTaskManager)(nil) // Asserts that interface is indeed implemented

type testTaskManager struct {
	sync.Mutex
	taskLists map[taskListID]*testTaskListManager
	logger    log.Logger
}

func newTestTaskManager(logger log.Logger) *testTaskManager {
	return &testTaskManager{taskLists: make(map[taskListID]*testTaskListManager), logger: logger}
}

func (m *testTaskManager) GetName() string {
	return "test"
}

func (m *testTaskManager) Close() {
	return
}

func (m *testTaskManager) getTaskListManager(id *taskListID) *testTaskListManager {
	m.Lock()
	defer m.Unlock()
	result, ok := m.taskLists[*id]
	if ok {
		return result
	}
	result = newTestTaskListManager()
	m.taskLists[*id] = result
	return result
}

type testTaskListManager struct {
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

func newTestTaskListManager() *testTaskListManager {
	return &testTaskListManager{tasks: treemap.NewWith(Int64Comparator)}
}

func newTestTaskListID(domainID string, name string, taskType int) *taskListID {
	result, err := newTaskListID(domainID, name, taskType)
	if err != nil {
		panic(fmt.Sprintf("newTaskListID failed with error %v", err))
	}
	return result
}

// LeaseTaskList provides a mock function with given fields: request
func (m *testTaskManager) LeaseTaskList(request *persistence.LeaseTaskListRequest) (*persistence.LeaseTaskListResponse, error) {
	tlm := m.getTaskListManager(newTestTaskListID(request.DomainID, request.TaskList, request.TaskType))
	tlm.Lock()
	defer tlm.Unlock()
	tlm.rangeID++
	m.logger.Debug(fmt.Sprintf("LeaseTaskList rangeID=%v", tlm.rangeID))

	return &persistence.LeaseTaskListResponse{
		TaskListInfo: &persistence.TaskListInfo{
			AckLevel: tlm.ackLevel,
			DomainID: request.DomainID,
			Name:     request.TaskList,
			TaskType: request.TaskType,
			RangeID:  tlm.rangeID,
			Kind:     request.TaskListKind,
		},
	}, nil
}

// UpdateTaskList provides a mock function with given fields: request
func (m *testTaskManager) UpdateTaskList(request *persistence.UpdateTaskListRequest) (*persistence.UpdateTaskListResponse, error) {
	m.logger.Debug(fmt.Sprintf("UpdateTaskList taskListInfo=%v, ackLevel=%v", request.TaskListInfo, request.TaskListInfo.AckLevel))

	tli := request.TaskListInfo
	tlm := m.getTaskListManager(newTestTaskListID(tli.DomainID, tli.Name, tli.TaskType))

	tlm.Lock()
	defer tlm.Unlock()
	if tlm.rangeID != tli.RangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task list: name=%v, type=%v", tli.Name, tli.TaskType),
		}
	}
	tlm.ackLevel = tli.AckLevel
	return &persistence.UpdateTaskListResponse{}, nil
}

// CompleteTask provides a mock function with given fields: request
func (m *testTaskManager) CompleteTask(request *persistence.CompleteTaskRequest) error {
	m.logger.Debug(fmt.Sprintf("CompleteTask taskID=%v, ackLevel=%v", request.TaskID, request.TaskList.AckLevel))
	if request.TaskID <= 0 {
		panic(fmt.Errorf("Invalid taskID=%v", request.TaskID))
	}

	tli := request.TaskList
	tlm := m.getTaskListManager(newTestTaskListID(tli.DomainID, tli.Name, tli.TaskType))

	tlm.Lock()
	defer tlm.Unlock()

	tlm.tasks.Remove(request.TaskID)
	return nil
}

func (m *testTaskManager) CompleteTasksLessThan(request *persistence.CompleteTasksLessThanRequest) (int, error) {
	tlm := m.getTaskListManager(newTestTaskListID(request.DomainID, request.TaskListName, request.TaskType))
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

func (m *testTaskManager) ListTaskList(
	request *persistence.ListTaskListRequest) (*persistence.ListTaskListResponse, error) {
	return nil, fmt.Errorf("unsupported operation")
}

func (m *testTaskManager) DeleteTaskList(request *persistence.DeleteTaskListRequest) error {
	m.Lock()
	defer m.Unlock()
	key := newTestTaskListID(request.DomainID, request.TaskListName, request.TaskListType)
	delete(m.taskLists, *key)
	return nil
}

// CreateTask provides a mock function with given fields: request
func (m *testTaskManager) CreateTasks(request *persistence.CreateTasksRequest) (*persistence.CreateTasksResponse, error) {
	domainID := request.TaskListInfo.DomainID
	taskList := request.TaskListInfo.Name
	taskType := request.TaskListInfo.TaskType
	rangeID := request.TaskListInfo.RangeID

	tlm := m.getTaskListManager(newTestTaskListID(domainID, taskList, taskType))
	tlm.Lock()
	defer tlm.Unlock()

	// First validate the entire batch
	for _, task := range request.Tasks {
		m.logger.Debug(fmt.Sprintf("testTaskManager.CreateTask taskID=%v, rangeID=%v", task.TaskID, rangeID))
		if task.TaskID <= 0 {
			panic(fmt.Errorf("Invalid taskID=%v", task.TaskID))
		}

		if tlm.rangeID != rangeID {
			m.logger.Debug(fmt.Sprintf("testTaskManager.CreateTask ConditionFailedError taskID=%v, rangeID: %v, db rangeID: %v",
				task.TaskID, rangeID, tlm.rangeID))

			return nil, &persistence.ConditionFailedError{
				Msg: fmt.Sprintf("testTaskManager.CreateTask failed. TaskList: %v, taskType: %v, rangeID: %v, db rangeID: %v",
					taskList, taskType, rangeID, tlm.rangeID),
			}
		}
		_, ok := tlm.tasks.Get(task.TaskID)
		if ok {
			panic(fmt.Sprintf("Duplicated TaskID %v", task.TaskID))
		}
	}

	// Then insert all tasks if no errors
	for _, task := range request.Tasks {
		scheduleID := task.Data.ScheduleID
		info := &persistence.TaskInfo{
			DomainID:   domainID,
			RunID:      *task.Execution.RunId,
			ScheduleID: scheduleID,
			TaskID:     task.TaskID,
			WorkflowID: *task.Execution.WorkflowId,
		}
		if task.Data.ScheduleToStartTimeout != 0 {
			info.Expiry = time.Now().Add(time.Duration(task.Data.ScheduleToStartTimeout) * time.Second)
		}
		tlm.tasks.Put(task.TaskID, info)
		tlm.createTaskCount++
	}

	return &persistence.CreateTasksResponse{}, nil
}

// GetTasks provides a mock function with given fields: request
func (m *testTaskManager) GetTasks(request *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
	m.logger.Debug(fmt.Sprintf("testTaskManager.GetTasks readLevel=%v, maxReadLevel=%v", request.ReadLevel, request.MaxReadLevel))

	tlm := m.getTaskListManager(newTestTaskListID(request.DomainID, request.TaskList, request.TaskType))
	tlm.Lock()
	defer tlm.Unlock()
	var tasks []*persistence.TaskInfo

	it := tlm.tasks.Iterator()
	for it.Next() {
		taskID := it.Key().(int64)
		if taskID <= request.ReadLevel {
			continue
		}
		if taskID > *request.MaxReadLevel {
			break
		}
		tasks = append(tasks, it.Value().(*persistence.TaskInfo))
	}
	return &persistence.GetTasksResponse{
		Tasks: tasks,
	}, nil
}

// getTaskCount returns number of tasks in a task list
func (m *testTaskManager) getTaskCount(taskList *taskListID) int {
	tlm := m.getTaskListManager(taskList)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.tasks.Size()
}

// getCreateTaskCount returns how many times CreateTask was called
func (m *testTaskManager) getCreateTaskCount(taskList *taskListID) int {
	tlm := m.getTaskListManager(taskList)
	tlm.Lock()
	defer tlm.Unlock()
	return tlm.createTaskCount
}

func (m *testTaskManager) String() string {
	m.Lock()
	defer m.Unlock()
	var result string
	for id, tl := range m.taskLists {
		tl.Lock()
		if id.taskType == persistence.TaskListTypeActivity {
			result += "Activity"
		} else {
			result += "Decision"
		}
		result += " task list " + id.name
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
	config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(100 * time.Millisecond)
	config.MaxTaskDeleteBatchSize = dynamicconfig.GetIntPropertyFilteredByTaskListInfo(1)
	return config
}
