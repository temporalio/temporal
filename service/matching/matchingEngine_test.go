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
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gohistory "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"

	"github.com/davecgh/go-spew/spew"
	"github.com/emirpasic/gods/maps/treemap"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/cache"
)

type (
	matchingEngineSuite struct {
		suite.Suite
		historyClient        *mocks.HistoryClient
		matchingEngine       *matchingEngineImpl
		taskManager          *testTaskManager
		mockExecutionManager *mocks.ExecutionManager
		logger               bark.Logger
		callContext          context.Context
		domainCache          *cache.DomainCacheMock
		sync.Mutex
	}
)

func TestMatchingEngineSuite(t *testing.T) {
	s := new(matchingEngineSuite)
	suite.Run(t, s)
}

func (s *matchingEngineSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
	l := log.New()
	//l.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(l)
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
	fmt.Fprintf(w, fmt.Sprintf("%v\n", s.taskManager))
	fmt.Fprintf(w, fmt.Sprintf("%v\n", s.matchingEngine))
}

func (s *matchingEngineSuite) TearDownSuite() {
}

func (s *matchingEngineSuite) SetupTest() {
	s.Lock()
	defer s.Unlock()
	s.mockExecutionManager = &mocks.ExecutionManager{}
	s.historyClient = &mocks.HistoryClient{}
	s.taskManager = newTestTaskManager(s.logger)
	s.domainCache = &cache.DomainCacheMock{}
	s.domainCache.On("GetDomainByID", mock.Anything).Return(cache.CreateDomainCacheEntry("domainName"), nil)

	s.matchingEngine = s.newMatchingEngine(defaultTestConfig(), s.taskManager)
	s.matchingEngine.Start()
}

func (s *matchingEngineSuite) newMatchingEngine(
	config *Config, taskMgr persistence.TaskManager,
) *matchingEngineImpl {
	return newMatchingEngine(config, taskMgr, s.historyClient, s.logger, s.domainCache)
}

func newMatchingEngine(
	config *Config, taskMgr persistence.TaskManager, historyClient history.Client,
	logger bark.Logger, domainCache cache.DomainCache,
) *matchingEngineImpl {
	return &matchingEngineImpl{
		taskManager:     taskMgr,
		historyService:  historyClient,
		taskLists:       make(map[taskListID]taskListManager),
		logger:          logger,
		metricsClient:   metrics.NewClient(tally.NoopScope, metrics.Matching),
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
		config:          config,
		domainCache:     domainCache,
	}
}

func (s *matchingEngineSuite) TearDownTest() {
	s.mockExecutionManager.AssertExpectations(s.T())
	s.matchingEngine.Stop()
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
	s.PollForTasksEmptyResultTest(persistence.TaskListTypeActivity)
}

func (s *matchingEngineSuite) TestPollForDecisionTasksEmptyResult() {
	s.PollForTasksEmptyResultTest(persistence.TaskListTypeDecision)
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
	s.historyClient.On("RecordDecisionTaskStarted", mock.Anything,
		mock.AnythingOfType("*history.RecordDecisionTaskStartedRequest")).Return(
		func(ctx context.Context, taskRequest *gohistory.RecordDecisionTaskStartedRequest) *gohistory.RecordDecisionTaskStartedResponse {
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
			return response
		}, nil)

	addRequest := matching.AddDecisionTaskRequest{
		DomainUUID:                    common.StringPtr(domainID),
		Execution:                     &execution,
		ScheduleId:                    &scheduleID,
		TaskList:                      stickyTaskList,
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
	}

	_, err := s.matchingEngine.AddDecisionTask(&addRequest)
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

func (s *matchingEngineSuite) PollForTasksEmptyResultTest(taskType int) {
	s.matchingEngine.config.RangeSize = 2 // to test that range is not updated without tasks
	s.matchingEngine.config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(10 * time.Millisecond)

	domainID := "domainId"
	tl := "makeToast"
	identity := "selfDrivingToaster"

	taskList := &workflow.TaskList{}
	taskList.Name = &tl
	var taskListType workflow.TaskListType
	tlID := newTaskListID(domainID, tl, taskType)
	//const rangeID = 123
	const pollCount = 10
	for i := 0; i < pollCount; i++ {
		if taskType == persistence.TaskListTypeActivity {
			pollResp, err := s.matchingEngine.PollForActivityTask(s.callContext, &matching.PollForActivityTaskRequest{
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
			resp, err := s.matchingEngine.PollForDecisionTask(s.callContext, &matching.PollForDecisionTaskRequest{
				DomainUUID: common.StringPtr(domainID),
				PollRequest: &workflow.PollForDecisionTaskRequest{
					TaskList: taskList,
					Identity: &identity},
			})
			s.NoError(err)
			s.Equal(emptyPollForDecisionTaskResponse, resp)

			taskListType = workflow.TaskListTypeDecision
		}
		// check the poller information
		descResp, err := s.matchingEngine.DescribeTaskList(s.callContext, &matching.DescribeTaskListRequest{
			DomainUUID: common.StringPtr(domainID),
			DescRequest: &workflow.DescribeTaskListRequest{
				TaskList:     taskList,
				TaskListType: &taskListType,
			},
		})
		s.NoError(err)
		s.Equal(1, len(descResp.Pollers))
		s.Equal(identity, descResp.Pollers[0].GetIdentity())
		s.NotEmpty(descResp.Pollers[0].GetLastAccessTime())
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

			_, err = s.matchingEngine.AddActivityTask(&addRequest)
		} else {
			addRequest := matching.AddDecisionTaskRequest{
				DomainUUID:                    common.StringPtr(domainID),
				Execution:                     &execution,
				ScheduleId:                    &scheduleID,
				TaskList:                      taskList,
				ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			}

			_, err = s.matchingEngine.AddDecisionTask(&addRequest)
		}
		s.NoError(err)
	}
	s.EqualValues(taskCount, s.taskManager.getTaskCount(&taskListID{
		domainID:     domainID,
		taskListName: tl,
		taskType:     taskType,
	}))

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

	tlID := &taskListID{
		domainID:     domainID,
		taskListName: tl,
		taskType:     persistence.TaskListTypeActivity,
	}
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
	_, err = s.matchingEngine.AddActivityTask(&addRequest)
	s.Error(err)

	// test race
	tlmImpl.taskWriter.stopped = 0
	_, err = s.matchingEngine.AddActivityTask(&addRequest)
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
	tlID := &taskListID{domainID: domainID, taskListName: tl, taskType: persistence.TaskListTypeActivity}
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

		_, err := s.matchingEngine.AddActivityTask(&addRequest)
		s.NoError(err)
	}
	s.EqualValues(taskCount, s.taskManager.getTaskCount(tlID))

	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")

	identity := "nobody"

	// History service is using mock
	s.historyClient.On("RecordActivityTaskStarted", mock.Anything,
		mock.AnythingOfType("*history.RecordActivityTaskStartedRequest")).Return(
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) *gohistory.RecordActivityTaskStartedResponse {
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
			return resp
		}, nil)

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
			s.logger.Debugf("empty poll returned")
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
	tlID := &taskListID{domainID: domainID, taskListName: tl, taskType: persistence.TaskListTypeActivity}
	tlKind := common.TaskListKindPtr(workflow.TaskListKindNormal)
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test
	// So we can get snapshots
	scope := tally.NewTestScope("test", nil)
	s.matchingEngine.metricsClient = metrics.NewClient(scope, metrics.Matching)

	dispatchTTL := time.Nanosecond
	dPtr := _defaultTaskDispatchRPS
	tlConfig, err := newTaskListConfig(tlID, s.matchingEngine.config, s.domainCache)
	s.NoError(err)
	mgr := newTaskListManagerWithRateLimiter(
		s.matchingEngine, tlID, tlKind, s.domainCache, tlConfig,
		newRateLimiter(&dPtr, dispatchTTL, _minBurst),
	)
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
	s.historyClient.On("RecordActivityTaskStarted", mock.Anything,
		mock.AnythingOfType("*history.RecordActivityTaskStartedRequest")).Return(
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) *gohistory.RecordActivityTaskStartedResponse {
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
			}
		}, nil)

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
			result, pollErr = s.matchingEngine.PollForActivityTask(s.callContext, &matching.PollForActivityTaskRequest{
				DomainUUID: common.StringPtr(domainID),
				PollRequest: &workflow.PollForActivityTaskRequest{
					TaskList:         taskList,
					Identity:         &identity,
					TaskListMetadata: &workflow.TaskListMetadata{MaxTasksPerSecond: &maxDispatch},
				},
			})
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
		_, err := s.matchingEngine.AddActivityTask(&addRequest)
		wg.Wait()
		s.NoError(err)
		s.NoError(pollErr)
		s.NotNil(result)
		if len(result.TaskToken) == 0 {
			s.logger.Debugf("empty poll returned")
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

		taskToken, _ := s.matchingEngine.tokenSerializer.Serialize(token)
		//s.EqualValues(scheduleID, result.TaskToken)

		s.EqualValues(string(taskToken), string(result.TaskToken))
	}

	time.Sleep(20 * time.Millisecond) // So any buffer tasks from 0 rps get picked up
	syncCtr := scope.Snapshot().Counters()["test.sync.throttle.count+operation=TaskListMgr"]
	s.Equal(1, int(syncCtr.Value()))                         // Check times zero rps is set = throttle counter
	s.EqualValues(1, s.taskManager.getCreateTaskCount(tlID)) // Check times zero rps is set = Tasks stored in persistence
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
	expectedRange := int64(initialRangeID + taskCount/rangeSize)
	if taskCount%rangeSize > 0 {
		expectedRange++
	}
	// Due to conflicts some ids are skipped and more real ranges are used.
	s.True(expectedRange <= s.taskManager.getTaskListManager(tlID).rangeID)
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
	s.logger.Infof("Number of tasks throttled: %d", throttleCt)
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
	tlID := &taskListID{domainID: domainID, taskListName: tl, taskType: persistence.TaskListTypeActivity}
	tlKind := common.TaskListKindPtr(workflow.TaskListKindNormal)
	dispatchTTL := time.Nanosecond
	s.matchingEngine.config.RangeSize = rangeSize // override to low number for the test
	dPtr := _defaultTaskDispatchRPS
	tlConfig, err := newTaskListConfig(tlID, s.matchingEngine.config, s.domainCache)
	s.NoError(err)
	mgr := newTaskListManagerWithRateLimiter(
		s.matchingEngine, tlID, tlKind, s.domainCache, tlConfig,
		newRateLimiter(&dPtr, dispatchTTL, _minBurst),
	)
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

				_, err := s.matchingEngine.AddActivityTask(&addRequest)
				if err != nil {
					s.logger.Infof("Failure in AddActivityTask: %v", err)
					i--
				}
			}
		}()
	}

	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")

	identity := "nobody"

	// History service is using mock
	s.historyClient.On("RecordActivityTaskStarted", mock.Anything,
		mock.AnythingOfType("*history.RecordActivityTaskStartedRequest")).Return(
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) *gohistory.RecordActivityTaskStartedResponse {
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
			}
		}, nil)

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
					s.logger.Debugf("empty poll returned")
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

	syncCtr := scope.Snapshot().Counters()["test.sync.throttle.count+operation=TaskListMgr"]
	bufCtr := scope.Snapshot().Counters()["test.buffer.throttle.count+operation=TaskListMgr"]
	return syncCtr.Value() + bufCtr.Value()
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
	tlID := &taskListID{domainID: domainID, taskListName: tl, taskType: persistence.TaskListTypeDecision}
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

				_, err := s.matchingEngine.AddDecisionTask(&addRequest)
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
	s.historyClient.On("RecordDecisionTaskStarted", mock.Anything,
		mock.AnythingOfType("*history.RecordDecisionTaskStartedRequest")).Return(
		func(ctx context.Context, taskRequest *gohistory.RecordDecisionTaskStartedRequest) *gohistory.RecordDecisionTaskStartedResponse {
			s.logger.Debug("Mock Received RecordDecisionTaskStartedRequest")
			return &gohistory.RecordDecisionTaskStartedResponse{
				PreviousStartedEventId: &startedEventID,
				StartedEventId:         &startedEventID,
				ScheduledEventId:       &scheduleID,
				WorkflowType:           workflowType,
			}
		}, nil)
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
					s.logger.Debugf("empty poll returned")
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
	tlID := &taskListID{domainID: domainID, taskListName: tl, taskType: persistence.TaskListTypeActivity}
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
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
				}

				_, err := engine.AddActivityTask(&addRequest)
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
	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")

	identity := "nobody"

	startedTasks := make(map[int64]bool)

	// History service is using mock
	s.historyClient.On("RecordActivityTaskStarted", mock.Anything,
		mock.AnythingOfType("*history.RecordActivityTaskStartedRequest")).Return(
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) *gohistory.RecordActivityTaskStartedResponse {
			if _, ok := startedTasks[*taskRequest.TaskId]; ok {
				return nil
			}
			s.logger.Debugf("Mock Received RecordActivityTaskStartedRequest for taskID=%v", taskRequest.TaskId)

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
			}
		},
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) error {
			if _, ok := startedTasks[*taskRequest.TaskId]; ok {
				s.logger.Debugf("From error function Mock Received DUPLICATED RecordActivityTaskStartedRequest for taskID=%v", taskRequest.TaskId)
				return &workflow.EntityNotExistsError{Message: "already started"}
			}
			startedTasks[*taskRequest.TaskId] = true
			return nil
		})
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
					s.logger.Debugf("empty poll returned")
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
	tlID := &taskListID{domainID: domainID, taskListName: tl, taskType: persistence.TaskListTypeDecision}
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
					ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
				}

				_, err := engine.AddDecisionTask(&addRequest)
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
	s.historyClient.On("RecordDecisionTaskStarted", mock.Anything,
		mock.AnythingOfType("*history.RecordDecisionTaskStartedRequest")).Return(
		func(ctx context.Context, taskRequest *gohistory.RecordDecisionTaskStartedRequest) *gohistory.RecordDecisionTaskStartedResponse {
			if _, ok := startedTasks[*taskRequest.TaskId]; ok {
				return nil
			}
			s.logger.Debugf("Mock Received RecordDecisionTaskStartedRequest for taskID=%v", taskRequest.TaskId)
			s.logger.Debug("Mock Received RecordDecisionTaskStartedRequest")
			return &gohistory.RecordDecisionTaskStartedResponse{
				PreviousStartedEventId: &startedEventID,
				StartedEventId:         &startedEventID,
				ScheduledEventId:       &scheduleID,
				WorkflowType:           workflowType,
			}
		},
		func(ctx context.Context, taskRequest *gohistory.RecordDecisionTaskStartedRequest) error {
			if _, ok := startedTasks[*taskRequest.TaskId]; ok {
				s.logger.Debugf("From error function Mock Received DUPLICATED RecordDecisionTaskStartedRequest for taskID=%v", taskRequest.TaskId)
				return &gohistory.EventAlreadyStartedError{Message: "already started"}
			}
			startedTasks[*taskRequest.TaskId] = true
			return nil
		})
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
					s.logger.Debugf("empty poll returned")
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
	tlID := &taskListID{domainID: domainID, taskListName: tl, taskType: persistence.TaskListTypeActivity}
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

	_, err := s.matchingEngine.AddActivityTask(&addRequest)
	s.NoError(err)
	s.EqualValues(1, s.taskManager.getTaskCount(tlID))

	ctx, err := s.matchingEngine.getTask(context.Background(), tlID, nil, tlKind)
	s.NoError(err)

	ctx.completeTask(errors.New("test error"))
	s.EqualValues(1, s.taskManager.getTaskCount(tlID))
	ctx2, err := s.matchingEngine.getTask(context.Background(), tlID, nil, tlKind)
	s.NoError(err)

	s.NotEqual(ctx.info.TaskID, ctx2.info.TaskID)
	s.Equal(ctx.info.WorkflowID, ctx2.info.WorkflowID)
	s.Equal(ctx.info.RunID, ctx2.info.RunID)
	s.Equal(ctx.info.ScheduleID, ctx2.info.ScheduleID)

	ctx2.completeTask(nil)
	s.EqualValues(0, s.taskManager.getTaskCount(tlID))
}

func (s *matchingEngineSuite) TestTaskListManagerGetTaskBatch() {
	runID := "run1"
	workflowID := "workflow1"
	workflowExecution := workflow.WorkflowExecution{RunId: &runID, WorkflowId: &workflowID}

	domainID := "domainId"
	tl := "makeToast"
	tlID := &taskListID{domainID: domainID, taskListName: tl, taskType: persistence.TaskListTypeActivity}

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

		_, err := s.matchingEngine.AddActivityTask(&addRequest)
		s.NoError(err)
	}
	tlMgr, ok := s.matchingEngine.taskLists[*tlID].(*taskListManagerImpl)
	s.True(ok, "taskListManger doesn't implement taskListManager interface")
	s.EqualValues(taskCount, s.taskManager.getTaskCount(tlID))

	// stop getTasksPump and taskWriter
	if !atomic.CompareAndSwapInt32(&tlMgr.stopped, 0, 1) {
		return
	}
	close(tlMgr.shutdownCh)
	tlMgr.taskWriter.Stop()

	// setReadLevel should NEVER be called without updating ackManager.outstandingTasks
	// This is only for unit test purpose
	tlMgr.taskAckManager.setReadLevel(tlMgr.taskWriter.GetMaxReadLevel())
	tasks, readLevel, isReadBatchDone, err := tlMgr.getTaskBatch()
	s.Nil(err)
	s.EqualValues(0, len(tasks))
	s.EqualValues(tlMgr.taskWriter.GetMaxReadLevel(), readLevel)
	s.True(isReadBatchDone)

	tlMgr.taskAckManager.setReadLevel(0)
	tasks, readLevel, isReadBatchDone, err = tlMgr.getTaskBatch()
	s.Nil(err)
	s.EqualValues(rangeSize, len(tasks))
	s.EqualValues(rangeSize, readLevel)
	s.True(isReadBatchDone)

	activityTypeName := "activity1"
	activityID := "activityId1"
	activityType := &workflow.ActivityType{Name: &activityTypeName}
	activityInput := []byte("Activity1 Input")
	identity := "nobody"

	// History service is using mock
	s.historyClient.On("RecordActivityTaskStarted", mock.Anything,
		mock.AnythingOfType("*history.RecordActivityTaskStartedRequest")).Return(
		func(ctx context.Context, taskRequest *gohistory.RecordActivityTaskStartedRequest) *gohistory.RecordActivityTaskStartedResponse {
			s.logger.Debug("Mock Received RecordActivityTaskStartedRequest")
			return &gohistory.RecordActivityTaskStartedResponse{
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
		}, nil)

	time.Sleep(time.Second)
	// complete rangeSize events
	for i := int64(0); i < rangeSize; i++ {
		result, err := s.matchingEngine.PollForActivityTask(s.callContext, &matching.PollForActivityTaskRequest{
			DomainUUID: common.StringPtr(domainID),
			PollRequest: &workflow.PollForActivityTaskRequest{
				TaskList: taskList,
				Identity: &identity},
		})

		s.NoError(err)
		s.NotNil(result)
		if len(result.TaskToken) == 0 {
			s.logger.Debugf("empty poll returned")
			continue
		}
	}
	s.EqualValues(taskCount-rangeSize, s.taskManager.getTaskCount(tlID))
	tasks, readLevel, isReadBatchDone, err = tlMgr.getTaskBatch()
	s.Nil(err)
	s.True(0 < len(tasks) && len(tasks) <= rangeSize)
	s.EqualValues(rangeSize*2, readLevel)
	s.True(isReadBatchDone)

	tlMgr.engine.removeTaskListManager(tlMgr.taskListID)
}

func (s *matchingEngineSuite) TestTaskListManagerGetTaskBatch_ReadBatchDone() {
	domainID := "domainId"
	tl := "makeToast"
	tlID := &taskListID{domainID: domainID, taskListName: tl, taskType: persistence.TaskListTypeActivity}
	tlNormal := workflow.TaskListKindNormal

	const rangeSize = 10
	const maxReadLevel = int64(120)
	config := defaultTestConfig()
	config.RangeSize = rangeSize
	tlMgr0, err := newTaskListManager(s.matchingEngine, tlID, &tlNormal, config)
	s.NoError(err)
	tlMgr, ok := tlMgr0.(*taskListManagerImpl)
	s.True(ok, "taskListManger doesn't implement taskListManager interface")

	tlMgr.taskAckManager.setReadLevel(0)
	atomic.StoreInt64(&tlMgr.taskWriter.maxReadLevel, maxReadLevel)
	tasks, readLevel, isReadBatchDone, err := tlMgr.getTaskBatch()
	s.Empty(tasks)
	s.Equal(int64(rangeSize*10), readLevel)
	s.False(isReadBatchDone)
	s.NoError(err)

	tlMgr.taskAckManager.setReadLevel(readLevel)
	tasks, readLevel, isReadBatchDone, err = tlMgr.getTaskBatch()
	s.Empty(tasks)
	s.Equal(maxReadLevel, readLevel)
	s.True(isReadBatchDone)
	s.NoError(err)
}

func newActivityTaskScheduledEvent(eventID int64, decisionTaskCompletedEventID int64,
	scheduleAttributes *workflow.ScheduleActivityTaskDecisionAttributes) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventTypeActivityTaskScheduled)
	attributes := &workflow.ActivityTaskScheduledEventAttributes{}
	attributes.ActivityId = scheduleAttributes.ActivityId
	attributes.ActivityType = scheduleAttributes.ActivityType
	attributes.TaskList = scheduleAttributes.TaskList
	attributes.Input = scheduleAttributes.Input
	attributes.ScheduleToCloseTimeoutSeconds = common.Int32Ptr(*scheduleAttributes.ScheduleToCloseTimeoutSeconds)
	attributes.ScheduleToStartTimeoutSeconds = common.Int32Ptr(*scheduleAttributes.ScheduleToStartTimeoutSeconds)
	attributes.StartToCloseTimeoutSeconds = common.Int32Ptr(*scheduleAttributes.StartToCloseTimeoutSeconds)
	attributes.HeartbeatTimeoutSeconds = common.Int32Ptr(*scheduleAttributes.HeartbeatTimeoutSeconds)
	attributes.DecisionTaskCompletedEventId = common.Int64Ptr(decisionTaskCompletedEventID)
	historyEvent.ActivityTaskScheduledEventAttributes = attributes

	return historyEvent
}

func newActivityTaskStartedEvent(eventID, scheduledEventID int64,
	request *workflow.PollForActivityTaskRequest) *workflow.HistoryEvent {
	historyEvent := newHistoryEvent(eventID, workflow.EventTypeActivityTaskStarted)
	attributes := &workflow.ActivityTaskStartedEventAttributes{}
	attributes.ScheduledEventId = common.Int64Ptr(scheduledEventID)
	attributes.Identity = common.StringPtr(*request.Identity)
	historyEvent.ActivityTaskStartedEventAttributes = attributes

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
	logger    bark.Logger
}

func newTestTaskManager(logger bark.Logger) *testTaskManager {
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

// LeaseTaskList provides a mock function with given fields: request
func (m *testTaskManager) LeaseTaskList(request *persistence.LeaseTaskListRequest) (*persistence.LeaseTaskListResponse, error) {
	tlm := m.getTaskListManager(newTaskListID(request.DomainID, request.TaskList, request.TaskType))
	tlm.Lock()
	defer tlm.Unlock()
	tlm.rangeID++
	m.logger.Debugf("LeaseTaskList rangeID=%v", tlm.rangeID)

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
	m.logger.Debugf("UpdateTaskList taskListInfo=%v, ackLevel=%v", request.TaskListInfo, request.TaskListInfo.AckLevel)

	tli := request.TaskListInfo
	tlm := m.getTaskListManager(newTaskListID(tli.DomainID, tli.Name, tli.TaskType))

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
	m.logger.Debugf("CompleteTask taskID=%v, ackLevel=%v", request.TaskID, request.TaskList.AckLevel)
	if request.TaskID <= 0 {
		panic(fmt.Errorf("Invalid taskID=%v", request.TaskID))
	}

	tli := request.TaskList
	tlm := m.getTaskListManager(newTaskListID(tli.DomainID, tli.Name, tli.TaskType))

	tlm.Lock()
	defer tlm.Unlock()

	tlm.tasks.Remove(request.TaskID)
	return nil
}

func (m *testTaskManager) CompleteTasksLessThan(request *persistence.CompleteTasksLessThanRequest) (int, error) {
	tlm := m.getTaskListManager(newTaskListID(request.DomainID, request.TaskListName, request.TaskType))
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
	key := newTaskListID(request.DomainID, request.TaskListName, request.TaskListType)
	delete(m.taskLists, *key)
	return nil
}

// CreateTask provides a mock function with given fields: request
func (m *testTaskManager) CreateTasks(request *persistence.CreateTasksRequest) (*persistence.CreateTasksResponse, error) {
	domainID := request.TaskListInfo.DomainID
	taskList := request.TaskListInfo.Name
	taskType := request.TaskListInfo.TaskType
	rangeID := request.TaskListInfo.RangeID

	tlm := m.getTaskListManager(newTaskListID(domainID, taskList, taskType))
	tlm.Lock()
	defer tlm.Unlock()

	// First validate the entire batch
	for _, task := range request.Tasks {
		m.logger.Debugf("testTaskManager.CreateTask taskID=%v, rangeID=%v", task.TaskID, rangeID)
		if task.TaskID <= 0 {
			panic(fmt.Errorf("Invalid taskID=%v", task.TaskID))
		}

		if tlm.rangeID != rangeID {
			m.logger.Debugf("testTaskManager.CreateTask ConditionFailedError taskID=%v, rangeID: %v, db rangeID: %v",
				task.TaskID, rangeID, tlm.rangeID)

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
		tlm.tasks.Put(task.TaskID, &persistence.TaskInfo{
			DomainID:   domainID,
			RunID:      *task.Execution.RunId,
			ScheduleID: scheduleID,
			TaskID:     task.TaskID,
			WorkflowID: *task.Execution.WorkflowId,
		})
		tlm.createTaskCount++
	}

	return &persistence.CreateTasksResponse{}, nil
}

// GetTasks provides a mock function with given fields: request
func (m *testTaskManager) GetTasks(request *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
	m.logger.Debugf("testTaskManager.GetTasks readLevel=%v, maxReadLevel=%v", request.ReadLevel, request.MaxReadLevel)

	tlm := m.getTaskListManager(newTaskListID(request.DomainID, request.TaskList, request.TaskType))
	tlm.Lock()
	defer tlm.Unlock()
	if tlm.rangeID != request.RangeID {
		return nil, &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("testTaskManager.GetTasks failed. TaskList: %v, taskType: %v, rangeID: %v, db rangeID: %v",
				request.TaskList, request.TaskType, request.RangeID, tlm.rangeID),
		}
	}
	var tasks []*persistence.TaskInfo

	it := tlm.tasks.Iterator()
	for it.Next() {
		taskID := it.Key().(int64)
		if taskID <= request.ReadLevel {
			continue
		}
		if taskID > request.MaxReadLevel {
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
		result += " task list " + id.taskListName
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
		log.Infof("Current time: %v, Application time: %v, Difference: %v", currentTime, t, diff)
		return false
	}
	return true
}

func defaultTestConfig() *Config {
	config := NewConfig(dynamicconfig.NewNopCollection())
	config.LongPollExpirationInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskListInfo(100 * time.Millisecond)
	return config
}
