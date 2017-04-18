package history

import (
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/uber-common/bark"
	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	transferQueueProcessorSuite struct {
		suite.Suite
		persistence.TestBase
		processor         *transferQueueProcessorImpl
		mockMatching      *mocks.MatchingClient
		mockVisibilityMgr *mocks.VisibilityManager
		logger            bark.Logger
	}
)

func TestTransferQueueProcessorSuite(t *testing.T) {
	s := new(transferQueueProcessorSuite)
	suite.Run(t, s)
}

func (s *transferQueueProcessorSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	logger := log.New()
	logger.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(logger)

	s.SetupWorkflowStore()
	s.mockMatching = &mocks.MatchingClient{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	cache := newHistoryCache(historyCacheMaxSize, s.ShardContext, s.logger)
	s.processor = newTransferQueueProcessor(s.ShardContext, s.mockVisibilityMgr, s.mockMatching, cache).(*transferQueueProcessorImpl)
}

func (s *transferQueueProcessorSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *transferQueueProcessorSuite) SetupTest() {
	// First cleanup transfer tasks from other tests and reset shard context
	s.ClearTransferQueue()
}

func (s *transferQueueProcessorSuite) TestSingleDecisionTask() {
	domainID := "b677a307-8261-40ea-b239-ab2ec78e443b"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("single-decisiontask-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}
	taskList := "single-decisiontask-queue"
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, taskList, "wType", 10, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	s.processor.processTransferTasks(tasksCh)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			s.mockMatching.On("AddDecisionTask", mock.Anything, createAddRequestFromTask(task)).Once().Return(nil)
			if task.ScheduleID == firstEventID+1 {
				s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", mock.Anything).Once().Return(nil)
			}
			s.processor.processTransferTask(task)
		default:
			break workerPump
		}
	}

	s.mockMatching.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
}

func (s *transferQueueProcessorSuite) TestManyTransferTasks() {
	domainID := "c867e7d6-0f0f-41df-a59c-1cd3eb1436f5"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("many-transfertasks-test"),
		RunId: common.StringPtr("57d5f005-bdaa-42a5-a1c5-b9c45d8699a9")}
	taskList := "many-transfertasks-queue"
	activityTaskScheduleIds := []int64{2, 3, 4, 5, 6}
	task0, err0 := s.CreateWorkflowExecutionManyTasks(domainID, workflowExecution, taskList, nil, 7, 0, nil,
		activityTaskScheduleIds)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	s.processor.processTransferTasks(tasksCh)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			s.mockMatching.On("AddActivityTask", mock.Anything, createAddRequestFromTask(task)).Once().Return(nil)
			s.processor.processTransferTask(task)
		default:
			break workerPump
		}
	}

	s.mockMatching.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
}

func (s *transferQueueProcessorSuite) TestDeleteExecutionTransferTasks() {
	domainID := "f5f1ece7-000d-495d-81c3-918ac29006ed"
	workflowID := "delete-execution-transfertasks-test"
	runID := "79fc8984-f78f-41cf-8fa1-4d383edb2cfd"
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	taskList := "delete-execution-transfertasks-queue"
	identity := "delete-execution-transfertasks-test"
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, taskList, "wType", 10, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	builder := newMutableStateBuilder(s.logger)
	info1, _ := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	builder.Load(info1)
	startedEvent := addDecisionTaskStartedEvent(builder, int64(2), taskList, identity)
	completeDecisionEvent := addDecisionTaskCompletedEvent(builder, int64(2), startedEvent.GetEventId(), nil, identity)
	addCompleteWorkflowEvent(builder, completeDecisionEvent.GetEventId(), []byte("result"))

	updatedInfo1 := copyWorkflowExecutionInfo(builder.executionInfo)
	err1 := s.UpdateWorkflowExecutionAndDelete(updatedInfo1, int64(3))
	s.Nil(err1, "No error expected.")

	newExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("delete-execution-transfertasks-test"),
		RunId: common.StringPtr("d3ac892e-9fc1-4def-84fa-bfc44b9128cc")}
	_, err2 := s.CreateWorkflowExecution(domainID, newExecution, taskList, "wType", 10, nil, 3, 0, 2, nil)
	s.NotNil(err2, "Entity exist error expected.")
	s.logger.Infof("Error creating new execution: %v", err2)

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	s.processor.processTransferTasks(tasksCh)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			if task.TaskType == persistence.TransferTaskTypeDecisionTask {
				s.mockMatching.On("AddDecisionTask", mock.Anything, createAddRequestFromTask(task)).Once().Return(nil)
				if task.ScheduleID == firstEventID+1 {
					s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", mock.Anything).Once().Return(nil)
				}
			} else if task.TaskType == persistence.TransferTaskTypeDeleteExecution {
				s.mockVisibilityMgr.On("RecordWorkflowExecutionClosed", mock.Anything).Once().Return(nil)
			}
			s.processor.processTransferTask(task)
		default:
			break workerPump
		}
	}

	_, err3 := s.CreateWorkflowExecution(domainID, newExecution, taskList, "wType", 10, nil, 3, 0, 2, nil)
	s.Nil(err3, "No error expected.")
	s.logger.Infof("Execution created successfully: %v", err3)
	s.mockMatching.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
}

func createAddRequestFromTask(task *persistence.TransferTaskInfo) interface{} {
	var res interface{}
	domainID := task.DomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}
	taskList := &workflow.TaskList{
		Name: &task.TaskList,
	}
	if task.TaskType == persistence.TransferTaskTypeActivityTask {
		res = &m.AddActivityTaskRequest{
			DomainUUID:       common.StringPtr(domainID),
			SourceDomainUUID: common.StringPtr(domainID),
			Execution:        &execution,
			TaskList:         taskList,
			ScheduleId:       &task.ScheduleID,
		}
	} else if task.TaskType == persistence.TransferTaskTypeDecisionTask {
		res = &m.AddDecisionTaskRequest{
			DomainUUID: common.StringPtr(domainID),
			Execution:  &execution,
			TaskList:   taskList,
			ScheduleId: &task.ScheduleID,
		}
	}
	return res
}

func containsID(list []int64, scheduleID int64) bool {
	for _, id := range list {
		if id == scheduleID {
			return true
		}
	}

	return false
}
