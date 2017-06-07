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
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	transferQueueProcessorSuite struct {
		suite.Suite
		persistence.TestBase
		processor         *transferQueueProcessorImpl
		mockMatching      *mocks.MatchingClient
		mockHistoryClient *mocks.HistoryClient
		mockMetadataMgr   *mocks.MetadataManager
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
	s.mockHistoryClient = &mocks.HistoryClient{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	historyCache := newHistoryCache(historyCacheMaxSize, s.ShardContext, s.logger)
	domainCache := cache.NewDomainCache(s.mockMetadataMgr, s.logger)
	s.processor = newTransferQueueProcessor(s.ShardContext, s.mockVisibilityMgr, s.mockMatching, s.mockHistoryClient, historyCache, domainCache).(*transferQueueProcessorImpl)
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
			s.mockMatching.On("AddDecisionTask", mock.Anything, createAddRequestFromTask(task, 0)).Once().Return(nil)
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
	activityCount := 5
	timeoutSeconds := int32(10)
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, taskList, "wType", 10, nil, 2, 0, 1, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")
	s.mockMatching.On("AddDecisionTask", mock.Anything, mock.Anything).Once().Return(nil)

	builder := newMutableStateBuilder(s.logger)
	info, _ := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	builder.Load(info)
	addDecisionTaskStartedEvent(builder, int64(1), taskList, "identity")
	addDecisionTaskCompletedEvent(builder, int64(1), int64(2), nil, "identity")

	transferTasks := []persistence.Task{}
	for i := 0; i < activityCount; i++ {
		_, ai := addActivityTaskScheduledEvent(builder, int64(3), "activityID", "aType", taskList, nil, timeoutSeconds, timeoutSeconds, timeoutSeconds)
		transferTasks = append(transferTasks,
			&persistence.ActivityTask{
				TaskID:     s.GetNextSequenceNumber(),
				DomainID:   domainID,
				TaskList:   taskList,
				ScheduleID: int64(ai.ScheduleID),
			})
		s.Equal(ai.ScheduleToCloseTimeout, timeoutSeconds)
	}
	updatedInfo := copyWorkflowExecutionInfo(builder.executionInfo)
	err1 := s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo, int64(2), transferTasks, builder.updateActivityInfos)
	s.Nil(err1)

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	s.processor.processTransferTasks(tasksCh)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			if task.TaskType == persistence.TransferTaskTypeActivityTask {
				s.mockMatching.On("AddActivityTask", mock.Anything, createAddRequestFromTask(task, timeoutSeconds)).Once().Return(nil)
			}
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

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	s.processor.processTransferTasks(tasksCh)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			if task.TaskType == persistence.TransferTaskTypeDecisionTask {
				s.mockMatching.On("AddDecisionTask", mock.Anything, createAddRequestFromTask(task, 0)).Once().Return(nil)
				if task.ScheduleID == firstEventID+1 {
					s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", mock.Anything).Once().Return(nil)
				}
			} else if task.TaskType == persistence.TransferTaskTypeDeleteExecution {
				s.mockMetadataMgr.On("GetDomain", mock.Anything).Once().Return(&persistence.GetDomainResponse{
					Config: &persistence.DomainConfig{
						Retention: 3600,
					},
				}, nil)
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

func (s *transferQueueProcessorSuite) TestDeleteExecutionTransferTasksDomainNotExist() {
	domainID := "1399c0d5-f119-42d3-bd03-bedb6cf96e46"
	workflowID := "delete-execution-transfertasks-domain-test"
	runID := "623525ab-da2b-4715-8756-2d6263d81524"
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	taskList := "delete-execution-transfertasks-domain-queue"
	identity := "delete-execution-transfertasks-domain-test"
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

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	s.processor.processTransferTasks(tasksCh)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			if task.TaskType == persistence.TransferTaskTypeDecisionTask {
				s.mockMatching.On("AddDecisionTask", mock.Anything, createAddRequestFromTask(task, 0)).Once().Return(nil)
				if task.ScheduleID == firstEventID+1 {
					s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", mock.Anything).Once().Return(nil)
				}
			} else if task.TaskType == persistence.TransferTaskTypeDeleteExecution {
				s.mockMetadataMgr.On("GetDomain", mock.Anything).Once().Return(nil, &workflow.EntityNotExistsError{})
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

func (s *transferQueueProcessorSuite) TestCancelRemoteExecutionTransferTasks() {
	domainID := "f5f1ece7-000d-495d-81c3-918ac29006ed"
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("cancel-transfer-test"),
		RunId:      common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}
	taskList := "cancel-transfer-queue"
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, taskList, "wType", 10, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	builder := newMutableStateBuilder(s.logger)
	info, _ := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	builder.Load(info)
	addDecisionTaskStartedEvent(builder, int64(2), taskList, "identity")

	transferTasks := []persistence.Task{&persistence.CancelExecutionTask{
		TaskID:           s.GetNextSequenceNumber(),
		TargetDomainID:   "f2bfaab6-7e8b-4fac-9a62-17da8d37becb",
		TargetWorkflowID: "target-workflow_id",
		TargetRunID:      "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6",
		ScheduleID:       1,
	}}
	updatedInfo := copyWorkflowExecutionInfo(builder.executionInfo)
	err1 := s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo, int64(3), transferTasks, nil)
	s.Nil(err1, "No error expected.")

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	s.processor.processTransferTasks(tasksCh)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			s.logger.Infof("Processing transfer task type: %v", task.TaskType)
			if task.TaskType == persistence.TransferTaskTypeDecisionTask {
				s.mockMatching.On("AddDecisionTask", mock.Anything, createAddRequestFromTask(task, 0)).Once().Return(nil)
				if task.ScheduleID == firstEventID+1 {
					s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", mock.Anything).Once().Return(nil)
				}
			} else if task.TaskType == persistence.TransferTaskTypeCancelExecution {
				s.mockHistoryClient.On("RequestCancelWorkflowExecution", mock.Anything, mock.Anything).Return(nil).Once()
			}
			s.processor.processTransferTask(task)
		default:
			break workerPump
		}
	}

	s.mockMatching.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockHistoryClient.AssertExpectations(s.T())
}

func (s *transferQueueProcessorSuite) TestCancelRemoteExecutionTransferTask_RequestFail() {
	domainID := "f5f1ece7-000d-495d-81c3-918ac29006ed"
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("cancel-transfer-fail-test"),
		RunId:      common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}
	taskList := "cancel-transfer-fail-queue"
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, taskList, "wType", 10, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	builder := newMutableStateBuilder(s.logger)
	info, _ := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	builder.Load(info)
	addDecisionTaskStartedEvent(builder, int64(2), taskList, "identity")

	transferTasks := []persistence.Task{&persistence.CancelExecutionTask{
		TaskID:           s.GetNextSequenceNumber(),
		TargetDomainID:   "f2bfaab6-7e8b-4fac-9a62-17da8d37becb",
		TargetWorkflowID: "target-workflow_id",
		TargetRunID:      "0d00698f-08e1-4d36-a3e2-3bf109f5d2d6",
		ScheduleID:       1,
	}}
	updatedInfo := copyWorkflowExecutionInfo(builder.executionInfo)
	err1 := s.UpdateWorkflowExecutionWithTransferTasks(updatedInfo, int64(3), transferTasks, nil)
	s.Nil(err1, "No error expected.")

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	s.processor.processTransferTasks(tasksCh)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			s.logger.Infof("Processing transfer task type: %v", task.TaskType)
			if task.TaskType == persistence.TransferTaskTypeDecisionTask {
				s.mockMatching.On("AddDecisionTask", mock.Anything, createAddRequestFromTask(task, 0)).Once().Return(nil)
				if task.ScheduleID == firstEventID+1 {
					s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", mock.Anything).Once().Return(nil)
				}
			} else if task.TaskType == persistence.TransferTaskTypeCancelExecution {
				s.mockHistoryClient.On("RequestCancelWorkflowExecution", mock.Anything, mock.Anything).
					Return(&workflow.EntityNotExistsError{}).Once()
			}
			s.processor.processTransferTask(task)
		default:
			break workerPump
		}
	}

	s.mockMatching.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
	s.mockHistoryClient.AssertExpectations(s.T())
}

func (s *transferQueueProcessorSuite) TestCompleteTaskAfterExecutionDeleted() {
	domainID := "b677a307-8261-40ea-b239-ab2ec78e443b"
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("complete-task-execution-deleted-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}
	taskList := "complete-task-execution-deleted-queue"
	task0, err0 := s.CreateWorkflowExecution(domainID, workflowExecution, taskList, "wType", 10, nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	s.processor.processTransferTasks(tasksCh)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			if task.ScheduleID == firstEventID+1 {
				s.mockVisibilityMgr.On("RecordWorkflowExecutionStarted", mock.Anything).Once().Return(&workflow.EntityNotExistsError{})
			}
			s.processor.processTransferTask(task)
		default:
			break workerPump
		}
	}

	s.mockVisibilityMgr.AssertExpectations(s.T())
}

func createAddRequestFromTask(task *persistence.TransferTaskInfo, scheduleToStartTimeout int32) interface{} {
	var res interface{}
	domainID := task.DomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}
	taskList := &workflow.TaskList{
		Name: &task.TaskList,
	}
	if task.TaskType == persistence.TransferTaskTypeActivityTask {
		res = &m.AddActivityTaskRequest{
			DomainUUID:                    common.StringPtr(domainID),
			SourceDomainUUID:              common.StringPtr(domainID),
			Execution:                     &execution,
			TaskList:                      taskList,
			ScheduleId:                    &task.ScheduleID,
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(scheduleToStartTimeout),
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
