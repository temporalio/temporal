package history

import (
	"math"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

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
		processor    *transferQueueProcessorImpl
		mockMatching *mocks.MatchingClient
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

	s.SetupWorkflowStore()
	s.mockMatching = &mocks.MatchingClient{}
	s.processor = newTransferQueueProcessor(s.ShardContext, s.mockMatching).(*transferQueueProcessorImpl)
}

func (s *transferQueueProcessorSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *transferQueueProcessorSuite) SetupTest() {
	// First cleanup transfer tasks from other tests and reset shard context
	s.ClearTransferQueue()
	s.processor.UpdateMaxAllowedReadLevel(math.MaxInt64)
}

func (s *transferQueueProcessorSuite) TestNoTransferTask() {
	tasksCh := make(chan *persistence.TransferTaskInfo)
	newPollInterval := s.processor.processTransferTasks(tasksCh, transferProcessorMinPollInterval)
	s.Equal(2*transferProcessorMinPollInterval, newPollInterval)
}

func (s *transferQueueProcessorSuite) TestSingleDecisionTask() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("single-decisiontask-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}
	taskList := "single-decisiontask-queue"
	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, "decisiontask-scheduled", nil, 3, 0, 2, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	newPollInterval := s.processor.processTransferTasks(tasksCh, time.Second)
	s.Equal(transferProcessorMinPollInterval, newPollInterval)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			s.mockMatching.On("AddDecisionTask", mock.Anything, createAddRequestFromTask(task)).Once().Return(nil)
			s.processor.processTransferTask(task)
		default:
			break workerPump
		}
	}

	s.mockMatching.AssertExpectations(s.T())
}

func (s *transferQueueProcessorSuite) TestManyTransferTasks() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("many-transfertasks-test"),
		RunId: common.StringPtr("57d5f005-bdaa-42a5-a1c5-b9c45d8699a9")}
	taskList := "many-transfertasks-queue"
	activityTaskScheduleIds := []int64{2, 3, 4, 5, 6}
	task0, err0 := s.CreateWorkflowExecutionManyTasks(workflowExecution, taskList, "t1;t2;t3;t4;t5", nil, 7, 0, nil,
		activityTaskScheduleIds)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	tasksCh := make(chan *persistence.TransferTaskInfo, 10)
	newPollInterval := s.processor.processTransferTasks(tasksCh, time.Second)
	s.Equal(transferProcessorMinPollInterval, newPollInterval)
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
}

func createAddRequestFromTask(task *persistence.TransferTaskInfo) interface{} {
	var res interface{}
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}
	taskList := &workflow.TaskList{
		Name: &task.TaskList,
	}
	if task.TaskType == persistence.TaskListTypeActivity {
		res = &m.AddActivityTaskRequest{
			Execution:  &execution,
			TaskList:   taskList,
			ScheduleId: &task.ScheduleID,
		}
	} else {
		res = &m.AddDecisionTaskRequest{
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
