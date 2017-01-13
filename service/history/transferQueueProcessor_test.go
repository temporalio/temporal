package history

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/persistence"
)

type (
	transferQueueProcessorSuite struct {
		suite.Suite
		persistence.TestBase
		processor *transferQueueProcessorImpl
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
	s.processor = newTransferQueueProcessor(s.ShardContext, s.WorkflowMgr, s.TaskMgr,
		bark.NewLoggerFromLogrus(log.New())).(*transferQueueProcessorImpl)
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
	tasksCh := make(chan *persistence.TaskInfo)
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

	tasksCh := make(chan *persistence.TaskInfo, 10)
	newPollInterval := s.processor.processTransferTasks(tasksCh, time.Second)
	s.Equal(transferProcessorMinPollInterval, newPollInterval)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			s.processor.processTransferTask(task)
		default:
			break workerPump
		}
	}

	tasks1, err1 := s.GetTasks(taskList, persistence.TaskTypeDecision, time.Second, 1)
	s.Nil(err1)
	s.NotEmpty(tasks1)
	s.Equal(1, len(tasks1))

	dTask := tasks1[0]
	s.Equal(int64(2), dTask.Info.ScheduleID)
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

	tasksCh := make(chan *persistence.TaskInfo, 10)
	newPollInterval := s.processor.processTransferTasks(tasksCh, time.Second)
	s.Equal(transferProcessorMinPollInterval, newPollInterval)
workerPump:
	for {
		select {
		case task := <-tasksCh:
			s.processor.processTransferTask(task)
		default:
			break workerPump
		}
	}

	tasks1, err1 := s.GetTasks(taskList, persistence.TaskTypeActivity, time.Second, 10)
	s.Nil(err1)
	s.NotEmpty(tasks1)
	s.Equal(len(activityTaskScheduleIds), len(tasks1))

	for _, t := range tasks1 {
		s.True(containsID(activityTaskScheduleIds, t.Info.ScheduleID),
			fmt.Sprintf("ScheduleID: %v, TaskList: %v", string(t.Info.ScheduleID), t.Info.TaskList))
		s.Equal(workflowExecution.GetWorkflowId(), t.Info.WorkflowID)
		s.Equal(workflowExecution.GetRunId(), t.Info.RunID)
		s.Equal(taskList, t.Info.TaskList)
		s.Equal(1, t.Info.DeliveryCount)
		s.Equal(persistence.TaskTypeActivity, t.Info.TaskType)
	}
}

func containsID(list []int64, scheduleID int64) bool {
	for _, id := range list {
		if id == scheduleID {
			return true
		}
	}

	return false
}
