package workflow

import (
	"os"
	"testing"
	"time"

	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/persistence"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
)

type (
	timerQueueProcessorSuite struct {
		suite.Suite
		TestBase
		engineImpl *historyEngineImpl
		logger     bark.Logger
	}
)

func TestTimerQueueProcessorSuite(t *testing.T) {
	s := new(timerQueueProcessorSuite)
	suite.Run(t, s)
}

func (s *timerQueueProcessorSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
	s.SetupWorkflowStore()

	s.logger = bark.NewLoggerFromLogrus(log.New())
	resp, err := s.WorkflowMgr.GetShard(&persistence.GetShardRequest{ShardID: 1})
	if err != nil {
		log.Fatal(err)
	}

	shard := &shardContextImpl{shardInfo: resp.ShardInfo}
	s.engineImpl = &historyEngineImpl{
		shard:            shard,
		executionManager: s.WorkflowMgr,
		txProcessor:      newTransferQueueProcessor(shard, s.WorkflowMgr, s.TaskMgr, s.logger),
		logger:           s.logger,
		tokenSerializer:  newJSONTaskTokenSerializer(),
	}
}

func (s *timerQueueProcessorSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *timerQueueProcessorSuite) getHistoryAndTimers(timeOuts []int32) ([]byte, []persistence.Task) {
	// Generate first decision task event.
	logger := bark.NewLoggerFromLogrus(log.New())
	tBuilder := newTimerBuilder(&localSeqNumGenerator{counter: 1}, logger)
	builder := newHistoryBuilder(tBuilder, logger)
	builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{})

	timerTasks := []persistence.Task{}
	builder.AddDecisionTaskScheduledEvent("taskList", 1)

	counter := int64(3)
	for _, timeOut := range timeOuts {
		timerTasks = append(timerTasks, tBuilder.CreateUserTimerTask(timeOut, "tid", counter))
		builder.AddTimerStartedEvent(counter,
			&workflow.StartTimerDecisionAttributes{
				TimerId:                   common.StringPtr(uuid.New()),
				StartToFireTimeoutSeconds: common.Int64Ptr(int64(timeOut))})
		counter++
	}

	// Serialize the history
	h, serializedError := builder.Serialize()
	s.Nil(serializedError)
	return h, timerTasks
}

func (s *timerQueueProcessorSuite) TestSingleTimerTask() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("single-timer-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "single-timer-queue"
	h, tt := s.getHistoryAndTimers([]int32{1})
	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, tt)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	for {
		timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))
}

func (s *timerQueueProcessorSuite) TestManyTimerTasks() {
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("multiple-timer-test"),
		RunId: common.StringPtr("0d00698f-08e1-4d36-a3e2-3bf109f5d2d6")}

	taskList := "multiple-timer-queue"
	h, tt := s.getHistoryAndTimers([]int32{1, 2, 3})
	task0, err0 := s.CreateWorkflowExecution(workflowExecution, taskList, string(h), nil, 3, 0, 2, tt[0:1])
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := newTimerQueueProcessor(s.engineImpl, s.WorkflowMgr, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	for {
		timerInfo, err := s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
		// fmt.Printf("TestManyTimerTasks: GetTimerIndexTasks: Response Count: %d \n", len(timerInfo))
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks(MinTimerKey, MaxTimerKey)
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))

	s.Equal(uint64(3), processor.timerFiredCount)
}
