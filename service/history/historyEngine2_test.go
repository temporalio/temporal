package history

import (
	"errors"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	"github.com/pborman/uuid"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	engine2Suite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		builder            *historyBuilder
		historyEngine      *historyEngineImpl
		mockMatchingClient *mocks.MatchingClient
		mockExecutionMgr   *mocks.ExecutionManager
		mockShardManager   *mocks.ShardManager
		shardClosedCh      chan int
		logger             bark.Logger
	}
)

func TestEngine2Suite(t *testing.T) {
	s := new(engine2Suite)
	suite.Run(t, s)
}

func (s *engine2Suite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.logger = bark.NewLoggerFromLogrus(log.New())
	s.builder = newHistoryBuilder(s.logger)
}

func (s *engine2Suite) TearDownSuite() {

}

func (s *engine2Suite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	shardID := 0
	s.mockMatchingClient = &mocks.MatchingClient{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockShardManager = &mocks.ShardManager{}
	s.shardClosedCh = make(chan int, 100)

	mockShard := &shardContextImpl{
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		rangeSize:                 defaultRangeSize,
		maxTransferSequenceNumber: 100000,
		closeCh:                   s.shardClosedCh,
		logger:                    s.logger,
	}

	cache := newHistoryCache(mockShard, s.logger)
	txProcessor := newTransferQueueProcessor(mockShard, s.mockMatchingClient, cache)
	tracker := newPendingTaskTracker(mockShard, txProcessor, s.logger)
	h := &historyEngineImpl{
		shard:            mockShard,
		executionManager: s.mockExecutionMgr,
		txProcessor:      txProcessor,
		tracker:          tracker,
		cache:            cache,
		logger:           s.logger,
		tokenSerializer:  common.NewJSONTaskTokenSerializer(),
	}
	h.timerProcessor = newTimerQueueProcessor(h, s.mockExecutionMgr, s.logger)
	s.historyEngine = h
}

func (s *engine2Suite) TearDownTest() {
	s.mockMatchingClient.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfNoExecution() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	ms := createMutableState(3)
	addDecisionToMutableState(ms, 2, emptyEventID, "reqId", 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfGetExecutionFailed() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	ms := createMutableState(3)
	addDecisionToMutableState(ms, 2, emptyEventID, "reqId", 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.EqualError(err, "FAILED")
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfTaskAlreadyStarted() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	startedEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)

	ms := createMutableState(4)
	addDecisionToMutableState(ms, scheduleEvent.GetEventId(), startedEvent.GetEventId(), uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	history, _ := builder.Serialize()
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			WorkflowID:           "wId",
			RunID:                "rId",
			TaskList:             tl,
			History:              history,
			ExecutionContext:     nil,
			State:                persistence.WorkflowStateRunning,
			NextEventID:          builder.nextEventID,
			LastProcessedEvent:   emptyEventID,
			LastUpdatedTimestamp: time.Time{}},
	}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
	s.logger.Errorf("RecordDecisionTaskStarted failed with: %v", err)
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfTaskAlreadyCompleted() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	startedEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)
	addDecisionTaskCompletedEvent(builder, scheduleEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)

	ms := createMutableState(5)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
	s.logger.Errorf("RecordDecisionTaskStarted failed with: %v", err)
}

func (s *engine2Suite) TestRecordDecisionTaskStartedConflictOnUpdate() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)

	history, _ := builder.Serialize()
	wfResponse1 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			WorkflowID:           "wId",
			RunID:                "rId",
			TaskList:             tl,
			History:              history,
			ExecutionContext:     nil,
			State:                persistence.WorkflowStateRunning,
			NextEventID:          builder.nextEventID,
			LastProcessedEvent:   emptyEventID,
			LastUpdatedTimestamp: time.Time{}},
	}

	wfResponse2 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			WorkflowID:           "wId",
			RunID:                "rId",
			TaskList:             tl,
			History:              history,
			ExecutionContext:     nil,
			State:                persistence.WorkflowStateRunning,
			NextEventID:          builder.nextEventID,
			LastProcessedEvent:   emptyEventID,
			LastUpdatedTimestamp: time.Time{}},
	}

	ms := createMutableState(3)
	addDecisionToMutableState(ms, scheduleEvent.GetEventId(), emptyEventID, "requestid-ignored", 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse1, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	ms2 := createMutableState(3)
	addDecisionToMutableState(ms2, scheduleEvent.GetEventId(), emptyEventID, "requestid-ignored", 1)
	gwmsResponse2 := &persistence.GetWorkflowMutableStateResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse2, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", response.GetWorkflowType().GetName())
	s.False(response.IsSetPreviousStartedEventId())
	s.Equal(scheduleEvent.GetEventId()+1, response.GetStartedEventId())
}

func (s *engine2Suite) TestRecordDecisionTaskRetrySameRequest() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	tl := "testTaskList"
	identity := "testIdentity"
	requestID := "testRecordDecisionTaskRetrySameRequestID"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)

	history1, _ := builder.Serialize()
	wfResponse1 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			WorkflowID:           "wId",
			RunID:                "rId",
			TaskList:             tl,
			History:              history1,
			ExecutionContext:     nil,
			State:                persistence.WorkflowStateRunning,
			NextEventID:          builder.nextEventID,
			LastProcessedEvent:   emptyEventID,
			LastUpdatedTimestamp: time.Time{}},
	}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse1, nil).Once()

	ms := createMutableState(3)
	addDecisionToMutableState(ms, scheduleEvent.GetEventId(), emptyEventID, requestID, 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	startedEventID := addDecisionTaskStartedEventWithRequestID(builder, scheduleEvent.GetEventId(), requestID, tl,
		identity)

	history2, _ := builder.Serialize()
	wfResponse2 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			WorkflowID:           "wId",
			RunID:                "rId",
			TaskList:             tl,
			History:              history2,
			ExecutionContext:     nil,
			State:                persistence.WorkflowStateRunning,
			NextEventID:          builder.nextEventID,
			LastProcessedEvent:   emptyEventID,
			LastUpdatedTimestamp: time.Time{}},
	}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse2, nil).Once()

	ms2 := createMutableState(4)
	addDecisionToMutableState(ms2, scheduleEvent.GetEventId(), startedEventID.GetEventId(), requestID, 1)
	gwmsResponse2 := &persistence.GetWorkflowMutableStateResponse{State: ms2}
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse2, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr(requestID),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})

	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", response.GetWorkflowType().GetName())
	s.False(response.IsSetPreviousStartedEventId())
	s.Equal(startedEventID.GetEventId(), response.GetStartedEventId())
}

func (s *engine2Suite) TestRecordDecisionTaskRetryDifferentRequest() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	tl := "testTaskList"
	identity := "testIdentity"
	requestID := "testRecordDecisionTaskRetrySameRequestID"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)

	ms := createMutableState(3)
	addDecisionToMutableState(ms, scheduleEvent.GetEventId(), emptyEventID, requestID, 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	history1, _ := builder.Serialize()
	wfResponse1 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			WorkflowID:           "wId",
			RunID:                "rId",
			TaskList:             tl,
			History:              history1,
			ExecutionContext:     nil,
			State:                persistence.WorkflowStateRunning,
			NextEventID:          builder.nextEventID,
			LastProcessedEvent:   emptyEventID,
			LastUpdatedTimestamp: time.Time{}},
	}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse1, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	// Add event.
	startedEvent := addDecisionTaskStartedEventWithRequestID(builder, scheduleEvent.GetEventId(), "some_other_req", tl, identity)

	history2, _ := builder.Serialize()
	wfResponse2 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			WorkflowID:           "wId",
			RunID:                "rId",
			TaskList:             tl,
			History:              history2,
			ExecutionContext:     nil,
			State:                persistence.WorkflowStateRunning,
			NextEventID:          builder.nextEventID,
			LastProcessedEvent:   emptyEventID,
			LastUpdatedTimestamp: time.Time{}},
	}

	ms2 := createMutableState(4)
	addDecisionToMutableState(ms2, scheduleEvent.GetEventId(), startedEvent.GetEventId(), "some_other_req", 1)
	gwmsResponse2 := &persistence.GetWorkflowMutableStateResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse2, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse2, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr(requestID),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})

	s.Nil(response)
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
	s.logger.Infof("Failed with error: %v", err)
}

func (s *engine2Suite) TestRecordDecisionTaskStartedMaxAttemptsExceeded() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	tl := "testTaskList"
	identity := "testIdentity"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(builder, tl, 100)

	history, _ := builder.Serialize()

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(3)
		addDecisionToMutableState(ms, 2, emptyEventID, "reqId", 1)
		gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

		wfResponse := &persistence.GetWorkflowExecutionResponse{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				WorkflowID:           "wId",
				RunID:                "rId",
				TaskList:             tl,
				History:              history,
				ExecutionContext:     nil,
				State:                persistence.WorkflowStateRunning,
				NextEventID:          builder.nextEventID,
				LastProcessedEvent:   emptyEventID,
				LastUpdatedTimestamp: time.Time{}},
		}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	}

	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(
		&persistence.ConditionFailedError{}).Times(conditionalRetryCount)

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})

	s.NotNil(err)
	s.Nil(response)
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engine2Suite) TestRecordDecisionTaskSuccess() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	tl := "testTaskList"
	identity := "testIdentity"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 100)

	ms := createMutableState(3)
	addDecisionToMutableState(ms, scheduledEvent.GetEventId(), emptyEventID, "reqId", 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	history, _ := builder.Serialize()
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			WorkflowID:           "wId",
			RunID:                "rId",
			TaskList:             tl,
			History:              history,
			ExecutionContext:     nil,
			State:                persistence.WorkflowStateRunning,
			NextEventID:          builder.nextEventID,
			LastProcessedEvent:   emptyEventID,
			LastUpdatedTimestamp: time.Time{}},
	}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(2),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForDecisionTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})

	s.Nil(err)
	s.NotNil(response)
	s.Equal("wType", response.GetWorkflowType().GetName())
	s.False(response.IsSetPreviousStartedEventId())
	s.Equal(scheduledEvent.GetEventId()+1, response.GetStartedEventId())
}

func (s *engine2Suite) TestRecordActivityTaskStartedIfNoExecution() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	response, err := s.historyEngine.RecordActivityTaskStarted(&h.RecordActivityTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(5),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForActivityTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	if err != nil {
		s.logger.Errorf("Unexpected Error: %v", err)
	}
	s.Nil(response)
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engine2Suite) TestRecordActivityTaskStartedSuccess() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	scheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)

	history, _ := builder.Serialize()
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			WorkflowID:           "wId",
			RunID:                "rId",
			TaskList:             tl,
			History:              history,
			ExecutionContext:     nil,
			State:                persistence.WorkflowStateRunning,
			NextEventID:          builder.nextEventID,
			LastProcessedEvent:   emptyEventID,
			LastUpdatedTimestamp: time.Time{},
		},
	}

	ms1 := createMutableState(6)
	addActivityToMutableState(ms1, scheduledEvent.GetEventId(), emptyEventID, activityID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse1 := &persistence.GetWorkflowMutableStateResponse{State: ms1}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	response, err := s.historyEngine.RecordActivityTaskStarted(&h.RecordActivityTaskStartedRequest{
		WorkflowExecution: workflowExecution,
		ScheduleId:        common.Int64Ptr(5),
		TaskId:            common.Int64Ptr(100),
		RequestId:         common.StringPtr("reqId"),
		PollRequest: &workflow.PollForActivityTaskRequest{
			TaskList: &workflow.TaskList{
				Name: common.StringPtr(tl),
			},
			Identity: common.StringPtr(identity),
		},
	})
	s.Nil(err)
	s.NotNil(response)
	s.Equal(scheduledEvent, response.GetScheduledEvent())
	s.Equal(scheduledEvent.GetEventId()+1, response.GetStartedEvent().GetEventId())
	s.Equal("reqId", response.GetStartedEvent().GetActivityTaskStartedEventAttributes().GetRequestId())
}
