package history

import (
	"errors"
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

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
		historyEngine      *historyEngineImpl
		mockMatchingClient *mocks.MatchingClient
		mockExecutionMgr   *mocks.ExecutionManager
		mockHistoryMgr     *mocks.HistoryManager
		mockShardManager   *mocks.ShardManager
		shardClosedCh      chan int
		eventSerializer    historyEventSerializer
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
}

func (s *engine2Suite) TearDownSuite() {
}

func (s *engine2Suite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	shardID := 0
	s.mockMatchingClient = &mocks.MatchingClient{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockShardManager = &mocks.ShardManager{}
	s.shardClosedCh = make(chan int, 100)
	s.eventSerializer = newJSONHistoryEventSerializer()

	mockShard := &shardContextImpl{
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		historyMgr:                s.mockHistoryMgr,
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
		historyMgr:       s.mockHistoryMgr,
		txProcessor:      txProcessor,
		tracker:          tracker,
		cache:            cache,
		logger:           s.logger,
		tokenSerializer:  common.NewJSONTaskTokenSerializer(),
		hSerializer:      newJSONHistorySerializer(),
	}
	h.timerProcessor = newTimerQueueProcessor(h, s.mockExecutionMgr, s.logger)
	s.historyEngine = h
}

func (s *engine2Suite) TearDownTest() {
	s.mockMatchingClient.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
}

func (s *engine2Suite) TestRecordDecisionTaskStartedIfNoExecution() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: &workflowExecution,
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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	addDecisionTaskCompletedEvent(msBuilder, int64(2), int64(3), nil, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: &workflowExecution,
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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockHistoryMgr.On("GetWorkflowExecutionHistory", mock.Anything).Return(&persistence.GetWorkflowExecutionHistoryResponse{
		Events:        [][]byte{[]byte("event1;event2;event3")},
		NextPageToken: []byte{},
	}, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: &workflowExecution,
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
	s.Equal(int64(3), response.GetStartedEventId())
}

func (s *engine2Suite) TestRecordDecisionTaskRetrySameRequest() {
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	tl := "testTaskList"
	identity := "testIdentity"
	requestID := "testRecordDecisionTaskRetrySameRequestID"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	startedEventID := addDecisionTaskStartedEventWithRequestID(msBuilder, int64(2), requestID, tl, identity)
	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryMgr.On("GetWorkflowExecutionHistory", mock.Anything).Return(&persistence.GetWorkflowExecutionHistoryResponse{
		Events:        [][]byte{[]byte("event1;event2;event3")},
		NextPageToken: []byte{},
	}, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: &workflowExecution,
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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	tl := "testTaskList"
	identity := "testIdentity"
	requestID := "testRecordDecisionTaskRetrySameRequestID"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	// Add event.
	addDecisionTaskStartedEventWithRequestID(msBuilder, int64(2), "some_other_req", tl, identity)
	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: &workflowExecution,
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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	tl := "testTaskList"
	identity := "testIdentity"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Times(
		conditionalRetryCount)
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(
		&persistence.ConditionFailedError{}).Times(conditionalRetryCount)

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: &workflowExecution,
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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	tl := "testTaskList"
	identity := "testIdentity"

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, false)
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockHistoryMgr.On("GetWorkflowExecutionHistory", mock.Anything).Return(
		&persistence.GetWorkflowExecutionHistoryResponse{
			Events:        [][]byte{[]byte("event1;event2;event3")},
			NextPageToken: []byte{},
		}, nil).Once()

	response, err := s.historyEngine.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: &workflowExecution,
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
	s.Equal(int64(3), response.GetStartedEventId())
}

func (s *engine2Suite) TestRecordActivityTaskStartedIfNoExecution() {
	workflowExecution := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

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
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}

	identity := "testIdentity"
	tl := "testTaskList"

	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := s.createExecutionStartedState(workflowExecution, tl, identity, true)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, int64(2), int64(3), nil, identity)
	scheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	response, err := s.historyEngine.RecordActivityTaskStarted(&h.RecordActivityTaskStartedRequest{
		WorkflowExecution: &workflowExecution,
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

func (s *engine2Suite) createExecutionStartedState(we workflow.WorkflowExecution, tl, identity string,
	startDecision bool) *mutableStateBuilder {
	msBuilder := newMutableStateBuilder(s.logger)
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	if startDecision {
		addDecisionTaskStartedEvent(msBuilder, scheduleEvent.GetEventId(), tl, identity)
	}

	return msBuilder
}
