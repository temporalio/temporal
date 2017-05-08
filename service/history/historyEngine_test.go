package history

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	"github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	engineSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		mockHistoryEngine  *historyEngineImpl
		mockMatchingClient *mocks.MatchingClient
		mockHistoryClient  *mocks.HistoryClient
		mockMetadataMgr    *mocks.MetadataManager
		mockVisibilityMgr  *mocks.VisibilityManager
		mockExecutionMgr   *mocks.ExecutionManager
		mockHistoryMgr     *mocks.HistoryManager
		mockShardManager   *mocks.ShardManager
		shardClosedCh      chan int
		eventSerializer    historyEventSerializer
		logger             bark.Logger
	}
)

func TestEngineSuite(t *testing.T) {
	s := new(engineSuite)
	suite.Run(t, s)
}

func (s *engineSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.logger = bark.NewLoggerFromLogrus(log.New())
}

func (s *engineSuite) TearDownSuite() {

}

func (s *engineSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())

	shardID := 0
	s.mockMatchingClient = &mocks.MatchingClient{}
	s.mockHistoryClient = &mocks.HistoryClient{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockVisibilityMgr = &mocks.VisibilityManager{}
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

	historyCache := newHistoryCache(historyCacheMaxSize, mockShard, s.logger)
	txProcessor := newTransferQueueProcessor(mockShard, s.mockVisibilityMgr, s.mockMatchingClient, s.mockHistoryClient, historyCache)
	h := &historyEngineImpl{
		shard:              mockShard,
		executionManager:   s.mockExecutionMgr,
		historyMgr:         s.mockHistoryMgr,
		txProcessor:        txProcessor,
		historyCache:       historyCache,
		domainCache:        cache.NewDomainCache(s.mockMetadataMgr, s.logger),
		logger:             s.logger,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		hSerializerFactory: persistence.NewHistorySerializerFactory(),
	}
	h.timerProcessor = newTimerQueueProcessor(h, s.mockExecutionMgr, s.logger)
	s.mockHistoryEngine = h
}

func (s *engineSuite) TearDownTest() {
	s.mockMatchingClient.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockVisibilityMgr.AssertExpectations(s.T())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedInvalidToken() {
	domainID := "domainId"
	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        invalidToken,
			Decisions:        nil,
			ExecutionContext: nil,
			Identity:         &identity,
		},
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfNoExecution() {
	domainID := "domainId"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfGetExecutionFailed() {
	domainID := "domainId"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedUpdateExecutionFailed() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, scheduleEvent.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskCompleted() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	startedEvent := addDecisionTaskStartedEvent(msBuilder, scheduleEvent.GetEventId(), tl, identity)
	addDecisionTaskCompletedEvent(msBuilder, scheduleEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskNotStarted() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken: taskToken,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedConflictOnUpdate() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	context := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")
	activity3ID := "activity3"
	activity3Type := "activity_type3"
	activity3Input := []byte("input3")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	decisionScheduledEvent1, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent1.GetEventId(), tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent1.GetEventId(),
		decisionStartedEvent1.GetEventId(), nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.GetEventId(),
		activity1ID, activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.GetEventId(),
		activity2ID, activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.GetEventId(), tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, activity2ScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskCompletedEvent(msBuilder, activity1ScheduledEvent.GetEventId(),
		activity1StartedEvent.GetEventId(), activity1Result, identity)
	decisionScheduledEvent2, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent2 := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent2.GetEventId(), tl, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: decisionScheduledEvent2.GetEventId(),
	})

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
		ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:   common.StringPtr(activity3ID),
			ActivityType: &workflow.ActivityType{Name: common.StringPtr(activity3Type)},
			TaskList:     &workflow.TaskList{Name: &tl},
			Input:        activity3Input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	addActivityTaskCompletedEvent(msBuilder, activity2ScheduledEvent.GetEventId(),
		activity2StartedEvent.GetEventId(), activity2Result, identity)

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(
		&persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: context,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	s.Equal(int64(16), ms2.ExecutionInfo.NextEventID)
	s.Equal(decisionStartedEvent2.GetEventId(), ms2.ExecutionInfo.LastProcessedEvent)
	s.Equal(context, ms2.ExecutionInfo.ExecutionContext)

	executionBuilder := s.getBuilder(domainID, we)
	activity3Attributes := s.getActivityScheduledEvent(executionBuilder, 14).GetActivityTaskScheduledEventAttributes()
	s.Equal(activity3ID, activity3Attributes.GetActivityId())
	s.Equal(activity3Type, activity3Attributes.GetActivityType().GetName())
	s.Equal(int64(13), activity3Attributes.GetDecisionTaskCompletedEventId())
	s.Equal(tl, activity3Attributes.GetTaskList().GetName())
	s.Equal(activity3Input, activity3Attributes.GetInput())
	s.Equal(int32(100), activity3Attributes.GetScheduleToCloseTimeoutSeconds())
	s.Equal(int32(10), activity3Attributes.GetScheduleToStartTimeoutSeconds())
	s.Equal(int32(50), activity3Attributes.GetStartToCloseTimeoutSeconds())
	s.Equal(int32(5), activity3Attributes.GetHeartbeatTimeoutSeconds())

	di, ok := executionBuilder.GetPendingDecision(15)
	s.True(ok)
	s.Equal(int32(200), di.DecisionTimeout)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedMaxAttemptsExceeded() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	input := []byte("input")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, scheduleEvent.GetEventId(), tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
		ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:   common.StringPtr("activity1"),
			ActivityType: &workflow.ActivityType{Name: common.StringPtr("activity_type1")},
			TaskList:     &workflow.TaskList{Name: &tl},
			Input:        input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
		},
	}}

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(
			&persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: context,
			Identity:         &identity,
		},
	})
	s.NotNil(err)
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedCompleteWorkflowFailed() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	context := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")
	workflowResult := []byte("workflow result")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	decisionScheduledEvent1, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent1.GetEventId(), tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent1.GetEventId(),
		decisionStartedEvent1.GetEventId(), nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.GetEventId(),
		activity1ID, activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.GetEventId(),
		activity2ID, activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.GetEventId(), tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, activity2ScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskCompletedEvent(msBuilder, activity1ScheduledEvent.GetEventId(),
		activity1StartedEvent.GetEventId(), activity1Result, identity)
	decisionScheduledEvent2, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent2.GetEventId(), tl, identity)
	addActivityTaskCompletedEvent(msBuilder, activity2ScheduledEvent.GetEventId(),
		activity2StartedEvent.GetEventId(), activity2Result, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: decisionScheduledEvent2.GetEventId(),
	})

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
		CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
			Result_: workflowResult,
		},
	}}

	for i := 0; i < 2; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: context,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(15), executionBuilder.executionInfo.NextEventID)
	s.Equal(decisionStartedEvent1.GetEventId(), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(context, executionBuilder.executionInfo.ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.True(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedFailWorkflowFailed() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	identity := "testIdentity"
	context := []byte("context")
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")
	reason := "workflow fail reason"
	details := []byte("workflow fail details")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	decisionScheduledEvent1, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent1.GetEventId(), tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent1.GetEventId(),
		decisionStartedEvent1.GetEventId(), nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.GetEventId(), activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.GetEventId(), activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.GetEventId(), tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, activity2ScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskCompletedEvent(msBuilder, activity1ScheduledEvent.GetEventId(),
		activity1StartedEvent.GetEventId(), activity1Result, identity)
	decisionScheduledEvent2, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent2.GetEventId(), tl, identity)
	addActivityTaskCompletedEvent(msBuilder, activity2ScheduledEvent.GetEventId(),
		activity2StartedEvent.GetEventId(), activity2Result, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: decisionScheduledEvent2.GetEventId(),
	})

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_FailWorkflowExecution),
		FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
			Reason:  &reason,
			Details: details,
		},
	}}

	for i := 0; i < 2; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	}

	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: context,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(15), executionBuilder.executionInfo.NextEventID)
	s.Equal(decisionStartedEvent1.GetEventId(), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(context, executionBuilder.executionInfo.ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.True(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSingleActivityScheduledDecision() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	input := []byte("input")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, scheduleEvent.GetEventId(), tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_ScheduleActivityTask),
		ScheduleActivityTaskDecisionAttributes: &workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:   common.StringPtr("activity1"),
			ActivityType: &workflow.ActivityType{Name: common.StringPtr("activity_type1")},
			TaskList:     &workflow.TaskList{Name: &tl},
			Input:        input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(100),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(10),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(50),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(5),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: context,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(context, executionBuilder.executionInfo.ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.False(executionBuilder.HasPendingDecisionTask())

	activity1Attributes := s.getActivityScheduledEvent(executionBuilder, int64(5)).GetActivityTaskScheduledEventAttributes()
	s.Equal("activity1", activity1Attributes.GetActivityId())
	s.Equal("activity_type1", activity1Attributes.GetActivityType().GetName())
	s.Equal(int64(4), activity1Attributes.GetDecisionTaskCompletedEventId())
	s.Equal(tl, activity1Attributes.GetTaskList().GetName())
	s.Equal(input, activity1Attributes.GetInput())
	s.Equal(int32(100), activity1Attributes.GetScheduleToCloseTimeoutSeconds())
	s.Equal(int32(10), activity1Attributes.GetScheduleToStartTimeoutSeconds())
	s.Equal(int32(50), activity1Attributes.GetStartToCloseTimeoutSeconds())
	s.Equal(int32(5), activity1Attributes.GetHeartbeatTimeoutSeconds())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedCompleteWorkflowSuccess() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	workflowResult := []byte("success")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, scheduleEvent.GetEventId(), tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
		CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
			Result_: workflowResult,
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: context,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(context, executionBuilder.executionInfo.ExecutionContext)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.executionInfo.State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedFailWorkflowSuccess() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	details := []byte("fail workflow details")
	reason := "fail workflow reason"

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, scheduleEvent.GetEventId(), tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_FailWorkflowExecution),
		FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
			Reason:  &reason,
			Details: details,
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: context,
			Identity:         &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(context, executionBuilder.executionInfo.ExecutionContext)
	s.Equal(persistence.WorkflowStateCompleted, executionBuilder.executionInfo.State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRespondActivityTaskCompletedInvalidToken() {
	domainID := "domainId"
	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: invalidToken,
			Result_:   nil,
			Identity:  &identity,
		},
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoExecution() {
	domainID := "domainId"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfGetExecutionFailed() {
	domainID := "domainId"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedUpdateExecutionFailed() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result_:   activityResult,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskCompleted() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskCompletedEvent(msBuilder, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(),
		activityResult, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result_:   activityResult,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskNotStarted() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result_:   activityResult,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedConflictOnUpdate() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	activity1Result := []byte("activity1_result")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	decisionScheduledEvent1, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent1.GetEventId(), tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent1.GetEventId(),
		decisionStartedEvent1.GetEventId(), nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.GetEventId(), activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.GetEventId(), activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskStartedEvent(msBuilder, activity2ScheduledEvent.GetEventId(), tl, identity)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result_:   activity1Result,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(11), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(10))
	s.True(ok)
	s.Equal(int32(200), di.DecisionTimeout)
	s.Equal(int64(10), di.ScheduleID)
	s.Equal(emptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskCompletedMaxAttemptsExceeded() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result_:   activityResult,
			Identity:  &identity,
		},
	})
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedSuccess() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&history.RespondActivityTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondActivityTaskCompletedRequest{
			TaskToken: taskToken,
			Result_:   activityResult,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(9), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(8))
	s.True(ok)
	s.Equal(int32(200), di.DecisionTimeout)
	s.Equal(int64(8), di.ScheduleID)
	s.Equal(emptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskFailedInvalidToken() {
	domainID := "domainId"
	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: invalidToken,
			Identity:  &identity,
		},
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfNoExecution() {
	domainID := "domainId"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil,
		&workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfGetExecutionFailed() {
	domainID := "domainId"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil,
		errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailedUpdateExecutionFailed() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(errors.New("FAILED")).Once()
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskCompleted() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "fail reason"
	details := []byte("fail details")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskFailedEvent(msBuilder, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(),
		failReason, details, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   details,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskNotStarted() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedConflictOnUpdate() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activity1ID := "activity1"
	activity1Type := "activity_type1"
	activity1Input := []byte("input1")
	failReason := "fail reason"
	details := []byte("fail details.")
	activity2ID := "activity2"
	activity2Type := "activity_type2"
	activity2Input := []byte("input2")
	activity2Result := []byte("activity2_result")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 25, 200, identity)
	decisionScheduledEvent1, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent1.GetEventId(), tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent1.GetEventId(),
		decisionStartedEvent1.GetEventId(), nil, identity)
	activity1ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.GetEventId(), activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent1.GetEventId(), activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activity1ScheduledEvent.GetEventId(), tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(msBuilder, activity2ScheduledEvent.GetEventId(), tl, identity)

	ms1 := createMutableState(msBuilder)
	gwmsResponse1 := &persistence.GetWorkflowExecutionResponse{State: ms1}

	addActivityTaskCompletedEvent(msBuilder, activity2ScheduledEvent.GetEventId(),
		activity2StartedEvent.GetEventId(), activity2Result, identity)
	addDecisionTaskScheduledEvent(msBuilder)

	ms2 := createMutableState(msBuilder)
	gwmsResponse2 := &persistence.GetWorkflowExecutionResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   details,
			Identity:  &identity,
		},
	})
	s.Nil(err, s.printHistory(msBuilder))
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(12), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(10))
	s.True(ok)
	s.Equal(int32(200), di.DecisionTimeout)
	s.Equal(int64(10), di.ScheduleID)
	s.Equal(emptyEventID, di.StartedID)
}

func (s *engineSuite) TestRespondActivityTaskFailedMaxAttemptsExceeded() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)

	for i := 0; i < conditionalRetryCount; i++ {
		ms := createMutableState(msBuilder)
		gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Identity:  &identity,
		},
	})
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedSuccess() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "failed"
	failDetails := []byte("fail details.")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&history.RespondActivityTaskFailedRequest{
		DomainUUID: common.StringPtr(domainID),
		FailedRequest: &workflow.RespondActivityTaskFailedRequest{
			TaskToken: taskToken,
			Reason:    &failReason,
			Details:   failDetails,
			Identity:  &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(9), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(8))
	s.True(ok)
	s.Equal(int32(200), di.DecisionTimeout)
	s.Equal(int64(8), di.ScheduleID)
	s.Equal(emptyEventID, di.StartedID)
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_NoTimer() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)

	// No HeartBeat timer running.
	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(&history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(domainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   detais,
		},
	})
	s.Nil(err)
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_TimerRunning() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	// HeartBeat timer running.
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(&history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(domainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   detais,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(7), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Scheduled() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(&history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Started() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)
	msBuilder.AddActivityTaskCancelRequestedEvent(decisionCompletedEvent.GetEventId(), activityID, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(&history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: taskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(10), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)

	s.True(executionBuilder.HasPendingDecisionTask())
	di, ok := executionBuilder.GetPendingDecision(int64(9))
	s.True(ok)
	s.Equal(int32(200), di.DecisionTimeout)
	s.Equal(int64(9), di.ScheduleID)
	s.Equal(emptyEventID, di.StartedID)
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_NotScheduled() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	activityID := "activity1_id"

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)
	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(7), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Scheduled() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 6,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1)
	decisionScheduled2Event, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, decisionScheduled2Event.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(11), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(7), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_NoHeartBeat() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 7,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 0)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)
	decisionScheduled2Event, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, decisionScheduled2Event.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(11), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(8), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.False(executionBuilder.HasPendingDecisionTask())

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	activityTaskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(&history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(domainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(hbResponse.GetCancelRequested())

	// Try cancelling the request.
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(&history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)

	executionBuilder = s.getBuilder(domainID, we)
	s.Equal(int64(13), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(8), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.True(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Success() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 7,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent, _ := addActivityTaskScheduledEvent(msBuilder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(msBuilder, activityScheduledEvent.GetEventId(), tl, identity)
	decisionScheduled2Event, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, decisionScheduled2Event.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(11), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(8), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.False(executionBuilder.HasPendingDecisionTask())

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	activityTaskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(&history.RecordActivityTaskHeartbeatRequest{
		DomainUUID: common.StringPtr(domainID),
		HeartbeatRequest: &workflow.RecordActivityTaskHeartbeatRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(hbResponse.GetCancelRequested())

	// Try cancelling the request.
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(&history.RespondActivityTaskCanceledRequest{
		DomainUUID: common.StringPtr(domainID),
		CancelRequest: &workflow.RespondActivityTaskCanceledRequest{
			TaskToken: activityTaskToken,
			Identity:  &identity,
			Details:   []byte("details"),
		},
	})
	s.Nil(err)

	executionBuilder = s.getBuilder(domainID, we)
	s.Equal(int64(13), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(8), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.True(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestUserTimer_RespondDecisionTaskCompleted() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 6,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	decisionStartedEvent := addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(msBuilder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	addTimerStartedEvent(msBuilder, decisionCompletedEvent.GetEventId(), timerID, 10)
	decision2ScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, decision2ScheduledEvent.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CancelTimer),
		CancelTimerDecisionAttributes: &workflow.CancelTimerDecisionAttributes{
			TimerId: common.StringPtr(timerID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(10), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(7), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) TestCancelTimer_RespondDecisionTaskCompleted_NoStartTimer() {
	domainID := "domainId"
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("wId"),
		RunId:      common.StringPtr("rId"),
	}
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: we.GetWorkflowId(),
		RunID:      we.GetRunId(),
		ScheduleID: 2,
	})
	identity := "testIdentity"
	timerID := "t1"

	msBuilder := newMutableStateBuilder(bark.NewLoggerFromLogrus(log.New()))
	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(msBuilder, we, "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent, _ := addDecisionTaskScheduledEvent(msBuilder)
	addDecisionTaskStartedEvent(msBuilder, decisionScheduledEvent.GetEventId(), tl, identity)

	ms := createMutableState(msBuilder)
	gwmsResponse := &persistence.GetWorkflowExecutionResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CancelTimer),
		CancelTimerDecisionAttributes: &workflow.CancelTimerDecisionAttributes{
			TimerId: common.StringPtr(timerID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&history.RespondDecisionTaskCompletedRequest{
		DomainUUID: common.StringPtr(domainID),
		CompleteRequest: &workflow.RespondDecisionTaskCompletedRequest{
			TaskToken:        taskToken,
			Decisions:        decisions,
			ExecutionContext: []byte("context"),
			Identity:         &identity,
		},
	})
	s.Nil(err)

	executionBuilder := s.getBuilder(domainID, we)
	s.Equal(int64(6), executionBuilder.executionInfo.NextEventID)
	s.Equal(int64(3), executionBuilder.executionInfo.LastProcessedEvent)
	s.Equal(persistence.WorkflowStateRunning, executionBuilder.executionInfo.State)
	s.False(executionBuilder.HasPendingDecisionTask())
}

func (s *engineSuite) getBuilder(domainID string, we workflow.WorkflowExecution) *mutableStateBuilder {
	context, release, err := s.mockHistoryEngine.historyCache.getOrCreateWorkflowExecution(domainID, we)
	if err != nil {
		return nil
	}
	defer release()

	return context.msBuilder
}

func (s *engineSuite) getActivityScheduledEvent(msBuilder *mutableStateBuilder,
	scheduleID int64) *workflow.HistoryEvent {

	ai, ok := msBuilder.GetActivityInfo(scheduleID)
	if !ok {
		return nil
	}

	event, err := s.eventSerializer.Deserialize(ai.ScheduledEvent)
	if err != nil {
		s.logger.Errorf("Error Deserializing Event: %v", err)
	}

	return event
}

func (s *engineSuite) getActivityStartedEvent(msBuilder *mutableStateBuilder,
	scheduleID int64) *workflow.HistoryEvent {

	ai, ok := msBuilder.GetActivityInfo(scheduleID)
	if !ok {
		return nil
	}

	event, err := s.eventSerializer.Deserialize(ai.StartedEvent)
	if err != nil {
		s.logger.Errorf("Error Deserializing Event: %v", err)
	}

	return event
}

func (s *engineSuite) printHistory(builder *mutableStateBuilder) string {
	history, err := builder.hBuilder.Serialize()
	if err != nil {
		s.logger.Errorf("Error serializing history: %v", err)
		return ""
	}

	//s.logger.Info(string(history))
	return history.String()
}

func addWorkflowExecutionStartedEvent(builder *mutableStateBuilder, workflowExecution workflow.WorkflowExecution,
	workflowType, taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32,
	identity string) *workflow.HistoryEvent {
	e := builder.AddWorkflowExecutionStartedEvent("domainId", workflowExecution, &workflow.StartWorkflowExecutionRequest{
		WorkflowId:   common.StringPtr(workflowExecution.GetWorkflowId()),
		WorkflowType: &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		TaskList:     &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:        input,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
		Identity:                            common.StringPtr(identity),
	})

	return e
}

func addDecisionTaskScheduledEvent(builder *mutableStateBuilder) (*workflow.HistoryEvent, *decisionInfo) {
	return builder.AddDecisionTaskScheduledEvent()
}

func addDecisionTaskStartedEvent(builder *mutableStateBuilder, scheduleID int64, taskList,
	identity string) *workflow.HistoryEvent {
	return addDecisionTaskStartedEventWithRequestID(builder, scheduleID, uuid.New(), taskList, identity)
}

func addDecisionTaskStartedEventWithRequestID(builder *mutableStateBuilder, scheduleID int64, requestID string,
	taskList, identity string) *workflow.HistoryEvent {
	e := builder.AddDecisionTaskStartedEvent(scheduleID, requestID, &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return e
}

func addDecisionTaskCompletedEvent(builder *mutableStateBuilder, scheduleID, startedID int64, context []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         common.StringPtr(identity),
	})

	return e
}

func addActivityTaskScheduledEvent(builder *mutableStateBuilder, decisionCompletedID int64, activityID, activityType,
	taskList string, input []byte, timeout, queueTimeout, hearbeatTimeout int32) (*workflow.HistoryEvent,
	*persistence.ActivityInfo) {
	return builder.AddActivityTaskScheduledEvent(decisionCompletedID, &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId:   common.StringPtr(activityID),
		ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityType)},
		TaskList:     &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:        input,
		ScheduleToCloseTimeoutSeconds: common.Int32Ptr(timeout),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(queueTimeout),
		HeartbeatTimeoutSeconds:       common.Int32Ptr(hearbeatTimeout),
	})
}

func addActivityTaskStartedEvent(builder *mutableStateBuilder, scheduleID int64,
	taskList, identity string) *workflow.HistoryEvent {
	ai, _ := builder.GetActivityInfo(scheduleID)
	return builder.AddActivityTaskStartedEvent(ai, scheduleID, uuid.New(), &workflow.PollForActivityTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})
}

func addActivityTaskCompletedEvent(builder *mutableStateBuilder, scheduleID, startedID int64, result []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflow.RespondActivityTaskCompletedRequest{
		Result_:  result,
		Identity: common.StringPtr(identity),
	})

	return e
}

func addActivityTaskFailedEvent(builder *mutableStateBuilder, scheduleID, startedID int64, reason string, details []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflow.RespondActivityTaskFailedRequest{
		Reason:   common.StringPtr(reason),
		Details:  details,
		Identity: common.StringPtr(identity),
	})

	return e
}

func addTimerStartedEvent(builder *mutableStateBuilder, decisionCompletedEventID int64, timerID string,
	timeOut int64) (*workflow.HistoryEvent, *persistence.TimerInfo) {
	return builder.AddTimerStartedEvent(decisionCompletedEventID,
		&workflow.StartTimerDecisionAttributes{
			TimerId:                   common.StringPtr(timerID),
			StartToFireTimeoutSeconds: common.Int64Ptr(timeOut),
		})
}

func addCompleteWorkflowEvent(builder *mutableStateBuilder, decisionCompletedEventID int64,
	result []byte) *workflow.HistoryEvent {
	e := builder.AddCompletedWorkflowEvent(decisionCompletedEventID, &workflow.CompleteWorkflowExecutionDecisionAttributes{
		Result_: result,
	})

	return e
}

func createMutableState(builder *mutableStateBuilder) *persistence.WorkflowMutableState {
	info := copyWorkflowExecutionInfo(builder.executionInfo)
	activityInfos := make(map[int64]*persistence.ActivityInfo)
	for id, info := range builder.pendingActivityInfoIDs {
		activityInfos[id] = copyActivityInfo(info)
	}
	timerInfos := make(map[string]*persistence.TimerInfo)
	for id, info := range builder.pendingTimerInfoIDs {
		timerInfos[id] = copyTimerInfo(info)
	}
	return &persistence.WorkflowMutableState{
		ExecutionInfo: info,
		ActivitInfos:  activityInfos,
		TimerInfos:    timerInfos,
	}
}

func copyWorkflowExecutionInfo(sourceInfo *persistence.WorkflowExecutionInfo) *persistence.WorkflowExecutionInfo {
	return &persistence.WorkflowExecutionInfo{
		DomainID:             sourceInfo.DomainID,
		WorkflowID:           sourceInfo.WorkflowID,
		RunID:                sourceInfo.RunID,
		TaskList:             sourceInfo.TaskList,
		WorkflowTypeName:     sourceInfo.WorkflowTypeName,
		DecisionTimeoutValue: sourceInfo.DecisionTimeoutValue,
		ExecutionContext:     sourceInfo.ExecutionContext,
		State:                sourceInfo.State,
		NextEventID:          sourceInfo.NextEventID,
		LastProcessedEvent:   sourceInfo.LastProcessedEvent,
		LastUpdatedTimestamp: sourceInfo.LastUpdatedTimestamp,
		CreateRequestID:      sourceInfo.CreateRequestID,
		DecisionScheduleID:   sourceInfo.DecisionScheduleID,
		DecisionStartedID:    sourceInfo.DecisionStartedID,
		DecisionRequestID:    sourceInfo.DecisionRequestID,
		DecisionTimeout:      sourceInfo.DecisionTimeout,
	}
}

func copyActivityInfo(sourceInfo *persistence.ActivityInfo) *persistence.ActivityInfo {
	return &persistence.ActivityInfo{
		ScheduleID:             sourceInfo.ScheduleID,
		ScheduledEvent:         sourceInfo.ScheduledEvent,
		StartedID:              sourceInfo.StartedID,
		StartedEvent:           sourceInfo.StartedEvent,
		ActivityID:             sourceInfo.ActivityID,
		RequestID:              sourceInfo.RequestID,
		Details:                sourceInfo.Details,
		ScheduleToStartTimeout: sourceInfo.ScheduleToStartTimeout,
		ScheduleToCloseTimeout: sourceInfo.ScheduleToCloseTimeout,
		StartToCloseTimeout:    sourceInfo.StartToCloseTimeout,
		HeartbeatTimeout:       sourceInfo.HeartbeatTimeout,
		CancelRequested:        sourceInfo.CancelRequested,
		CancelRequestID:        sourceInfo.CancelRequestID,
	}
}

func copyTimerInfo(sourceInfo *persistence.TimerInfo) *persistence.TimerInfo {
	return &persistence.TimerInfo{
		TimerID:    sourceInfo.TimerID,
		StartedID:  sourceInfo.StartedID,
		ExpiryTime: sourceInfo.ExpiryTime,
		TaskID:     sourceInfo.TaskID,
	}
}
