package history

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	engineSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		builder            *historyBuilder
		mockHistoryEngine  *historyEngineImpl
		mockMatchingClient *mocks.MatchingClient
		mockExecutionMgr   *mocks.ExecutionManager
		mockShardManager   *mocks.ShardManager
		shardClosedCh      chan int
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
	s.builder = newHistoryBuilder(s.logger)
}

func (s *engineSuite) TearDownSuite() {

}

func (s *engineSuite) SetupTest() {
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
	s.mockHistoryEngine = h
}

func (s *engineSuite) TearDownTest() {
	s.mockMatchingClient.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedInvalidToken() {
	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        invalidToken,
		Decisions:        nil,
		ExecutionContext: nil,
		Identity:         &identity,
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfNoExecution() {
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, 2, 3, uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfGetExecutionFailed() {
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, 2, 3, uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedUpdateExecutionFailed() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	startedEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)

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
			DecisionPending:      true},
	}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, scheduleEvent.GetEventId(), startedEvent.GetEventId(), uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskCompleted() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	startedEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)
	addDecisionTaskCompletedEvent(builder, scheduleEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)

	ms := &persistence.WorkflowMutableState{ActivitInfos: make(map[int64]*persistence.ActivityInfo)}
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskNotStarted() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(builder, tl, 100)

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, 2, emptyEventID, "reqId", 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedConflictOnUpdate() {
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

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 25, 200, identity)
	decisionScheduledEvent1 := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(builder, decisionScheduledEvent1.GetEventId(), tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent1.GetEventId(),
		decisionStartedEvent1.GetEventId(), nil, identity)
	activity1ScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent1.GetEventId(), activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent1.GetEventId(), activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(builder, activity1ScheduledEvent.GetEventId(), tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(builder, activity2ScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskCompletedEvent(builder, activity1ScheduledEvent.GetEventId(),
		activity1StartedEvent.GetEventId(), activity1Result, identity)
	decisionScheduledEvent2 := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent2 := addDecisionTaskStartedEvent(builder, decisionScheduledEvent2.GetEventId(), tl, identity)

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

	history, _ := builder.Serialize()
	info1 := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: decisionStartedEvent1.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse1 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info1,
	}

	addActivityTaskCompletedEvent(builder, activity2ScheduledEvent.GetEventId(),
		activity2StartedEvent.GetEventId(), activity2Result, identity)
	history2, _ := builder.Serialize()
	info2 := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history2, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: decisionStartedEvent1.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse2 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info2,
	}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, decisionScheduledEvent2.GetEventId(), decisionStartedEvent2.GetEventId(), uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	ms2 := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms2, decisionScheduledEvent2.GetEventId(), decisionStartedEvent2.GetEventId(), uuid.New(), 1)
	gwmsResponse2 := &persistence.GetWorkflowMutableStateResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse1, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse2, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err, string(history))
	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info2)
	s.Equal(int64(16), info2.NextEventID)
	s.Equal(decisionStartedEvent2.GetEventId(), info2.LastProcessedEvent)
	s.Equal(context, info2.ExecutionContext)

	completedEvent := updatedBuilder.GetEvent(13)
	s.Equal(decisionScheduledEvent2.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetScheduledEventId())
	s.Equal(decisionStartedEvent2.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetStartedEventId())
	s.Equal(context, completedEvent.GetDecisionTaskCompletedEventAttributes().GetExecutionContext())
	s.Equal(identity, completedEvent.GetDecisionTaskCompletedEventAttributes().GetIdentity())

	activity3Attributes := updatedBuilder.GetEvent(14).GetActivityTaskScheduledEventAttributes()
	s.Equal(activity3ID, activity3Attributes.GetActivityId())
	s.Equal(activity3Type, activity3Attributes.GetActivityType().GetName())
	s.Equal(completedEvent.GetEventId(), activity3Attributes.GetDecisionTaskCompletedEventId())
	s.Equal(tl, activity3Attributes.GetTaskList().GetName())
	s.Equal(activity3Input, activity3Attributes.GetInput())
	s.Equal(int32(100), activity3Attributes.GetScheduleToCloseTimeoutSeconds())
	s.Equal(int32(10), activity3Attributes.GetScheduleToStartTimeoutSeconds())
	s.Equal(int32(50), activity3Attributes.GetStartToCloseTimeoutSeconds())
	s.Equal(int32(5), activity3Attributes.GetHeartbeatTimeoutSeconds())

	decisionScheduledEventAttributes := updatedBuilder.GetEvent(15).GetDecisionTaskScheduledEventAttributes()
	s.Equal(tl, decisionScheduledEventAttributes.GetTaskList().GetName())
	s.Equal(int32(200), decisionScheduledEventAttributes.GetStartToCloseTimeoutSeconds())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedMaxAttemptsExceeded() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	input := []byte("input")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	startEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)

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

	history, _ := builder.Serialize()
	for i := 0; i < conditionalRetryCount; i++ {
		info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history,
			ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
			LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
		wfResponse := &persistence.GetWorkflowExecutionResponse{
			ExecutionInfo: info,
		}

		ms := &persistence.WorkflowMutableState{}
		addDecisionToMutableState(ms, scheduleEvent.GetEventId(), startEvent.GetEventId(), uuid.New(), 1)
		gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.NotNil(err)
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedCompleteWorkflowFailed() {
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
	workflowResult := []byte("workflow result")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 25, 200, identity)
	decisionScheduledEvent1 := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(builder, decisionScheduledEvent1.GetEventId(), tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent1.GetEventId(),
		decisionStartedEvent1.GetEventId(), nil, identity)
	activity1ScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent1.GetEventId(), activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent1.GetEventId(), activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(builder, activity1ScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskStartedEvent(builder, activity2ScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskCompletedEvent(builder, activity1ScheduledEvent.GetEventId(),
		activity1StartedEvent.GetEventId(), activity1Result, identity)
	decisionScheduledEvent2 := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent2 := addDecisionTaskStartedEvent(builder, decisionScheduledEvent2.GetEventId(), tl, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: decisionScheduledEvent2.GetEventId(),
	})

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
		CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
			Result_: workflowResult,
		},
	}}

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: decisionStartedEvent1.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, decisionScheduledEvent2.GetEventId(), decisionStartedEvent2.GetEventId(), uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err, string(history))
	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(14), info.NextEventID)
	s.Equal(decisionStartedEvent2.GetEventId(), info.LastProcessedEvent)
	s.Equal(context, info.ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, info.State)

	completedEvent := updatedBuilder.GetEvent(12)
	s.Equal(decisionScheduledEvent2.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetScheduledEventId())
	s.Equal(decisionStartedEvent2.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetStartedEventId())
	s.Equal(context, completedEvent.GetDecisionTaskCompletedEventAttributes().GetExecutionContext())
	s.Equal(identity, completedEvent.GetDecisionTaskCompletedEventAttributes().GetIdentity())

	attributes := updatedBuilder.GetEvent(13).GetCompleteWorkflowExecutionFailedEventAttributes()
	s.Equal(workflow.WorkflowCompleteFailedCause_UNHANDLED_DECISION, attributes.GetCause())
	s.Equal(completedEvent.GetEventId(), attributes.GetDecisionTaskCompletedEventId())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedFailWorkflowFailed() {
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
	reason := "workflow fail reason"
	details := []byte("workflow fail details")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 25, 200, identity)
	decisionScheduledEvent1 := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(builder, decisionScheduledEvent1.GetEventId(), tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent1.GetEventId(),
		decisionStartedEvent1.GetEventId(), nil, identity)
	activity1ScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent1.GetEventId(), activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent1.GetEventId(), activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(builder, activity1ScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskStartedEvent(builder, activity2ScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskCompletedEvent(builder, activity1ScheduledEvent.GetEventId(),
		activity1StartedEvent.GetEventId(), activity1Result, identity)
	decisionScheduledEvent2 := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent2 := addDecisionTaskStartedEvent(builder, decisionScheduledEvent2.GetEventId(), tl, identity)

	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: decisionScheduledEvent2.GetEventId(),
	})

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_FailWorkflowExecution),
		FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
			Reason:  &reason,
			Details: details,
		},
	}}

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: decisionStartedEvent1.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, decisionScheduledEvent2.GetEventId(), decisionStartedEvent2.GetEventId(), uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err, string(history))
	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(14), info.NextEventID)
	s.Equal(decisionStartedEvent2.GetEventId(), info.LastProcessedEvent)
	s.Equal(context, info.ExecutionContext)
	s.Equal(persistence.WorkflowStateRunning, info.State)

	completedEvent := updatedBuilder.GetEvent(12)
	s.Equal(decisionScheduledEvent2.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetScheduledEventId())
	s.Equal(decisionStartedEvent2.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetStartedEventId())
	s.Equal(context, completedEvent.GetDecisionTaskCompletedEventAttributes().GetExecutionContext())
	s.Equal(identity, completedEvent.GetDecisionTaskCompletedEventAttributes().GetIdentity())

	attributes := updatedBuilder.GetEvent(13).GetCompleteWorkflowExecutionFailedEventAttributes()
	s.Equal(workflow.WorkflowCompleteFailedCause_UNHANDLED_DECISION, attributes.GetCause())
	s.Equal(completedEvent.GetEventId(), attributes.GetDecisionTaskCompletedEventId())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedSingleActivityScheduledDecision() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	input := []byte("input")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	startedEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

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

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, scheduleEvent.GetEventId(), startedEvent.GetEventId(), uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err)
	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(6), info.NextEventID)
	s.Equal(int64(3), info.LastProcessedEvent)
	s.Equal(context, info.ExecutionContext)

	completedEvent := updatedBuilder.GetEvent(4)
	s.Equal(scheduleEvent.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetScheduledEventId())
	s.Equal(startedEvent.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetStartedEventId())
	s.Equal(context, completedEvent.GetDecisionTaskCompletedEventAttributes().GetExecutionContext())
	s.Equal(identity, completedEvent.GetDecisionTaskCompletedEventAttributes().GetIdentity())

	activity1Attributes := updatedBuilder.GetEvent(5).GetActivityTaskScheduledEventAttributes()
	s.Equal("activity1", activity1Attributes.GetActivityId())
	s.Equal("activity_type1", activity1Attributes.GetActivityType().GetName())
	s.Equal(completedEvent.GetEventId(), activity1Attributes.GetDecisionTaskCompletedEventId())
	s.Equal(tl, activity1Attributes.GetTaskList().GetName())
	s.Equal(input, activity1Attributes.GetInput())
	s.Equal(int32(100), activity1Attributes.GetScheduleToCloseTimeoutSeconds())
	s.Equal(int32(10), activity1Attributes.GetScheduleToStartTimeoutSeconds())
	s.Equal(int32(50), activity1Attributes.GetStartToCloseTimeoutSeconds())
	s.Equal(int32(5), activity1Attributes.GetHeartbeatTimeoutSeconds())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedCompleteWorkflowSuccess() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	workflowResult := []byte("success")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	startedEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CompleteWorkflowExecution),
		CompleteWorkflowExecutionDecisionAttributes: &workflow.CompleteWorkflowExecutionDecisionAttributes{
			Result_: workflowResult,
		},
	}}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, scheduleEvent.GetEventId(), startedEvent.GetEventId(), uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err)
	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(6), info.NextEventID)
	s.Equal(int64(3), info.LastProcessedEvent)
	s.Equal(context, info.ExecutionContext)

	completedEvent := updatedBuilder.GetEvent(4)
	s.Equal(scheduleEvent.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetScheduledEventId())
	s.Equal(startedEvent.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetStartedEventId())
	s.Equal(context, completedEvent.GetDecisionTaskCompletedEventAttributes().GetExecutionContext())
	s.Equal(identity, completedEvent.GetDecisionTaskCompletedEventAttributes().GetIdentity())

	workflowCompletedAttributes := updatedBuilder.GetEvent(5).GetWorkflowExecutionCompletedEventAttributes()
	s.Equal(workflowResult, workflowCompletedAttributes.GetResult_())
	s.Equal(completedEvent.GetEventId(), workflowCompletedAttributes.GetDecisionTaskCompletedEventId())
}

func (s *engineSuite) TestRespondDecisionTaskCompletedFailWorkflowSuccess() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	details := []byte("fail workflow details")
	reason := "fail workflow reason"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	startedEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_FailWorkflowExecution),
		FailWorkflowExecutionDecisionAttributes: &workflow.FailWorkflowExecutionDecisionAttributes{
			Reason:  &reason,
			Details: details,
		},
	}}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, scheduleEvent.GetEventId(), startedEvent.GetEventId(), uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err)
	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(6), info.NextEventID)
	s.Equal(int64(3), info.LastProcessedEvent)
	s.Equal(context, info.ExecutionContext)

	completedEvent := updatedBuilder.GetEvent(4)
	s.Equal(scheduleEvent.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetScheduledEventId())
	s.Equal(startedEvent.GetEventId(), completedEvent.GetDecisionTaskCompletedEventAttributes().GetStartedEventId())
	s.Equal(context, completedEvent.GetDecisionTaskCompletedEventAttributes().GetExecutionContext())
	s.Equal(identity, completedEvent.GetDecisionTaskCompletedEventAttributes().GetIdentity())

	attributes := updatedBuilder.GetEvent(5).GetWorkflowExecutionFailedEventAttributes()
	s.Equal(reason, attributes.GetReason())
	s.Equal(details, attributes.GetDetails())
	s.Equal(completedEvent.GetEventId(), attributes.GetDecisionTaskCompletedEventId())
}

func (s *engineSuite) TestRespondActivityTaskCompletedInvalidToken() {
	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: invalidToken,
		Result_:   nil,
		Identity:  &identity,
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfNoExecution() {
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, 2, 3, "act-id1", 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfGetExecutionFailed() {
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, 2, 3, "act-id1", 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedUpdateExecutionFailed() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), activityID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Result_:   activityResult,
		Identity:  &identity,
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskCompleted() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskCompletedEvent(builder, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(),
		activityResult, identity)
	addDecisionTaskScheduledEvent(builder, tl, 200)

	ms := &persistence.WorkflowMutableState{ActivitInfos: make(map[int64]*persistence.ActivityInfo)}
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Result_:   activityResult,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfTaskNotStarted() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), emptyEventID, activityID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Result_:   activityResult,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedConflictOnUpdate() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
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
	activity2Result := []byte("activity2_result")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 25, 200, identity)
	decisionScheduledEvent1 := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(builder, decisionScheduledEvent1.GetEventId(), tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent1.GetEventId(),
		decisionStartedEvent1.GetEventId(), nil, identity)
	activity1ScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent1.GetEventId(), activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent1.GetEventId(), activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(builder, activity1ScheduledEvent.GetEventId(), tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(builder, activity2ScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info1 := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: decisionStartedEvent1.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse1 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info1,
	}

	addActivityTaskCompletedEvent(builder, activity2ScheduledEvent.GetEventId(),
		activity2StartedEvent.GetEventId(), activity2Result, identity)
	addDecisionTaskScheduledEvent(builder, tl, 200)
	history2, _ := builder.Serialize()
	info2 := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history2, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: decisionStartedEvent1.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse2 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info2,
	}

	ms1 := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms1, activity1ScheduledEvent.GetEventId(), activity1StartedEvent.GetEventId(), activity1ID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse1 := &persistence.GetWorkflowMutableStateResponse{State: ms1}

	ms2 := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms2, activity1ScheduledEvent.GetEventId(), activity1StartedEvent.GetEventId(), activity1ID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse2 := &persistence.GetWorkflowMutableStateResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse1, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse2, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Result_:   activity1Result,
		Identity:  &identity,
	})
	s.Nil(err, string(history))
	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info2)
	s.Equal(int64(12), info2.NextEventID)
	s.Equal(int64(3), info2.LastProcessedEvent)

	completedEvent := updatedBuilder.GetEvent(11)
	s.Equal(workflow.EventType_ActivityTaskCompleted, completedEvent.GetEventType())
	attributes := completedEvent.GetActivityTaskCompletedEventAttributes()
	s.Equal(activity1ScheduledEvent.GetEventId(), attributes.GetScheduledEventId())
	s.Equal(activity1StartedEvent.GetEventId(), attributes.GetStartedEventId())
	s.Equal(activity1Result, attributes.GetResult_())
	s.Equal(identity, attributes.GetIdentity())
}

func (s *engineSuite) TestRespondActivityTaskCompletedMaxAttemptsExceeded() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	for i := 0; i < conditionalRetryCount; i++ {
		info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
			LastProcessedEvent: decisionStartedEvent.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
		wfResponse := &persistence.GetWorkflowExecutionResponse{
			ExecutionInfo: info,
		}

		ms := &persistence.WorkflowMutableState{}
		addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), activityID, 1, 1, 1, 1, emptyEventID)
		gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
		s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Result_:   activityResult,
		Identity:  &identity,
	})
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedSuccess() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), activityID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Result_:   activityResult,
		Identity:  &identity,
	})
	s.Nil(err)
	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(9), info.NextEventID)
	s.Equal(int64(3), info.LastProcessedEvent)

	completedEvent := updatedBuilder.GetEvent(7)
	s.Equal(workflow.EventType_ActivityTaskCompleted, completedEvent.GetEventType())
	attributes := completedEvent.GetActivityTaskCompletedEventAttributes()
	s.Equal(activityScheduledEvent.GetEventId(), attributes.GetScheduledEventId())
	s.Equal(activityStartedEvent.GetEventId(), attributes.GetStartedEventId())
	s.Equal(activityResult, attributes.GetResult_())
	s.Equal(identity, attributes.GetIdentity())

	decisionEvent := updatedBuilder.GetEvent(8)
	s.Equal(workflow.EventType_DecisionTaskScheduled, decisionEvent.GetEventType())
	decisionAttributes := decisionEvent.GetDecisionTaskScheduledEventAttributes()
	s.Equal(tl, decisionAttributes.GetTaskList().GetName())
	s.Equal(int32(200), decisionAttributes.GetStartToCloseTimeoutSeconds())
}

func (s *engineSuite) TestRespondActivityTaskFailedInvalidToken() {
	invalidToken, _ := json.Marshal("bad token")
	identity := "testIdentity"

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: invalidToken,
		Identity:  &identity,
	})

	s.NotNil(err)
	s.IsType(&workflow.BadRequestError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfNoExecution() {
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, 2, 3, "act-id1", 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfGetExecutionFailed() {
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, 2, 3, "act-id1", 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailedUpdateExecutionFailed() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()

	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), activityID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskCompleted() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "fail reason"
	details := []byte("fail details")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)
	addActivityTaskFailedEvent(builder, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(),
		failReason, details, identity)
	addDecisionTaskScheduledEvent(builder, tl, 200)

	ms := &persistence.WorkflowMutableState{ActivitInfos: make(map[int64]*persistence.ActivityInfo)}
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Reason:    &failReason,
		Details:   details,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfTaskNotStarted() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), emptyEventID, activityID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedConflictOnUpdate() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
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

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 25, 200, identity)
	decisionScheduledEvent1 := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent1 := addDecisionTaskStartedEvent(builder, decisionScheduledEvent1.GetEventId(), tl, identity)
	decisionCompletedEvent1 := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent1.GetEventId(),
		decisionStartedEvent1.GetEventId(), nil, identity)
	activity1ScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent1.GetEventId(), activity1ID,
		activity1Type, tl, activity1Input, 100, 10, 5)
	activity2ScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent1.GetEventId(), activity2ID,
		activity2Type, tl, activity2Input, 100, 10, 5)
	activity1StartedEvent := addActivityTaskStartedEvent(builder, activity1ScheduledEvent.GetEventId(), tl, identity)
	activity2StartedEvent := addActivityTaskStartedEvent(builder, activity2ScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info1 := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: decisionStartedEvent1.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse1 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info1,
	}

	addActivityTaskCompletedEvent(builder, activity2ScheduledEvent.GetEventId(),
		activity2StartedEvent.GetEventId(), activity2Result, identity)
	addDecisionTaskScheduledEvent(builder, tl, 200)
	history2, _ := builder.Serialize()
	info2 := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history2, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: decisionStartedEvent1.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse2 := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info2,
	}

	ms1 := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms1, activity1ScheduledEvent.GetEventId(), activity1StartedEvent.GetEventId(), activity1ID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse1:= &persistence.GetWorkflowMutableStateResponse{State: ms1}

	ms2 := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms2, activity1ScheduledEvent.GetEventId(), activity1StartedEvent.GetEventId(), activity1ID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse2 := &persistence.GetWorkflowMutableStateResponse{State: ms2}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse1, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse1, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse2, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse2, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Reason:    &failReason,
		Details:   details,
		Identity:  &identity,
	})
	s.Nil(err, string(history))
	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info2)
	s.Equal(int64(12), info2.NextEventID)
	s.Equal(int64(3), info2.LastProcessedEvent)

	completedEvent := updatedBuilder.GetEvent(11)
	s.Equal(workflow.EventType_ActivityTaskFailed, completedEvent.GetEventType())
	attributes := completedEvent.GetActivityTaskFailedEventAttributes()
	s.Equal(activity1ScheduledEvent.GetEventId(), attributes.GetScheduledEventId())
	s.Equal(activity1StartedEvent.GetEventId(), attributes.GetStartedEventId())
	s.Equal(failReason, attributes.GetReason())
	s.Equal(details, attributes.GetDetails())
	s.Equal(identity, attributes.GetIdentity())
}

func (s *engineSuite) TestRespondActivityTaskFailedMaxAttemptsExceeded() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	for i := 0; i < conditionalRetryCount; i++ {
		info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
			LastProcessedEvent: decisionStartedEvent.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
		wfResponse := &persistence.GetWorkflowExecutionResponse{
			ExecutionInfo: info,
		}

		ms := &persistence.WorkflowMutableState{}
		addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), activityID, 1, 1, 1, 1, emptyEventID)
		gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
		s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
		s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()
	}

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.Equal(ErrMaxAttemptsExceeded, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedSuccess() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	failReason := "failed"
	failDetails := []byte("fail details.")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), activityID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Reason:    &failReason,
		Details:   failDetails,
		Identity:  &identity,
	})
	s.Nil(err)
	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(9), info.NextEventID)
	s.Equal(int64(3), info.LastProcessedEvent)

	completedEvent := updatedBuilder.GetEvent(7)
	s.Equal(workflow.EventType_ActivityTaskFailed, completedEvent.GetEventType())
	attributes := completedEvent.GetActivityTaskFailedEventAttributes()
	s.Equal(activityScheduledEvent.GetEventId(), attributes.GetScheduledEventId())
	s.Equal(activityStartedEvent.GetEventId(), attributes.GetStartedEventId())
	s.Equal(failReason, attributes.GetReason())
	s.Equal(failDetails, attributes.GetDetails())
	s.Equal(identity, attributes.GetIdentity())

	decisionEvent := updatedBuilder.GetEvent(8)
	s.Equal(workflow.EventType_DecisionTaskScheduled, decisionEvent.GetEventType())
	decisionAttributes := decisionEvent.GetDecisionTaskScheduledEventAttributes()
	s.Equal(tl, decisionAttributes.GetTaskList().GetName())
	s.Equal(int32(200), decisionAttributes.GetStartToCloseTimeoutSeconds())
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_NoTimer() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1)
	addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	// No HeartBeat timer running.
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(&persistence.GetWorkflowMutableStateResponse{}, nil).Once()

	detais := []byte("details")

	_, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(&workflow.RecordActivityTaskHeartbeatRequest{
		TaskToken: taskToken,
		Identity:  &identity,
		Details:   detais,
	})
	s.NotNil(err)
}

func (s *engineSuite) TestRecordActivityTaskHeartBeatSuccess_TimerRunning() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))

	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), activityID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	// HeartBeat timer running.
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	detais := []byte("details")

	response, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(&workflow.RecordActivityTaskHeartbeatRequest{
		TaskToken: taskToken,
		Identity:  &identity,
		Details:   detais,
	})
	s.Nil(err)
	s.NotNil(response)

	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(7), info.NextEventID)
	s.Equal(int64(3), info.LastProcessedEvent)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Scheduled() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))

	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1)

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), emptyEventID, "act-id1", 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(&workflow.RespondActivityTaskCanceledRequest{
		TaskToken: taskToken,
		Identity:  &identity,
		Details:   []byte("details"),
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCanceled_Started() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))

	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)
	actCancelRequestEvent := builder.AddActivityTaskCancelRequestedEvent(decisionCompletedEvent.GetEventId(), activityID)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), "act-id1", 1, 1, 1, 1, actCancelRequestEvent.GetEventId())
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCanceled(&workflow.RespondActivityTaskCanceledRequest{
		TaskToken: taskToken,
		Identity:  &identity,
		Details:   []byte("details"),
	})
	s.Nil(err)

	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(10), info.NextEventID)
	s.Equal(int64(3), info.LastProcessedEvent)

	updatedEvent := updatedBuilder.GetEvent(8)
	s.Equal(workflow.EventType_ActivityTaskCanceled, updatedEvent.GetEventType())
	s.Equal(activityScheduledEvent.GetEventId(), updatedEvent.GetActivityTaskCanceledEventAttributes().GetScheduledEventId())
	s.Equal(activityStartedEvent.GetEventId(), updatedEvent.GetActivityTaskCanceledEventAttributes().GetStartedEventId())
	s.Equal(actCancelRequestEvent.GetEventId(), updatedEvent.GetActivityTaskCanceledEventAttributes().GetLatestCancelRequestedEventId())
	s.Equal("details", string(updatedEvent.GetActivityTaskCanceledEventAttributes().GetDetails()))
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_NotScheduled() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	activityID := "activity1_id"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))

	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, decisionScheduledEvent.GetEventId(), decisionStartedEvent.GetEventId(), uuid.New(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: []byte("context"),
		Identity:         &identity,
	})
	s.Nil(err)

	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(7), info.NextEventID)
	s.Equal(int64(3), info.LastProcessedEvent)

	updatedEvent := updatedBuilder.GetEvent(6)
	s.Equal(workflow.EventType_RequestCancelActivityTaskFailed, updatedEvent.GetEventType())
	s.Equal(activityID, updatedEvent.GetRequestCancelActivityTaskFailedEventAttributes().GetActivityId())
	s.Equal(activityCancelationMsgActivityIDUnknown, updatedEvent.GetRequestCancelActivityTaskFailedEventAttributes().GetCause())
	s.Equal(int64(4), updatedEvent.GetRequestCancelActivityTaskFailedEventAttributes().GetDecisionTaskCompletedEventId())
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Scheduled() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 6,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))

	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1)
	decisionScheduled2Event := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStarted2Event := addDecisionTaskStartedEvent(builder, decisionScheduled2Event.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, decisionScheduled2Event.GetEventId(), decisionStarted2Event.GetEventId(), uuid.New(), 1)
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), emptyEventID, activityID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: []byte("context"),
		Identity:         &identity,
	})
	s.Nil(err)

	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(11), info.NextEventID)
	s.Equal(int64(7), info.LastProcessedEvent)

	updatedEvent := updatedBuilder.GetEvent(10)
	s.Equal(workflow.EventType_ActivityTaskCanceled, updatedEvent.GetEventType())
	s.Equal(activityScheduledEvent.GetEventId(), updatedEvent.GetActivityTaskCanceledEventAttributes().GetScheduledEventId())
	s.Equal(emptyEventID, updatedEvent.GetActivityTaskCanceledEventAttributes().GetStartedEventId())
	s.Equal(int64(9), updatedEvent.GetActivityTaskCanceledEventAttributes().GetLatestCancelRequestedEventId())
	s.Equal(activityCancelationMsgActivityNotStarted, string(updatedEvent.GetActivityTaskCanceledEventAttributes().GetDetails()))
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_NoHeartBeat() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 7,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))

	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 0 /* heart beat timeout */)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)
	decisionScheduled2Event := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStart2Event := addDecisionTaskStartedEvent(builder, decisionScheduled2Event.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, decisionScheduled2Event.GetEventId(), decisionStart2Event.GetEventId(), uuid.New(), 1)
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), activityID, 1, 1, 1, 0, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: []byte("context"),
		Identity:         &identity,
	})
	s.Nil(err)

	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(11), info.NextEventID)
	s.Equal(int64(8), info.LastProcessedEvent)

	updatedEvent := updatedBuilder.GetEvent(10)
	s.Equal(workflow.EventType_ActivityTaskCancelRequested, updatedEvent.GetEventType())

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	activityTaskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(&workflow.RecordActivityTaskHeartbeatRequest{
		TaskToken: activityTaskToken,
		Identity:  &identity,
		Details:   []byte("details"),
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(hbResponse.GetCancelRequested())

	// Try cancelling the request.
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(&workflow.RespondActivityTaskCanceledRequest{
		TaskToken: activityTaskToken,
		Identity:  &identity,
		Details:   []byte("details"),
	})
	s.Nil(err)

	updatedBuilder = newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(13), info.NextEventID)
	s.Equal(int64(8), info.LastProcessedEvent)

	updatedEvent = updatedBuilder.GetEvent(11)
	s.Equal(workflow.EventType_ActivityTaskCanceled, updatedEvent.GetEventType())
	s.Equal(activityScheduledEvent.GetEventId(), updatedEvent.GetActivityTaskCanceledEventAttributes().GetScheduledEventId())
	s.Equal(activityStartedEvent.GetEventId(), updatedEvent.GetActivityTaskCanceledEventAttributes().GetStartedEventId())
	s.Equal(int64(10), updatedEvent.GetActivityTaskCanceledEventAttributes().GetLatestCancelRequestedEventId())
	s.Equal("details", string(updatedEvent.GetActivityTaskCanceledEventAttributes().GetDetails()))
}

func (s *engineSuite) TestRequestCancel_RespondDecisionTaskCompleted_Success() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 7,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))

	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 1 /* heart beat timeout */)
	activityStartedEvent := addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)
	decisionScheduled2Event := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStarted2Event := addDecisionTaskStartedEvent(builder, decisionScheduled2Event.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, decisionScheduled2Event.GetEventId(), decisionStarted2Event.GetEventId(), uuid.New(), 1)
	addActivityToMutableState(ms, activityScheduledEvent.GetEventId(), activityStartedEvent.GetEventId(), activityID, 1, 1, 1, 1, emptyEventID)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_RequestCancelActivityTask),
		RequestCancelActivityTaskDecisionAttributes: &workflow.RequestCancelActivityTaskDecisionAttributes{
			ActivityId: common.StringPtr(activityID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: []byte("context"),
		Identity:         &identity,
	})
	s.Nil(err)

	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(11), info.NextEventID)
	s.Equal(int64(8), info.LastProcessedEvent)

	updatedEvent := updatedBuilder.GetEvent(10)
	s.Equal(workflow.EventType_ActivityTaskCancelRequested, updatedEvent.GetEventType())
	s.Equal(activityID, updatedEvent.GetActivityTaskCancelRequestedEventAttributes().GetActivityId())
	s.Equal(int64(9), updatedEvent.GetActivityTaskCancelRequestedEventAttributes().GetDecisionTaskCompletedEventId())

	// Try recording activity heartbeat
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	activityTaskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})

	hbResponse, err := s.mockHistoryEngine.RecordActivityTaskHeartbeat(&workflow.RecordActivityTaskHeartbeatRequest{
		TaskToken: activityTaskToken,
		Identity:  &identity,
		Details:   []byte("details"),
	})
	s.Nil(err)
	s.NotNil(hbResponse)
	s.True(hbResponse.GetCancelRequested())

	// Try cancelling the request.
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err = s.mockHistoryEngine.RespondActivityTaskCanceled(&workflow.RespondActivityTaskCanceledRequest{
		TaskToken: activityTaskToken,
		Identity:  &identity,
		Details:   []byte("details"),
	})
	s.Nil(err)

	updatedBuilder = newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(13), info.NextEventID)
	s.Equal(int64(8), info.LastProcessedEvent)

	updatedEvent = updatedBuilder.GetEvent(11)
	s.Equal(workflow.EventType_ActivityTaskCanceled, updatedEvent.GetEventType())
	s.Equal(activityScheduledEvent.GetEventId(), updatedEvent.GetActivityTaskCanceledEventAttributes().GetScheduledEventId())
	s.Equal(activityStartedEvent.GetEventId(), updatedEvent.GetActivityTaskCanceledEventAttributes().GetStartedEventId())
	s.Equal(int64(10), updatedEvent.GetActivityTaskCanceledEventAttributes().GetLatestCancelRequestedEventId())
	s.Equal("details", string(updatedEvent.GetActivityTaskCanceledEventAttributes().GetDetails()))
}

func (s *engineSuite) TestUserTimer_RespondDecisionTaskCompleted() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 6,
	})
	identity := "testIdentity"
	timerID := "t1"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))

	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	timerStartedEvent := addTimerStartedEvent(builder, decisionCompletedEvent.GetEventId(), timerID, 10)
	decision2ScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decision2StartedEvent := addDecisionTaskStartedEvent(builder, decision2ScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, decision2ScheduledEvent.GetEventId(), decision2StartedEvent.GetEventId(), uuid.New(), 1)
	addUserTimerToMutableState(ms, timerID, timerStartedEvent.GetEventId(), 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CancelTimer),
		CancelTimerDecisionAttributes: &workflow.CancelTimerDecisionAttributes{
			TimerId: common.StringPtr(timerID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: []byte("context"),
		Identity:         &identity,
	})
	s.Nil(err)

	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(10), info.NextEventID)
	s.Equal(int64(7), info.LastProcessedEvent)

	updatedEvent := updatedBuilder.GetEvent(9)
	s.Equal(workflow.EventType_TimerCanceled, updatedEvent.GetEventType())
	s.Equal(timerID, updatedEvent.GetTimerCanceledEventAttributes().GetTimerId())
}

func (s *engineSuite) TestCancelTimer_RespondDecisionTaskCompleted_NoStartTimer() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&common.TaskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	timerID := "t1"

	builder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))

	// Verify cancel timer with a start event.
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	ms := &persistence.WorkflowMutableState{}
	addDecisionToMutableState(ms, decisionScheduledEvent.GetEventId(), decisionStartedEvent.GetEventId(), uuid.New(), 1)
	addUserTimerToMutableState(ms, "t1-diff", emptyEventID, 1)
	gwmsResponse := &persistence.GetWorkflowMutableStateResponse{State: ms}

	decisions := []*workflow.Decision{{
		DecisionType: workflow.DecisionTypePtr(workflow.DecisionType_CancelTimer),
		CancelTimerDecisionAttributes: &workflow.CancelTimerDecisionAttributes{
			TimerId: common.StringPtr(timerID),
		},
	}}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("GetWorkflowMutableState", mock.Anything).Return(gwmsResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: []byte("context"),
		Identity:         &identity,
	})
	s.Nil(err)

	updatedBuilder := newHistoryBuilder(bark.NewLoggerFromLogrus(log.New()))
	updatedBuilder.loadExecutionInfo(info)
	s.Equal(int64(6), info.NextEventID)
	s.Equal(int64(3), info.LastProcessedEvent)

	updatedEvent := updatedBuilder.GetEvent(5)
	s.Equal(workflow.EventType_CancelTimerFailed, updatedEvent.GetEventType())
	s.Equal(timerID, updatedEvent.GetCancelTimerFailedEventAttributes().GetTimerId())
	s.Equal(timerCancelationMsgTimerIDUnknown, updatedEvent.GetCancelTimerFailedEventAttributes().GetCause())
}

func addWorkflowExecutionStartedEvent(builder *historyBuilder, workflowID, workflowType, taskList string, input []byte,
	executionStartToCloseTimeout, taskStartToCloseTimeout int32, identity string) *workflow.HistoryEvent {
	e := builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{
		WorkflowId:   common.StringPtr(workflowID),
		WorkflowType: &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		TaskList:     &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:        input,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
		Identity:                            common.StringPtr(identity),
	})

	return e
}

func addDecisionTaskScheduledEvent(builder *historyBuilder, taskList string,
	timeout int32) *workflow.HistoryEvent {
	e := builder.AddDecisionTaskScheduledEvent(taskList, timeout)

	return e
}

func addDecisionTaskStartedEvent(builder *historyBuilder, scheduleID int64,
	taskList, identity string) *workflow.HistoryEvent {
	return addDecisionTaskStartedEventWithRequestID(builder, scheduleID, uuid.New(), taskList, identity)
}

func addDecisionTaskStartedEventWithRequestID(builder *historyBuilder, scheduleID int64, requestID string,
	taskList, identity string) *workflow.HistoryEvent {
	e := builder.AddDecisionTaskStartedEvent(scheduleID, requestID, &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return e
}

func addDecisionTaskCompletedEvent(builder *historyBuilder, scheduleID, startedID int64, context []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         common.StringPtr(identity),
	})

	return e
}

func addActivityTaskScheduledEvent(builder *historyBuilder, decisionCompletedID int64, activityID, activityType,
	taskList string, input []byte, timeout, queueTimeout, hearbeatTimeout int32) *workflow.HistoryEvent {
	e := builder.AddActivityTaskScheduledEvent(decisionCompletedID, &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId:   common.StringPtr(activityID),
		ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityType)},
		TaskList:     &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:        input,
		ScheduleToCloseTimeoutSeconds: common.Int32Ptr(timeout),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(queueTimeout),
		HeartbeatTimeoutSeconds:       common.Int32Ptr(hearbeatTimeout),
	})

	return e
}

func addActivityTaskStartedEvent(builder *historyBuilder, scheduleID int64,
	taskList, identity string) *workflow.HistoryEvent {
	e := builder.AddActivityTaskStartedEvent(scheduleID, uuid.New(), &workflow.PollForActivityTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return e
}

func addActivityTaskCompletedEvent(builder *historyBuilder, scheduleID, startedID int64, result []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflow.RespondActivityTaskCompletedRequest{
		Result_:  result,
		Identity: common.StringPtr(identity),
	})

	return e
}

func addActivityTaskFailedEvent(builder *historyBuilder, scheduleID, startedID int64, reason string, details []byte,
	identity string) *workflow.HistoryEvent {
	e := builder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflow.RespondActivityTaskFailedRequest{
		Reason:   common.StringPtr(reason),
		Details:  details,
		Identity: common.StringPtr(identity),
	})

	return e
}

func addActivityToMutableState(wms *persistence.WorkflowMutableState, scheduleID, startedID int64, activityID string,
	scheduleToStartTimeout, scheduleToCloseTimeout, startToCloseTimeout, heartBeatTimeout int32, cancelRequestedID int64) {
	if wms.ActivitInfos == nil {
		wms.ActivitInfos = make(map[int64]*persistence.ActivityInfo)
	}
	cancelRequested := cancelRequestedID != emptyEventID
	wms.ActivitInfos[scheduleID] = &persistence.ActivityInfo{
		ScheduleID: scheduleID, StartedID: startedID, ActivityID: activityID,
		ScheduleToStartTimeout: scheduleToStartTimeout, ScheduleToCloseTimeout: scheduleToCloseTimeout, StartToCloseTimeout: startToCloseTimeout,
		HeartbeatTimeout: heartBeatTimeout, Details: []byte("details-old"),
		CancelRequested: cancelRequested, CancelRequestID: cancelRequestedID}
}

func addDecisionToMutableState(wms *persistence.WorkflowMutableState, scheduleID, startedID int64,
	requestID string, startToCloseTimeout int32) {
	if wms.ActivitInfos == nil {
		wms.ActivitInfos = make(map[int64]*persistence.ActivityInfo)
	}
	wms.Decision = &persistence.DecisionInfo{
		ScheduleID: scheduleID,
		StartedID: startedID,
		RequestID: requestID,
		StartToCloseTimeout: startToCloseTimeout,
	}
}

func addUserTimerToMutableState(wms *persistence.WorkflowMutableState, timerID string, startedID int64,
	delayInSec int32) {
	if wms.TimerInfos == nil {
		wms.TimerInfos = make(map[string]*persistence.TimerInfo)
	}
	expiryTime := time.Now().Add(time.Duration(delayInSec) * time.Second)
	wms.TimerInfos[timerID] = &persistence.TimerInfo{
		TimerID:    timerID,
		StartedID:  startedID,
		ExpiryTime: expiryTime,
		TaskID:     emptyTimerID}
}

func addTimerStartedEvent(builder *historyBuilder, decisionCompletedEventID int64, timerID string, timeOut int64) *workflow.HistoryEvent {
	e := builder.AddTimerStartedEvent(decisionCompletedEventID,
		&workflow.StartTimerDecisionAttributes{
			TimerId:                   common.StringPtr(timerID),
			StartToFireTimeoutSeconds: common.Int64Ptr(timeOut),
		})

	return e
}

func addCompleteWorkflowEvent(builder *historyBuilder, decisionCompletedEventID int64,
	result []byte) *workflow.HistoryEvent {
	e := builder.AddCompletedWorkflowEvent(decisionCompletedEventID, &workflow.CompleteWorkflowExecutionDecisionAttributes{
		Result_: result,
	})

	return e
}