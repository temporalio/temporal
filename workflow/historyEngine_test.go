package workflow

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/persistence"
	"code.uber.internal/devexp/minions/persistence/mocks"
)

type (
	engineSuite struct {
		suite.Suite
		TestBase
		engine            Engine
		builder           *historyBuilder
		mockHistoryEngine *historyEngineImpl
		mockTaskMgr       *mocks.TaskManager
		mockExecutionMgr  *mocks.ExecutionManager
		logger            bark.Logger
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

	s.SetupWorkflowStore()
	s.engine = NewWorkflowEngine(s.WorkflowMgr, s.TaskMgr, bark.NewLoggerFromLogrus(log.New()))

	s.logger = bark.NewLoggerFromLogrus(log.New())
	s.builder = newHistoryBuilder(nil, s.logger)
}

func (s *engineSuite) TearDownSuite() {
	s.TearDownWorkflowStore()
}

func (s *engineSuite) SetupTest() {
	s.mockTaskMgr = &mocks.TaskManager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}

	mockShard := &shardContextImpl{
		shardInfo:              &persistence.ShardInfo{ShardID: 1, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber: 1,
	}

	txProcessor := newTransferQueueProcessor(mockShard, s.mockExecutionMgr, s.mockTaskMgr, s.logger)
	tracker := newPendingTaskTracker(mockShard, txProcessor, s.logger)
	s.mockHistoryEngine = &historyEngineImpl{
		shard:            mockShard,
		executionManager: s.mockExecutionMgr,
		txProcessor:      txProcessor,
		tracker:          tracker,
		logger:           s.logger,
		tokenSerializer:  newJSONTaskTokenSerializer(),
	}
}

func (s *engineSuite) TearDownTest() {
	s.mockTaskMgr.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
}

func (s *engineSuite) TestStartWorkflowExecution() {
	id := "engine-start-workflow-test"
	wt := "engine-start-workflow-test-type"
	tl := "engine-start-workflow-test-tasklist"
	identity := "worker1"

	workflowType := workflow.NewWorkflowType()
	workflowType.Name = common.StringPtr(wt)

	taskList := workflow.NewTaskList()
	taskList.Name = common.StringPtr(tl)

	request := &workflow.StartWorkflowExecutionRequest{
		WorkflowId:   common.StringPtr(id),
		WorkflowType: workflowType,
		TaskList:     taskList,
		Input:        nil,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(100),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(1),
		Identity:                            common.StringPtr(identity),
	}

	_, err0 := s.engine.StartWorkflowExecution(request)
	s.Nil(err0)

	we1, err1 := s.engine.StartWorkflowExecution(request)
	s.NotNil(err1)
	s.IsType(workflow.NewWorkflowExecutionAlreadyStartedError(), err1)
	log.Infof("Start workflow execution failed with error: %v", err1.Error())
	s.Nil(we1)
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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfGetExecutionFailed() {
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondDecisionTaskCompletedUpdateExecutionFailed() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)

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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	startedEvent := addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)
	addDecisionTaskCompletedEvent(builder, scheduleEvent.GetEventId(), startedEvent.GetEventId(), nil, identity)

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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondDecisionTaskCompletedIfTaskNotStarted() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	addDecisionTaskScheduledEvent(builder, tl, 100)

	history, _ := builder.Serialize()
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning,
			NextEventID: builder.nextEventID, LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true},
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
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

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	taskToken, _ := json.Marshal(&taskToken{
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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse1, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse2, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err, string(history))
	updatedBuilder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	input := []byte("input")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	scheduleEvent := addDecisionTaskScheduledEvent(builder, tl, 100)
	addDecisionTaskStartedEvent(builder, scheduleEvent.GetEventId(), tl, identity)

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
		info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
			LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
		wfResponse := &persistence.GetWorkflowExecutionResponse{
			ExecutionInfo: info,
		}
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

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	taskToken, _ := json.Marshal(&taskToken{
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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err, string(history))
	updatedBuilder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	taskToken, _ := json.Marshal(&taskToken{
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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err, string(history))
	updatedBuilder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	input := []byte("input")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err)
	updatedBuilder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	workflowResult := []byte("success")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("DeleteWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err)
	updatedBuilder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"
	context := []byte("context")
	details := []byte("fail workflow details")
	reason := "fail workflow reason"

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("DeleteWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondDecisionTaskCompleted(&workflow.RespondDecisionTaskCompletedRequest{
		TaskToken:        taskToken,
		Decisions:        decisions,
		ExecutionContext: context,
		Identity:         &identity,
	})
	s.Nil(err)
	updatedBuilder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskCompletedIfGetExecutionFailed() {
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskCompletedUpdateExecutionFailed() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()

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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)

	history, _ := builder.Serialize()
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
			LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true},
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()

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
	taskToken, _ := json.Marshal(&taskToken{
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

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse1, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse2, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Result_:   activity1Result,
		Identity:  &identity,
	})
	s.Nil(err, string(history))
	updatedBuilder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	for i := 0; i < conditionalRetryCount; i++ {
		info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
			LastProcessedEvent: decisionStartedEvent.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
		wfResponse := &persistence.GetWorkflowExecutionResponse{
			ExecutionInfo: info,
		}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")
	activityResult := []byte("activity result")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskCompleted(&workflow.RespondActivityTaskCompletedRequest{
		TaskToken: taskToken,
		Result_:   activityResult,
		Identity:  &identity,
	})
	s.Nil(err)
	updatedBuilder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, &workflow.EntityNotExistsError{}).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedIfGetExecutionFailed() {
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 2,
	})
	identity := "testIdentity"

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(nil, errors.New("FAILED")).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.EqualError(err, "FAILED")
}

func (s *engineSuite) TestRespondActivityTaskFailedUpdateExecutionFailed() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()

	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

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
	taskToken, _ := json.Marshal(&taskToken{
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

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	history, _ := builder.Serialize()
	info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
		LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: info,
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()

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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)

	history, _ := builder.Serialize()
	wfResponse := &persistence.GetWorkflowExecutionResponse{
		ExecutionInfo: &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
			LastProcessedEvent: emptyEventID, LastUpdatedTimestamp: time.Time{}, DecisionPending: true},
	}

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Identity:  &identity,
	})
	s.NotNil(err)
	s.IsType(&workflow.EntityNotExistsError{}, err)
}

func (s *engineSuite) TestRespondActivityTaskFailedConflictOnUpdate() {
	tl := "testTaskList"
	taskToken, _ := json.Marshal(&taskToken{
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

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse1, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(&persistence.ConditionFailedError{}).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse2, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Reason:    &failReason,
		Details:   details,
		Identity:  &identity,
	})
	s.Nil(err, string(history))
	updatedBuilder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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
	taskToken, _ := json.Marshal(&taskToken{
		WorkflowID: "wId",
		RunID:      "rId",
		ScheduleID: 5,
	})
	identity := "testIdentity"
	activityID := "activity1_id"
	activityType := "activity_type1"
	activityInput := []byte("input1")

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
	addWorkflowExecutionStartedEvent(builder, "wId", "wType", tl, []byte("input"), 100, 200, identity)
	decisionScheduledEvent := addDecisionTaskScheduledEvent(builder, tl, 30)
	decisionStartedEvent := addDecisionTaskStartedEvent(builder, decisionScheduledEvent.GetEventId(), tl, identity)
	decisionCompletedEvent := addDecisionTaskCompletedEvent(builder, decisionScheduledEvent.GetEventId(),
		decisionStartedEvent.GetEventId(), nil, identity)
	activityScheduledEvent := addActivityTaskScheduledEvent(builder, decisionCompletedEvent.GetEventId(), activityID,
		activityType, tl, activityInput, 100, 10, 5)
	addActivityTaskStartedEvent(builder, activityScheduledEvent.GetEventId(), tl, identity)

	history, _ := builder.Serialize()
	for i := 0; i < conditionalRetryCount; i++ {
		info := &persistence.WorkflowExecutionInfo{WorkflowID: "wId", RunID: "rId", TaskList: tl, History: history, ExecutionContext: nil, State: persistence.WorkflowStateRunning, NextEventID: builder.nextEventID,
			LastProcessedEvent: decisionStartedEvent.GetEventId(), LastUpdatedTimestamp: time.Time{}, DecisionPending: true}
		wfResponse := &persistence.GetWorkflowExecutionResponse{
			ExecutionInfo: info,
		}
		s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
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
	taskToken, _ := json.Marshal(&taskToken{
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

	builder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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

	s.mockExecutionMgr.On("GetWorkflowExecution", mock.Anything).Return(wfResponse, nil).Once()
	s.mockExecutionMgr.On("UpdateWorkflowExecution", mock.Anything).Return(nil).Once()

	err := s.mockHistoryEngine.RespondActivityTaskFailed(&workflow.RespondActivityTaskFailedRequest{
		TaskToken: taskToken,
		Reason:    &failReason,
		Details:   failDetails,
		Identity:  &identity,
	})
	s.Nil(err)
	updatedBuilder := newHistoryBuilder(nil, bark.NewLoggerFromLogrus(log.New()))
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
