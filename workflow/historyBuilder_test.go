package workflow

import (
	"testing"

	log "github.com/Sirupsen/logrus"

	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
)

type (
	historyBuilderSuite struct {
		suite.Suite
		builder *historyBuilder
		logger  bark.Logger
	}
)

func TestHistoryBuilderSuite(t *testing.T) {
	s := new(historyBuilderSuite)
	suite.Run(t, s)
}

func (s *historyBuilderSuite) SetupTest() {
	s.logger = bark.NewLoggerFromLogrus(log.New())
	s.builder = newHistoryBuilder(s.logger)
}

func (s *historyBuilderSuite) TestHistoryBuilderDynamicSuccess() {
	id := "dynamic-historybuilder-success-test"
	wt := "dynamic-historybuilder-success-type"
	tl := "dynamic-historybuilder-success-tasklist"
	identity := "dynamic-historybuilder-success-worker"
	input := []byte("dynamic-historybuilder-success-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(id, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.builder.nextEventID)

	decisionScheduledEvent := s.addDecisionTaskScheduledEvent(tl, taskTimeout)
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent, 2, tl, taskTimeout)
	s.Equal(int64(3), s.builder.nextEventID)
	decisionRunning0, decisionStartedID0 := s.builder.isDecisionTaskRunning(2)
	s.True(decisionRunning0)
	s.Equal(emptyEventID, decisionStartedID0)
	s.Equal(emptyEventID, s.builder.previousDecisionStartedEvent())

	decisionStartedEvent := s.addDecisionTaskStartedEvent(2, tl, identity)
	s.validateDecisionTaskStartedEvent(decisionStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.builder.nextEventID)
	decisionRunning1, decisionStartedID1 := s.builder.isDecisionTaskRunning(2)
	s.True(decisionRunning1)
	s.Equal(int64(3), decisionStartedID1)
	s.Equal(emptyEventID, s.builder.previousDecisionStartedEvent())

	decisionContext := []byte("dynamic-historybuilder-success-context")
	decisionCompletedEvent := s.addDecisionTaskCompletedEvent(2, 3, decisionContext, identity)
	s.validateDecisionTaskCompletedEvent(decisionCompletedEvent, 4, 2, 3, decisionContext, identity)
	s.Equal(int64(5), s.builder.nextEventID)
	decisionRunning2, _ := s.builder.isDecisionTaskRunning(2)
	s.False(decisionRunning2)
	s.Equal(int64(3), s.builder.previousDecisionStartedEvent())

	activityTaskList := "dynamic-historybuilder-success-activity-tasklist"
	activityTimeout := int32(60)
	queueTimeout := int32(20)
	hearbeatTimeout := int32(10)

	activity1ID := "activity1"
	activity1Type := "dynamic-historybuilder-success-activity1-type"
	activity1Input := []byte("dynamic-historybuilder-success-activity1-input")
	activity1Result := []byte("dynamic-historybuilder-success-activity1-result")
	activity1ScheduledEvent := s.addActivityTaskScheduledEvent(4, activity1ID, activity1Type, activityTaskList,
		activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.validateActivityTaskScheduledEvent(activity1ScheduledEvent, 5, 4, activity1ID, activity1Type, activityTaskList,
		activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(6), s.builder.nextEventID)
	activity1Running0, activity1Started0 := s.builder.isActivityTaskRunning(5)
	s.True(activity1Running0)
	s.Equal(emptyEventID, activity1Started0)
	s.Equal(int64(3), s.builder.previousDecisionStartedEvent())

	activity2ID := "activity2"
	activity2Type := "dynamic-historybuilder-success-activity2-type"
	activity2Input := []byte("dynamic-historybuilder-success-activity2-input")
	activity2Reason := "dynamic-historybuilder-success-activity2-failed"
	activity2Details := []byte("dynamic-historybuilder-success-activity2-callstack")
	activity2ScheduledEvent := s.addActivityTaskScheduledEvent(4, activity2ID, activity2Type, activityTaskList,
		activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.validateActivityTaskScheduledEvent(activity2ScheduledEvent, 6, 4, activity2ID, activity2Type, activityTaskList,
		activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(7), s.builder.nextEventID)
	activity2Running0, activity2Started0 := s.builder.isActivityTaskRunning(6)
	s.True(activity2Running0)
	s.Equal(emptyEventID, activity2Started0)
	s.Equal(int64(3), s.builder.previousDecisionStartedEvent())

	activityStartedEvent := s.addActivityTaskStartedEvent(5, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activityStartedEvent, 7, 5, identity)
	s.Equal(int64(8), s.builder.nextEventID)
	activity1Running1, activity1Started1 := s.builder.isActivityTaskRunning(5)
	s.True(activity1Running1)
	s.Equal(int64(7), activity1Started1)
	s.Equal(int64(3), s.builder.previousDecisionStartedEvent())

	activityCompletedEvent := s.addActivityTaskCompletedEvent(5, 7, activity1Result, identity)
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, 8, 5, 7, activity1Result, identity)
	s.Equal(int64(9), s.builder.nextEventID)
	activity1Running2, _ := s.builder.isActivityTaskRunning(5)
	s.False(activity1Running2)
	s.Equal(int64(3), s.builder.previousDecisionStartedEvent())

	decisionScheduledEvent2 := s.addDecisionTaskScheduledEvent(tl, taskTimeout)
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent2, 9, tl, taskTimeout)
	s.Equal(int64(10), s.builder.nextEventID)
	decisionRunning3, decisionStartedID3 := s.builder.isDecisionTaskRunning(9)
	s.True(decisionRunning3)
	s.Equal(emptyEventID, decisionStartedID3)
	s.Equal(int64(3), s.builder.previousDecisionStartedEvent())

	activity2StartedEvent := s.addActivityTaskStartedEvent(6, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activity2StartedEvent, 10, 6, identity)
	s.Equal(int64(11), s.builder.nextEventID)
	activity2Running1, activity2Started1 := s.builder.isActivityTaskRunning(6)
	s.True(activity2Running1)
	s.Equal(int64(10), activity2Started1)
	s.Equal(int64(3), s.builder.previousDecisionStartedEvent())

	activity2FailedEvent := s.addActivityTaskFailedEvent(6, 10, activity2Reason, activity2Details, identity)
	s.validateActivityTaskFailedEvent(activity2FailedEvent, 11, 6, 10, activity2Reason, activity2Details, identity)
	s.Equal(int64(12), s.builder.nextEventID)
	activity2Running2, _ := s.builder.isActivityTaskRunning(6)
	s.False(activity2Running2)
	s.Equal(int64(3), s.builder.previousDecisionStartedEvent())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowStartFailures() {
	id := "historybuilder-workflowstart-failures-test"
	wt := "historybuilder-workflowstart-failures-type"
	tl := "historybuilder-workflowstart-failures-tasklist"
	identity := "historybuilder-workflowstart-failures-worker"
	input := []byte("historybuilder-workflowstart-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(id, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.builder.nextEventID)

	decisionScheduledEvent := s.addDecisionTaskScheduledEvent(tl, taskTimeout)
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent, 2, tl, taskTimeout)
	s.Equal(int64(3), s.builder.nextEventID)
	decisionRunning0, decisionStartedID0 := s.builder.isDecisionTaskRunning(2)
	s.True(decisionRunning0)
	s.Equal(emptyEventID, decisionStartedID0)
	s.Equal(emptyEventID, s.builder.previousDecisionStartedEvent())

	workflowStartedEvent2 := s.addWorkflowExecutionStartedEvent(id, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Nil(workflowStartedEvent2)
	s.Equal(int64(3), s.builder.nextEventID, s.printHistory())
	decisionRunning1, decisionStartedID1 := s.builder.isDecisionTaskRunning(2)
	s.True(decisionRunning1)
	s.Equal(emptyEventID, decisionStartedID1)
	s.Equal(emptyEventID, s.builder.previousDecisionStartedEvent())
}

func (s *historyBuilderSuite) TestHistoryBuilderDecisionScheduledFailures() {
	id := "historybuilder-decisionscheduled-failures-test"
	wt := "historybuilder-decisionscheduled-failures-type"
	tl := "historybuilder-decisionscheduled-failures-tasklist"
	identity := "historybuilder-decisionscheduled-failures-worker"
	input := []byte("historybuilder-decisionscheduled-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(id, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.builder.nextEventID)

	decisionScheduledEvent := s.addDecisionTaskScheduledEvent(tl, taskTimeout)
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent, 2, tl, taskTimeout)
	s.Equal(int64(3), s.builder.nextEventID)
	decisionRunning0, decisionStartedID0 := s.builder.isDecisionTaskRunning(2)
	s.True(decisionRunning0)
	s.Equal(emptyEventID, decisionStartedID0)
	s.Equal(emptyEventID, s.builder.previousDecisionStartedEvent())

	decisionScheduledEvent2 := s.addDecisionTaskScheduledEvent(tl, taskTimeout)
	s.Nil(decisionScheduledEvent2)
	s.Equal(int64(3), s.builder.nextEventID)
	decisionRunning1, decisionStartedID1 := s.builder.isDecisionTaskRunning(2)
	s.True(decisionRunning1)
	s.Equal(emptyEventID, decisionStartedID1)
	s.Equal(emptyEventID, s.builder.previousDecisionStartedEvent())
}

func (s *historyBuilderSuite) TestHistoryBuilderDecisionStartedFailures() {
	id := "historybuilder-decisionstarted-failures-test"
	wt := "historybuilder-decisionstarted-failures-type"
	tl := "historybuilder-decisionstarted-failures-tasklist"
	identity := "historybuilder-decisionstarted-failures-worker"
	input := []byte("historybuilder-decisionstarted-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(id, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.builder.nextEventID)

	decisionStartedEvent := s.addDecisionTaskStartedEvent(2, tl, identity)
	s.Nil(decisionStartedEvent)
	s.Equal(int64(2), s.builder.nextEventID)
	decisionRunning1, _ := s.builder.isDecisionTaskRunning(2)
	s.False(decisionRunning1)
	s.Equal(emptyEventID, s.builder.previousDecisionStartedEvent())

	decisionScheduledEvent := s.addDecisionTaskScheduledEvent(tl, taskTimeout)
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent, 2, tl, taskTimeout)
	s.Equal(int64(3), s.builder.nextEventID)
	decisionRunning0, decisionStartedID0 := s.builder.isDecisionTaskRunning(2)
	s.True(decisionRunning0)
	s.Equal(emptyEventID, decisionStartedID0)
	s.Equal(emptyEventID, s.builder.previousDecisionStartedEvent())

	decisionStartedEvent1 := s.addDecisionTaskStartedEvent(100, tl, identity)
	s.Nil(decisionStartedEvent1)
	s.Equal(int64(3), s.builder.nextEventID)
	decisionRunning2, decisionStartedID2 := s.builder.isDecisionTaskRunning(2)
	s.True(decisionRunning2)
	s.Equal(emptyEventID, decisionStartedID2)
	s.Equal(emptyEventID, s.builder.previousDecisionStartedEvent())

	decisionStartedEvent2 := s.addDecisionTaskStartedEvent(2, tl, identity)
	s.validateDecisionTaskStartedEvent(decisionStartedEvent2, 3, 2, identity)
	s.Equal(int64(4), s.builder.nextEventID)
	decisionRunning3, decisionStartedID3 := s.builder.isDecisionTaskRunning(2)
	s.True(decisionRunning3)
	s.Equal(int64(3), decisionStartedID3)
	s.Equal(emptyEventID, s.builder.previousDecisionStartedEvent())
}

func (s *historyBuilderSuite) addWorkflowExecutionStartedEvent(workflowID, workflowType, taskList string, input []byte,
	executionStartToCloseTimeout, taskStartToCloseTimeout int32, identity string) *workflow.HistoryEvent {
	e := s.builder.AddWorkflowExecutionStartedEvent(&workflow.StartWorkflowExecutionRequest{
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

func (s *historyBuilderSuite) addDecisionTaskScheduledEvent(taskList string, timeout int32) *workflow.HistoryEvent {
	e := s.builder.AddDecisionTaskScheduledEvent(taskList, timeout)

	return e
}

func (s *historyBuilderSuite) addDecisionTaskStartedEvent(scheduleID int64,
	taskList, identity string) *workflow.HistoryEvent {
	e := s.builder.AddDecisionTaskStartedEvent(scheduleID, &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) addDecisionTaskCompletedEvent(scheduleID, startedID int64, context []byte,
	identity string) *workflow.HistoryEvent {
	e := s.builder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) addActivityTaskScheduledEvent(decisionCompletedID int64, activityID, activityType,
	taskList string, input []byte, timeout, queueTimeout, hearbeatTimeout int32) *workflow.HistoryEvent {
	e := s.builder.AddActivityTaskScheduledEvent(decisionCompletedID, &workflow.ScheduleActivityTaskDecisionAttributes{
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

func (s *historyBuilderSuite) addActivityTaskStartedEvent(scheduleID int64,
	taskList, identity string) *workflow.HistoryEvent {
	e := s.builder.AddActivityTaskStartedEvent(scheduleID, &workflow.PollForActivityTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) addActivityTaskCompletedEvent(scheduleID, startedID int64, result []byte,
	identity string) *workflow.HistoryEvent {
	e := s.builder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflow.RespondActivityTaskCompletedRequest{
		Result_:  result,
		Identity: common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) addActivityTaskFailedEvent(scheduleID, startedID int64, reason string, details []byte,
	identity string) *workflow.HistoryEvent {
	e := s.builder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflow.RespondActivityTaskFailedRequest{
		Reason:   common.StringPtr(reason),
		Details:  details,
		Identity: common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) validateWorkflowExecutionStartedEvent(event *workflow.HistoryEvent, workflowType,
	taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventType_WorkflowExecutionStarted, event.GetEventType())
	s.Equal(firstEventID, event.GetEventId())
	attributes := event.GetWorkflowExecutionStartedEventAttributes()
	s.NotNil(attributes)
	s.Equal(workflowType, attributes.GetWorkflowType().GetName())
	s.Equal(taskList, attributes.GetTaskList().GetName())
	s.Equal(input, attributes.GetInput())
	s.Equal(executionStartToCloseTimeout, attributes.GetExecutionStartToCloseTimeoutSeconds())
	s.Equal(taskStartToCloseTimeout, attributes.GetTaskStartToCloseTimeoutSeconds())
	s.Equal(identity, attributes.GetIdentity())
}

func (s *historyBuilderSuite) validateDecisionTaskScheduledEvent(event *workflow.HistoryEvent, eventID int64,
	taskList string, timeout int32) {
	s.NotNil(event)
	s.Equal(workflow.EventType_DecisionTaskScheduled, event.GetEventType())
	s.Equal(eventID, event.GetEventId())
	attributes := event.GetDecisionTaskScheduledEventAttributes()
	s.NotNil(attributes)
	s.Equal(taskList, attributes.GetTaskList().GetName())
	s.Equal(timeout, attributes.GetStartToCloseTimeoutSeconds())
}

func (s *historyBuilderSuite) validateDecisionTaskStartedEvent(event *workflow.HistoryEvent, eventID, scheduleID int64,
	identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventType_DecisionTaskStarted, event.GetEventType())
	s.Equal(eventID, event.GetEventId())
	attributes := event.GetDecisionTaskStartedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.GetScheduledEventId())
	s.Equal(identity, attributes.GetIdentity())
}

func (s *historyBuilderSuite) validateDecisionTaskCompletedEvent(event *workflow.HistoryEvent, eventID,
	scheduleID, startedID int64, context []byte, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventType_DecisionTaskCompleted, event.GetEventType())
	s.Equal(eventID, event.GetEventId())
	attributes := event.GetDecisionTaskCompletedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.GetScheduledEventId())
	s.Equal(startedID, attributes.GetStartedEventId())
	s.Equal(context, attributes.GetExecutionContext())
	s.Equal(identity, attributes.GetIdentity())
}

func (s *historyBuilderSuite) validateActivityTaskScheduledEvent(event *workflow.HistoryEvent, eventID, decisionID int64,
	activityID, activityType, taskList string, input []byte, timeout, queueTimeout, hearbeatTimeout int32) {
	s.NotNil(event)
	s.Equal(workflow.EventType_ActivityTaskScheduled, event.GetEventType())
	s.Equal(eventID, event.GetEventId())
	attributes := event.GetActivityTaskScheduledEventAttributes()
	s.NotNil(attributes)
	s.Equal(decisionID, attributes.GetDecisionTaskCompletedEventId())
	s.Equal(activityID, attributes.GetActivityId())
	s.Equal(activityType, attributes.GetActivityType().GetName())
	s.Equal(taskList, attributes.GetTaskList().GetName())
	s.Equal(input, attributes.GetInput())
	s.Equal(timeout, attributes.GetScheduleToCloseTimeoutSeconds())
	s.Equal(queueTimeout, attributes.GetScheduleToStartTimeoutSeconds())
	s.Equal(hearbeatTimeout, attributes.GetHeartbeatTimeoutSeconds())
}

func (s *historyBuilderSuite) validateActivityTaskStartedEvent(event *workflow.HistoryEvent, eventID, scheduleID int64,
	identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventType_ActivityTaskStarted, event.GetEventType())
	s.Equal(eventID, event.GetEventId())
	attributes := event.GetActivityTaskStartedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.GetScheduledEventId())
	s.Equal(identity, attributes.GetIdentity())
}

func (s *historyBuilderSuite) validateActivityTaskCompletedEvent(event *workflow.HistoryEvent, eventID,
	scheduleID, startedID int64, result []byte, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventType_ActivityTaskCompleted, event.GetEventType())
	s.Equal(eventID, event.GetEventId())
	attributes := event.GetActivityTaskCompletedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.GetScheduledEventId())
	s.Equal(startedID, attributes.GetStartedEventId())
	s.Equal(result, attributes.GetResult_())
	s.Equal(identity, attributes.GetIdentity())
}

func (s *historyBuilderSuite) validateActivityTaskFailedEvent(event *workflow.HistoryEvent, eventID,
	scheduleID, startedID int64, reason string, details []byte, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventType_ActivityTaskFailed, event.GetEventType())
	s.Equal(eventID, event.GetEventId())
	attributes := event.GetActivityTaskFailedEventAttributes()
	s.NotNil(attributes)
	s.Equal(scheduleID, attributes.GetScheduledEventId())
	s.Equal(startedID, attributes.GetStartedEventId())
	s.Equal(reason, attributes.GetReason())
	s.Equal(details, attributes.GetDetails())
	s.Equal(identity, attributes.GetIdentity())
}

func (s *historyBuilderSuite) printHistory() string {
	history, err := s.builder.Serialize()
	if err != nil {
		s.logger.Errorf("Error serializing history: %v", err)
		return ""
	}

	//s.logger.Info(string(history))
	return string(history)
}
