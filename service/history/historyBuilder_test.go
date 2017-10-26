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
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
)

type (
	historyBuilderSuite struct {
		suite.Suite
		// override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test,
		// not merely log an error
		*require.Assertions
		domainID  string
		msBuilder *mutableStateBuilder
		builder   *historyBuilder
		logger    bark.Logger
	}
)

func TestHistoryBuilderSuite(t *testing.T) {
	s := new(historyBuilderSuite)
	suite.Run(t, s)
}

func (s *historyBuilderSuite) SetupTest() {
	s.logger = bark.NewLoggerFromLogrus(log.New())
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.Assertions = require.New(s.T())
	s.domainID = "history-builder-test-domain"
	s.msBuilder = newMutableStateBuilder(NewConfig(1), s.logger)
	s.builder = newHistoryBuilder(s.msBuilder, s.logger)
}

func (s *historyBuilderSuite) TestHistoryBuilderDynamicSuccess() {
	id := "dynamic-historybuilder-success-test-workflow-id"
	rid := "dynamic-historybuilder-success-test-run-id"
	wt := "dynamic-historybuilder-success-type"
	tl := "dynamic-historybuilder-success-tasklist"
	identity := "dynamic-historybuilder-success-worker"
	input := []byte("dynamic-historybuilder-success-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(rid),
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	decisionScheduledEvent, _ := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, decisionRunning0 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning0)
	s.Equal(emptyEventID, di0.StartedID)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())

	decisionStartedEvent := s.addDecisionTaskStartedEvent(2, tl, identity)
	s.validateDecisionTaskStartedEvent(decisionStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	di1, decisionRunning1 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning1)
	s.NotNil(di1)
	decisionStartedID1 := di1.StartedID
	s.Equal(int64(3), decisionStartedID1)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())

	decisionContext := []byte("dynamic-historybuilder-success-context")
	decisionCompletedEvent := s.addDecisionTaskCompletedEvent(2, 3, decisionContext, identity)
	s.validateDecisionTaskCompletedEvent(decisionCompletedEvent, 4, 2, 3, decisionContext, identity)
	s.Equal(int64(5), s.getNextEventID())
	_, decisionRunning2 := s.msBuilder.GetPendingDecision(2)
	s.False(decisionRunning2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activityTaskList := "dynamic-historybuilder-success-activity-tasklist"
	activityTimeout := int32(60)
	queueTimeout := int32(20)
	hearbeatTimeout := int32(10)

	activity1ID := "activity1"
	activity1Type := "dynamic-historybuilder-success-activity1-type"
	activity1Input := []byte("dynamic-historybuilder-success-activity1-input")
	activity1Result := []byte("dynamic-historybuilder-success-activity1-result")
	activity1ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.validateActivityTaskScheduledEvent(activity1ScheduledEvent, 5, 4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(6), s.getNextEventID())
	ai0, activity1Running0 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running0)
	s.Equal(emptyEventID, ai0.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity2ID := "activity2"
	activity2Type := "dynamic-historybuilder-success-activity2-type"
	activity2Input := []byte("dynamic-historybuilder-success-activity2-input")
	activity2Reason := "dynamic-historybuilder-success-activity2-failed"
	activity2Details := []byte("dynamic-historybuilder-success-activity2-callstack")
	activity2ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.validateActivityTaskScheduledEvent(activity2ScheduledEvent, 6, 4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(7), s.getNextEventID())
	ai2, activity2Running0 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running0)
	s.Equal(emptyEventID, ai2.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activityStartedEvent := s.addActivityTaskStartedEvent(5, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activityStartedEvent, 7, 5, identity)
	s.Equal(int64(8), s.getNextEventID())
	ai3, activity1Running1 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running1)
	s.Equal(int64(7), ai3.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activityCompletedEvent := s.addActivityTaskCompletedEvent(5, 7, activity1Result, identity)
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, 8, 5, 7, activity1Result,
		identity)
	s.Equal(int64(9), s.getNextEventID())
	_, activity1Running2 := s.msBuilder.GetActivityInfo(5)
	s.False(activity1Running2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	decisionScheduledEvent2, _ := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent2, 9, tl, taskTimeout)
	s.Equal(int64(10), s.getNextEventID())
	di3, decisionRunning3 := s.msBuilder.GetPendingDecision(9)
	s.True(decisionRunning3)
	s.Equal(emptyEventID, di3.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity2StartedEvent := s.addActivityTaskStartedEvent(6, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activity2StartedEvent, 10, 6, identity)
	s.Equal(int64(11), s.getNextEventID())
	ai4, activity2Running1 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running1)
	s.Equal(int64(10), ai4.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activity2FailedEvent := s.addActivityTaskFailedEvent(6, 10, activity2Reason, activity2Details,
		identity)
	s.validateActivityTaskFailedEvent(activity2FailedEvent, 11, 6, 10, activity2Reason,
		activity2Details, identity)
	s.Equal(int64(12), s.getNextEventID())
	_, activity2Running2 := s.msBuilder.GetActivityInfo(6)
	s.False(activity2Running2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderWorkflowStartFailures() {
	id := "historybuilder-workflowstart-failures-test-workflow-id"
	rid := "historybuilder-workflowstart-failures-test-run-id"
	wt := "historybuilder-workflowstart-failures-type"
	tl := "historybuilder-workflowstart-failures-tasklist"
	identity := "historybuilder-workflowstart-failures-worker"
	input := []byte("historybuilder-workflowstart-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(rid),
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	decisionScheduledEvent, _ := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, decisionRunning0 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning0)
	s.Equal(emptyEventID, di0.StartedID)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())

	workflowStartedEvent2 := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Nil(workflowStartedEvent2)
	s.Equal(int64(3), s.getNextEventID(), s.printHistory())
	di1, decisionRunning1 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning1)
	s.Equal(emptyEventID, di1.StartedID)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderDecisionScheduledFailures() {
	id := "historybuilder-decisionscheduled-failures-test-workflow-id"
	rid := "historybuilder-decisionscheduled-failures-test-run-id"
	wt := "historybuilder-decisionscheduled-failures-type"
	tl := "historybuilder-decisionscheduled-failures-tasklist"
	identity := "historybuilder-decisionscheduled-failures-worker"
	input := []byte("historybuilder-decisionscheduled-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(rid),
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	decisionScheduledEvent, _ := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, decisionRunning0 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning0)
	s.Equal(emptyEventID, di0.StartedID)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())

	decisionScheduledEvent2, _ := s.addDecisionTaskScheduledEvent()
	s.Nil(decisionScheduledEvent2)
	s.Equal(int64(3), s.getNextEventID())
	di1, decisionRunning1 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning1)
	s.Equal(emptyEventID, di1.StartedID)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderDecisionStartedFailures() {
	id := "historybuilder-decisionstarted-failures-test-workflow-id"
	rid := "historybuilder-decisionstarted-failures-test-run-id"
	wt := "historybuilder-decisionstarted-failures-type"
	tl := "historybuilder-decisionstarted-failures-tasklist"
	identity := "historybuilder-decisionstarted-failures-worker"
	input := []byte("historybuilder-decisionstarted-failures-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(rid),
	}

	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	decisionStartedEvent := s.addDecisionTaskStartedEvent(2, tl, identity)
	s.Nil(decisionStartedEvent)
	s.Equal(int64(2), s.getNextEventID())
	_, decisionRunning1 := s.msBuilder.GetPendingDecision(2)
	s.False(decisionRunning1)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())

	decisionScheduledEvent, _ := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, decisionRunning0 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning0)
	s.Equal(emptyEventID, di0.StartedID)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())

	decisionStartedEvent1 := s.addDecisionTaskStartedEvent(100, tl, identity)
	s.Nil(decisionStartedEvent1)
	s.Equal(int64(3), s.getNextEventID())
	di2, decisionRunning2 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning2)
	s.Equal(emptyEventID, di2.StartedID)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())

	decisionStartedEvent2 := s.addDecisionTaskStartedEvent(2, tl, identity)
	s.validateDecisionTaskStartedEvent(decisionStartedEvent2, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	di3, decisionRunning3 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning3)
	s.Equal(int64(3), di3.StartedID)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())
}

func (s *historyBuilderSuite) TestHistoryBuilderFlushBufferedEvents() {
	id := "flush-buffered-events-test-workflow-id"
	rid := "flush-buffered-events-test-run-id"
	wt := "flush-buffered-events-type"
	tl := "flush-buffered-events-tasklist"
	identity := "flush-buffered-events-worker"
	input := []byte("flush-buffered-events-input")
	execTimeout := int32(60)
	taskTimeout := int32(10)
	we := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(id),
		RunId:      common.StringPtr(rid),
	}

	// 1 execution started
	workflowStartedEvent := s.addWorkflowExecutionStartedEvent(we, wt, tl, input, execTimeout, taskTimeout, identity)
	s.validateWorkflowExecutionStartedEvent(workflowStartedEvent, wt, tl, input, execTimeout, taskTimeout, identity)
	s.Equal(int64(2), s.getNextEventID())

	// 2 decision scheduled
	decisionScheduledEvent, _ := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(decisionScheduledEvent, 2, tl, taskTimeout)
	s.Equal(int64(3), s.getNextEventID())
	di0, decisionRunning0 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning0)
	s.Equal(emptyEventID, di0.StartedID)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())

	// 3 decision started
	decisionStartedEvent := s.addDecisionTaskStartedEvent(2, tl, identity)
	s.validateDecisionTaskStartedEvent(decisionStartedEvent, 3, 2, identity)
	s.Equal(int64(4), s.getNextEventID())
	di1, decisionRunning1 := s.msBuilder.GetPendingDecision(2)
	s.True(decisionRunning1)
	s.NotNil(di1)
	decisionStartedID1 := di1.StartedID
	s.Equal(int64(3), decisionStartedID1)
	s.Equal(emptyEventID, s.getPreviousDecisionStartedEventID())

	// 4 decision completed
	decisionContext := []byte("flush-buffered-events-context")
	decisionCompletedEvent := s.addDecisionTaskCompletedEvent(2, 3, decisionContext, identity)
	s.validateDecisionTaskCompletedEvent(decisionCompletedEvent, 4, 2, 3, decisionContext, identity)
	s.Equal(int64(5), s.getNextEventID())
	_, decisionRunning2 := s.msBuilder.GetPendingDecision(2)
	s.False(decisionRunning2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	activityTaskList := "flush-buffered-events-activity-tasklist"
	activityTimeout := int32(60)
	queueTimeout := int32(20)
	hearbeatTimeout := int32(10)

	// 5 activity1 scheduled
	activity1ID := "activity1"
	activity1Type := "flush-buffered-events-activity1-type"
	activity1Input := []byte("flush-buffered-events-activity1-input")
	activity1Result := []byte("flush-buffered-events-activity1-result")
	activity1ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.validateActivityTaskScheduledEvent(activity1ScheduledEvent, 5, 4, activity1ID, activity1Type,
		activityTaskList, activity1Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(6), s.getNextEventID())
	ai0, activity1Running0 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running0)
	s.Equal(emptyEventID, ai0.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 6 activity 2 scheduled
	activity2ID := "activity2"
	activity2Type := "flush-buffered-events-activity2-type"
	activity2Input := []byte("flush-buffered-events-activity2-input")
	activity2ScheduledEvent, _ := s.addActivityTaskScheduledEvent(4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.validateActivityTaskScheduledEvent(activity2ScheduledEvent, 6, 4, activity2ID, activity2Type,
		activityTaskList, activity2Input, activityTimeout, queueTimeout, hearbeatTimeout)
	s.Equal(int64(7), s.getNextEventID())
	ai2, activity2Running0 := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running0)
	s.Equal(emptyEventID, ai2.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 7 activity1 started
	activityStartedEvent := s.addActivityTaskStartedEvent(5, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activityStartedEvent, 7, 5, identity)
	s.Equal(int64(8), s.getNextEventID())
	ai3, activity1Running1 := s.msBuilder.GetActivityInfo(5)
	s.True(activity1Running1)
	s.Equal(int64(7), ai3.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 8 activity1 completed
	activityCompletedEvent := s.addActivityTaskCompletedEvent(5, 7, activity1Result, identity)
	s.validateActivityTaskCompletedEvent(activityCompletedEvent, 8, 5, 7, activity1Result,
		identity)
	s.Equal(int64(9), s.getNextEventID())
	_, activity1Running2 := s.msBuilder.GetActivityInfo(5)
	s.False(activity1Running2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 9 decision2 scheduled
	decision2ScheduledEvent, _ := s.addDecisionTaskScheduledEvent()
	s.validateDecisionTaskScheduledEvent(decision2ScheduledEvent, 9, tl, taskTimeout)
	s.Equal(int64(10), s.getNextEventID())
	di3, decisionRunning3 := s.msBuilder.GetPendingDecision(9)
	s.True(decisionRunning3)
	s.Equal(emptyEventID, di3.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 10 decision2 started
	decision2StartedEvent := s.addDecisionTaskStartedEvent(9, tl, identity)
	s.validateDecisionTaskStartedEvent(decision2StartedEvent, 10, 9, identity)
	s.Equal(int64(11), s.getNextEventID())
	di2, decision2Running := s.msBuilder.GetPendingDecision(9)
	s.True(decision2Running)
	s.NotNil(di2)
	decision2StartedID := di2.StartedID
	s.Equal(int64(10), decision2StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 11 (buffered) activity2 started
	activity2StartedEvent := s.addActivityTaskStartedEvent(6, activityTaskList, identity)
	s.validateActivityTaskStartedEvent(activity2StartedEvent, bufferedEventID, 6, identity)
	s.Equal(int64(11), s.getNextEventID())
	ai4, activity2Running := s.msBuilder.GetActivityInfo(6)
	s.True(activity2Running)
	s.Equal(bufferedEventID, ai4.StartedID)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 12 (buffered) activity2 failed
	activity2Reason := "flush-buffered-events-activity2-failed"
	activity2Details := []byte("flush-buffered-events-activity2-callstack")
	activity2FailedEvent := s.addActivityTaskFailedEvent(6, bufferedEventID, activity2Reason, activity2Details, identity)
	s.validateActivityTaskFailedEvent(activity2FailedEvent, bufferedEventID, 6, bufferedEventID, activity2Reason,
		activity2Details, identity)
	s.Equal(int64(11), s.getNextEventID())
	_, activity2Running2 := s.msBuilder.GetActivityInfo(6)
	s.False(activity2Running2)
	s.Equal(int64(3), s.getPreviousDecisionStartedEventID())

	// 13 (eventId will be 11) decision completed
	decision2Context := []byte("flush-buffered-events-context")
	decision2CompletedEvent := s.addDecisionTaskCompletedEvent(9, 10, decision2Context, identity)
	s.validateDecisionTaskCompletedEvent(decision2CompletedEvent, 11, 9, 10, decision2Context, identity)
	s.Equal(int64(11), decision2CompletedEvent.GetEventId())
	s.Equal(int64(12), s.getNextEventID())
	_, decision2Running2 := s.msBuilder.GetPendingDecision(2)
	s.False(decision2Running2)
	s.Equal(int64(10), s.getPreviousDecisionStartedEventID())

	// flush buffered events. 12: Activity2Started, 13: Activity2Failed
	s.msBuilder.FlushBufferedEvents()
	s.Equal(int64(14), s.getNextEventID())
	activity2StartedEvent2 := s.msBuilder.hBuilder.history[11]
	s.Equal(int64(12), activity2StartedEvent2.GetEventId())
	s.Equal(workflow.EventTypeActivityTaskStarted, activity2StartedEvent2.GetEventType())

	activity2FailedEvent2 := s.msBuilder.hBuilder.history[12]
	s.Equal(int64(13), activity2FailedEvent2.GetEventId())
	s.Equal(workflow.EventTypeActivityTaskFailed, activity2FailedEvent2.GetEventType())
	s.Equal(int64(12), activity2FailedEvent2.ActivityTaskFailedEventAttributes.GetStartedEventId())
}

func (s *historyBuilderSuite) getNextEventID() int64 {
	return s.msBuilder.executionInfo.NextEventID
}

func (s *historyBuilderSuite) getPreviousDecisionStartedEventID() int64 {
	return s.msBuilder.executionInfo.LastProcessedEvent
}

func (s *historyBuilderSuite) addWorkflowExecutionStartedEvent(we workflow.WorkflowExecution, workflowType,
	taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32,
	identity string) *workflow.HistoryEvent {
	e := s.msBuilder.AddWorkflowExecutionStartedEvent(s.domainID, we, &workflow.StartWorkflowExecutionRequest{
		WorkflowId:   common.StringPtr(*we.WorkflowId),
		WorkflowType: &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		TaskList:     &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:        input,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
		Identity:                            common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) addDecisionTaskScheduledEvent() (*workflow.HistoryEvent, *decisionInfo) {
	return s.msBuilder.AddDecisionTaskScheduledEvent()
}

func (s *historyBuilderSuite) addDecisionTaskStartedEvent(scheduleID int64,
	taskList, identity string) *workflow.HistoryEvent {
	e := s.msBuilder.AddDecisionTaskStartedEvent(scheduleID, uuid.New(), &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) addDecisionTaskCompletedEvent(scheduleID, startedID int64, context []byte,
	identity string) *workflow.HistoryEvent {
	e := s.msBuilder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) addActivityTaskScheduledEvent(decisionCompletedID int64, activityID, activityType,
	taskList string, input []byte, timeout, queueTimeout, hearbeatTimeout int32) (*workflow.HistoryEvent,
	*persistence.ActivityInfo) {
	return s.msBuilder.AddActivityTaskScheduledEvent(decisionCompletedID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:   common.StringPtr(activityID),
			ActivityType: &workflow.ActivityType{Name: common.StringPtr(activityType)},
			TaskList:     &workflow.TaskList{Name: common.StringPtr(taskList)},
			Input:        input,
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(queueTimeout),
			HeartbeatTimeoutSeconds:       common.Int32Ptr(hearbeatTimeout),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
		})
}

func (s *historyBuilderSuite) addActivityTaskStartedEvent(scheduleID int64, taskList,
	identity string) *workflow.HistoryEvent {
	ai, _ := s.msBuilder.GetActivityInfo(scheduleID)
	e := s.msBuilder.AddActivityTaskStartedEvent(ai, scheduleID, uuid.New(), &workflow.PollForActivityTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) addActivityTaskCompletedEvent(scheduleID, startedID int64, result []byte,
	identity string) *workflow.HistoryEvent {
	e := s.msBuilder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflow.RespondActivityTaskCompletedRequest{
		Result:   result,
		Identity: common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) addActivityTaskFailedEvent(scheduleID, startedID int64, reason string, details []byte,
	identity string) *workflow.HistoryEvent {
	e := s.msBuilder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflow.RespondActivityTaskFailedRequest{
		Reason:   common.StringPtr(reason),
		Details:  details,
		Identity: common.StringPtr(identity),
	})

	return e
}

func (s *historyBuilderSuite) validateWorkflowExecutionStartedEvent(event *workflow.HistoryEvent, workflowType,
	taskList string, input []byte, executionStartToCloseTimeout, taskStartToCloseTimeout int32, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeWorkflowExecutionStarted, *event.EventType)
	s.Equal(firstEventID, *event.EventId)
	attributes := event.WorkflowExecutionStartedEventAttributes
	s.NotNil(attributes)
	s.Equal(workflowType, *attributes.WorkflowType.Name)
	s.Equal(taskList, *attributes.TaskList.Name)
	s.Equal(input, attributes.Input)
	s.Equal(executionStartToCloseTimeout, *attributes.ExecutionStartToCloseTimeoutSeconds)
	s.Equal(taskStartToCloseTimeout, *attributes.TaskStartToCloseTimeoutSeconds)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateDecisionTaskScheduledEvent(event *workflow.HistoryEvent, eventID int64,
	taskList string, timeout int32) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeDecisionTaskScheduled, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.DecisionTaskScheduledEventAttributes
	s.NotNil(attributes)
	s.Equal(taskList, *attributes.TaskList.Name)
	s.Equal(timeout, *attributes.StartToCloseTimeoutSeconds)
}

func (s *historyBuilderSuite) validateDecisionTaskStartedEvent(event *workflow.HistoryEvent, eventID, scheduleID int64,
	identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeDecisionTaskStarted, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.DecisionTaskStartedEventAttributes
	s.NotNil(attributes)
	s.Equal(scheduleID, *attributes.ScheduledEventId)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateDecisionTaskCompletedEvent(event *workflow.HistoryEvent, eventID,
	scheduleID, startedID int64, context []byte, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeDecisionTaskCompleted, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.DecisionTaskCompletedEventAttributes
	s.NotNil(attributes)
	s.Equal(scheduleID, *attributes.ScheduledEventId)
	s.Equal(startedID, *attributes.StartedEventId)
	s.Equal(context, attributes.ExecutionContext)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateActivityTaskScheduledEvent(event *workflow.HistoryEvent, eventID, decisionID int64,
	activityID, activityType, taskList string, input []byte, timeout, queueTimeout, hearbeatTimeout int32) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeActivityTaskScheduled, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.ActivityTaskScheduledEventAttributes
	s.NotNil(attributes)
	s.Equal(decisionID, *attributes.DecisionTaskCompletedEventId)
	s.Equal(activityID, *attributes.ActivityId)
	s.Equal(activityType, *attributes.ActivityType.Name)
	s.Equal(taskList, *attributes.TaskList.Name)
	s.Equal(input, attributes.Input)
	s.Equal(timeout, *attributes.ScheduleToCloseTimeoutSeconds)
	s.Equal(queueTimeout, *attributes.ScheduleToStartTimeoutSeconds)
	s.Equal(hearbeatTimeout, *attributes.HeartbeatTimeoutSeconds)
}

func (s *historyBuilderSuite) validateActivityTaskStartedEvent(event *workflow.HistoryEvent, eventID, scheduleID int64,
	identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeActivityTaskStarted, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.ActivityTaskStartedEventAttributes
	s.NotNil(attributes)
	s.Equal(scheduleID, *attributes.ScheduledEventId)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateActivityTaskCompletedEvent(event *workflow.HistoryEvent, eventID,
	scheduleID, startedID int64, result []byte, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeActivityTaskCompleted, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.ActivityTaskCompletedEventAttributes
	s.NotNil(attributes)
	s.Equal(scheduleID, *attributes.ScheduledEventId)
	s.Equal(startedID, *attributes.StartedEventId)
	s.Equal(result, attributes.Result)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) validateActivityTaskFailedEvent(event *workflow.HistoryEvent, eventID,
	scheduleID, startedID int64, reason string, details []byte, identity string) {
	s.NotNil(event)
	s.Equal(workflow.EventTypeActivityTaskFailed, *event.EventType)
	s.Equal(eventID, *event.EventId)
	attributes := event.ActivityTaskFailedEventAttributes
	s.NotNil(attributes)
	s.Equal(scheduleID, *attributes.ScheduledEventId)
	s.Equal(startedID, *attributes.StartedEventId)
	s.Equal(reason, *attributes.Reason)
	s.Equal(details, attributes.Details)
	s.Equal(identity, *attributes.Identity)
}

func (s *historyBuilderSuite) printHistory() string {
	history, err := s.builder.Serialize()
	if err != nil {
		s.logger.Errorf("Error serializing history: %v", err)
		return ""
	}

	//s.logger.Info(string(history))
	return history.String()
}
