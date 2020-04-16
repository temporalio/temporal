// Copyright (c) 2020 Uber Technologies, Inc.
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

package testing

import (
	"github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/execution"
)

// AddWorkflowExecutionStartedEventWithParent adds WorkflowExecutionStarted event with parent workflow info
func AddWorkflowExecutionStartedEventWithParent(
	builder execution.MutableState,
	workflowExecution workflow.WorkflowExecution,
	workflowType string,
	taskList string,
	input []byte,
	executionStartToCloseTimeout,
	taskStartToCloseTimeout int32,
	parentInfo *history.ParentExecutionInfo,
	identity string,
) *workflow.HistoryEvent {

	startRequest := &workflow.StartWorkflowExecutionRequest{
		WorkflowId:                          common.StringPtr(*workflowExecution.WorkflowId),
		WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		TaskList:                            &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:                               input,
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
		Identity:                            common.StringPtr(identity),
	}

	event, _ := builder.AddWorkflowExecutionStartedEvent(
		workflowExecution,
		&history.StartWorkflowExecutionRequest{
			DomainUUID:          common.StringPtr(DomainID),
			StartRequest:        startRequest,
			ParentExecutionInfo: parentInfo,
		},
	)

	return event
}

// AddWorkflowExecutionStartedEvent adds WorkflowExecutionStarted event
func AddWorkflowExecutionStartedEvent(
	builder execution.MutableState,
	workflowExecution workflow.WorkflowExecution,
	workflowType string,
	taskList string,
	input []byte,
	executionStartToCloseTimeout int32,
	taskStartToCloseTimeout int32,
	identity string,
) *workflow.HistoryEvent {
	return AddWorkflowExecutionStartedEventWithParent(builder, workflowExecution, workflowType, taskList, input,
		executionStartToCloseTimeout, taskStartToCloseTimeout, nil, identity)
}

// AddDecisionTaskScheduledEvent adds DecisionTaskScheduled event
func AddDecisionTaskScheduledEvent(
	builder execution.MutableState,
) *execution.DecisionInfo {
	di, _ := builder.AddDecisionTaskScheduledEvent(false)
	return di
}

// AddDecisionTaskStartedEvent adds DecisionTaskStarted event
func AddDecisionTaskStartedEvent(
	builder execution.MutableState,
	scheduleID int64,
	taskList string,
	identity string,
) *workflow.HistoryEvent {
	return AddDecisionTaskStartedEventWithRequestID(builder, scheduleID, RunID, taskList, identity)
}

// AddDecisionTaskStartedEventWithRequestID adds DecisionTaskStarted event with requestID
func AddDecisionTaskStartedEventWithRequestID(
	builder execution.MutableState,
	scheduleID int64,
	requestID string,
	taskList string,
	identity string,
) *workflow.HistoryEvent {
	event, _, _ := builder.AddDecisionTaskStartedEvent(scheduleID, requestID, &workflow.PollForDecisionTaskRequest{
		TaskList: &workflow.TaskList{Name: common.StringPtr(taskList)},
		Identity: common.StringPtr(identity),
	})

	return event
}

// AddDecisionTaskCompletedEvent adds DecisionTaskCompleted event
func AddDecisionTaskCompletedEvent(
	builder execution.MutableState,
	scheduleID int64,
	startedID int64,
	context []byte,
	identity string,
) *workflow.HistoryEvent {
	event, _ := builder.AddDecisionTaskCompletedEvent(scheduleID, startedID, &workflow.RespondDecisionTaskCompletedRequest{
		ExecutionContext: context,
		Identity:         common.StringPtr(identity),
	}, config.DefaultHistoryMaxAutoResetPoints)

	builder.FlushBufferedEvents() //nolint:errcheck

	return event
}

// AddActivityTaskScheduledEvent adds ActivityTaskScheduled event
func AddActivityTaskScheduledEvent(
	builder execution.MutableState,
	decisionCompletedID int64,
	activityID string,
	activityType string,
	taskList string,
	input []byte,
	scheduleToCloseTimeout int32,
	scheduleToStartTimeout int32,
	startToCloseTimeout int32,
	heartbeatTimeout int32,
) (*workflow.HistoryEvent,
	*persistence.ActivityInfo) {

	event, ai, _ := builder.AddActivityTaskScheduledEvent(decisionCompletedID, &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    common.StringPtr(activityID),
		ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityType)},
		TaskList:                      &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:                         input,
		ScheduleToCloseTimeoutSeconds: common.Int32Ptr(scheduleToCloseTimeout),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(scheduleToStartTimeout),
		StartToCloseTimeoutSeconds:    common.Int32Ptr(startToCloseTimeout),
		HeartbeatTimeoutSeconds:       common.Int32Ptr(heartbeatTimeout),
	})

	return event, ai
}

// AddActivityTaskScheduledEventWithRetry adds ActivityTaskScheduled event with retry policy
func AddActivityTaskScheduledEventWithRetry(
	builder execution.MutableState,
	decisionCompletedID int64,
	activityID string,
	activityType string,
	taskList string,
	input []byte,
	scheduleToCloseTimeout int32,
	scheduleToStartTimeout int32,
	startToCloseTimeout int32,
	heartbeatTimeout int32,
	retryPolicy *workflow.RetryPolicy,
) (*workflow.HistoryEvent, *persistence.ActivityInfo) {

	event, ai, _ := builder.AddActivityTaskScheduledEvent(decisionCompletedID, &workflow.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    common.StringPtr(activityID),
		ActivityType:                  &workflow.ActivityType{Name: common.StringPtr(activityType)},
		TaskList:                      &workflow.TaskList{Name: common.StringPtr(taskList)},
		Input:                         input,
		ScheduleToCloseTimeoutSeconds: common.Int32Ptr(scheduleToCloseTimeout),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(scheduleToStartTimeout),
		StartToCloseTimeoutSeconds:    common.Int32Ptr(startToCloseTimeout),
		HeartbeatTimeoutSeconds:       common.Int32Ptr(heartbeatTimeout),
		RetryPolicy:                   retryPolicy,
	})

	return event, ai
}

// AddActivityTaskStartedEvent adds ActivityTaskStarted event
func AddActivityTaskStartedEvent(
	builder execution.MutableState,
	scheduleID int64,
	identity string,
) *workflow.HistoryEvent {
	ai, _ := builder.GetActivityInfo(scheduleID)
	event, _ := builder.AddActivityTaskStartedEvent(ai, scheduleID, RunID, identity)
	return event
}

// AddActivityTaskCompletedEvent adds ActivityTaskCompleted event
func AddActivityTaskCompletedEvent(
	builder execution.MutableState,
	scheduleID int64,
	startedID int64,
	result []byte,
	identity string,
) *workflow.HistoryEvent {
	event, _ := builder.AddActivityTaskCompletedEvent(scheduleID, startedID, &workflow.RespondActivityTaskCompletedRequest{
		Result:   result,
		Identity: common.StringPtr(identity),
	})

	return event
}

// AddActivityTaskFailedEvent adds ActivityTaskFailed event
func AddActivityTaskFailedEvent(
	builder execution.MutableState,
	scheduleID int64,
	startedID int64,
	reason string,
	details []byte,
	identity string,
) *workflow.HistoryEvent {
	event, _ := builder.AddActivityTaskFailedEvent(scheduleID, startedID, &workflow.RespondActivityTaskFailedRequest{
		Reason:   common.StringPtr(reason),
		Details:  details,
		Identity: common.StringPtr(identity),
	})

	return event
}

// AddTimerStartedEvent adds TimerStarted event
func AddTimerStartedEvent(
	builder execution.MutableState,
	decisionCompletedEventID int64,
	timerID string,
	timeOut int64,
) (*workflow.HistoryEvent, *persistence.TimerInfo) {
	event, ti, _ := builder.AddTimerStartedEvent(decisionCompletedEventID,
		&workflow.StartTimerDecisionAttributes{
			TimerId:                   common.StringPtr(timerID),
			StartToFireTimeoutSeconds: common.Int64Ptr(timeOut),
		})
	return event, ti
}

// AddTimerFiredEvent adds TimerFired event
func AddTimerFiredEvent(
	mutableState execution.MutableState,
	timerID string,
) *workflow.HistoryEvent {
	event, _ := mutableState.AddTimerFiredEvent(timerID)
	return event
}

// AddRequestCancelInitiatedEvent adds RequestCancelExternalWorkflowExecutionInitiated event
func AddRequestCancelInitiatedEvent(
	builder execution.MutableState,
	decisionCompletedEventID int64,
	cancelRequestID string,
	domain string,
	workflowID string,
	runID string,
) (*workflow.HistoryEvent, *persistence.RequestCancelInfo) {
	event, rci, _ := builder.AddRequestCancelExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID,
		cancelRequestID, &workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes{
			Domain:     common.StringPtr(domain),
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		})

	return event, rci
}

// AddCancelRequestedEvent adds ExternalWorkflowExecutionCancelRequested event
func AddCancelRequestedEvent(
	builder execution.MutableState,
	initiatedID int64,
	domain string,
	workflowID string,
	runID string,
) *workflow.HistoryEvent {
	event, _ := builder.AddExternalWorkflowExecutionCancelRequested(initiatedID, domain, workflowID, runID)
	return event
}

// AddRequestSignalInitiatedEvent adds SignalExternalWorkflowExecutionInitiated event
func AddRequestSignalInitiatedEvent(
	builder execution.MutableState,
	decisionCompletedEventID int64,
	signalRequestID string,
	domain string,
	workflowID string,
	runID string,
	signalName string,
	input []byte,
	control []byte,
) (*workflow.HistoryEvent, *persistence.SignalInfo) {
	event, si, _ := builder.AddSignalExternalWorkflowExecutionInitiatedEvent(decisionCompletedEventID, signalRequestID,
		&workflow.SignalExternalWorkflowExecutionDecisionAttributes{
			Domain: common.StringPtr(domain),
			Execution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			SignalName: common.StringPtr(signalName),
			Input:      input,
			Control:    control,
		})

	return event, si
}

// AddSignaledEvent adds ExternalWorkflowExecutionSignaled event
func AddSignaledEvent(
	builder execution.MutableState,
	initiatedID int64,
	domain string,
	workflowID string,
	runID string,
	control []byte,
) *workflow.HistoryEvent {
	event, _ := builder.AddExternalWorkflowExecutionSignaled(initiatedID, domain, workflowID, runID, control)
	return event
}

// AddStartChildWorkflowExecutionInitiatedEvent adds ChildWorkflowExecutionInitiated event
func AddStartChildWorkflowExecutionInitiatedEvent(
	builder execution.MutableState,
	decisionCompletedID int64,
	createRequestID string,
	domain string,
	workflowID string,
	workflowType string,
	tasklist string,
	input []byte,
	executionStartToCloseTimeout int32,
	taskStartToCloseTimeout int32,
) (*workflow.HistoryEvent,
	*persistence.ChildExecutionInfo) {

	event, cei, _ := builder.AddStartChildWorkflowExecutionInitiatedEvent(decisionCompletedID, createRequestID,
		&workflow.StartChildWorkflowExecutionDecisionAttributes{
			Domain:                              common.StringPtr(domain),
			WorkflowId:                          common.StringPtr(workflowID),
			WorkflowType:                        &workflow.WorkflowType{Name: common.StringPtr(workflowType)},
			TaskList:                            &workflow.TaskList{Name: common.StringPtr(tasklist)},
			Input:                               input,
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(executionStartToCloseTimeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(taskStartToCloseTimeout),
			Control:                             nil,
		})
	return event, cei
}

// AddChildWorkflowExecutionStartedEvent adds ChildWorkflowExecutionStarted event
func AddChildWorkflowExecutionStartedEvent(
	builder execution.MutableState,
	initiatedID int64,
	domain string,
	workflowID string,
	runID string,
	workflowType string,
) *workflow.HistoryEvent {
	event, _ := builder.AddChildWorkflowExecutionStartedEvent(
		common.StringPtr(domain),
		&workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		&workflow.WorkflowType{Name: common.StringPtr(workflowType)},
		initiatedID,
		&workflow.Header{},
	)
	return event
}

// AddChildWorkflowExecutionCompletedEvent adds ChildWorkflowExecutionCompleted event
func AddChildWorkflowExecutionCompletedEvent(
	builder execution.MutableState,
	initiatedID int64,
	childExecution *workflow.WorkflowExecution,
	attributes *workflow.WorkflowExecutionCompletedEventAttributes,
) *workflow.HistoryEvent {
	event, _ := builder.AddChildWorkflowExecutionCompletedEvent(initiatedID, childExecution, attributes)
	return event
}

// AddCompleteWorkflowEvent adds WorkflowExecutionCompleted event
func AddCompleteWorkflowEvent(
	builder execution.MutableState,
	decisionCompletedEventID int64,
	result []byte,
) *workflow.HistoryEvent {
	event, _ := builder.AddCompletedWorkflowEvent(decisionCompletedEventID, &workflow.CompleteWorkflowExecutionDecisionAttributes{
		Result: result,
	})
	return event
}

// AddFailWorkflowEvent adds WorkflowExecutionFailed event
func AddFailWorkflowEvent(
	builder execution.MutableState,
	decisionCompletedEventID int64,
	reason string,
	details []byte,
) *workflow.HistoryEvent {
	event, _ := builder.AddFailWorkflowEvent(decisionCompletedEventID, &workflow.FailWorkflowExecutionDecisionAttributes{
		Reason:  &reason,
		Details: details,
	})
	return event
}
