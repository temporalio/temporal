// Copyright (c) 2019 Uber Technologies, Inc.
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
	"time"

	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

const (
	timeout              = int32(10000)
	cause                = "NDC test"
	signal               = "NDC signal"
	checksum             = "NDC checksum"
	childWorkflowPrefix  = "child-"
	reason               = "NDC reason"
	workflowType         = "test-workflow-type"
	taskList             = "taskList"
	identity             = "identity"
	decisionTaskAttempts = 0
	childWorkflowID      = "child-workflowID"
	externalWorkflowID   = "external-workflowID"
)

var (
	globalTaskID int64 = 1
)

// InitializeHistoryEventGenerator initializes the history event generator
func InitializeHistoryEventGenerator(
	domain string,
	defaultVersion int64,
) Generator {

	generator := NewEventGenerator(time.Now().UnixNano())
	generator.SetVersion(defaultVersion)
	// Functions
	notPendingDecisionTask := func(input ...interface{}) bool {
		count := 0
		history := input[0].([]Vertex)
		for _, e := range history {
			switch e.GetName() {
			case shared.EventTypeDecisionTaskScheduled.String():
				count++
			case shared.EventTypeDecisionTaskCompleted.String(),
				shared.EventTypeDecisionTaskFailed.String(),
				shared.EventTypeDecisionTaskTimedOut.String():
				count--
			}
		}
		return count <= 0
	}
	containActivityComplete := func(input ...interface{}) bool {
		history := input[0].([]Vertex)
		for _, e := range history {
			if e.GetName() == shared.EventTypeActivityTaskCompleted.String() {
				return true
			}
		}
		return false
	}
	hasPendingActivity := func(input ...interface{}) bool {
		count := 0
		history := input[0].([]Vertex)
		for _, e := range history {
			switch e.GetName() {
			case shared.EventTypeActivityTaskScheduled.String():
				count++
			case shared.EventTypeActivityTaskCanceled.String(),
				shared.EventTypeActivityTaskFailed.String(),
				shared.EventTypeActivityTaskTimedOut.String(),
				shared.EventTypeActivityTaskCompleted.String():
				count--
			}
		}
		return count > 0
	}
	canDoBatch := func(currentBatch []Vertex, history []Vertex) bool {
		if len(currentBatch) == 0 {
			return true
		}

		hasPendingDecisionTask := false
		for _, event := range history {
			switch event.GetName() {
			case shared.EventTypeDecisionTaskScheduled.String():
				hasPendingDecisionTask = true
			case shared.EventTypeDecisionTaskCompleted.String(),
				shared.EventTypeDecisionTaskFailed.String(),
				shared.EventTypeDecisionTaskTimedOut.String():
				hasPendingDecisionTask = false
			}
		}
		if hasPendingDecisionTask {
			return false
		}
		if currentBatch[len(currentBatch)-1].GetName() == shared.EventTypeDecisionTaskScheduled.String() {
			return false
		}
		if currentBatch[0].GetName() == shared.EventTypeDecisionTaskCompleted.String() {
			return len(currentBatch) == 1
		}
		return true
	}

	// Setup decision task model
	decisionModel := NewHistoryEventModel()
	decisionSchedule := NewHistoryEventVertex(shared.EventTypeDecisionTaskScheduled.String())
	decisionSchedule.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeDecisionTaskScheduled.Ptr()
		historyEvent.DecisionTaskScheduledEventAttributes = &shared.DecisionTaskScheduledEventAttributes{
			TaskList: &shared.TaskList{
				Name: common.StringPtr(taskList),
				Kind: shared.TaskListKindNormal.Ptr(),
			},
			StartToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			Attempt:                    common.Int64Ptr(decisionTaskAttempts),
		}
		return historyEvent
	})
	decisionStart := NewHistoryEventVertex(shared.EventTypeDecisionTaskStarted.String())
	decisionStart.SetIsStrictOnNextVertex(true)
	decisionStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeDecisionTaskStarted.Ptr()
		historyEvent.DecisionTaskStartedEventAttributes = &shared.DecisionTaskStartedEventAttributes{
			ScheduledEventId: lastEvent.EventId,
			Identity:         common.StringPtr(identity),
			RequestId:        common.StringPtr(uuid.New()),
		}
		return historyEvent
	})
	decisionFail := NewHistoryEventVertex(shared.EventTypeDecisionTaskFailed.String())
	decisionFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeDecisionTaskFailed.Ptr()
		historyEvent.DecisionTaskFailedEventAttributes = &shared.DecisionTaskFailedEventAttributes{
			ScheduledEventId: lastEvent.GetDecisionTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Cause:            common.DecisionTaskFailedCausePtr(shared.DecisionTaskFailedCauseUnhandledDecision),
			Identity:         common.StringPtr(identity),
			ForkEventVersion: common.Int64Ptr(version),
		}
		return historyEvent
	})
	decisionTimedOut := NewHistoryEventVertex(shared.EventTypeDecisionTaskTimedOut.String())
	decisionTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeDecisionTaskTimedOut.Ptr()
		historyEvent.DecisionTaskTimedOutEventAttributes = &shared.DecisionTaskTimedOutEventAttributes{
			ScheduledEventId: lastEvent.GetDecisionTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			TimeoutType:      shared.TimeoutTypeScheduleToStart.Ptr(),
		}
		return historyEvent
	})
	decisionComplete := NewHistoryEventVertex(shared.EventTypeDecisionTaskCompleted.String())
	decisionComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeDecisionTaskCompleted.Ptr()
		historyEvent.DecisionTaskCompletedEventAttributes = &shared.DecisionTaskCompletedEventAttributes{
			ScheduledEventId: lastEvent.GetDecisionTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         common.StringPtr(identity),
			BinaryChecksum:   common.StringPtr(checksum),
		}
		return historyEvent
	})
	decisionComplete.SetIsStrictOnNextVertex(true)
	decisionComplete.SetMaxNextVertex(2)
	decisionScheduleToStart := NewHistoryEventEdge(decisionSchedule, decisionStart)
	decisionStartToComplete := NewHistoryEventEdge(decisionStart, decisionComplete)
	decisionStartToFail := NewHistoryEventEdge(decisionStart, decisionFail)
	decisionStartToTimedOut := NewHistoryEventEdge(decisionStart, decisionTimedOut)
	decisionFailToSchedule := NewHistoryEventEdge(decisionFail, decisionSchedule)
	decisionFailToSchedule.SetCondition(notPendingDecisionTask)
	decisionTimedOutToSchedule := NewHistoryEventEdge(decisionTimedOut, decisionSchedule)
	decisionTimedOutToSchedule.SetCondition(notPendingDecisionTask)
	decisionModel.AddEdge(decisionScheduleToStart, decisionStartToComplete, decisionStartToFail, decisionStartToTimedOut,
		decisionFailToSchedule, decisionTimedOutToSchedule)

	// Setup workflow model
	workflowModel := NewHistoryEventModel()

	workflowStart := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionStarted.String())
	workflowStart.SetDataFunc(func(input ...interface{}) interface{} {
		historyEvent := getDefaultHistoryEvent(1, defaultVersion)
		historyEvent.EventType = shared.EventTypeWorkflowExecutionStarted.Ptr()
		historyEvent.WorkflowExecutionStartedEventAttributes = &shared.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &shared.WorkflowType{
				Name: common.StringPtr(workflowType),
			},
			TaskList: &shared.TaskList{
				Name: common.StringPtr(taskList),
				Kind: shared.TaskListKindNormal.Ptr(),
			},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(timeout),
			Identity:                            common.StringPtr(identity),
			FirstExecutionRunId:                 common.StringPtr(uuid.New()),
		}
		return historyEvent
	})
	workflowSignal := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionSignaled.String())
	workflowSignal.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeWorkflowExecutionSignaled.Ptr()
		historyEvent.WorkflowExecutionSignaledEventAttributes = &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signal),
			Identity:   common.StringPtr(identity),
		}
		return historyEvent
	})
	workflowComplete := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionCompleted.String())
	workflowComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeWorkflowExecutionCompleted.Ptr()
		historyEvent.WorkflowExecutionCompletedEventAttributes = &shared.WorkflowExecutionCompletedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	continueAsNew := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionContinuedAsNew.String())
	continueAsNew.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeWorkflowExecutionContinuedAsNew.Ptr()
		historyEvent.WorkflowExecutionContinuedAsNewEventAttributes = &shared.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: common.StringPtr(uuid.New()),
			WorkflowType: &shared.WorkflowType{
				Name: common.StringPtr(workflowType),
			},
			TaskList: &shared.TaskList{
				Name: common.StringPtr(taskList),
				Kind: shared.TaskListKindNormal.Ptr(),
			},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(timeout),
			DecisionTaskCompletedEventId:        common.Int64Ptr(eventID - 1),
			Initiator:                           shared.ContinueAsNewInitiatorDecider.Ptr(),
		}
		return historyEvent
	})
	workflowFail := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionFailed.String())
	workflowFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeWorkflowExecutionFailed.Ptr()
		historyEvent.WorkflowExecutionFailedEventAttributes = &shared.WorkflowExecutionFailedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	workflowCancel := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionCanceled.String())
	workflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeWorkflowExecutionCanceled.Ptr()
		historyEvent.WorkflowExecutionCanceledEventAttributes = &shared.WorkflowExecutionCanceledEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	workflowCancelRequest := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionCancelRequested.String())
	workflowCancelRequest.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeWorkflowExecutionCancelRequested.Ptr()
		historyEvent.WorkflowExecutionCancelRequestedEventAttributes = &shared.WorkflowExecutionCancelRequestedEventAttributes{
			Cause:                    common.StringPtr(""),
			ExternalInitiatedEventId: common.Int64Ptr(1),
			ExternalWorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(externalWorkflowID),
				RunId:      common.StringPtr(uuid.New()),
			},
			Identity: common.StringPtr(identity),
		}
		return historyEvent
	})
	workflowTerminate := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionTerminated.String())
	workflowTerminate.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeWorkflowExecutionTerminated.Ptr()
		historyEvent.WorkflowExecutionTerminatedEventAttributes = &shared.WorkflowExecutionTerminatedEventAttributes{
			Identity: common.StringPtr(identity),
			Reason:   common.StringPtr(reason),
		}
		return historyEvent
	})
	workflowTimedOut := NewHistoryEventVertex(shared.EventTypeWorkflowExecutionTimedOut.String())
	workflowTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeWorkflowExecutionTimedOut.Ptr()
		historyEvent.WorkflowExecutionTimedOutEventAttributes = &shared.WorkflowExecutionTimedOutEventAttributes{
			TimeoutType: shared.TimeoutTypeStartToClose.Ptr(),
		}
		return historyEvent
	})
	workflowStartToSignal := NewHistoryEventEdge(workflowStart, workflowSignal)
	workflowStartToDecisionSchedule := NewHistoryEventEdge(workflowStart, decisionSchedule)
	workflowStartToDecisionSchedule.SetCondition(notPendingDecisionTask)
	workflowSignalToDecisionSchedule := NewHistoryEventEdge(workflowSignal, decisionSchedule)
	workflowSignalToDecisionSchedule.SetCondition(notPendingDecisionTask)
	decisionCompleteToWorkflowComplete := NewHistoryEventEdge(decisionComplete, workflowComplete)
	decisionCompleteToWorkflowComplete.SetCondition(containActivityComplete)
	decisionCompleteToWorkflowFailed := NewHistoryEventEdge(decisionComplete, workflowFail)
	decisionCompleteToWorkflowFailed.SetCondition(containActivityComplete)
	decisionCompleteToCAN := NewHistoryEventEdge(decisionComplete, continueAsNew)
	decisionCompleteToCAN.SetCondition(containActivityComplete)
	workflowCancelRequestToCancel := NewHistoryEventEdge(workflowCancelRequest, workflowCancel)
	workflowModel.AddEdge(workflowStartToSignal, workflowStartToDecisionSchedule, workflowSignalToDecisionSchedule,
		decisionCompleteToCAN, decisionCompleteToWorkflowComplete, decisionCompleteToWorkflowFailed, workflowCancelRequestToCancel)

	// Setup activity model
	activityModel := NewHistoryEventModel()
	activitySchedule := NewHistoryEventVertex(shared.EventTypeActivityTaskScheduled.String())
	activitySchedule.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeActivityTaskScheduled.Ptr()
		historyEvent.ActivityTaskScheduledEventAttributes = &shared.ActivityTaskScheduledEventAttributes{
			ActivityId: common.StringPtr(uuid.New()),
			ActivityType: common.ActivityTypePtr(shared.ActivityType{
				Name: common.StringPtr("activity"),
			}),
			Domain: common.StringPtr(domain),
			TaskList: common.TaskListPtr(shared.TaskList{
				Name: common.StringPtr(taskList),
				Kind: common.TaskListKindPtr(shared.TaskListKindNormal),
			}),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(timeout),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(timeout),
			DecisionTaskCompletedEventId:  lastEvent.EventId,
		}
		return historyEvent
	})
	activityStart := NewHistoryEventVertex(shared.EventTypeActivityTaskStarted.String())
	activityStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeActivityTaskStarted.Ptr()
		historyEvent.ActivityTaskStartedEventAttributes = &shared.ActivityTaskStartedEventAttributes{
			ScheduledEventId: lastEvent.EventId,
			Identity:         common.StringPtr(identity),
			RequestId:        common.StringPtr(uuid.New()),
			Attempt:          common.Int32Ptr(0),
		}
		return historyEvent
	})
	activityComplete := NewHistoryEventVertex(shared.EventTypeActivityTaskCompleted.String())
	activityComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeActivityTaskCompleted.Ptr()
		historyEvent.ActivityTaskCompletedEventAttributes = &shared.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         common.StringPtr(identity),
		}
		return historyEvent
	})
	activityFail := NewHistoryEventVertex(shared.EventTypeActivityTaskFailed.String())
	activityFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeActivityTaskFailed.Ptr()
		historyEvent.ActivityTaskFailedEventAttributes = &shared.ActivityTaskFailedEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         common.StringPtr(identity),
			Reason:           common.StringPtr(reason),
		}
		return historyEvent
	})
	activityTimedOut := NewHistoryEventVertex(shared.EventTypeActivityTaskTimedOut.String())
	activityTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeActivityTaskTimedOut.Ptr()
		historyEvent.ActivityTaskTimedOutEventAttributes = &shared.ActivityTaskTimedOutEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			TimeoutType:      common.TimeoutTypePtr(shared.TimeoutTypeScheduleToClose),
		}
		return historyEvent
	})
	activityCancelRequest := NewHistoryEventVertex(shared.EventTypeActivityTaskCancelRequested.String())
	activityCancelRequest.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeActivityTaskCancelRequested.Ptr()
		historyEvent.ActivityTaskCancelRequestedEventAttributes = &shared.ActivityTaskCancelRequestedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.GetActivityTaskScheduledEventAttributes().DecisionTaskCompletedEventId,
			ActivityId:                   lastEvent.GetActivityTaskScheduledEventAttributes().ActivityId,
		}
		return historyEvent
	})
	activityCancel := NewHistoryEventVertex(shared.EventTypeActivityTaskCanceled.String())
	activityCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeActivityTaskCanceled.Ptr()
		historyEvent.ActivityTaskCanceledEventAttributes = &shared.ActivityTaskCanceledEventAttributes{
			LatestCancelRequestedEventId: lastEvent.EventId,
			ScheduledEventId:             lastEvent.EventId,
			StartedEventId:               lastEvent.EventId,
			Identity:                     common.StringPtr(identity),
		}
		return historyEvent
	})
	activityCancelRequestFail := NewHistoryEventVertex(shared.EventTypeRequestCancelActivityTaskFailed.String())
	activityCancelRequestFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		versionBump := input[2].(int64)
		subVersion := input[3].(int64)
		version := lastGeneratedEvent.GetVersion() + versionBump + subVersion
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeRequestCancelActivityTaskFailed.Ptr()
		historyEvent.RequestCancelActivityTaskFailedEventAttributes = &shared.RequestCancelActivityTaskFailedEventAttributes{
			ActivityId:                   common.StringPtr(uuid.New()),
			DecisionTaskCompletedEventId: lastEvent.GetActivityTaskCancelRequestedEventAttributes().DecisionTaskCompletedEventId,
		}
		return historyEvent
	})
	decisionCompleteToATSchedule := NewHistoryEventEdge(decisionComplete, activitySchedule)

	activityScheduleToStart := NewHistoryEventEdge(activitySchedule, activityStart)
	activityScheduleToStart.SetCondition(hasPendingActivity)

	activityStartToComplete := NewHistoryEventEdge(activityStart, activityComplete)
	activityStartToComplete.SetCondition(hasPendingActivity)

	activityStartToFail := NewHistoryEventEdge(activityStart, activityFail)
	activityStartToFail.SetCondition(hasPendingActivity)

	activityStartToTimedOut := NewHistoryEventEdge(activityStart, activityTimedOut)
	activityStartToTimedOut.SetCondition(hasPendingActivity)

	activityCompleteToDecisionSchedule := NewHistoryEventEdge(activityComplete, decisionSchedule)
	activityCompleteToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityFailToDecisionSchedule := NewHistoryEventEdge(activityFail, decisionSchedule)
	activityFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityTimedOutToDecisionSchedule := NewHistoryEventEdge(activityTimedOut, decisionSchedule)
	activityTimedOutToDecisionSchedule.SetCondition(notPendingDecisionTask)
	activityCancelToDecisionSchedule := NewHistoryEventEdge(activityCancel, decisionSchedule)
	activityCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)

	// TODO: bypass activity cancel request event. Support this event later.
	//activityScheduleToActivityCancelRequest := NewHistoryEventEdge(activitySchedule, activityCancelRequest)
	//activityScheduleToActivityCancelRequest.SetCondition(hasPendingActivity)
	activityCancelReqToCancel := NewHistoryEventEdge(activityCancelRequest, activityCancel)
	activityCancelReqToCancel.SetCondition(hasPendingActivity)

	activityCancelReqToCancelFail := NewHistoryEventEdge(activityCancelRequest, activityCancelRequestFail)
	activityCancelRequestFailToDecisionSchedule := NewHistoryEventEdge(activityCancelRequestFail, decisionSchedule)
	activityCancelRequestFailToDecisionSchedule.SetCondition(notPendingDecisionTask)

	activityModel.AddEdge(decisionCompleteToATSchedule, activityScheduleToStart, activityStartToComplete,
		activityStartToFail, activityStartToTimedOut, decisionCompleteToATSchedule, activityCompleteToDecisionSchedule,
		activityFailToDecisionSchedule, activityTimedOutToDecisionSchedule, activityCancelReqToCancel,
		activityCancelReqToCancelFail, activityCancelToDecisionSchedule, activityCancelRequestFailToDecisionSchedule)

	// Setup timer model
	timerModel := NewHistoryEventModel()
	timerStart := NewHistoryEventVertex(shared.EventTypeTimerStarted.String())
	timerStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeTimerStarted.Ptr()
		historyEvent.TimerStartedEventAttributes = &shared.TimerStartedEventAttributes{
			TimerId:                      common.StringPtr(uuid.New()),
			StartToFireTimeoutSeconds:    common.Int64Ptr(10),
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	timerFired := NewHistoryEventVertex(shared.EventTypeTimerFired.String())
	timerFired.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeTimerFired.Ptr()
		historyEvent.TimerFiredEventAttributes = &shared.TimerFiredEventAttributes{
			TimerId:        lastEvent.GetTimerStartedEventAttributes().TimerId,
			StartedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	timerCancel := NewHistoryEventVertex(shared.EventTypeTimerCanceled.String())
	timerCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeTimerCanceled.Ptr()
		historyEvent.TimerCanceledEventAttributes = &shared.TimerCanceledEventAttributes{
			TimerId:                      lastEvent.GetTimerStartedEventAttributes().TimerId,
			StartedEventId:               lastEvent.EventId,
			DecisionTaskCompletedEventId: lastEvent.GetTimerStartedEventAttributes().DecisionTaskCompletedEventId,
			Identity:                     common.StringPtr(identity),
		}
		return historyEvent
	})
	timerStartToFire := NewHistoryEventEdge(timerStart, timerFired)
	timerStartToCancel := NewHistoryEventEdge(timerStart, timerCancel)

	decisionCompleteToTimerStart := NewHistoryEventEdge(decisionComplete, timerStart)
	timerFiredToDecisionSchedule := NewHistoryEventEdge(timerFired, decisionSchedule)
	timerFiredToDecisionSchedule.SetCondition(notPendingDecisionTask)
	timerCancelToDecisionSchedule := NewHistoryEventEdge(timerCancel, decisionSchedule)
	timerCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)
	timerModel.AddEdge(timerStartToFire, timerStartToCancel, decisionCompleteToTimerStart, timerFiredToDecisionSchedule, timerCancelToDecisionSchedule)

	// Setup child workflow model
	childWorkflowModel := NewHistoryEventModel()
	childWorkflowInitial := NewHistoryEventVertex(shared.EventTypeStartChildWorkflowExecutionInitiated.String())
	childWorkflowInitial.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeStartChildWorkflowExecutionInitiated.Ptr()
		historyEvent.StartChildWorkflowExecutionInitiatedEventAttributes = &shared.StartChildWorkflowExecutionInitiatedEventAttributes{
			Domain:     common.StringPtr(domain),
			WorkflowId: common.StringPtr(childWorkflowID),
			WorkflowType: common.WorkflowTypePtr(shared.WorkflowType{
				Name: common.StringPtr(childWorkflowPrefix + workflowType),
			}),
			TaskList: common.TaskListPtr(shared.TaskList{
				Name: common.StringPtr(taskList),
				Kind: common.TaskListKindPtr(shared.TaskListKindNormal),
			}),
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(timeout),
			DecisionTaskCompletedEventId:        lastEvent.EventId,
			WorkflowIdReusePolicy:               shared.WorkflowIdReusePolicyRejectDuplicate.Ptr(),
		}
		return historyEvent
	})
	childWorkflowInitialFail := NewHistoryEventVertex(shared.EventTypeStartChildWorkflowExecutionFailed.String())
	childWorkflowInitialFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeStartChildWorkflowExecutionFailed.Ptr()
		historyEvent.StartChildWorkflowExecutionFailedEventAttributes = &shared.StartChildWorkflowExecutionFailedEventAttributes{
			Domain:     common.StringPtr(domain),
			WorkflowId: common.StringPtr(childWorkflowID),
			WorkflowType: common.WorkflowTypePtr(shared.WorkflowType{
				Name: common.StringPtr(childWorkflowPrefix + workflowType),
			}),
			Cause:                        shared.ChildWorkflowExecutionFailedCauseWorkflowAlreadyRunning.Ptr(),
			InitiatedEventId:             lastEvent.EventId,
			DecisionTaskCompletedEventId: lastEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes().DecisionTaskCompletedEventId,
		}
		return historyEvent
	})
	childWorkflowStart := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionStarted.String())
	childWorkflowStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionStarted.Ptr()
		historyEvent.ChildWorkflowExecutionStartedEventAttributes = &shared.ChildWorkflowExecutionStartedEventAttributes{
			Domain: common.StringPtr(domain),
			WorkflowType: common.WorkflowTypePtr(shared.WorkflowType{
				Name: common.StringPtr(childWorkflowPrefix + workflowType),
			}),
			InitiatedEventId: lastEvent.EventId,
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(childWorkflowID),
				RunId:      common.StringPtr(uuid.New()),
			},
		}
		return historyEvent
	})
	childWorkflowCancel := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionCanceled.String())
	childWorkflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionCanceled.Ptr()
		historyEvent.ChildWorkflowExecutionCanceledEventAttributes = &shared.ChildWorkflowExecutionCanceledEventAttributes{
			Domain: common.StringPtr(domain),
			WorkflowType: common.WorkflowTypePtr(shared.WorkflowType{
				Name: common.StringPtr(childWorkflowPrefix + workflowType),
			}),
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(childWorkflowID),
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	childWorkflowComplete := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionCompleted.String())
	childWorkflowComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionCompleted.Ptr()
		historyEvent.ChildWorkflowExecutionCompletedEventAttributes = &shared.ChildWorkflowExecutionCompletedEventAttributes{
			Domain: common.StringPtr(domain),
			WorkflowType: common.WorkflowTypePtr(shared.WorkflowType{
				Name: common.StringPtr(childWorkflowPrefix + workflowType),
			}),
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(childWorkflowID),
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	childWorkflowFail := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionFailed.String())
	childWorkflowFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionFailed.Ptr()
		historyEvent.ChildWorkflowExecutionFailedEventAttributes = &shared.ChildWorkflowExecutionFailedEventAttributes{
			Domain: common.StringPtr(domain),
			WorkflowType: common.WorkflowTypePtr(shared.WorkflowType{
				Name: common.StringPtr(childWorkflowPrefix + workflowType),
			}),
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(childWorkflowID),
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	childWorkflowTerminate := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionTerminated.String())
	childWorkflowTerminate.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionTerminated.Ptr()
		historyEvent.ChildWorkflowExecutionTerminatedEventAttributes = &shared.ChildWorkflowExecutionTerminatedEventAttributes{
			Domain: common.StringPtr(domain),
			WorkflowType: common.WorkflowTypePtr(shared.WorkflowType{
				Name: common.StringPtr(childWorkflowPrefix + workflowType),
			}),
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(childWorkflowID),
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	childWorkflowTimedOut := NewHistoryEventVertex(shared.EventTypeChildWorkflowExecutionTimedOut.String())
	childWorkflowTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionTimedOut.Ptr()
		historyEvent.ChildWorkflowExecutionTimedOutEventAttributes = &shared.ChildWorkflowExecutionTimedOutEventAttributes{
			Domain: common.StringPtr(domain),
			WorkflowType: common.WorkflowTypePtr(shared.WorkflowType{
				Name: common.StringPtr(childWorkflowPrefix + workflowType),
			}),
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(childWorkflowID),
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
			TimeoutType:    common.TimeoutTypePtr(shared.TimeoutTypeScheduleToClose),
		}
		return historyEvent
	})
	decisionCompleteToChildWorkflowInitial := NewHistoryEventEdge(decisionComplete, childWorkflowInitial)
	childWorkflowInitialToFail := NewHistoryEventEdge(childWorkflowInitial, childWorkflowInitialFail)
	childWorkflowInitialToStart := NewHistoryEventEdge(childWorkflowInitial, childWorkflowStart)
	childWorkflowStartToCancel := NewHistoryEventEdge(childWorkflowStart, childWorkflowCancel)
	childWorkflowStartToFail := NewHistoryEventEdge(childWorkflowStart, childWorkflowFail)
	childWorkflowStartToComplete := NewHistoryEventEdge(childWorkflowStart, childWorkflowComplete)
	childWorkflowStartToTerminate := NewHistoryEventEdge(childWorkflowStart, childWorkflowTerminate)
	childWorkflowStartToTimedOut := NewHistoryEventEdge(childWorkflowStart, childWorkflowTimedOut)
	childWorkflowCancelToDecisionSchedule := NewHistoryEventEdge(childWorkflowCancel, decisionSchedule)
	childWorkflowCancelToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowFailToDecisionSchedule := NewHistoryEventEdge(childWorkflowFail, decisionSchedule)
	childWorkflowFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowCompleteToDecisionSchedule := NewHistoryEventEdge(childWorkflowComplete, decisionSchedule)
	childWorkflowCompleteToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowTerminateToDecisionSchedule := NewHistoryEventEdge(childWorkflowTerminate, decisionSchedule)
	childWorkflowTerminateToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowTimedOutToDecisionSchedule := NewHistoryEventEdge(childWorkflowTimedOut, decisionSchedule)
	childWorkflowTimedOutToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowInitialFailToDecisionSchedule := NewHistoryEventEdge(childWorkflowInitialFail, decisionSchedule)
	childWorkflowInitialFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	childWorkflowModel.AddEdge(decisionCompleteToChildWorkflowInitial, childWorkflowInitialToFail, childWorkflowInitialToStart,
		childWorkflowStartToCancel, childWorkflowStartToFail, childWorkflowStartToComplete, childWorkflowStartToTerminate,
		childWorkflowStartToTimedOut, childWorkflowCancelToDecisionSchedule, childWorkflowFailToDecisionSchedule,
		childWorkflowCompleteToDecisionSchedule, childWorkflowTerminateToDecisionSchedule, childWorkflowTimedOutToDecisionSchedule,
		childWorkflowInitialFailToDecisionSchedule)

	// Setup external workflow model
	externalWorkflowModel := NewHistoryEventModel()
	externalWorkflowSignal := NewHistoryEventVertex(shared.EventTypeSignalExternalWorkflowExecutionInitiated.String())
	externalWorkflowSignal.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeSignalExternalWorkflowExecutionInitiated.Ptr()
		historyEvent.SignalExternalWorkflowExecutionInitiatedEventAttributes = &shared.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
			Domain:                       common.StringPtr(domain),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(externalWorkflowID),
				RunId:      common.StringPtr(uuid.New()),
			},
			SignalName:        common.StringPtr("signal"),
			ChildWorkflowOnly: common.BoolPtr(false),
		}
		return historyEvent
	})
	externalWorkflowSignalFailed := NewHistoryEventVertex(shared.EventTypeSignalExternalWorkflowExecutionFailed.String())
	externalWorkflowSignalFailed.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeSignalExternalWorkflowExecutionFailed.Ptr()
		historyEvent.SignalExternalWorkflowExecutionFailedEventAttributes = &shared.SignalExternalWorkflowExecutionFailedEventAttributes{
			Cause:                        common.SignalExternalWorkflowExecutionFailedCausePtr(shared.SignalExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution),
			DecisionTaskCompletedEventId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().DecisionTaskCompletedEventId,
			Domain:                       common.StringPtr(domain),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
			InitiatedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	externalWorkflowSignaled := NewHistoryEventVertex(shared.EventTypeExternalWorkflowExecutionSignaled.String())
	externalWorkflowSignaled.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeExternalWorkflowExecutionSignaled.Ptr()
		historyEvent.ExternalWorkflowExecutionSignaledEventAttributes = &shared.ExternalWorkflowExecutionSignaledEventAttributes{
			InitiatedEventId: lastEvent.EventId,
			Domain:           common.StringPtr(domain),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
		}
		return historyEvent
	})
	externalWorkflowCancel := NewHistoryEventVertex(shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated.String())
	externalWorkflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated.Ptr()
		historyEvent.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes =
			&shared.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
				DecisionTaskCompletedEventId: lastEvent.EventId,
				Domain:                       common.StringPtr(domain),
				WorkflowExecution: &shared.WorkflowExecution{
					WorkflowId: common.StringPtr(externalWorkflowID),
					RunId:      common.StringPtr(uuid.New()),
				},
				ChildWorkflowOnly: common.BoolPtr(false),
			}
		return historyEvent
	})
	externalWorkflowCancelFail := NewHistoryEventVertex(shared.EventTypeRequestCancelExternalWorkflowExecutionFailed.String())
	externalWorkflowCancelFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeRequestCancelExternalWorkflowExecutionFailed.Ptr()
		historyEvent.RequestCancelExternalWorkflowExecutionFailedEventAttributes = &shared.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			Cause:                        common.CancelExternalWorkflowExecutionFailedCausePtr(shared.CancelExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution),
			DecisionTaskCompletedEventId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().DecisionTaskCompletedEventId,
			Domain:                       common.StringPtr(domain),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
			InitiatedEventId: lastEvent.EventId,
		}
		return historyEvent
	})
	externalWorkflowCanceled := NewHistoryEventVertex(shared.EventTypeExternalWorkflowExecutionCancelRequested.String())
	externalWorkflowCanceled.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*shared.HistoryEvent)
		lastGeneratedEvent := input[1].(*shared.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = shared.EventTypeExternalWorkflowExecutionCancelRequested.Ptr()
		historyEvent.ExternalWorkflowExecutionCancelRequestedEventAttributes = &shared.ExternalWorkflowExecutionCancelRequestedEventAttributes{
			InitiatedEventId: lastEvent.EventId,
			Domain:           common.StringPtr(domain),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
		}
		return historyEvent
	})
	decisionCompleteToExternalWorkflowSignal := NewHistoryEventEdge(decisionComplete, externalWorkflowSignal)
	decisionCompleteToExternalWorkflowCancel := NewHistoryEventEdge(decisionComplete, externalWorkflowCancel)
	externalWorkflowSignalToFail := NewHistoryEventEdge(externalWorkflowSignal, externalWorkflowSignalFailed)
	externalWorkflowSignalToSignaled := NewHistoryEventEdge(externalWorkflowSignal, externalWorkflowSignaled)
	externalWorkflowCancelToFail := NewHistoryEventEdge(externalWorkflowCancel, externalWorkflowCancelFail)
	externalWorkflowCancelToCanceled := NewHistoryEventEdge(externalWorkflowCancel, externalWorkflowCanceled)
	externalWorkflowSignaledToDecisionSchedule := NewHistoryEventEdge(externalWorkflowSignaled, decisionSchedule)
	externalWorkflowSignaledToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowSignalFailedToDecisionSchedule := NewHistoryEventEdge(externalWorkflowSignalFailed, decisionSchedule)
	externalWorkflowSignalFailedToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowCanceledToDecisionSchedule := NewHistoryEventEdge(externalWorkflowCanceled, decisionSchedule)
	externalWorkflowCanceledToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowCancelFailToDecisionSchedule := NewHistoryEventEdge(externalWorkflowCancelFail, decisionSchedule)
	externalWorkflowCancelFailToDecisionSchedule.SetCondition(notPendingDecisionTask)
	externalWorkflowModel.AddEdge(decisionCompleteToExternalWorkflowSignal, decisionCompleteToExternalWorkflowCancel,
		externalWorkflowSignalToFail, externalWorkflowSignalToSignaled, externalWorkflowCancelToFail, externalWorkflowCancelToCanceled,
		externalWorkflowSignaledToDecisionSchedule, externalWorkflowSignalFailedToDecisionSchedule,
		externalWorkflowCanceledToDecisionSchedule, externalWorkflowCancelFailToDecisionSchedule)

	// Config event generator
	generator.SetBatchGenerationRule(canDoBatch)
	generator.AddInitialEntryVertex(workflowStart)
	generator.AddExitVertex(workflowComplete, workflowFail, workflowTerminate, workflowTimedOut, continueAsNew)
	// generator.AddRandomEntryVertex(workflowSignal, workflowTerminate, workflowTimedOut)
	generator.AddModel(decisionModel)
	generator.AddModel(workflowModel)
	generator.AddModel(activityModel)
	generator.AddModel(timerModel)
	generator.AddModel(childWorkflowModel)
	generator.AddModel(externalWorkflowModel)
	return generator
}

func getDefaultHistoryEvent(
	eventID int64,
	version int64,
) *shared.HistoryEvent {

	globalTaskID++
	return &shared.HistoryEvent{
		EventId:   common.Int64Ptr(eventID),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		TaskId:    common.Int64Ptr(globalTaskID),
		Version:   common.Int64Ptr(version),
	}
}

func copyConnections(
	originalMap map[string][]Edge,
) map[string][]Edge {

	newMap := make(map[string][]Edge)
	for key, value := range originalMap {
		newMap[key] = copyEdges(value)
	}
	return newMap
}

func copyExitVertices(
	originalMap map[string]bool,
) map[string]bool {

	newMap := make(map[string]bool)
	for key, value := range originalMap {
		newMap[key] = value
	}
	return newMap
}

func copyVertex(vertex []Vertex) []Vertex {
	newVertex := make([]Vertex, len(vertex))
	for idx, v := range vertex {
		newVertex[idx] = v.DeepCopy()
	}
	return newVertex
}

func copyEdges(edges []Edge) []Edge {
	newEdges := make([]Edge, len(edges))
	for idx, e := range edges {
		newEdges[idx] = e.DeepCopy()
	}
	return newEdges
}
