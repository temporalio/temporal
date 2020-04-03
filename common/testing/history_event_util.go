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
	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
)

const (
	timeout              = int32(10000)
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
	namespace string,
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
			case eventpb.EventTypeDecisionTaskScheduled.String():
				count++
			case eventpb.EventTypeDecisionTaskCompleted.String(),
				eventpb.EventTypeDecisionTaskFailed.String(),
				eventpb.EventTypeDecisionTaskTimedOut.String():
				count--
			}
		}
		return count <= 0
	}
	containActivityComplete := func(input ...interface{}) bool {
		history := input[0].([]Vertex)
		for _, e := range history {
			if e.GetName() == eventpb.EventTypeActivityTaskCompleted.String() {
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
			case eventpb.EventTypeActivityTaskScheduled.String():
				count++
			case eventpb.EventTypeActivityTaskCanceled.String(),
				eventpb.EventTypeActivityTaskFailed.String(),
				eventpb.EventTypeActivityTaskTimedOut.String(),
				eventpb.EventTypeActivityTaskCompleted.String():
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
			case eventpb.EventTypeDecisionTaskScheduled.String():
				hasPendingDecisionTask = true
			case eventpb.EventTypeDecisionTaskCompleted.String(),
				eventpb.EventTypeDecisionTaskFailed.String(),
				eventpb.EventTypeDecisionTaskTimedOut.String():
				hasPendingDecisionTask = false
			}
		}
		if hasPendingDecisionTask {
			return false
		}
		if currentBatch[len(currentBatch)-1].GetName() == eventpb.EventTypeDecisionTaskScheduled.String() {
			return false
		}
		if currentBatch[0].GetName() == eventpb.EventTypeDecisionTaskCompleted.String() {
			return len(currentBatch) == 1
		}
		return true
	}

	// Setup decision task model
	decisionModel := NewHistoryEventModel()
	decisionSchedule := NewHistoryEventVertex(eventpb.EventTypeDecisionTaskScheduled.String())
	decisionSchedule.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeDecisionTaskScheduled
		historyEvent.Attributes = &eventpb.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &decisionpb.DecisionTaskScheduledEventAttributes{
			TaskList: &tasklistpb.TaskList{
				Name: taskList,
				Kind: tasklistpb.TaskListKindNormal,
			},
			StartToCloseTimeoutSeconds: timeout,
			Attempt:                    decisionTaskAttempts,
		}}
		return historyEvent
	})
	decisionStart := NewHistoryEventVertex(eventpb.EventTypeDecisionTaskStarted.String())
	decisionStart.SetIsStrictOnNextVertex(true)
	decisionStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeDecisionTaskStarted
		historyEvent.Attributes = &eventpb.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &decisionpb.DecisionTaskStartedEventAttributes{
			ScheduledEventId: lastEvent.EventId,
			Identity:         identity,
			RequestId:        uuid.New(),
		}}
		return historyEvent
	})
	decisionFail := NewHistoryEventVertex(eventpb.EventTypeDecisionTaskFailed.String())
	decisionFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeDecisionTaskFailed
		historyEvent.Attributes = &eventpb.HistoryEvent_DecisionTaskFailedEventAttributes{DecisionTaskFailedEventAttributes: &decisionpb.DecisionTaskFailedEventAttributes{
			ScheduledEventId: lastEvent.GetDecisionTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Cause:            eventpb.DecisionTaskFailedCauseUnhandledDecision,
			Identity:         identity,
			ForkEventVersion: version,
		}}
		return historyEvent
	})
	decisionTimedOut := NewHistoryEventVertex(eventpb.EventTypeDecisionTaskTimedOut.String())
	decisionTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeDecisionTaskTimedOut
		historyEvent.Attributes = &eventpb.HistoryEvent_DecisionTaskTimedOutEventAttributes{DecisionTaskTimedOutEventAttributes: &decisionpb.DecisionTaskTimedOutEventAttributes{
			ScheduledEventId: lastEvent.GetDecisionTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			TimeoutType:      eventpb.TimeoutTypeScheduleToStart,
		}}
		return historyEvent
	})
	decisionComplete := NewHistoryEventVertex(eventpb.EventTypeDecisionTaskCompleted.String())
	decisionComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeDecisionTaskCompleted
		historyEvent.Attributes = &eventpb.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &decisionpb.DecisionTaskCompletedEventAttributes{
			ScheduledEventId: lastEvent.GetDecisionTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         identity,
			BinaryChecksum:   checksum,
		}}
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

	workflowStart := NewHistoryEventVertex(eventpb.EventTypeWorkflowExecutionStarted.String())
	workflowStart.SetDataFunc(func(input ...interface{}) interface{} {
		historyEvent := getDefaultHistoryEvent(1, defaultVersion)
		historyEvent.EventType = eventpb.EventTypeWorkflowExecutionStarted
		historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &eventpb.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &commonpb.WorkflowType{
				Name: workflowType,
			},
			TaskList: &tasklistpb.TaskList{
				Name: taskList,
				Kind: tasklistpb.TaskListKindNormal,
			},
			ExecutionStartToCloseTimeoutSeconds: timeout,
			TaskStartToCloseTimeoutSeconds:      timeout,
			Identity:                            identity,
			FirstExecutionRunId:                 uuid.New(),
		}}
		return historyEvent
	})
	workflowSignal := NewHistoryEventVertex(eventpb.EventTypeWorkflowExecutionSignaled.String())
	workflowSignal.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeWorkflowExecutionSignaled
		historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &eventpb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signal,
			Identity:   identity,
		}}
		return historyEvent
	})
	workflowComplete := NewHistoryEventVertex(eventpb.EventTypeWorkflowExecutionCompleted.String())
	workflowComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeWorkflowExecutionCompleted
		historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &eventpb.WorkflowExecutionCompletedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	continueAsNew := NewHistoryEventVertex(eventpb.EventTypeWorkflowExecutionContinuedAsNew.String())
	continueAsNew.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeWorkflowExecutionContinuedAsNew
		historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &eventpb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: uuid.New(),
			WorkflowType: &commonpb.WorkflowType{
				Name: workflowType,
			},
			TaskList: &tasklistpb.TaskList{
				Name: taskList,
				Kind: tasklistpb.TaskListKindNormal,
			},
			ExecutionStartToCloseTimeoutSeconds: timeout,
			TaskStartToCloseTimeoutSeconds:      timeout,
			DecisionTaskCompletedEventId:        eventID - 1,
			Initiator:                           commonpb.ContinueAsNewInitiatorDecider,
		}}
		return historyEvent
	})
	workflowFail := NewHistoryEventVertex(eventpb.EventTypeWorkflowExecutionFailed.String())
	workflowFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeWorkflowExecutionFailed
		historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &eventpb.WorkflowExecutionFailedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	workflowCancel := NewHistoryEventVertex(eventpb.EventTypeWorkflowExecutionCanceled.String())
	workflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeWorkflowExecutionCanceled
		historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: &eventpb.WorkflowExecutionCanceledEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	workflowCancelRequest := NewHistoryEventVertex(eventpb.EventTypeWorkflowExecutionCancelRequested.String())
	workflowCancelRequest.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeWorkflowExecutionCancelRequested
		historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &eventpb.WorkflowExecutionCancelRequestedEventAttributes{
			Cause:                    "",
			ExternalInitiatedEventId: 1,
			ExternalWorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: externalWorkflowID,
				RunId:      uuid.New(),
			},
			Identity: identity,
		}}
		return historyEvent
	})
	workflowTerminate := NewHistoryEventVertex(eventpb.EventTypeWorkflowExecutionTerminated.String())
	workflowTerminate.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeWorkflowExecutionTerminated
		historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &eventpb.WorkflowExecutionTerminatedEventAttributes{
			Identity: identity,
			Reason:   reason,
		}}
		return historyEvent
	})
	workflowTimedOut := NewHistoryEventVertex(eventpb.EventTypeWorkflowExecutionTimedOut.String())
	workflowTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeWorkflowExecutionTimedOut
		historyEvent.Attributes = &eventpb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &eventpb.WorkflowExecutionTimedOutEventAttributes{
			TimeoutType: eventpb.TimeoutTypeStartToClose,
		}}
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
	activitySchedule := NewHistoryEventVertex(eventpb.EventTypeActivityTaskScheduled.String())
	activitySchedule.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeActivityTaskScheduled
		historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &eventpb.ActivityTaskScheduledEventAttributes{
			ActivityId:   uuid.New(),
			ActivityType: &commonpb.ActivityType{Name: "activity"},
			Namespace:    namespace,
			TaskList: &tasklistpb.TaskList{
				Name: taskList,
				Kind: tasklistpb.TaskListKindNormal,
			},
			ScheduleToCloseTimeoutSeconds: timeout,
			ScheduleToStartTimeoutSeconds: timeout,
			StartToCloseTimeoutSeconds:    timeout,
			DecisionTaskCompletedEventId:  lastEvent.EventId,
		}}
		return historyEvent
	})
	activityStart := NewHistoryEventVertex(eventpb.EventTypeActivityTaskStarted.String())
	activityStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeActivityTaskStarted
		historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &eventpb.ActivityTaskStartedEventAttributes{
			ScheduledEventId: lastEvent.EventId,
			Identity:         identity,
			RequestId:        uuid.New(),
			Attempt:          0,
		}}
		return historyEvent
	})
	activityComplete := NewHistoryEventVertex(eventpb.EventTypeActivityTaskCompleted.String())
	activityComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeActivityTaskCompleted
		historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &eventpb.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         identity,
		}}
		return historyEvent
	})
	activityFail := NewHistoryEventVertex(eventpb.EventTypeActivityTaskFailed.String())
	activityFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeActivityTaskFailed
		historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &eventpb.ActivityTaskFailedEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         identity,
			Reason:           reason,
		}}
		return historyEvent
	})
	activityTimedOut := NewHistoryEventVertex(eventpb.EventTypeActivityTaskTimedOut.String())
	activityTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeActivityTaskTimedOut
		historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &eventpb.ActivityTaskTimedOutEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			TimeoutType:      eventpb.TimeoutTypeScheduleToClose,
		}}
		return historyEvent
	})
	activityCancelRequest := NewHistoryEventVertex(eventpb.EventTypeActivityTaskCancelRequested.String())
	activityCancelRequest.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeActivityTaskCancelRequested
		historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: &eventpb.ActivityTaskCancelRequestedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.GetActivityTaskScheduledEventAttributes().DecisionTaskCompletedEventId,
			ActivityId:                   lastEvent.GetActivityTaskScheduledEventAttributes().ActivityId,
		}}
		return historyEvent
	})
	activityCancel := NewHistoryEventVertex(eventpb.EventTypeActivityTaskCanceled.String())
	activityCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeActivityTaskCanceled
		historyEvent.Attributes = &eventpb.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: &eventpb.ActivityTaskCanceledEventAttributes{
			LatestCancelRequestedEventId: lastEvent.EventId,
			ScheduledEventId:             lastEvent.EventId,
			StartedEventId:               lastEvent.EventId,
			Identity:                     identity,
		}}
		return historyEvent
	})
	activityCancelRequestFail := NewHistoryEventVertex(eventpb.EventTypeRequestCancelActivityTaskFailed.String())
	activityCancelRequestFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		versionBump := input[2].(int64)
		subVersion := input[3].(int64)
		version := lastGeneratedEvent.GetVersion() + versionBump + subVersion
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeRequestCancelActivityTaskFailed
		historyEvent.Attributes = &eventpb.HistoryEvent_RequestCancelActivityTaskFailedEventAttributes{RequestCancelActivityTaskFailedEventAttributes: &eventpb.RequestCancelActivityTaskFailedEventAttributes{
			ActivityId:                   uuid.New(),
			DecisionTaskCompletedEventId: lastEvent.GetActivityTaskCancelRequestedEventAttributes().DecisionTaskCompletedEventId,
		}}
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
	timerStart := NewHistoryEventVertex(eventpb.EventTypeTimerStarted.String())
	timerStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeTimerStarted
		historyEvent.Attributes = &eventpb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &eventpb.TimerStartedEventAttributes{
			TimerId:                      uuid.New(),
			StartToFireTimeoutSeconds:    10,
			DecisionTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	timerFired := NewHistoryEventVertex(eventpb.EventTypeTimerFired.String())
	timerFired.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeTimerFired
		historyEvent.Attributes = &eventpb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &eventpb.TimerFiredEventAttributes{
			TimerId:        lastEvent.GetTimerStartedEventAttributes().TimerId,
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	timerCancel := NewHistoryEventVertex(eventpb.EventTypeTimerCanceled.String())
	timerCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeTimerCanceled
		historyEvent.Attributes = &eventpb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: &eventpb.TimerCanceledEventAttributes{
			TimerId:                      lastEvent.GetTimerStartedEventAttributes().TimerId,
			StartedEventId:               lastEvent.EventId,
			DecisionTaskCompletedEventId: lastEvent.GetTimerStartedEventAttributes().DecisionTaskCompletedEventId,
			Identity:                     identity,
		}}
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
	childWorkflowInitial := NewHistoryEventVertex(eventpb.EventTypeStartChildWorkflowExecutionInitiated.String())
	childWorkflowInitial.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeStartChildWorkflowExecutionInitiated
		historyEvent.Attributes = &eventpb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &eventpb.StartChildWorkflowExecutionInitiatedEventAttributes{
			Namespace:    namespace,
			WorkflowId:   childWorkflowID,
			WorkflowType: &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			TaskList: &tasklistpb.TaskList{
				Name: taskList,
				Kind: tasklistpb.TaskListKindNormal,
			},
			ExecutionStartToCloseTimeoutSeconds: timeout,
			TaskStartToCloseTimeoutSeconds:      timeout,
			DecisionTaskCompletedEventId:        lastEvent.EventId,
			WorkflowIdReusePolicy:               commonpb.WorkflowIdReusePolicyRejectDuplicate,
		}}
		return historyEvent
	})
	childWorkflowInitialFail := NewHistoryEventVertex(eventpb.EventTypeStartChildWorkflowExecutionFailed.String())
	childWorkflowInitialFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeStartChildWorkflowExecutionFailed
		historyEvent.Attributes = &eventpb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &eventpb.StartChildWorkflowExecutionFailedEventAttributes{
			Namespace:                    namespace,
			WorkflowId:                   childWorkflowID,
			WorkflowType:                 &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			Cause:                        eventpb.ChildWorkflowExecutionFailedCauseWorkflowAlreadyRunning,
			InitiatedEventId:             lastEvent.EventId,
			DecisionTaskCompletedEventId: lastEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes().DecisionTaskCompletedEventId,
		}}
		return historyEvent
	})
	childWorkflowStart := NewHistoryEventVertex(eventpb.EventTypeChildWorkflowExecutionStarted.String())
	childWorkflowStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeChildWorkflowExecutionStarted
		historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &eventpb.ChildWorkflowExecutionStartedEventAttributes{
			Namespace:        namespace,
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.EventId,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      uuid.New(),
			},
		}}
		return historyEvent
	})
	childWorkflowCancel := NewHistoryEventVertex(eventpb.EventTypeChildWorkflowExecutionCanceled.String())
	childWorkflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeChildWorkflowExecutionCanceled
		historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &eventpb.ChildWorkflowExecutionCanceledEventAttributes{
			Namespace:        namespace,
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowComplete := NewHistoryEventVertex(eventpb.EventTypeChildWorkflowExecutionCompleted.String())
	childWorkflowComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeChildWorkflowExecutionCompleted
		historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &eventpb.ChildWorkflowExecutionCompletedEventAttributes{
			Namespace:        namespace,
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowFail := NewHistoryEventVertex(eventpb.EventTypeChildWorkflowExecutionFailed.String())
	childWorkflowFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeChildWorkflowExecutionFailed
		historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &eventpb.ChildWorkflowExecutionFailedEventAttributes{
			Namespace:        namespace,
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowTerminate := NewHistoryEventVertex(eventpb.EventTypeChildWorkflowExecutionTerminated.String())
	childWorkflowTerminate.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeChildWorkflowExecutionTerminated
		historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &eventpb.ChildWorkflowExecutionTerminatedEventAttributes{
			Namespace:        namespace,
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowTimedOut := NewHistoryEventVertex(eventpb.EventTypeChildWorkflowExecutionTimedOut.String())
	childWorkflowTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeChildWorkflowExecutionTimedOut
		historyEvent.Attributes = &eventpb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &eventpb.ChildWorkflowExecutionTimedOutEventAttributes{
			Namespace:        namespace,
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
			TimeoutType:    eventpb.TimeoutTypeScheduleToClose,
		}}
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
	externalWorkflowSignal := NewHistoryEventVertex(eventpb.EventTypeSignalExternalWorkflowExecutionInitiated.String())
	externalWorkflowSignal.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeSignalExternalWorkflowExecutionInitiated
		historyEvent.Attributes = &eventpb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: &eventpb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			DecisionTaskCompletedEventId: lastEvent.EventId,
			Namespace:                    namespace,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: externalWorkflowID,
				RunId:      uuid.New(),
			},
			SignalName:        "signal",
			ChildWorkflowOnly: false,
		}}
		return historyEvent
	})
	externalWorkflowSignalFailed := NewHistoryEventVertex(eventpb.EventTypeSignalExternalWorkflowExecutionFailed.String())
	externalWorkflowSignalFailed.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeSignalExternalWorkflowExecutionFailed
		historyEvent.Attributes = &eventpb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: &eventpb.SignalExternalWorkflowExecutionFailedEventAttributes{
			Cause:                        eventpb.SignalExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution,
			DecisionTaskCompletedEventId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().DecisionTaskCompletedEventId,
			Namespace:                    namespace,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
			InitiatedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	externalWorkflowSignaled := NewHistoryEventVertex(eventpb.EventTypeExternalWorkflowExecutionSignaled.String())
	externalWorkflowSignaled.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeExternalWorkflowExecutionSignaled
		historyEvent.Attributes = &eventpb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: &eventpb.ExternalWorkflowExecutionSignaledEventAttributes{
			InitiatedEventId: lastEvent.EventId,
			Namespace:        namespace,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
		}}
		return historyEvent
	})
	externalWorkflowCancel := NewHistoryEventVertex(eventpb.EventTypeRequestCancelExternalWorkflowExecutionInitiated.String())
	externalWorkflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeRequestCancelExternalWorkflowExecutionInitiated
		historyEvent.Attributes = &eventpb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &eventpb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
				DecisionTaskCompletedEventId: lastEvent.EventId,
				Namespace:                    namespace,
				WorkflowExecution: &executionpb.WorkflowExecution{
					WorkflowId: externalWorkflowID,
					RunId:      uuid.New(),
				},
				ChildWorkflowOnly: false,
			}}
		return historyEvent
	})
	externalWorkflowCancelFail := NewHistoryEventVertex(eventpb.EventTypeRequestCancelExternalWorkflowExecutionFailed.String())
	externalWorkflowCancelFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeRequestCancelExternalWorkflowExecutionFailed
		historyEvent.Attributes = &eventpb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: &eventpb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			Cause:                        eventpb.CancelExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution,
			DecisionTaskCompletedEventId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().DecisionTaskCompletedEventId,
			Namespace:                    namespace,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
			InitiatedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	externalWorkflowCanceled := NewHistoryEventVertex(eventpb.EventTypeExternalWorkflowExecutionCancelRequested.String())
	externalWorkflowCanceled.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*eventpb.HistoryEvent)
		lastGeneratedEvent := input[1].(*eventpb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = eventpb.EventTypeExternalWorkflowExecutionCancelRequested
		historyEvent.Attributes = &eventpb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: &eventpb.ExternalWorkflowExecutionCancelRequestedEventAttributes{
			InitiatedEventId: lastEvent.EventId,
			Namespace:        namespace,
			WorkflowExecution: &executionpb.WorkflowExecution{
				WorkflowId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
		}}
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
) *eventpb.HistoryEvent {

	globalTaskID++
	return &eventpb.HistoryEvent{
		EventId:   eventID,
		Timestamp: time.Now().UnixNano(),
		TaskId:    globalTaskID,
		Version:   version,
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
