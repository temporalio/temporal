// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
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
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	timeout              = 10000 * time.Second
	signal               = "NDC signal"
	checksum             = "NDC checksum"
	childWorkflowPrefix  = "child-"
	reason               = "NDC reason"
	workflowType         = "test-workflow-type"
	taskQueue            = "taskQueue"
	identity             = "identity"
	workflowTaskAttempts = 1
	childWorkflowID      = "child-workflowID"
	externalWorkflowID   = "external-workflowID"
)

var (
	globalTaskID int64 = 1
)

// InitializeHistoryEventGenerator initializes the history event generator
func InitializeHistoryEventGenerator(
	nsName namespace.Name,
	nsID namespace.ID,
	defaultVersion int64,
) Generator {

	generator := NewEventGenerator(time.Now().UnixNano())
	generator.SetVersion(defaultVersion)
	// Functions
	notPendingWorkflowTask := func(input ...interface{}) bool {
		count := 0
		history := input[0].([]Vertex)
		for _, e := range history {
			switch e.GetName() {
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED.String():
				count++
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED.String(),
				enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED.String(),
				enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT.String():
				count--
			}
		}
		return count <= 0
	}
	containActivityComplete := func(input ...interface{}) bool {
		history := input[0].([]Vertex)
		for _, e := range history {
			if e.GetName() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED.String() {
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
			case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED.String():
				count++
			case enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED.String(),
				enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED.String(),
				enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT.String(),
				enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED.String():
				count--
			}
		}
		return count > 0
	}
	canDoBatch := func(currentBatch []Vertex, history []Vertex) bool {
		if len(currentBatch) == 0 {
			return true
		}

		hasPendingWorkflowTask := false
		for _, event := range history {
			switch event.GetName() {
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED.String():
				hasPendingWorkflowTask = true
			case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED.String(),
				enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED.String(),
				enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT.String():
				hasPendingWorkflowTask = false
			}
		}
		if hasPendingWorkflowTask {
			return false
		}
		if currentBatch[len(currentBatch)-1].GetName() == enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED.String() {
			return false
		}
		if currentBatch[0].GetName() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED.String() {
			return len(currentBatch) == 1
		}
		return true
	}

	// Setup workflow task model
	historyEventModel := NewHistoryEventModel()
	workflowTaskSchedule := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED.String())
	workflowTaskSchedule.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			StartToCloseTimeout: timestamp.DurationPtr(timeout),
			Attempt:             workflowTaskAttempts,
		}}
		return historyEvent
	})
	workflowTaskStart := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED.String())
	workflowTaskStart.SetIsStrictOnNextVertex(true)
	workflowTaskStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId: lastEvent.EventId,
			Identity:         identity,
			RequestId:        uuid.New(),
		}}
		return historyEvent
	})
	workflowTaskFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED.String())
	workflowTaskFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: lastEvent.GetWorkflowTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Cause:            enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND,
			Identity:         identity,
			ForkEventVersion: version,
		}}
		return historyEvent
	})
	workflowTaskTimedOut := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT.String())
	workflowTaskTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
			ScheduledEventId: lastEvent.GetWorkflowTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			TimeoutType:      enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		}}
		return historyEvent
	})
	workflowTaskComplete := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED.String())
	workflowTaskComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: lastEvent.GetWorkflowTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         identity,
			BinaryChecksum:   checksum,
		}}
		return historyEvent
	})
	workflowTaskComplete.SetIsStrictOnNextVertex(true)
	workflowTaskComplete.SetMaxNextVertex(2)
	workflowTaskScheduleToStart := NewHistoryEventEdge(workflowTaskSchedule, workflowTaskStart)
	workflowTaskStartToComplete := NewHistoryEventEdge(workflowTaskStart, workflowTaskComplete)
	workflowTaskStartToFail := NewHistoryEventEdge(workflowTaskStart, workflowTaskFail)
	workflowTaskStartToTimedOut := NewHistoryEventEdge(workflowTaskStart, workflowTaskTimedOut)
	workflowTaskFailToSchedule := NewHistoryEventEdge(workflowTaskFail, workflowTaskSchedule)
	workflowTaskFailToSchedule.SetCondition(notPendingWorkflowTask)
	workflowTaskTimedOutToSchedule := NewHistoryEventEdge(workflowTaskTimedOut, workflowTaskSchedule)
	workflowTaskTimedOutToSchedule.SetCondition(notPendingWorkflowTask)
	historyEventModel.AddEdge(workflowTaskScheduleToStart, workflowTaskStartToComplete, workflowTaskStartToFail, workflowTaskStartToTimedOut,
		workflowTaskFailToSchedule, workflowTaskTimedOutToSchedule)

	// Setup workflow model
	workflowModel := NewHistoryEventModel()

	workflowStart := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED.String())
	workflowStart.SetDataFunc(func(input ...interface{}) interface{} {
		historyEvent := getDefaultHistoryEvent(1, defaultVersion)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &commonpb.WorkflowType{
				Name: workflowType,
			},
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			WorkflowExecutionTimeout: timestamp.DurationPtr(timeout),
			WorkflowRunTimeout:       timestamp.DurationPtr(timeout),
			WorkflowTaskTimeout:      timestamp.DurationPtr(timeout),
			Identity:                 identity,
			FirstExecutionRunId:      uuid.New(),
			Attempt:                  1,
		}}
		return historyEvent
	})
	workflowSignal := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED.String())
	workflowSignal.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: signal,
			Identity:   identity,
		}}
		return historyEvent
	})
	workflowComplete := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED.String())
	workflowComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
			WorkflowTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	continueAsNew := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW.String())
	continueAsNew.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: uuid.New(),
			WorkflowType: &commonpb.WorkflowType{
				Name: workflowType,
			},
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			WorkflowRunTimeout:           timestamp.DurationPtr(timeout),
			WorkflowTaskTimeout:          timestamp.DurationPtr(timeout),
			WorkflowTaskCompletedEventId: eventID - 1,
			Initiator:                    enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
		}}
		return historyEvent
	})
	workflowFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED.String())
	workflowFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	workflowCancel := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED.String())
	workflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{
			WorkflowTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	workflowCancelRequest := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED.String())
	workflowCancelRequest.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
			Cause:                    "",
			ExternalInitiatedEventId: 1,
			ExternalWorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: externalWorkflowID,
				RunId:      uuid.New(),
			},
			Identity: identity,
		}}
		return historyEvent
	})
	workflowTerminate := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED.String())
	workflowTerminate.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
			Identity: identity,
			Reason:   reason,
		}}
		return historyEvent
	})
	workflowTimedOut := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT.String())
	workflowTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
		historyEvent.Attributes = &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
			RetryState: enumspb.RETRY_STATE_TIMEOUT,
		}}
		return historyEvent
	})
	workflowStartToSignal := NewHistoryEventEdge(workflowStart, workflowSignal)
	workflowStartToWorkflowTaskSchedule := NewHistoryEventEdge(workflowStart, workflowTaskSchedule)
	workflowStartToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	workflowSignalToWorkflowTaskSchedule := NewHistoryEventEdge(workflowSignal, workflowTaskSchedule)
	workflowSignalToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	workflowTaskCompleteToWorkflowComplete := NewHistoryEventEdge(workflowTaskComplete, workflowComplete)
	workflowTaskCompleteToWorkflowComplete.SetCondition(containActivityComplete)
	workflowTaskCompleteToWorkflowFailed := NewHistoryEventEdge(workflowTaskComplete, workflowFail)
	workflowTaskCompleteToWorkflowFailed.SetCondition(containActivityComplete)
	workflowTaskCompleteToCAN := NewHistoryEventEdge(workflowTaskComplete, continueAsNew)
	workflowTaskCompleteToCAN.SetCondition(containActivityComplete)
	workflowCancelRequestToCancel := NewHistoryEventEdge(workflowCancelRequest, workflowCancel)
	workflowModel.AddEdge(workflowStartToSignal, workflowStartToWorkflowTaskSchedule, workflowSignalToWorkflowTaskSchedule,
		workflowTaskCompleteToCAN, workflowTaskCompleteToWorkflowComplete, workflowTaskCompleteToWorkflowFailed, workflowCancelRequestToCancel)

	// Setup activity model
	activityModel := NewHistoryEventModel()
	activitySchedule := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED.String())
	activitySchedule.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
		historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			ActivityId:   uuid.New(),
			ActivityType: &commonpb.ActivityType{Name: "activity"},
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			ScheduleToCloseTimeout:       timestamp.DurationPtr(timeout),
			ScheduleToStartTimeout:       timestamp.DurationPtr(timeout),
			StartToCloseTimeout:          timestamp.DurationPtr(timeout),
			WorkflowTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	activityStart := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED.String())
	activityStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED
		historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
			ScheduledEventId: lastEvent.EventId,
			Identity:         identity,
			RequestId:        uuid.New(),
			Attempt:          1,
		}}
		return historyEvent
	})
	activityComplete := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED.String())
	activityComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED
		historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         identity,
		}}
		return historyEvent
	})
	activityFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED.String())
	activityFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED
		historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Identity:         identity,
			Failure:          failure.NewServerFailure(reason, false),
		}}
		return historyEvent
	})
	activityTimedOut := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT.String())
	activityTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
		historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().ScheduledEventId,
			StartedEventId:   lastEvent.EventId,
			Failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
				}},
			},
		}}
		return historyEvent
	})
	activityCancelRequest := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED.String())
	activityCancelRequest.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
		historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{
			WorkflowTaskCompletedEventId: lastEvent.GetActivityTaskScheduledEventAttributes().WorkflowTaskCompletedEventId,
			ScheduledEventId:             lastEvent.GetEventId(),
		}}
		return historyEvent
	})
	activityCancel := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED.String())
	activityCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED
		historyEvent.Attributes = &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
			LatestCancelRequestedEventId: lastEvent.EventId,
			ScheduledEventId:             lastEvent.EventId,
			StartedEventId:               lastEvent.EventId,
			Identity:                     identity,
		}}
		return historyEvent
	})
	workflowTaskCompleteToATSchedule := NewHistoryEventEdge(workflowTaskComplete, activitySchedule)

	activityScheduleToStart := NewHistoryEventEdge(activitySchedule, activityStart)
	activityScheduleToStart.SetCondition(hasPendingActivity)

	activityStartToComplete := NewHistoryEventEdge(activityStart, activityComplete)
	activityStartToComplete.SetCondition(hasPendingActivity)

	activityStartToFail := NewHistoryEventEdge(activityStart, activityFail)
	activityStartToFail.SetCondition(hasPendingActivity)

	activityStartToTimedOut := NewHistoryEventEdge(activityStart, activityTimedOut)
	activityStartToTimedOut.SetCondition(hasPendingActivity)

	activityCompleteToWorkflowTaskSchedule := NewHistoryEventEdge(activityComplete, workflowTaskSchedule)
	activityCompleteToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	activityFailToWorkflowTaskSchedule := NewHistoryEventEdge(activityFail, workflowTaskSchedule)
	activityFailToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	activityTimedOutToWorkflowTaskSchedule := NewHistoryEventEdge(activityTimedOut, workflowTaskSchedule)
	activityTimedOutToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	activityCancelToWorkflowTaskSchedule := NewHistoryEventEdge(activityCancel, workflowTaskSchedule)
	activityCancelToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)

	// TODO: bypass activity cancel request event. Support this event later.
	// activityScheduleToActivityCancelRequest := NewHistoryEventEdge(activitySchedule, activityCancelRequest)
	// activityScheduleToActivityCancelRequest.SetCondition(hasPendingActivity)
	activityCancelReqToCancel := NewHistoryEventEdge(activityCancelRequest, activityCancel)
	activityCancelReqToCancel.SetCondition(hasPendingActivity)

	activityModel.AddEdge(workflowTaskCompleteToATSchedule, activityScheduleToStart, activityStartToComplete,
		activityStartToFail, activityStartToTimedOut, workflowTaskCompleteToATSchedule, activityCompleteToWorkflowTaskSchedule,
		activityFailToWorkflowTaskSchedule, activityTimedOutToWorkflowTaskSchedule, activityCancelReqToCancel,
		activityCancelToWorkflowTaskSchedule)

	// Setup timer model
	timerModel := NewHistoryEventModel()
	timerStart := NewHistoryEventVertex(enumspb.EVENT_TYPE_TIMER_STARTED.String())
	timerStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_TIMER_STARTED
		historyEvent.Attributes = &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
			TimerId:                      uuid.New(),
			StartToFireTimeout:           timestamp.DurationPtr(10 * time.Second),
			WorkflowTaskCompletedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	timerFired := NewHistoryEventVertex(enumspb.EVENT_TYPE_TIMER_FIRED.String())
	timerFired.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_TIMER_FIRED
		historyEvent.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
			TimerId:        lastEvent.GetTimerStartedEventAttributes().TimerId,
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	timerCancel := NewHistoryEventVertex(enumspb.EVENT_TYPE_TIMER_CANCELED.String())
	timerCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_TIMER_CANCELED
		historyEvent.Attributes = &historypb.HistoryEvent_TimerCanceledEventAttributes{TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{
			TimerId:                      lastEvent.GetTimerStartedEventAttributes().TimerId,
			StartedEventId:               lastEvent.EventId,
			WorkflowTaskCompletedEventId: lastEvent.GetTimerStartedEventAttributes().WorkflowTaskCompletedEventId,
			Identity:                     identity,
		}}
		return historyEvent
	})
	timerStartToFire := NewHistoryEventEdge(timerStart, timerFired)
	timerStartToCancel := NewHistoryEventEdge(timerStart, timerCancel)

	workflowTaskCompleteToTimerStart := NewHistoryEventEdge(workflowTaskComplete, timerStart)
	timerFiredToWorkflowTaskSchedule := NewHistoryEventEdge(timerFired, workflowTaskSchedule)
	timerFiredToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	timerCancelToWorkflowTaskSchedule := NewHistoryEventEdge(timerCancel, workflowTaskSchedule)
	timerCancelToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	timerModel.AddEdge(timerStartToFire, timerStartToCancel, workflowTaskCompleteToTimerStart, timerFiredToWorkflowTaskSchedule, timerCancelToWorkflowTaskSchedule)

	// Setup child workflow model
	childWorkflowModel := NewHistoryEventModel()
	childWorkflowInitial := NewHistoryEventVertex(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED.String())
	childWorkflowInitial.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
		historyEvent.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
			Namespace:    nsName.String(),
			NamespaceId:  nsID.String(),
			WorkflowId:   childWorkflowID,
			WorkflowType: &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			WorkflowExecutionTimeout:     timestamp.DurationPtr(timeout),
			WorkflowRunTimeout:           timestamp.DurationPtr(timeout),
			WorkflowTaskTimeout:          timestamp.DurationPtr(timeout),
			WorkflowTaskCompletedEventId: lastEvent.EventId,
			WorkflowIdReusePolicy:        enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		}}
		return historyEvent
	})
	childWorkflowInitialFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED.String())
	childWorkflowInitialFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED
		historyEvent.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{
			Namespace:                    nsName.String(),
			NamespaceId:                  nsID.String(),
			WorkflowId:                   childWorkflowID,
			WorkflowType:                 &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			Cause:                        enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS,
			InitiatedEventId:             lastEvent.EventId,
			WorkflowTaskCompletedEventId: lastEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes().WorkflowTaskCompletedEventId,
		}}
		return historyEvent
	})
	childWorkflowStart := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED.String())
	childWorkflowStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
		historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.EventId,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      uuid.New(),
			},
		}}
		return historyEvent
	})
	childWorkflowCancel := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED.String())
	childWorkflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED
		historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowComplete := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED.String())
	childWorkflowComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
		historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED.String())
	childWorkflowFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED
		historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowTerminate := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED.String())
	childWorkflowTerminate.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED
		historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	childWorkflowTimedOut := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT.String())
	childWorkflowTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT
		historyEvent.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     &commonpb.WorkflowType{Name: childWorkflowPrefix + workflowType},
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().RunId,
			},
			StartedEventId: lastEvent.EventId,
			RetryState:     enumspb.RETRY_STATE_TIMEOUT,
		}}
		return historyEvent
	})
	workflowTaskCompleteToChildWorkflowInitial := NewHistoryEventEdge(workflowTaskComplete, childWorkflowInitial)
	childWorkflowInitialToFail := NewHistoryEventEdge(childWorkflowInitial, childWorkflowInitialFail)
	childWorkflowInitialToStart := NewHistoryEventEdge(childWorkflowInitial, childWorkflowStart)
	childWorkflowStartToCancel := NewHistoryEventEdge(childWorkflowStart, childWorkflowCancel)
	childWorkflowStartToFail := NewHistoryEventEdge(childWorkflowStart, childWorkflowFail)
	childWorkflowStartToComplete := NewHistoryEventEdge(childWorkflowStart, childWorkflowComplete)
	childWorkflowStartToTerminate := NewHistoryEventEdge(childWorkflowStart, childWorkflowTerminate)
	childWorkflowStartToTimedOut := NewHistoryEventEdge(childWorkflowStart, childWorkflowTimedOut)
	childWorkflowCancelToWorkflowTaskSchedule := NewHistoryEventEdge(childWorkflowCancel, workflowTaskSchedule)
	childWorkflowCancelToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	childWorkflowFailToWorkflowTaskSchedule := NewHistoryEventEdge(childWorkflowFail, workflowTaskSchedule)
	childWorkflowFailToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	childWorkflowCompleteToWorkflowTaskSchedule := NewHistoryEventEdge(childWorkflowComplete, workflowTaskSchedule)
	childWorkflowCompleteToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	childWorkflowTerminateToWorkflowTaskSchedule := NewHistoryEventEdge(childWorkflowTerminate, workflowTaskSchedule)
	childWorkflowTerminateToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	childWorkflowTimedOutToWorkflowTaskSchedule := NewHistoryEventEdge(childWorkflowTimedOut, workflowTaskSchedule)
	childWorkflowTimedOutToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	childWorkflowInitialFailToWorkflowTaskSchedule := NewHistoryEventEdge(childWorkflowInitialFail, workflowTaskSchedule)
	childWorkflowInitialFailToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	childWorkflowModel.AddEdge(workflowTaskCompleteToChildWorkflowInitial, childWorkflowInitialToFail, childWorkflowInitialToStart,
		childWorkflowStartToCancel, childWorkflowStartToFail, childWorkflowStartToComplete, childWorkflowStartToTerminate,
		childWorkflowStartToTimedOut, childWorkflowCancelToWorkflowTaskSchedule, childWorkflowFailToWorkflowTaskSchedule,
		childWorkflowCompleteToWorkflowTaskSchedule, childWorkflowTerminateToWorkflowTaskSchedule, childWorkflowTimedOutToWorkflowTaskSchedule,
		childWorkflowInitialFailToWorkflowTaskSchedule)

	// Setup external workflow model
	externalWorkflowModel := NewHistoryEventModel()
	externalWorkflowSignal := NewHistoryEventVertex(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED.String())
	externalWorkflowSignal.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
		historyEvent.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			WorkflowTaskCompletedEventId: lastEvent.EventId,
			Namespace:                    nsName.String(),
			NamespaceId:                  nsID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: externalWorkflowID,
				RunId:      uuid.New(),
			},
			SignalName:        "signal",
			ChildWorkflowOnly: false,
		}}
		return historyEvent
	})
	externalWorkflowSignalFailed := NewHistoryEventVertex(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED.String())
	externalWorkflowSignalFailed.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
		historyEvent.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{
			Cause:                        enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
			WorkflowTaskCompletedEventId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().WorkflowTaskCompletedEventId,
			Namespace:                    nsName.String(),
			NamespaceId:                  nsID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
			InitiatedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	externalWorkflowSignaled := NewHistoryEventVertex(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED.String())
	externalWorkflowSignaled.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED
		historyEvent.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{
			InitiatedEventId: lastEvent.EventId,
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
		}}
		return historyEvent
	})
	externalWorkflowCancel := NewHistoryEventVertex(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED.String())
	externalWorkflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
		historyEvent.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
				WorkflowTaskCompletedEventId: lastEvent.EventId,
				Namespace:                    nsName.String(),
				NamespaceId:                  nsID.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: externalWorkflowID,
					RunId:      uuid.New(),
				},
				ChildWorkflowOnly: false,
			}}
		return historyEvent
	})
	externalWorkflowCancelFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED.String())
	externalWorkflowCancelFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED
		historyEvent.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			Cause:                        enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
			WorkflowTaskCompletedEventId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().WorkflowTaskCompletedEventId,
			Namespace:                    nsName.String(),
			NamespaceId:                  nsID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
			InitiatedEventId: lastEvent.EventId,
		}}
		return historyEvent
	})
	externalWorkflowCanceled := NewHistoryEventVertex(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED.String())
	externalWorkflowCanceled.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.EventType = enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED
		historyEvent.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{
			InitiatedEventId: lastEvent.EventId,
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().WorkflowId,
				RunId:      lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().RunId,
			},
		}}
		return historyEvent
	})
	workflowTaskCompleteToExternalWorkflowSignal := NewHistoryEventEdge(workflowTaskComplete, externalWorkflowSignal)
	workflowTaskCompleteToExternalWorkflowCancel := NewHistoryEventEdge(workflowTaskComplete, externalWorkflowCancel)
	externalWorkflowSignalToFail := NewHistoryEventEdge(externalWorkflowSignal, externalWorkflowSignalFailed)
	externalWorkflowSignalToSignaled := NewHistoryEventEdge(externalWorkflowSignal, externalWorkflowSignaled)
	externalWorkflowCancelToFail := NewHistoryEventEdge(externalWorkflowCancel, externalWorkflowCancelFail)
	externalWorkflowCancelToCanceled := NewHistoryEventEdge(externalWorkflowCancel, externalWorkflowCanceled)
	externalWorkflowSignaledToWorkflowTaskSchedule := NewHistoryEventEdge(externalWorkflowSignaled, workflowTaskSchedule)
	externalWorkflowSignaledToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	externalWorkflowSignalFailedToWorkflowTaskSchedule := NewHistoryEventEdge(externalWorkflowSignalFailed, workflowTaskSchedule)
	externalWorkflowSignalFailedToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	externalWorkflowCanceledToWorkflowTaskSchedule := NewHistoryEventEdge(externalWorkflowCanceled, workflowTaskSchedule)
	externalWorkflowCanceledToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	externalWorkflowCancelFailToWorkflowTaskSchedule := NewHistoryEventEdge(externalWorkflowCancelFail, workflowTaskSchedule)
	externalWorkflowCancelFailToWorkflowTaskSchedule.SetCondition(notPendingWorkflowTask)
	externalWorkflowModel.AddEdge(workflowTaskCompleteToExternalWorkflowSignal, workflowTaskCompleteToExternalWorkflowCancel,
		externalWorkflowSignalToFail, externalWorkflowSignalToSignaled, externalWorkflowCancelToFail, externalWorkflowCancelToCanceled,
		externalWorkflowSignaledToWorkflowTaskSchedule, externalWorkflowSignalFailedToWorkflowTaskSchedule,
		externalWorkflowCanceledToWorkflowTaskSchedule, externalWorkflowCancelFailToWorkflowTaskSchedule)

	// Config event generator
	generator.SetBatchGenerationRule(canDoBatch)
	generator.AddInitialEntryVertex(workflowStart)
	generator.AddExitVertex(workflowComplete, workflowFail, workflowTerminate, workflowTimedOut, continueAsNew)
	// generator.AddRandomEntryVertex(workflowSignal, workflowTerminate, workflowTimedOut)
	generator.AddModel(historyEventModel)
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
) *historypb.HistoryEvent {

	globalTaskID++
	return &historypb.HistoryEvent{
		EventId:   eventID,
		EventTime: timestamp.TimePtr(time.Now().UTC()),
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
