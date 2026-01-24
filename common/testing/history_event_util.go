package testing

import (
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)
		historyEvent.SetWorkflowTaskScheduledEventAttributes(historypb.WorkflowTaskScheduledEventAttributes_builder{
			TaskQueue: taskqueuepb.TaskQueue_builder{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			}.Build(),
			StartToCloseTimeout: durationpb.New(timeout),
			Attempt:             workflowTaskAttempts,
		}.Build())
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
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED)
		historyEvent.SetWorkflowTaskStartedEventAttributes(historypb.WorkflowTaskStartedEventAttributes_builder{
			ScheduledEventId: lastEvent.GetEventId(),
			Identity:         identity,
			RequestId:        uuid.NewString(),
		}.Build())
		return historyEvent
	})
	workflowTaskFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED.String())
	workflowTaskFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED)
		historyEvent.SetWorkflowTaskFailedEventAttributes(historypb.WorkflowTaskFailedEventAttributes_builder{
			ScheduledEventId: lastEvent.GetWorkflowTaskStartedEventAttributes().GetScheduledEventId(),
			StartedEventId:   lastEvent.GetEventId(),
			Cause:            enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND,
			Identity:         identity,
			ForkEventVersion: version,
		}.Build())
		return historyEvent
	})
	workflowTaskTimedOut := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT.String())
	workflowTaskTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT)
		historyEvent.SetWorkflowTaskTimedOutEventAttributes(historypb.WorkflowTaskTimedOutEventAttributes_builder{
			ScheduledEventId: lastEvent.GetWorkflowTaskStartedEventAttributes().GetScheduledEventId(),
			StartedEventId:   lastEvent.GetEventId(),
			TimeoutType:      enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		}.Build())
		return historyEvent
	})
	workflowTaskComplete := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED.String())
	workflowTaskComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
		historyEvent.SetWorkflowTaskCompletedEventAttributes(historypb.WorkflowTaskCompletedEventAttributes_builder{
			ScheduledEventId: lastEvent.GetWorkflowTaskStartedEventAttributes().GetScheduledEventId(),
			StartedEventId:   lastEvent.GetEventId(),
			Identity:         identity,
			BinaryChecksum:   checksum,
		}.Build())
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
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
		historyEvent.SetWorkflowExecutionStartedEventAttributes(historypb.WorkflowExecutionStartedEventAttributes_builder{
			WorkflowType: commonpb.WorkflowType_builder{
				Name: workflowType,
			}.Build(),
			TaskQueue: taskqueuepb.TaskQueue_builder{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			}.Build(),
			WorkflowExecutionTimeout: durationpb.New(timeout),
			WorkflowRunTimeout:       durationpb.New(timeout),
			WorkflowTaskTimeout:      durationpb.New(timeout),
			Identity:                 identity,
			FirstExecutionRunId:      uuid.NewString(),
			Attempt:                  1,
		}.Build())
		return historyEvent
	})
	workflowSignal := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED.String())
	workflowSignal.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)
		historyEvent.SetWorkflowExecutionSignaledEventAttributes(historypb.WorkflowExecutionSignaledEventAttributes_builder{
			SignalName: signal,
			Identity:   identity,
		}.Build())
		return historyEvent
	})
	workflowComplete := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED.String())
	workflowComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED)
		historyEvent.SetWorkflowExecutionCompletedEventAttributes(historypb.WorkflowExecutionCompletedEventAttributes_builder{
			WorkflowTaskCompletedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	continueAsNew := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW.String())
	continueAsNew.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW)
		historyEvent.SetWorkflowExecutionContinuedAsNewEventAttributes(historypb.WorkflowExecutionContinuedAsNewEventAttributes_builder{
			NewExecutionRunId: uuid.NewString(),
			WorkflowType: commonpb.WorkflowType_builder{
				Name: workflowType,
			}.Build(),
			TaskQueue: taskqueuepb.TaskQueue_builder{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			}.Build(),
			WorkflowRunTimeout:           durationpb.New(timeout),
			WorkflowTaskTimeout:          durationpb.New(timeout),
			WorkflowTaskCompletedEventId: eventID - 1,
			Initiator:                    enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
		}.Build())
		return historyEvent
	})
	workflowFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED.String())
	workflowFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED)
		historyEvent.SetWorkflowExecutionFailedEventAttributes(historypb.WorkflowExecutionFailedEventAttributes_builder{
			WorkflowTaskCompletedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	workflowCancel := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED.String())
	workflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED)
		historyEvent.SetWorkflowExecutionCanceledEventAttributes(historypb.WorkflowExecutionCanceledEventAttributes_builder{
			WorkflowTaskCompletedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	workflowCancelRequest := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED.String())
	workflowCancelRequest.SetDataFunc(func(input ...interface{}) interface{} {
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED)
		historyEvent.SetWorkflowExecutionCancelRequestedEventAttributes(historypb.WorkflowExecutionCancelRequestedEventAttributes_builder{
			Cause:                    "",
			ExternalInitiatedEventId: 1,
			ExternalWorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: externalWorkflowID,
				RunId:      uuid.NewString(),
			}.Build(),
			Identity: identity,
		}.Build())
		return historyEvent
	})
	workflowTerminate := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED.String())
	workflowTerminate.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED)
		historyEvent.SetWorkflowExecutionTerminatedEventAttributes(historypb.WorkflowExecutionTerminatedEventAttributes_builder{
			Identity: identity,
			Reason:   reason,
		}.Build())
		return historyEvent
	})
	workflowTimedOut := NewHistoryEventVertex(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT.String())
	workflowTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		eventID := lastEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT)
		historyEvent.SetWorkflowExecutionTimedOutEventAttributes(historypb.WorkflowExecutionTimedOutEventAttributes_builder{
			RetryState: enumspb.RETRY_STATE_TIMEOUT,
		}.Build())
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
		historyEvent.SetEventType(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED)
		historyEvent.SetActivityTaskScheduledEventAttributes(historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId:   uuid.NewString(),
			ActivityType: commonpb.ActivityType_builder{Name: "activity"}.Build(),
			TaskQueue: taskqueuepb.TaskQueue_builder{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			}.Build(),
			ScheduleToCloseTimeout:       durationpb.New(timeout),
			ScheduleToStartTimeout:       durationpb.New(timeout),
			StartToCloseTimeout:          durationpb.New(timeout),
			WorkflowTaskCompletedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	activityStart := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED.String())
	activityStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED)
		historyEvent.SetActivityTaskStartedEventAttributes(historypb.ActivityTaskStartedEventAttributes_builder{
			ScheduledEventId: lastEvent.GetEventId(),
			Identity:         identity,
			RequestId:        uuid.NewString(),
			Attempt:          1,
		}.Build())
		return historyEvent
	})
	activityComplete := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED.String())
	activityComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED)
		historyEvent.SetActivityTaskCompletedEventAttributes(historypb.ActivityTaskCompletedEventAttributes_builder{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().GetScheduledEventId(),
			StartedEventId:   lastEvent.GetEventId(),
			Identity:         identity,
		}.Build())
		return historyEvent
	})
	activityFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED.String())
	activityFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED)
		historyEvent.SetActivityTaskFailedEventAttributes(historypb.ActivityTaskFailedEventAttributes_builder{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().GetScheduledEventId(),
			StartedEventId:   lastEvent.GetEventId(),
			Identity:         identity,
			Failure:          failure.NewServerFailure(reason, false),
		}.Build())
		return historyEvent
	})
	activityTimedOut := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT.String())
	activityTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT)
		historyEvent.SetActivityTaskTimedOutEventAttributes(historypb.ActivityTaskTimedOutEventAttributes_builder{
			ScheduledEventId: lastEvent.GetActivityTaskStartedEventAttributes().GetScheduledEventId(),
			StartedEventId:   lastEvent.GetEventId(),
			Failure: failurepb.Failure_builder{
				TimeoutFailureInfo: failurepb.TimeoutFailureInfo_builder{
					TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
				}.Build(),
			}.Build(),
		}.Build())
		return historyEvent
	})
	activityCancelRequest := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED.String())
	activityCancelRequest.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED)
		historyEvent.SetActivityTaskCancelRequestedEventAttributes(historypb.ActivityTaskCancelRequestedEventAttributes_builder{
			WorkflowTaskCompletedEventId: lastEvent.GetActivityTaskScheduledEventAttributes().GetWorkflowTaskCompletedEventId(),
			ScheduledEventId:             lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	activityCancel := NewHistoryEventVertex(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED.String())
	activityCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED)
		historyEvent.SetActivityTaskCanceledEventAttributes(historypb.ActivityTaskCanceledEventAttributes_builder{
			LatestCancelRequestedEventId: lastEvent.GetEventId(),
			ScheduledEventId:             lastEvent.GetEventId(),
			StartedEventId:               lastEvent.GetEventId(),
			Identity:                     identity,
		}.Build())
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
		historyEvent.SetEventType(enumspb.EVENT_TYPE_TIMER_STARTED)
		historyEvent.SetTimerStartedEventAttributes(historypb.TimerStartedEventAttributes_builder{
			TimerId:                      uuid.NewString(),
			StartToFireTimeout:           durationpb.New(10 * time.Second),
			WorkflowTaskCompletedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	timerFired := NewHistoryEventVertex(enumspb.EVENT_TYPE_TIMER_FIRED.String())
	timerFired.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_TIMER_FIRED)
		historyEvent.SetTimerFiredEventAttributes(historypb.TimerFiredEventAttributes_builder{
			TimerId:        lastEvent.GetTimerStartedEventAttributes().GetTimerId(),
			StartedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	timerCancel := NewHistoryEventVertex(enumspb.EVENT_TYPE_TIMER_CANCELED.String())
	timerCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_TIMER_CANCELED)
		historyEvent.SetTimerCanceledEventAttributes(historypb.TimerCanceledEventAttributes_builder{
			TimerId:                      lastEvent.GetTimerStartedEventAttributes().GetTimerId(),
			StartedEventId:               lastEvent.GetEventId(),
			WorkflowTaskCompletedEventId: lastEvent.GetTimerStartedEventAttributes().GetWorkflowTaskCompletedEventId(),
			Identity:                     identity,
		}.Build())
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
		historyEvent.SetEventType(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED)
		historyEvent.SetStartChildWorkflowExecutionInitiatedEventAttributes(historypb.StartChildWorkflowExecutionInitiatedEventAttributes_builder{
			Namespace:    nsName.String(),
			NamespaceId:  nsID.String(),
			WorkflowId:   childWorkflowID,
			WorkflowType: commonpb.WorkflowType_builder{Name: childWorkflowPrefix + workflowType}.Build(),
			TaskQueue: taskqueuepb.TaskQueue_builder{
				Name: taskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			}.Build(),
			WorkflowExecutionTimeout:     durationpb.New(timeout),
			WorkflowRunTimeout:           durationpb.New(timeout),
			WorkflowTaskTimeout:          durationpb.New(timeout),
			WorkflowTaskCompletedEventId: lastEvent.GetEventId(),
			WorkflowIdReusePolicy:        enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		}.Build())
		return historyEvent
	})
	childWorkflowInitialFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED.String())
	childWorkflowInitialFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED)
		historyEvent.SetStartChildWorkflowExecutionFailedEventAttributes(historypb.StartChildWorkflowExecutionFailedEventAttributes_builder{
			Namespace:                    nsName.String(),
			NamespaceId:                  nsID.String(),
			WorkflowId:                   childWorkflowID,
			WorkflowType:                 commonpb.WorkflowType_builder{Name: childWorkflowPrefix + workflowType}.Build(),
			Cause:                        enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS,
			InitiatedEventId:             lastEvent.GetEventId(),
			WorkflowTaskCompletedEventId: lastEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowTaskCompletedEventId(),
		}.Build())
		return historyEvent
	})
	childWorkflowStart := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED.String())
	childWorkflowStart.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED)
		historyEvent.SetChildWorkflowExecutionStartedEventAttributes(historypb.ChildWorkflowExecutionStartedEventAttributes_builder{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     commonpb.WorkflowType_builder{Name: childWorkflowPrefix + workflowType}.Build(),
			InitiatedEventId: lastEvent.GetEventId(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: childWorkflowID,
				RunId:      uuid.NewString(),
			}.Build(),
		}.Build())
		return historyEvent
	})
	childWorkflowCancel := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED.String())
	childWorkflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED)
		historyEvent.SetChildWorkflowExecutionCanceledEventAttributes(historypb.ChildWorkflowExecutionCanceledEventAttributes_builder{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     commonpb.WorkflowType_builder{Name: childWorkflowPrefix + workflowType}.Build(),
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetInitiatedEventId(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().GetRunId(),
			}.Build(),
			StartedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	childWorkflowComplete := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED.String())
	childWorkflowComplete.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED)
		historyEvent.SetChildWorkflowExecutionCompletedEventAttributes(historypb.ChildWorkflowExecutionCompletedEventAttributes_builder{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     commonpb.WorkflowType_builder{Name: childWorkflowPrefix + workflowType}.Build(),
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetInitiatedEventId(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().GetRunId(),
			}.Build(),
			StartedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	childWorkflowFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED.String())
	childWorkflowFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED)
		historyEvent.SetChildWorkflowExecutionFailedEventAttributes(historypb.ChildWorkflowExecutionFailedEventAttributes_builder{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     commonpb.WorkflowType_builder{Name: childWorkflowPrefix + workflowType}.Build(),
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetInitiatedEventId(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().GetRunId(),
			}.Build(),
			StartedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	childWorkflowTerminate := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED.String())
	childWorkflowTerminate.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED)
		historyEvent.SetChildWorkflowExecutionTerminatedEventAttributes(historypb.ChildWorkflowExecutionTerminatedEventAttributes_builder{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     commonpb.WorkflowType_builder{Name: childWorkflowPrefix + workflowType}.Build(),
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetInitiatedEventId(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().GetRunId(),
			}.Build(),
			StartedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	childWorkflowTimedOut := NewHistoryEventVertex(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT.String())
	childWorkflowTimedOut.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT)
		historyEvent.SetChildWorkflowExecutionTimedOutEventAttributes(historypb.ChildWorkflowExecutionTimedOutEventAttributes_builder{
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowType:     commonpb.WorkflowType_builder{Name: childWorkflowPrefix + workflowType}.Build(),
			InitiatedEventId: lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetInitiatedEventId(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: childWorkflowID,
				RunId:      lastEvent.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().GetRunId(),
			}.Build(),
			StartedEventId: lastEvent.GetEventId(),
			RetryState:     enumspb.RETRY_STATE_TIMEOUT,
		}.Build())
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
		historyEvent.SetEventType(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
		historyEvent.SetSignalExternalWorkflowExecutionInitiatedEventAttributes(historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes_builder{
			WorkflowTaskCompletedEventId: lastEvent.GetEventId(),
			Namespace:                    nsName.String(),
			NamespaceId:                  nsID.String(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: externalWorkflowID,
				RunId:      uuid.NewString(),
			}.Build(),
			SignalName:        "signal",
			ChildWorkflowOnly: false,
		}.Build())
		return historyEvent
	})
	externalWorkflowSignalFailed := NewHistoryEventVertex(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED.String())
	externalWorkflowSignalFailed.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED)
		historyEvent.SetSignalExternalWorkflowExecutionFailedEventAttributes(historypb.SignalExternalWorkflowExecutionFailedEventAttributes_builder{
			Cause:                        enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
			WorkflowTaskCompletedEventId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowTaskCompletedEventId(),
			Namespace:                    nsName.String(),
			NamespaceId:                  nsID.String(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().GetWorkflowId(),
				RunId:      lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().GetRunId(),
			}.Build(),
			InitiatedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	externalWorkflowSignaled := NewHistoryEventVertex(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED.String())
	externalWorkflowSignaled.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED)
		historyEvent.SetExternalWorkflowExecutionSignaledEventAttributes(historypb.ExternalWorkflowExecutionSignaledEventAttributes_builder{
			InitiatedEventId: lastEvent.GetEventId(),
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().GetWorkflowId(),
				RunId:      lastEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().GetRunId(),
			}.Build(),
		}.Build())
		return historyEvent
	})
	externalWorkflowCancel := NewHistoryEventVertex(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED.String())
	externalWorkflowCancel.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED)
		historyEvent.SetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes(historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes_builder{
			WorkflowTaskCompletedEventId: lastEvent.GetEventId(),
			Namespace:                    nsName.String(),
			NamespaceId:                  nsID.String(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: externalWorkflowID,
				RunId:      uuid.NewString(),
			}.Build(),
			ChildWorkflowOnly: false,
		}.Build())
		return historyEvent
	})
	externalWorkflowCancelFail := NewHistoryEventVertex(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED.String())
	externalWorkflowCancelFail.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED)
		historyEvent.SetRequestCancelExternalWorkflowExecutionFailedEventAttributes(historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes_builder{
			Cause:                        enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
			WorkflowTaskCompletedEventId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowTaskCompletedEventId(),
			Namespace:                    nsName.String(),
			NamespaceId:                  nsID.String(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().GetWorkflowId(),
				RunId:      lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().GetRunId(),
			}.Build(),
			InitiatedEventId: lastEvent.GetEventId(),
		}.Build())
		return historyEvent
	})
	externalWorkflowCanceled := NewHistoryEventVertex(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED.String())
	externalWorkflowCanceled.SetDataFunc(func(input ...interface{}) interface{} {
		lastEvent := input[0].(*historypb.HistoryEvent)
		lastGeneratedEvent := input[1].(*historypb.HistoryEvent)
		eventID := lastGeneratedEvent.GetEventId() + 1
		version := input[2].(int64)
		historyEvent := getDefaultHistoryEvent(eventID, version)
		historyEvent.SetEventType(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED)
		historyEvent.SetExternalWorkflowExecutionCancelRequestedEventAttributes(historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes_builder{
			InitiatedEventId: lastEvent.GetEventId(),
			Namespace:        nsName.String(),
			NamespaceId:      nsID.String(),
			WorkflowExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().GetWorkflowId(),
				RunId:      lastEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes().GetWorkflowExecution().GetRunId(),
			}.Build(),
		}.Build())
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
	return historypb.HistoryEvent_builder{
		EventId:   eventID,
		EventTime: timestamppb.New(time.Now().UTC()),
		TaskId:    globalTaskID,
		Version:   version,
	}.Build()
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
