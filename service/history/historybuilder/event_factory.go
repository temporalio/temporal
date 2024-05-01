// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package historybuilder

import (
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
)

type EventFactory struct {
	timeSource clock.TimeSource
	version    int64
}

func (b *EventFactory) CreateWorkflowExecutionStartedEvent(
	startTime time.Time,
	request *historyservice.StartWorkflowExecutionRequest,
	resetPoints *workflowpb.ResetPoints,
	prevRunID string,
	firstRunID string,
	originalRunID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, startTime)
	req := request.StartRequest
	attributes := &historypb.WorkflowExecutionStartedEventAttributes{
		WorkflowType:                    req.WorkflowType,
		TaskQueue:                       req.TaskQueue,
		Header:                          req.Header,
		Input:                           req.Input,
		WorkflowRunTimeout:              req.WorkflowRunTimeout,
		WorkflowExecutionTimeout:        req.WorkflowExecutionTimeout,
		WorkflowTaskTimeout:             req.WorkflowTaskTimeout,
		ContinuedExecutionRunId:         prevRunID,
		PrevAutoResetPoints:             resetPoints,
		Identity:                        req.Identity,
		RetryPolicy:                     req.RetryPolicy,
		Attempt:                         request.GetAttempt(),
		WorkflowExecutionExpirationTime: request.WorkflowExecutionExpirationTime,
		CronSchedule:                    req.CronSchedule,
		LastCompletionResult:            request.LastCompletionResult,
		ContinuedFailure:                request.GetContinuedFailure(),
		Initiator:                       request.ContinueAsNewInitiator,
		FirstWorkflowTaskBackoff:        request.FirstWorkflowTaskBackoff,
		FirstExecutionRunId:             firstRunID,
		OriginalExecutionRunId:          originalRunID,
		Memo:                            req.Memo,
		SearchAttributes:                req.SearchAttributes,
		WorkflowId:                      req.WorkflowId,
		SourceVersionStamp:              request.SourceVersionStamp,
		CompletionCallbacks:             req.CompletionCallbacks,
		RootWorkflowExecution:           request.RootExecutionInfo.GetExecution(),
		InheritedBuildId:                request.InheritedBuildId,
	}

	parentInfo := request.ParentExecutionInfo
	if parentInfo != nil {
		attributes.ParentWorkflowNamespaceId = parentInfo.NamespaceId
		attributes.ParentWorkflowNamespace = parentInfo.Namespace
		attributes.ParentWorkflowExecution = parentInfo.Execution
		attributes.ParentInitiatedEventId = parentInfo.InitiatedId
		attributes.ParentInitiatedEventVersion = parentInfo.InitiatedVersion
	}

	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
		WorkflowExecutionStartedEventAttributes: attributes,
	}
	return event
}

func (b *EventFactory) CreateWorkflowTaskScheduledEvent(
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeout *durationpb.Duration,
	attempt int32,
	scheduleTime time.Time,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduleTime)
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
		WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
			TaskQueue:           taskQueue,
			StartToCloseTimeout: startToCloseTimeout,
			Attempt:             attempt,
		},
	}

	return event
}

func (b *EventFactory) CreateWorkflowTaskStartedEvent(
	scheduledEventID int64,
	requestID string,
	identity string,
	startTime time.Time,
	suggestContinueAsNew bool,
	historySizeBytes int64,
	versioningStamp *commonpb.WorkerVersionStamp,
	buildIdRedirectCounter int64,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, startTime)
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
		WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
			ScheduledEventId:       scheduledEventID,
			Identity:               identity,
			RequestId:              requestID,
			SuggestContinueAsNew:   suggestContinueAsNew,
			HistorySizeBytes:       historySizeBytes,
			WorkerVersion:          versioningStamp,
			BuildIdRedirectCounter: buildIdRedirectCounter,
		},
	}

	return event
}

func (b *EventFactory) CreateWorkflowTaskCompletedEvent(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	checksum string,
	workerVersionStamp *commonpb.WorkerVersionStamp,
	sdkMetadata *sdkpb.WorkflowTaskCompletedMetadata,
	meteringMetadata *commonpb.MeteringMetadata,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
		WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			Identity:         identity,
			BinaryChecksum:   checksum,
			WorkerVersion:    workerVersionStamp,
			SdkMetadata:      sdkMetadata,
			MeteringMetadata: meteringMetadata,
		},
	}

	return event
}

func (b *EventFactory) CreateWorkflowTaskTimedOutEvent(
	scheduledEventID int64,
	startedEventID int64,
	timeoutType enumspb.TimeoutType,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskTimedOutEventAttributes{
		WorkflowTaskTimedOutEventAttributes: &historypb.WorkflowTaskTimedOutEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			TimeoutType:      timeoutType,
		},
	}

	return event
}

func (b *EventFactory) CreateWorkflowTaskFailedEvent(
	scheduledEventID int64,
	startedEventID int64,
	cause enumspb.WorkflowTaskFailedCause,
	failure *failurepb.Failure,
	identity string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
	checksum string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowTaskFailedEventAttributes{
		WorkflowTaskFailedEventAttributes: &historypb.WorkflowTaskFailedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			Cause:            cause,
			Failure:          failure,
			Identity:         identity,
			BaseRunId:        baseRunID,
			NewRunId:         newRunID,
			ForkEventVersion: forkEventVersion,
			BinaryChecksum:   checksum,
		},
	}
	return event
}

func (b *EventFactory) CreateActivityTaskScheduledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ScheduleActivityTaskCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
		ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			ActivityId:                   command.ActivityId,
			ActivityType:                 command.ActivityType,
			TaskQueue:                    command.TaskQueue,
			Header:                       command.Header,
			Input:                        command.Input,
			ScheduleToCloseTimeout:       command.ScheduleToCloseTimeout,
			ScheduleToStartTimeout:       command.ScheduleToStartTimeout,
			StartToCloseTimeout:          command.StartToCloseTimeout,
			HeartbeatTimeout:             command.HeartbeatTimeout,
			RetryPolicy:                  command.RetryPolicy,
			UseWorkflowBuildId:           command.UseWorkflowBuildId,
		},
	}
	return event
}

func (b *EventFactory) CreateActivityTaskStartedEvent(
	scheduledEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
	versioningStamp *commonpb.WorkerVersionStamp,
	redirectCounter int64,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
		ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
			ScheduledEventId:       scheduledEventID,
			Attempt:                attempt,
			Identity:               identity,
			RequestId:              requestID,
			LastFailure:            lastFailure,
			WorkerVersion:          versioningStamp,
			BuildIdRedirectCounter: redirectCounter,
		},
	}
	return event
}

func (b *EventFactory) CreateActivityTaskCompletedEvent(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
		ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			Result:           result,
			Identity:         identity,
		},
	}
	return event
}

func (b *EventFactory) CreateActivityTaskFailedEvent(
	scheduledEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
		ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			Failure:          failure,
			RetryState:       retryState,
			Identity:         identity,
		},
	}
	return event
}

func (b *EventFactory) CreateActivityTaskTimedOutEvent(
	scheduledEventID int64,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
		ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
			ScheduledEventId: scheduledEventID,
			StartedEventId:   startedEventID,
			Failure:          timeoutFailure,
			RetryState:       retryState,
		},
	}
	return event
}

func (b *EventFactory) CreateCompletedWorkflowEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CompleteWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
		WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Result:                       command.Result,
			NewExecutionRunId:            newExecutionRunID,
		},
	}
	return event
}

func (b *EventFactory) CreateFailWorkflowEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	command *commandpb.FailWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
		WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Failure:                      command.Failure,
			RetryState:                   retryState,
			NewExecutionRunId:            newExecutionRunID,
		},
	}
	return event
}

func (b *EventFactory) CreateTimeoutWorkflowEvent(
	retryState enumspb.RetryState,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
		WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
			RetryState:        retryState,
			NewExecutionRunId: newExecutionRunID,
		},
	}
	return event
}

func (b *EventFactory) CreateWorkflowExecutionTerminatedEvent(
	reason string,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
		WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
			Reason:   reason,
			Details:  details,
			Identity: identity,
		},
	}
	return event
}

func (b *EventFactory) CreateWorkflowExecutionUpdateAcceptedEvent(
	protocolInstanceID string,
	acceptedRequestMessageId string,
	acceptedRequestSequencingEventId int64,
	acceptedRequest *updatepb.Request,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{
		WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{
			ProtocolInstanceId:               protocolInstanceID,
			AcceptedRequestMessageId:         acceptedRequestMessageId,
			AcceptedRequestSequencingEventId: acceptedRequestSequencingEventId,
			AcceptedRequest:                  acceptedRequest,
		},
	}
	return event
}

func (b *EventFactory) CreateWorkflowExecutionUpdateCompletedEvent(
	acceptedEventID int64,
	updResp *updatepb.Response,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionUpdateCompletedEventAttributes{
		WorkflowExecutionUpdateCompletedEventAttributes: &historypb.WorkflowExecutionUpdateCompletedEventAttributes{
			AcceptedEventId: acceptedEventID,
			Meta:            updResp.GetMeta(),
			Outcome:         updResp.GetOutcome(),
		},
	}
	return event
}

func (b *EventFactory) CreateWorkflowExecutionUpdateAdmittedEvent(request *updatepb.Request, origin enumspb.UpdateAdmittedEventOrigin) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionUpdateAdmittedEventAttributes{
		WorkflowExecutionUpdateAdmittedEventAttributes: &historypb.WorkflowExecutionUpdateAdmittedEventAttributes{
			Request: request,
			Origin:  origin,
		},
	}
	return event
}

func (b EventFactory) CreateContinuedAsNewEvent(
	workflowTaskCompletedEventID int64,
	newRunID string,
	command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, b.timeSource.Now())
	attributes := &historypb.WorkflowExecutionContinuedAsNewEventAttributes{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		NewExecutionRunId:            newRunID,
		WorkflowType:                 command.WorkflowType,
		TaskQueue:                    command.TaskQueue,
		Header:                       command.Header,
		Input:                        command.Input,
		WorkflowRunTimeout:           command.WorkflowRunTimeout,
		WorkflowTaskTimeout:          command.WorkflowTaskTimeout,
		BackoffStartInterval:         command.BackoffStartInterval,
		Initiator:                    command.Initiator,
		Failure:                      command.Failure,
		LastCompletionResult:         command.LastCompletionResult,
		Memo:                         command.Memo,
		SearchAttributes:             command.SearchAttributes,
		InheritBuildId:               command.InheritBuildId,
	}
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
		WorkflowExecutionContinuedAsNewEventAttributes: attributes,
	}
	return event
}

func (b *EventFactory) CreateTimerStartedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartTimerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_TIMER_STARTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_TimerStartedEventAttributes{
		TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			TimerId:                      command.TimerId,
			StartToFireTimeout:           command.StartToFireTimeout,
		},
	}
	return event
}

func (b *EventFactory) CreateTimerFiredEvent(startedEventID int64, timerID string) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_TIMER_FIRED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_TimerFiredEventAttributes{
		TimerFiredEventAttributes: &historypb.TimerFiredEventAttributes{
			TimerId:        timerID,
			StartedEventId: startedEventID,
		},
	}
	return event
}

func (b *EventFactory) CreateActivityTaskCancelRequestedEvent(
	workflowTaskCompletedEventID int64,
	scheduledEventID int64,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCancelRequestedEventAttributes{
		ActivityTaskCancelRequestedEventAttributes: &historypb.ActivityTaskCancelRequestedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			ScheduledEventId:             scheduledEventID,
		},
	}
	return event
}

func (b *EventFactory) CreateActivityTaskCanceledEvent(
	scheduledEventID int64,
	startedEventID int64,
	latestCancelRequestedEventID int64,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
		ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
			ScheduledEventId:             scheduledEventID,
			StartedEventId:               startedEventID,
			LatestCancelRequestedEventId: latestCancelRequestedEventID,
			Details:                      details,
			Identity:                     identity,
		},
	}
	return event
}

func (b *EventFactory) CreateTimerCanceledEvent(
	workflowTaskCompletedEventID int64,
	startedEventID int64,
	timerID string,
	identity string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_TIMER_CANCELED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_TimerCanceledEventAttributes{
		TimerCanceledEventAttributes: &historypb.TimerCanceledEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			StartedEventId:               startedEventID,
			TimerId:                      timerID,
			Identity:                     identity,
		},
	}
	return event
}

func (b *EventFactory) CreateWorkflowExecutionCancelRequestedEvent(request *historyservice.RequestCancelWorkflowExecutionRequest) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCancelRequestedEventAttributes{
		WorkflowExecutionCancelRequestedEventAttributes: &historypb.WorkflowExecutionCancelRequestedEventAttributes{
			Cause:                     request.CancelRequest.Reason,
			Identity:                  request.CancelRequest.Identity,
			ExternalInitiatedEventId:  request.ExternalInitiatedEventId,
			ExternalWorkflowExecution: request.ExternalWorkflowExecution,
		},
	}
	return event
}

func (b *EventFactory) CreateWorkflowExecutionCanceledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CancelWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
		WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Details:                      command.Details,
		},
	}
	return event
}

func (b *EventFactory) CreateRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		b.timeSource.Now(),
	)
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Namespace:                    command.Namespace,
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: command.WorkflowId,
				RunId:      command.RunId,
			},
			Control:           command.Control,
			ChildWorkflowOnly: command.ChildWorkflowOnly,
			Reason:            command.Reason,
		},
	}
	return event
}

func (b *EventFactory) CreateRequestCancelExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	cause enumspb.CancelExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		b.timeSource.Now(),
	)
	event.Attributes = &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionFailedEventAttributes{
		RequestCancelExternalWorkflowExecutionFailedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			InitiatedEventId:             initiatedEventID,
			Namespace:                    targetNamespace.String(),
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Cause:   cause,
			Control: "",
		},
	}
	return event
}

func (b *EventFactory) CreateExternalWorkflowExecutionCancelRequested(
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		b.timeSource.Now(),
	)
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionCancelRequestedEventAttributes{
		ExternalWorkflowExecutionCancelRequestedEventAttributes: &historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes{
			InitiatedEventId: initiatedEventID,
			Namespace:        targetNamespace.String(),
			NamespaceId:      targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		},
	}
	return event
}

func (b *EventFactory) CreateSignalExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		b.timeSource.Now(),
	)
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
		SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Namespace:                    command.Namespace,
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: command.Execution.WorkflowId,
				RunId:      command.Execution.RunId,
			},
			SignalName:        command.SignalName,
			Input:             command.Input,
			Control:           command.Control,
			ChildWorkflowOnly: command.ChildWorkflowOnly,
			Header:            command.Header,
		},
	}
	return event
}

func (b *EventFactory) CreateUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{
		UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			SearchAttributes:             command.SearchAttributes,
		},
	}
	return event
}

func (b *EventFactory) CreateWorkflowPropertiesModifiedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ModifyWorkflowPropertiesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowPropertiesModifiedEventAttributes{
		WorkflowPropertiesModifiedEventAttributes: &historypb.WorkflowPropertiesModifiedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			UpsertedMemo:                 command.UpsertedMemo,
		},
	}
	return event
}

func (b *EventFactory) CreateSignalExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string,
	cause enumspb.SignalExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_SignalExternalWorkflowExecutionFailedEventAttributes{
		SignalExternalWorkflowExecutionFailedEventAttributes: &historypb.SignalExternalWorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			InitiatedEventId:             initiatedEventID,
			Namespace:                    targetNamespace.String(),
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Cause:   cause,
			Control: control,
		},
	}
	return event
}

func (b *EventFactory) CreateExternalWorkflowExecutionSignaled(
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ExternalWorkflowExecutionSignaledEventAttributes{
		ExternalWorkflowExecutionSignaledEventAttributes: &historypb.ExternalWorkflowExecutionSignaledEventAttributes{
			InitiatedEventId: initiatedEventID,
			Namespace:        targetNamespace.String(),
			NamespaceId:      targetNamespaceID.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Control: control,
		},
	}
	return event
}

func (b *EventFactory) CreateMarkerRecordedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RecordMarkerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_MARKER_RECORDED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_MarkerRecordedEventAttributes{
		MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
			MarkerName:                   command.MarkerName,
			Details:                      command.Details,
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Header:                       command.Header,
			Failure:                      command.Failure,
		},
	}
	return event
}

func (b *EventFactory) CreateWorkflowExecutionSignaledEvent(
	signalName string,
	input *commonpb.Payloads,
	identity string,
	header *commonpb.Header,
	skipGenerateWorkflowTask bool,
	externalWorkflowExecution *commonpb.WorkflowExecution,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{
		WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName:                signalName,
			Input:                     input,
			Identity:                  identity,
			Header:                    header,
			SkipGenerateWorkflowTask:  skipGenerateWorkflowTask,
			ExternalWorkflowExecution: externalWorkflowExecution,
		},
	}
	return event
}

func (b *EventFactory) CreateStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartChildWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
		StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			Namespace:                    command.Namespace,
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowId:                   command.WorkflowId,
			WorkflowType:                 command.WorkflowType,
			TaskQueue:                    command.TaskQueue,
			Header:                       command.Header,
			Input:                        command.Input,
			WorkflowExecutionTimeout:     command.WorkflowExecutionTimeout,
			WorkflowRunTimeout:           command.WorkflowRunTimeout,
			WorkflowTaskTimeout:          command.WorkflowTaskTimeout,
			Control:                      command.Control,
			WorkflowIdReusePolicy:        command.WorkflowIdReusePolicy,
			RetryPolicy:                  command.RetryPolicy,
			CronSchedule:                 command.CronSchedule,
			Memo:                         command.Memo,
			SearchAttributes:             command.SearchAttributes,
			ParentClosePolicy:            command.GetParentClosePolicy(),
			InheritBuildId:               command.InheritBuildId,
		},
	}
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionStartedEvent(
	initiatedID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{
		ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
			InitiatedEventId:  initiatedID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			Header:            header,
		},
	}
	return event
}

func (b *EventFactory) CreateStartChildWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	workflowType *commonpb.WorkflowType,
	control string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_StartChildWorkflowExecutionFailedEventAttributes{
		StartChildWorkflowExecutionFailedEventAttributes: &historypb.StartChildWorkflowExecutionFailedEventAttributes{
			WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
			InitiatedEventId:             initiatedID,
			Namespace:                    targetNamespace.String(),
			NamespaceId:                  targetNamespaceID.String(),
			WorkflowId:                   workflowID,
			WorkflowType:                 workflowType,
			Control:                      control,
			Cause:                        cause,
		},
	}
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionCompletedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
		ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedEventID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			Result:            result,
		},
	}
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionFailedEventAttributes{
		ChildWorkflowExecutionFailedEventAttributes: &historypb.ChildWorkflowExecutionFailedEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedEventID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			Failure:           failure,
			RetryState:        retryState,
		},
	}
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionCanceledEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	details *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionCanceledEventAttributes{
		ChildWorkflowExecutionCanceledEventAttributes: &historypb.ChildWorkflowExecutionCanceledEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedEventID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			Details:           details,
		},
	}
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionTerminatedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTerminatedEventAttributes{
		ChildWorkflowExecutionTerminatedEventAttributes: &historypb.ChildWorkflowExecutionTerminatedEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedEventID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
		},
	}
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionTimedOutEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT, b.timeSource.Now())
	event.Attributes = &historypb.HistoryEvent_ChildWorkflowExecutionTimedOutEventAttributes{
		ChildWorkflowExecutionTimedOutEventAttributes: &historypb.ChildWorkflowExecutionTimedOutEventAttributes{
			InitiatedEventId:  initiatedID,
			StartedEventId:    startedEventID,
			Namespace:         targetNamespace.String(),
			NamespaceId:       targetNamespaceID.String(),
			WorkflowExecution: execution,
			WorkflowType:      workflowType,
			RetryState:        retryState,
		},
	}
	return event
}

func (b *EventFactory) createHistoryEvent(
	eventType enumspb.EventType,
	time time.Time,
) *historypb.HistoryEvent {
	historyEvent := &historypb.HistoryEvent{}
	historyEvent.EventTime = timestamppb.New(time.UTC())
	historyEvent.EventType = eventType
	historyEvent.Version = b.version
	historyEvent.TaskId = common.EmptyEventTaskID

	return historyEvent
}
