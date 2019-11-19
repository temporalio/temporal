// Copyright (c) 2019 Temporal Technologies, Inc.
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

package adapter

import (
	"github.com/temporalio/temporal-proto/common"
	"github.com/temporalio/temporal-proto/enums"
	"github.com/temporalio/temporal/.gen/go/shared"
)

func toProtoHistory(in *shared.History) *common.History {
	if in == nil {
		return nil
	}
	var events []*common.HistoryEvent
	for _, event := range in.Events {
		events = append(events, toProtoHistoryEvent(event))
	}
	return &common.History{
		Events: events,
	}
}

func toProtoHistoryEvent(in *shared.HistoryEvent) *common.HistoryEvent {
	if in == nil {
		return nil
	}
	return &common.HistoryEvent{
		EventId:   in.GetEventId(),
		Timestamp: in.GetTimestamp(),
		EventType: enums.EventType(in.GetEventType()),
		Version:   in.GetVersion(),
		TaskId:    in.GetTaskId(),

		WorkflowExecutionStartedEventAttributes:                        toProtoWorkflowExecutionStartedEventAttributes(in.GetWorkflowExecutionStartedEventAttributes()),
		WorkflowExecutionCompletedEventAttributes:                      toProtoWorkflowExecutionCompletedEventAttributes(in.GetWorkflowExecutionCompletedEventAttributes()),
		WorkflowExecutionFailedEventAttributes:                         toProtoWorkflowExecutionFailedEventAttributes(in.GetWorkflowExecutionFailedEventAttributes()),
		WorkflowExecutionTimedOutEventAttributes:                       toProtoWorkflowExecutionTimedOutEventAttributes(in.GetWorkflowExecutionTimedOutEventAttributes()),
		DecisionTaskScheduledEventAttributes:                           toProtoDecisionTaskScheduledEventAttributes(in.GetDecisionTaskScheduledEventAttributes()),
		DecisionTaskStartedEventAttributes:                             toProtoDecisionTaskStartedEventAttributes(in.GetDecisionTaskStartedEventAttributes()),
		DecisionTaskCompletedEventAttributes:                           toProtoDecisionTaskCompletedEventAttributes(in.GetDecisionTaskCompletedEventAttributes()),
		DecisionTaskTimedOutEventAttributes:                            toProtoDecisionTaskTimedOutEventAttributes(in.GetDecisionTaskTimedOutEventAttributes()),
		DecisionTaskFailedEventAttributes:                              toProtoDecisionTaskFailedEventAttributes(in.GetDecisionTaskFailedEventAttributes()),
		ActivityTaskScheduledEventAttributes:                           toProtoActivityTaskScheduledEventAttributes(in.GetActivityTaskScheduledEventAttributes()),
		ActivityTaskStartedEventAttributes:                             toProtoActivityTaskStartedEventAttributes(in.GetActivityTaskStartedEventAttributes()),
		ActivityTaskCompletedEventAttributes:                           toProtoActivityTaskCompletedEventAttributes(in.GetActivityTaskCompletedEventAttributes()),
		ActivityTaskFailedEventAttributes:                              toProtoActivityTaskFailedEventAttributes(in.GetActivityTaskFailedEventAttributes()),
		ActivityTaskTimedOutEventAttributes:                            toProtoActivityTaskTimedOutEventAttributes(in.GetActivityTaskTimedOutEventAttributes()),
		TimerStartedEventAttributes:                                    toProtoTimerStartedEventAttributes(in.GetTimerStartedEventAttributes()),
		TimerFiredEventAttributes:                                      toProtoTimerFiredEventAttributes(in.GetTimerFiredEventAttributes()),
		ActivityTaskCancelRequestedEventAttributes:                     toProtoActivityTaskCancelRequestedEventAttributes(in.GetActivityTaskCancelRequestedEventAttributes()),
		RequestCancelActivityTaskFailedEventAttributes:                 toProtoRequestCancelActivityTaskFailedEventAttributes(in.GetRequestCancelActivityTaskFailedEventAttributes()),
		ActivityTaskCanceledEventAttributes:                            toProtoActivityTaskCanceledEventAttributes(in.GetActivityTaskCanceledEventAttributes()),
		TimerCanceledEventAttributes:                                   toProtoTimerCanceledEventAttributes(in.GetTimerCanceledEventAttributes()),
		CancelTimerFailedEventAttributes:                               toProtoCancelTimerFailedEventAttributes(in.GetCancelTimerFailedEventAttributes()),
		MarkerRecordedEventAttributes:                                  toProtoMarkerRecordedEventAttributes(in.GetMarkerRecordedEventAttributes()),
		WorkflowExecutionSignaledEventAttributes:                       toProtoWorkflowExecutionSignaledEventAttributes(in.GetWorkflowExecutionSignaledEventAttributes()),
		WorkflowExecutionTerminatedEventAttributes:                     toProtoWorkflowExecutionTerminatedEventAttributes(in.GetWorkflowExecutionTerminatedEventAttributes()),
		WorkflowExecutionCancelRequestedEventAttributes:                toProtoWorkflowExecutionCancelRequestedEventAttributes(in.GetWorkflowExecutionCancelRequestedEventAttributes()),
		WorkflowExecutionCanceledEventAttributes:                       toProtoWorkflowExecutionCanceledEventAttributes(in.GetWorkflowExecutionCanceledEventAttributes()),
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: toProtoRequestCancelExternalWorkflowExecutionInitiatedEventAttributes(in.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()),
		RequestCancelExternalWorkflowExecutionFailedEventAttributes:    toProtoRequestCancelExternalWorkflowExecutionFailedEventAttributes(in.GetRequestCancelExternalWorkflowExecutionFailedEventAttributes()),
		ExternalWorkflowExecutionCancelRequestedEventAttributes:        toProtoExternalWorkflowExecutionCancelRequestedEventAttributes(in.GetExternalWorkflowExecutionCancelRequestedEventAttributes()),
		WorkflowExecutionContinuedAsNewEventAttributes:                 toProtoWorkflowExecutionContinuedAsNewEventAttributes(in.GetWorkflowExecutionContinuedAsNewEventAttributes()),
		StartChildWorkflowExecutionInitiatedEventAttributes:            toProtoStartChildWorkflowExecutionInitiatedEventAttributes(in.GetStartChildWorkflowExecutionInitiatedEventAttributes()),
		StartChildWorkflowExecutionFailedEventAttributes:               toProtoStartChildWorkflowExecutionFailedEventAttributes(in.GetStartChildWorkflowExecutionFailedEventAttributes()),
		ChildWorkflowExecutionStartedEventAttributes:                   toProtoChildWorkflowExecutionStartedEventAttributes(in.GetChildWorkflowExecutionStartedEventAttributes()),
		ChildWorkflowExecutionCompletedEventAttributes:                 toProtoChildWorkflowExecutionCompletedEventAttributes(in.GetChildWorkflowExecutionCompletedEventAttributes()),
		ChildWorkflowExecutionFailedEventAttributes:                    toProtoChildWorkflowExecutionFailedEventAttributes(in.GetChildWorkflowExecutionFailedEventAttributes()),
		ChildWorkflowExecutionCanceledEventAttributes:                  toProtoChildWorkflowExecutionCanceledEventAttributes(in.GetChildWorkflowExecutionCanceledEventAttributes()),
		ChildWorkflowExecutionTimedOutEventAttributes:                  toProtoChildWorkflowExecutionTimedOutEventAttributes(in.GetChildWorkflowExecutionTimedOutEventAttributes()),
		ChildWorkflowExecutionTerminatedEventAttributes:                toProtoChildWorkflowExecutionTerminatedEventAttributes(in.GetChildWorkflowExecutionTerminatedEventAttributes()),
		SignalExternalWorkflowExecutionInitiatedEventAttributes:        toProtoSignalExternalWorkflowExecutionInitiatedEventAttributes(in.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()),
		SignalExternalWorkflowExecutionFailedEventAttributes:           toProtoSignalExternalWorkflowExecutionFailedEventAttributes(in.GetSignalExternalWorkflowExecutionFailedEventAttributes()),
		ExternalWorkflowExecutionSignaledEventAttributes:               toProtoExternalWorkflowExecutionSignaledEventAttributes(in.GetExternalWorkflowExecutionSignaledEventAttributes()),
		UpsertWorkflowSearchAttributesEventAttributes:                  toProtoUpsertWorkflowSearchAttributesEventAttributes(in.GetUpsertWorkflowSearchAttributesEventAttributes()),
	}
}

func toProtoWorkflowExecutionStartedEventAttributes(in *shared.WorkflowExecutionStartedEventAttributes) *common.WorkflowExecutionStartedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionStartedEventAttributes{
		WorkflowType:                        toProtoWorkflowType(in.GetWorkflowType()),
		ParentWorkflowDomain:                in.GetParentWorkflowDomain(),
		ParentWorkflowExecution:             toProtoWorkflowExecution(in.GetParentWorkflowExecution()),
		ParentInitiatedEventId:              in.GetParentInitiatedEventId(),
		TaskList:                            toProtoTaskList(in.GetTaskList()),
		Input:                               in.GetInput(),
		ExecutionStartToCloseTimeoutSeconds: in.GetExecutionStartToCloseTimeoutSeconds(),
		TaskStartToCloseTimeoutSeconds:      in.GetTaskStartToCloseTimeoutSeconds(),
		ContinuedExecutionRunId:             in.GetContinuedExecutionRunId(),
		Initiator:                           enums.ContinueAsNewInitiator(in.GetInitiator()),
		ContinuedFailureReason:              in.GetContinuedFailureReason(),
		ContinuedFailureDetails:             in.GetContinuedFailureDetails(),
		LastCompletionResult:                in.GetLastCompletionResult(),
		OriginalExecutionRunId:              in.GetOriginalExecutionRunId(),
		Identity:                            in.GetIdentity(),
		FirstExecutionRunId:                 in.GetFirstExecutionRunId(),
		RetryPolicy:                         toProtoRetryPolicy(in.GetRetryPolicy()),
		Attempt:                             in.GetAttempt(),
		ExpirationTimestamp:                 in.GetExpirationTimestamp(),
		CronSchedule:                        in.GetCronSchedule(),
		FirstDecisionTaskBackoffSeconds:     in.GetFirstDecisionTaskBackoffSeconds(),
		Memo:                                toProtoMemo(in.GetMemo()),
		SearchAttributes:                    toProtoSearchAttributes(in.GetSearchAttributes()),
		PrevAutoResetPoints:                 toProtoResetPoints(in.GetPrevAutoResetPoints()),
		Header:                              toProtoHeader(in.GetHeader()),
	}
}

func toProtoWorkflowExecutionCompletedEventAttributes(in *shared.WorkflowExecutionCompletedEventAttributes) *common.WorkflowExecutionCompletedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionCompletedEventAttributes{
		Result:                       in.GetResult(),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
	}
}

func toProtoWorkflowExecutionFailedEventAttributes(in *shared.WorkflowExecutionFailedEventAttributes) *common.WorkflowExecutionFailedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionFailedEventAttributes{
		Reason:                       in.GetReason(),
		Details:                      in.GetDetails(),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
	}
}

func toProtoWorkflowExecutionTimedOutEventAttributes(in *shared.WorkflowExecutionTimedOutEventAttributes) *common.WorkflowExecutionTimedOutEventAttributes {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionTimedOutEventAttributes{
		TimeoutType: enums.TimeoutType(in.GetTimeoutType()),
	}
}

func toProtoDecisionTaskScheduledEventAttributes(in *shared.DecisionTaskScheduledEventAttributes) *common.DecisionTaskScheduledEventAttributes {
	if in == nil {
		return nil
	}
	return &common.DecisionTaskScheduledEventAttributes{
		TaskList:                   toProtoTaskList(in.GetTaskList()),
		StartToCloseTimeoutSeconds: in.GetStartToCloseTimeoutSeconds(),
		Attempt:                    in.GetAttempt(),
	}
}

func toProtoDecisionTaskStartedEventAttributes(in *shared.DecisionTaskStartedEventAttributes) *common.DecisionTaskStartedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.DecisionTaskStartedEventAttributes{
		ScheduledEventId: in.GetScheduledEventId(),
		Identity:         in.GetIdentity(),
		RequestId:        in.GetRequestId(),
	}
}

func toProtoDecisionTaskCompletedEventAttributes(in *shared.DecisionTaskCompletedEventAttributes) *common.DecisionTaskCompletedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.DecisionTaskCompletedEventAttributes{
		ExecutionContext: in.GetExecutionContext(),
		ScheduledEventId: in.GetScheduledEventId(),
		StartedEventId:   in.GetStartedEventId(),
		Identity:         in.GetIdentity(),
		BinaryChecksum:   in.GetBinaryChecksum(),
	}
}

func toProtoDecisionTaskTimedOutEventAttributes(in *shared.DecisionTaskTimedOutEventAttributes) *common.DecisionTaskTimedOutEventAttributes {
	if in == nil {
		return nil
	}
	return &common.DecisionTaskTimedOutEventAttributes{
		ScheduledEventId: in.GetScheduledEventId(),
		StartedEventId:   in.GetStartedEventId(),
		TimeoutType:      enums.TimeoutType(in.GetTimeoutType()),
	}
}

func toProtoDecisionTaskFailedEventAttributes(in *shared.DecisionTaskFailedEventAttributes) *common.DecisionTaskFailedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.DecisionTaskFailedEventAttributes{
		ScheduledEventId: in.GetScheduledEventId(),
		StartedEventId:   in.GetStartedEventId(),
		Cause:            enums.DecisionTaskFailedCause(in.GetCause()),
		Details:          in.GetDetails(),
		Identity:         in.GetIdentity(),
		Reason:           in.GetReason(),
		BaseRunId:        in.GetBaseRunId(),
		NewRunId:         in.GetNewRunId(),
		ForkEventVersion: in.GetForkEventVersion(),
	}
}

func toProtoActivityTaskScheduledEventAttributes(in *shared.ActivityTaskScheduledEventAttributes) *common.ActivityTaskScheduledEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ActivityTaskScheduledEventAttributes{
		ActivityId:                    in.GetActivityId(),
		ActivityType:                  toProtoActivityType(in.GetActivityType()),
		Domain:                        in.GetDomain(),
		TaskList:                      toProtoTaskList(in.GetTaskList()),
		Input:                         in.GetInput(),
		ScheduleToCloseTimeoutSeconds: in.GetScheduleToCloseTimeoutSeconds(),
		ScheduleToStartTimeoutSeconds: in.GetScheduleToStartTimeoutSeconds(),
		StartToCloseTimeoutSeconds:    in.GetStartToCloseTimeoutSeconds(),
		HeartbeatTimeoutSeconds:       in.GetHeartbeatTimeoutSeconds(),
		DecisionTaskCompletedEventId:  in.GetDecisionTaskCompletedEventId(),
		RetryPolicy:                   toProtoRetryPolicy(in.GetRetryPolicy()),
		Header:                        toProtoHeader(in.GetHeader()),
	}
}

func toProtoActivityTaskStartedEventAttributes(in *shared.ActivityTaskStartedEventAttributes) *common.ActivityTaskStartedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ActivityTaskStartedEventAttributes{
		ScheduledEventId: in.GetScheduledEventId(),
		Identity:         in.GetIdentity(),
		RequestId:        in.GetRequestId(),
		Attempt:          in.GetAttempt(),
	}
}

func toProtoActivityTaskCompletedEventAttributes(in *shared.ActivityTaskCompletedEventAttributes) *common.ActivityTaskCompletedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ActivityTaskCompletedEventAttributes{
		Result:           in.GetResult(),
		ScheduledEventId: in.GetScheduledEventId(),
		StartedEventId:   in.GetStartedEventId(),
		Identity:         in.GetIdentity(),
	}
}

func toProtoActivityTaskFailedEventAttributes(in *shared.ActivityTaskFailedEventAttributes) *common.ActivityTaskFailedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ActivityTaskFailedEventAttributes{
		Reason:           in.GetReason(),
		Details:          in.GetDetails(),
		ScheduledEventId: in.GetScheduledEventId(),
		StartedEventId:   in.GetStartedEventId(),
		Identity:         in.GetIdentity(),
	}
}

func toProtoActivityTaskTimedOutEventAttributes(in *shared.ActivityTaskTimedOutEventAttributes) *common.ActivityTaskTimedOutEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ActivityTaskTimedOutEventAttributes{
		Details:            in.GetDetails(),
		ScheduledEventId:   in.GetScheduledEventId(),
		StartedEventId:     in.GetStartedEventId(),
		TimeoutType:        enums.TimeoutType(in.GetTimeoutType()),
		LastFailureReason:  in.GetLastFailureReason(),
		LastFailureDetails: in.GetLastFailureDetails(),
	}
}

func toProtoTimerStartedEventAttributes(in *shared.TimerStartedEventAttributes) *common.TimerStartedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.TimerStartedEventAttributes{
		TimerId:                      in.GetTimerId(),
		StartToFireTimeoutSeconds:    in.GetStartToFireTimeoutSeconds(),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
	}
}

func toProtoTimerFiredEventAttributes(in *shared.TimerFiredEventAttributes) *common.TimerFiredEventAttributes {
	if in == nil {
		return nil
	}
	return &common.TimerFiredEventAttributes{
		TimerId:        in.GetTimerId(),
		StartedEventId: in.GetStartedEventId(),
	}
}

func toProtoActivityTaskCancelRequestedEventAttributes(in *shared.ActivityTaskCancelRequestedEventAttributes) *common.ActivityTaskCancelRequestedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ActivityTaskCancelRequestedEventAttributes{
		ActivityId:                   in.GetActivityId(),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
	}
}

func toProtoRequestCancelActivityTaskFailedEventAttributes(in *shared.RequestCancelActivityTaskFailedEventAttributes) *common.RequestCancelActivityTaskFailedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.RequestCancelActivityTaskFailedEventAttributes{
		ActivityId:                   in.GetActivityId(),
		Cause:                        in.GetCause(),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
	}
}

func toProtoActivityTaskCanceledEventAttributes(in *shared.ActivityTaskCanceledEventAttributes) *common.ActivityTaskCanceledEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ActivityTaskCanceledEventAttributes{
		Details:                      in.GetDetails(),
		LatestCancelRequestedEventId: in.GetLatestCancelRequestedEventId(),
		ScheduledEventId:             in.GetScheduledEventId(),
		StartedEventId:               in.GetStartedEventId(),
		Identity:                     in.GetIdentity(),
	}
}

func toProtoTimerCanceledEventAttributes(in *shared.TimerCanceledEventAttributes) *common.TimerCanceledEventAttributes {
	if in == nil {
		return nil
	}
	return &common.TimerCanceledEventAttributes{
		TimerId:                      in.GetTimerId(),
		StartedEventId:               in.GetStartedEventId(),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
		Identity:                     in.GetIdentity(),
	}
}

func toProtoCancelTimerFailedEventAttributes(in *shared.CancelTimerFailedEventAttributes) *common.CancelTimerFailedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.CancelTimerFailedEventAttributes{
		TimerId:                      in.GetTimerId(),
		Cause:                        in.GetCause(),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
		Identity:                     in.GetIdentity(),
	}
}

func toProtoMarkerRecordedEventAttributes(in *shared.MarkerRecordedEventAttributes) *common.MarkerRecordedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.MarkerRecordedEventAttributes{
		MarkerName:                   in.GetMarkerName(),
		Details:                      in.GetDetails(),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
		Header:                       toProtoHeader(in.GetHeader()),
	}
}

func toProtoWorkflowExecutionSignaledEventAttributes(in *shared.WorkflowExecutionSignaledEventAttributes) *common.WorkflowExecutionSignaledEventAttributes {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionSignaledEventAttributes{
		SignalName: in.GetSignalName(),
		Input:      in.GetInput(),
		Identity:   in.GetIdentity(),
	}
}

func toProtoWorkflowExecutionTerminatedEventAttributes(in *shared.WorkflowExecutionTerminatedEventAttributes) *common.WorkflowExecutionTerminatedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionTerminatedEventAttributes{
		Reason:   in.GetReason(),
		Details:  in.GetDetails(),
		Identity: in.GetIdentity(),
	}
}

func toProtoWorkflowExecutionCancelRequestedEventAttributes(in *shared.WorkflowExecutionCancelRequestedEventAttributes) *common.WorkflowExecutionCancelRequestedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionCancelRequestedEventAttributes{
		Cause:                     in.GetCause(),
		ExternalInitiatedEventId:  in.GetExternalInitiatedEventId(),
		ExternalWorkflowExecution: toProtoWorkflowExecution(in.GetExternalWorkflowExecution()),
		Identity:                  in.GetIdentity(),
	}
}

func toProtoWorkflowExecutionCanceledEventAttributes(in *shared.WorkflowExecutionCanceledEventAttributes) *common.WorkflowExecutionCanceledEventAttributes {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionCanceledEventAttributes{
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
		Details:                      in.GetDetails(),
	}
}

func toProtoRequestCancelExternalWorkflowExecutionInitiatedEventAttributes(in *shared.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes) *common.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
		Domain:                       in.GetDomain(),
		WorkflowExecution:            toProtoWorkflowExecution(in.GetWorkflowExecution()),
		Control:                      in.GetControl(),
		ChildWorkflowOnly:            in.GetChildWorkflowOnly(),
	}
}

func toProtoRequestCancelExternalWorkflowExecutionFailedEventAttributes(in *shared.RequestCancelExternalWorkflowExecutionFailedEventAttributes) *common.RequestCancelExternalWorkflowExecutionFailedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
		Cause:                        enums.CancelExternalWorkflowExecutionFailedCause(in.GetCause()),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
		Domain:                       in.GetDomain(),
		WorkflowExecution:            toProtoWorkflowExecution(in.GetWorkflowExecution()),
		InitiatedEventId:             in.GetInitiatedEventId(),
		Control:                      in.GetControl(),
	}
}

func toProtoExternalWorkflowExecutionCancelRequestedEventAttributes(in *shared.ExternalWorkflowExecutionCancelRequestedEventAttributes) *common.ExternalWorkflowExecutionCancelRequestedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ExternalWorkflowExecutionCancelRequestedEventAttributes{
		InitiatedEventId:  in.GetInitiatedEventId(),
		Domain:            in.GetDomain(),
		WorkflowExecution: toProtoWorkflowExecution(in.GetWorkflowExecution()),
	}
}

func toProtoWorkflowExecutionContinuedAsNewEventAttributes(in *shared.WorkflowExecutionContinuedAsNewEventAttributes) *common.WorkflowExecutionContinuedAsNewEventAttributes {
	if in == nil {
		return nil
	}
	return &common.WorkflowExecutionContinuedAsNewEventAttributes{
		NewExecutionRunId:                   in.GetNewExecutionRunId(),
		WorkflowType:                        toProtoWorkflowType(in.GetWorkflowType()),
		TaskList:                            toProtoTaskList(in.GetTaskList()),
		Input:                               in.GetInput(),
		ExecutionStartToCloseTimeoutSeconds: in.GetExecutionStartToCloseTimeoutSeconds(),
		TaskStartToCloseTimeoutSeconds:      in.GetTaskStartToCloseTimeoutSeconds(),
		DecisionTaskCompletedEventId:        in.GetDecisionTaskCompletedEventId(),
		BackoffStartIntervalInSeconds:       in.GetBackoffStartIntervalInSeconds(),
		Initiator:                           enums.ContinueAsNewInitiator(in.GetInitiator()),
		FailureReason:                       in.GetFailureReason(),
		FailureDetails:                      in.GetFailureDetails(),
		LastCompletionResult:                in.GetLastCompletionResult(),
		Header:                              toProtoHeader(in.GetHeader()),
		Memo:                                toProtoMemo(in.GetMemo()),
		SearchAttributes:                    toProtoSearchAttributes(in.GetSearchAttributes()),
	}
}

func toProtoStartChildWorkflowExecutionInitiatedEventAttributes(in *shared.StartChildWorkflowExecutionInitiatedEventAttributes) *common.StartChildWorkflowExecutionInitiatedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.StartChildWorkflowExecutionInitiatedEventAttributes{
		Domain:                              in.GetDomain(),
		WorkflowId:                          in.GetWorkflowId(),
		WorkflowType:                        toProtoWorkflowType(in.GetWorkflowType()),
		TaskList:                            toProtoTaskList(in.GetTaskList()),
		Input:                               in.GetInput(),
		ExecutionStartToCloseTimeoutSeconds: in.GetExecutionStartToCloseTimeoutSeconds(),
		TaskStartToCloseTimeoutSeconds:      in.GetTaskStartToCloseTimeoutSeconds(),
		ParentClosePolicy:                   enums.ParentClosePolicy(in.GetParentClosePolicy()),
		Control:                             in.GetControl(),
		DecisionTaskCompletedEventId:        in.GetDecisionTaskCompletedEventId(),
		WorkflowIdReusePolicy:               enums.WorkflowIdReusePolicy(in.GetWorkflowIdReusePolicy()),
		RetryPolicy:                         toProtoRetryPolicy(in.GetRetryPolicy()),
		CronSchedule:                        in.GetCronSchedule(),
		Header:                              toProtoHeader(in.GetHeader()),
		Memo:                                toProtoMemo(in.GetMemo()),
		SearchAttributes:                    toProtoSearchAttributes(in.GetSearchAttributes()),
	}
}

func toProtoStartChildWorkflowExecutionFailedEventAttributes(in *shared.StartChildWorkflowExecutionFailedEventAttributes) *common.StartChildWorkflowExecutionFailedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.StartChildWorkflowExecutionFailedEventAttributes{
		Domain:                       in.GetDomain(),
		WorkflowId:                   in.GetWorkflowId(),
		WorkflowType:                 toProtoWorkflowType(in.GetWorkflowType()),
		Cause:                        enums.ChildWorkflowExecutionFailedCause(in.GetCause()),
		Control:                      in.GetControl(),
		InitiatedEventId:             in.GetInitiatedEventId(),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
	}
}

func toProtoChildWorkflowExecutionStartedEventAttributes(in *shared.ChildWorkflowExecutionStartedEventAttributes) *common.ChildWorkflowExecutionStartedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ChildWorkflowExecutionStartedEventAttributes{
		Domain:            in.GetDomain(),
		InitiatedEventId:  in.GetInitiatedEventId(),
		WorkflowExecution: toProtoWorkflowExecution(in.GetWorkflowExecution()),
		WorkflowType:      toProtoWorkflowType(in.GetWorkflowType()),
		Header:            toProtoHeader(in.GetHeader()),
	}
}

func toProtoChildWorkflowExecutionCompletedEventAttributes(in *shared.ChildWorkflowExecutionCompletedEventAttributes) *common.ChildWorkflowExecutionCompletedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ChildWorkflowExecutionCompletedEventAttributes{
		Result:            in.GetResult(),
		Domain:            in.GetDomain(),
		WorkflowExecution: toProtoWorkflowExecution(in.GetWorkflowExecution()),
		WorkflowType:      toProtoWorkflowType(in.GetWorkflowType()),
		InitiatedEventId:  in.GetInitiatedEventId(),
		StartedEventId:    in.GetStartedEventId(),
	}
}

func toProtoChildWorkflowExecutionFailedEventAttributes(in *shared.ChildWorkflowExecutionFailedEventAttributes) *common.ChildWorkflowExecutionFailedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ChildWorkflowExecutionFailedEventAttributes{
		Reason:            in.GetReason(),
		Details:           in.GetDetails(),
		Domain:            in.GetDomain(),
		WorkflowExecution: toProtoWorkflowExecution(in.GetWorkflowExecution()),
		WorkflowType:      toProtoWorkflowType(in.GetWorkflowType()),
		InitiatedEventId:  in.GetInitiatedEventId(),
		StartedEventId:    in.GetStartedEventId(),
	}
}

func toProtoChildWorkflowExecutionCanceledEventAttributes(in *shared.ChildWorkflowExecutionCanceledEventAttributes) *common.ChildWorkflowExecutionCanceledEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ChildWorkflowExecutionCanceledEventAttributes{
		Details:           in.GetDetails(),
		Domain:            in.GetDomain(),
		WorkflowExecution: toProtoWorkflowExecution(in.GetWorkflowExecution()),
		WorkflowType:      toProtoWorkflowType(in.GetWorkflowType()),
		InitiatedEventId:  in.GetInitiatedEventId(),
		StartedEventId:    in.GetStartedEventId(),
	}
}

func toProtoChildWorkflowExecutionTimedOutEventAttributes(in *shared.ChildWorkflowExecutionTimedOutEventAttributes) *common.ChildWorkflowExecutionTimedOutEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ChildWorkflowExecutionTimedOutEventAttributes{
		TimeoutType:       enums.TimeoutType(in.GetTimeoutType()),
		Domain:            in.GetDomain(),
		WorkflowExecution: toProtoWorkflowExecution(in.GetWorkflowExecution()),
		WorkflowType:      toProtoWorkflowType(in.GetWorkflowType()),
		InitiatedEventId:  in.GetInitiatedEventId(),
		StartedEventId:    in.GetStartedEventId(),
	}
}

func toProtoChildWorkflowExecutionTerminatedEventAttributes(in *shared.ChildWorkflowExecutionTerminatedEventAttributes) *common.ChildWorkflowExecutionTerminatedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ChildWorkflowExecutionTerminatedEventAttributes{
		Domain:            in.GetDomain(),
		WorkflowExecution: toProtoWorkflowExecution(in.GetWorkflowExecution()),
		WorkflowType:      toProtoWorkflowType(in.GetWorkflowType()),
		InitiatedEventId:  in.GetInitiatedEventId(),
		StartedEventId:    in.GetStartedEventId(),
	}
}

func toProtoSignalExternalWorkflowExecutionInitiatedEventAttributes(in *shared.SignalExternalWorkflowExecutionInitiatedEventAttributes) *common.SignalExternalWorkflowExecutionInitiatedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.SignalExternalWorkflowExecutionInitiatedEventAttributes{
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
		Domain:                       in.GetDomain(),
		WorkflowExecution:            toProtoWorkflowExecution(in.GetWorkflowExecution()),
		SignalName:                   in.GetSignalName(),
		Input:                        in.GetInput(),
		Control:                      in.GetControl(),
		ChildWorkflowOnly:            in.GetChildWorkflowOnly(),
	}
}

func toProtoSignalExternalWorkflowExecutionFailedEventAttributes(in *shared.SignalExternalWorkflowExecutionFailedEventAttributes) *common.SignalExternalWorkflowExecutionFailedEventAttributes {
	if in == nil {
		return nil
	}
	return &common.SignalExternalWorkflowExecutionFailedEventAttributes{
		Cause:                        enums.SignalExternalWorkflowExecutionFailedCause(in.GetCause()),
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
		Domain:                       in.GetDomain(),
		WorkflowExecution:            toProtoWorkflowExecution(in.GetWorkflowExecution()),
		InitiatedEventId:             in.GetInitiatedEventId(),
		Control:                      in.GetControl(),
	}
}

func toProtoExternalWorkflowExecutionSignaledEventAttributes(in *shared.ExternalWorkflowExecutionSignaledEventAttributes) *common.ExternalWorkflowExecutionSignaledEventAttributes {
	if in == nil {
		return nil
	}
	return &common.ExternalWorkflowExecutionSignaledEventAttributes{
		InitiatedEventId:  in.GetInitiatedEventId(),
		Domain:            in.GetDomain(),
		WorkflowExecution: toProtoWorkflowExecution(in.GetWorkflowExecution()),
		Control:           in.GetControl(),
	}
}

func toProtoUpsertWorkflowSearchAttributesEventAttributes(in *shared.UpsertWorkflowSearchAttributesEventAttributes) *common.UpsertWorkflowSearchAttributesEventAttributes {
	if in == nil {
		return nil
	}
	return &common.UpsertWorkflowSearchAttributesEventAttributes{
		DecisionTaskCompletedEventId: in.GetDecisionTaskCompletedEventId(),
		SearchAttributes:             toProtoSearchAttributes(in.GetSearchAttributes()),
	}
}
