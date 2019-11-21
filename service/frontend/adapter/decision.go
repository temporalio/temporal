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

// toProtoDecision ...
func toProtoDecision(in *shared.Decision) *common.Decision {
	if in == nil {
		return nil
	}

	return &common.Decision{
		DecisionType:                                             enums.DecisionType(in.GetDecisionType()),
		ScheduleActivityTaskDecisionAttributes:                   toProtoScheduleActivityTaskDecisionAttributes(in.GetScheduleActivityTaskDecisionAttributes()),
		StartTimerDecisionAttributes:                             toProtoStartTimerDecisionAttributes(in.GetStartTimerDecisionAttributes()),
		CompleteWorkflowExecutionDecisionAttributes:              toProtoCompleteWorkflowExecutionDecisionAttributes(in.GetCompleteWorkflowExecutionDecisionAttributes()),
		FailWorkflowExecutionDecisionAttributes:                  toProtoFailWorkflowExecutionDecisionAttributes(in.GetFailWorkflowExecutionDecisionAttributes()),
		RequestCancelActivityTaskDecisionAttributes:              toProtoRequestCancelActivityTaskDecisionAttributes(in.GetRequestCancelActivityTaskDecisionAttributes()),
		CancelTimerDecisionAttributes:                            toProtoCancelTimerDecisionAttributes(in.GetCancelTimerDecisionAttributes()),
		CancelWorkflowExecutionDecisionAttributes:                toProtoCancelWorkflowExecutionDecisionAttributes(in.GetCancelWorkflowExecutionDecisionAttributes()),
		RequestCancelExternalWorkflowExecutionDecisionAttributes: toProtoRequestCancelExternalWorkflowExecutionDecisionAttributes(in.GetRequestCancelExternalWorkflowExecutionDecisionAttributes()),
		RecordMarkerDecisionAttributes:                           toProtoRecordMarkerDecisionAttributes(in.GetRecordMarkerDecisionAttributes()),
		ContinueAsNewWorkflowExecutionDecisionAttributes:         toProtoContinueAsNewWorkflowExecutionDecisionAttributes(in.GetContinueAsNewWorkflowExecutionDecisionAttributes()),
		StartChildWorkflowExecutionDecisionAttributes:            toProtoStartChildWorkflowExecutionDecisionAttributes(in.GetStartChildWorkflowExecutionDecisionAttributes()),
		SignalExternalWorkflowExecutionDecisionAttributes:        toProtoSignalExternalWorkflowExecutionDecisionAttributes(in.GetSignalExternalWorkflowExecutionDecisionAttributes()),
		UpsertWorkflowSearchAttributesDecisionAttributes:         toProtoUpsertWorkflowSearchAttributesDecisionAttributes(in.GetUpsertWorkflowSearchAttributesDecisionAttributes()),
	}
}

// toProtoDecisions ...
func toProtoDecisions(in []*shared.Decision) []*common.Decision {
	if in == nil {
		return nil
	}

	var ret []*common.Decision
	for _, item := range in {
		ret = append(ret, toProtoDecision(item))
	}
	return ret
}

// toProtoScheduleActivityTaskDecisionAttributes ...
func toProtoScheduleActivityTaskDecisionAttributes(in *shared.ScheduleActivityTaskDecisionAttributes) *common.ScheduleActivityTaskDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    in.GetActivityId(),
		ActivityType:                  toProtoActivityType(in.GetActivityType()),
		Domain:                        in.GetDomain(),
		TaskList:                      toProtoTaskList(in.GetTaskList()),
		Input:                         in.GetInput(),
		ScheduleToCloseTimeoutSeconds: in.GetScheduleToCloseTimeoutSeconds(),
		ScheduleToStartTimeoutSeconds: in.GetScheduleToStartTimeoutSeconds(),
		StartToCloseTimeoutSeconds:    in.GetStartToCloseTimeoutSeconds(),
		HeartbeatTimeoutSeconds:       in.GetHeartbeatTimeoutSeconds(),
		RetryPolicy:                   toProtoRetryPolicy(in.GetRetryPolicy()),
		Header:                        toProtoHeader(in.GetHeader()),
	}
}

// toProtoRequestCancelActivityTaskDecisionAttributes ...
func toProtoRequestCancelActivityTaskDecisionAttributes(in *shared.RequestCancelActivityTaskDecisionAttributes) *common.RequestCancelActivityTaskDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.RequestCancelActivityTaskDecisionAttributes{
		ActivityId: in.GetActivityId(),
	}
}

// toProtoStartTimerDecisionAttributes ...
func toProtoStartTimerDecisionAttributes(in *shared.StartTimerDecisionAttributes) *common.StartTimerDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.StartTimerDecisionAttributes{
		TimerId:                   in.GetTimerId(),
		StartToFireTimeoutSeconds: in.GetStartToFireTimeoutSeconds(),
	}
}

// toProtoCompleteWorkflowExecutionDecisionAttributes ...
func toProtoCompleteWorkflowExecutionDecisionAttributes(in *shared.CompleteWorkflowExecutionDecisionAttributes) *common.CompleteWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.CompleteWorkflowExecutionDecisionAttributes{
		Result: in.GetResult(),
	}
}

// toProtoFailWorkflowExecutionDecisionAttributes ...
func toProtoFailWorkflowExecutionDecisionAttributes(in *shared.FailWorkflowExecutionDecisionAttributes) *common.FailWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.FailWorkflowExecutionDecisionAttributes{
		Reason:  in.GetReason(),
		Details: in.GetDetails(),
	}
}

// toProtoCancelTimerDecisionAttributes ...
func toProtoCancelTimerDecisionAttributes(in *shared.CancelTimerDecisionAttributes) *common.CancelTimerDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.CancelTimerDecisionAttributes{
		TimerId: in.GetTimerId(),
	}
}

// toProtoCancelWorkflowExecutionDecisionAttributes ...
func toProtoCancelWorkflowExecutionDecisionAttributes(in *shared.CancelWorkflowExecutionDecisionAttributes) *common.CancelWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.CancelWorkflowExecutionDecisionAttributes{
		Details: in.GetDetails(),
	}
}

// toProtoRequestCancelExternalWorkflowExecutionDecisionAttributes ...
func toProtoRequestCancelExternalWorkflowExecutionDecisionAttributes(in *shared.RequestCancelExternalWorkflowExecutionDecisionAttributes) *common.RequestCancelExternalWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.RequestCancelExternalWorkflowExecutionDecisionAttributes{
		Domain:            in.GetDomain(),
		WorkflowId:        in.GetWorkflowId(),
		RunId:             in.GetRunId(),
		Control:           in.GetControl(),
		ChildWorkflowOnly: in.GetChildWorkflowOnly(),
	}
}

// toProtoSignalExternalWorkflowExecutionDecisionAttributes ...
func toProtoSignalExternalWorkflowExecutionDecisionAttributes(in *shared.SignalExternalWorkflowExecutionDecisionAttributes) *common.SignalExternalWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.SignalExternalWorkflowExecutionDecisionAttributes{
		Domain:            in.GetDomain(),
		Execution:         toProtoWorkflowExecution(in.GetExecution()),
		SignalName:        in.GetSignalName(),
		Input:             in.GetInput(),
		Control:           in.GetControl(),
		ChildWorkflowOnly: in.GetChildWorkflowOnly(),
	}
}

// toProtoUpsertWorkflowSearchAttributesDecisionAttributes ...
func toProtoUpsertWorkflowSearchAttributesDecisionAttributes(in *shared.UpsertWorkflowSearchAttributesDecisionAttributes) *common.UpsertWorkflowSearchAttributesDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.UpsertWorkflowSearchAttributesDecisionAttributes{
		SearchAttributes: toProtoSearchAttributes(in.GetSearchAttributes()),
	}
}

// toProtoRecordMarkerDecisionAttributes ...
func toProtoRecordMarkerDecisionAttributes(in *shared.RecordMarkerDecisionAttributes) *common.RecordMarkerDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.RecordMarkerDecisionAttributes{
		MarkerName: in.GetMarkerName(),
		Details:    in.GetDetails(),
		Header:     toProtoHeader(in.GetHeader()),
	}
}

// toProtoContinueAsNewWorkflowExecutionDecisionAttributes ...
func toProtoContinueAsNewWorkflowExecutionDecisionAttributes(in *shared.ContinueAsNewWorkflowExecutionDecisionAttributes) *common.ContinueAsNewWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.ContinueAsNewWorkflowExecutionDecisionAttributes{
		WorkflowType:                        toProtoWorkflowType(in.GetWorkflowType()),
		TaskList:                            toProtoTaskList(in.GetTaskList()),
		Input:                               in.GetInput(),
		ExecutionStartToCloseTimeoutSeconds: in.GetExecutionStartToCloseTimeoutSeconds(),
		TaskStartToCloseTimeoutSeconds:      in.GetTaskStartToCloseTimeoutSeconds(),
		BackoffStartIntervalInSeconds:       in.GetBackoffStartIntervalInSeconds(),
		RetryPolicy:                         toProtoRetryPolicy(in.GetRetryPolicy()),
		Initiator:                           enums.ContinueAsNewInitiator(in.GetInitiator()),
		FailureReason:                       in.GetFailureReason(),
		FailureDetails:                      in.GetFailureDetails(),
		LastCompletionResult:                in.GetLastCompletionResult(),
		CronSchedule:                        in.GetCronSchedule(),
		Header:                              toProtoHeader(in.GetHeader()),
		Memo:                                toProtoMemo(in.GetMemo()),
		SearchAttributes:                    toProtoSearchAttributes(in.GetSearchAttributes()),
	}
}

// toProtoStartChildWorkflowExecutionDecisionAttributes ...
func toProtoStartChildWorkflowExecutionDecisionAttributes(in *shared.StartChildWorkflowExecutionDecisionAttributes) *common.StartChildWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &common.StartChildWorkflowExecutionDecisionAttributes{
		Domain:                              in.GetDomain(),
		WorkflowId:                          in.GetWorkflowId(),
		WorkflowType:                        toProtoWorkflowType(in.GetWorkflowType()),
		TaskList:                            toProtoTaskList(in.GetTaskList()),
		Input:                               in.GetInput(),
		ExecutionStartToCloseTimeoutSeconds: in.GetExecutionStartToCloseTimeoutSeconds(),
		TaskStartToCloseTimeoutSeconds:      in.GetTaskStartToCloseTimeoutSeconds(),
		ParentClosePolicy:                   enums.ParentClosePolicy(in.GetParentClosePolicy()),
		Control:                             in.GetControl(),
		WorkflowIdReusePolicy:               enums.WorkflowIdReusePolicy(in.GetWorkflowIdReusePolicy()),
		RetryPolicy:                         toProtoRetryPolicy(in.GetRetryPolicy()),
		CronSchedule:                        in.GetCronSchedule(),
		Header:                              toProtoHeader(in.GetHeader()),
		Memo:                                toProtoMemo(in.GetMemo()),
		SearchAttributes:                    toProtoSearchAttributes(in.GetSearchAttributes()),
	}
}
