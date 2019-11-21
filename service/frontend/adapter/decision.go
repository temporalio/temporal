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
	"github.com/temporalio/temporal/.gen/go/shared"
)

// toThriftDecisions ...
func toThriftDecisions(in []*common.Decision) []*shared.Decision {
	if in == nil {
		return nil
	}

	var ret []*shared.Decision
	for _, item := range in {
		ret = append(ret, toThriftDecision(item))
	}
	return ret
}

// toThriftDecision ...
func toThriftDecision(in *common.Decision) *shared.Decision {
	if in == nil {
		return nil
	}
	return &shared.Decision{
		DecisionType:                                             toThriftDecisionType(in.DecisionType),
		ScheduleActivityTaskDecisionAttributes:                   toThriftScheduleActivityTaskDecisionAttributes(in.GetScheduleActivityTaskDecisionAttributes()),
		StartTimerDecisionAttributes:                             toThriftStartTimerDecisionAttributes(in.GetStartTimerDecisionAttributes()),
		CompleteWorkflowExecutionDecisionAttributes:              toThriftCompleteWorkflowExecutionDecisionAttributes(in.GetCompleteWorkflowExecutionDecisionAttributes()),
		FailWorkflowExecutionDecisionAttributes:                  toThriftFailWorkflowExecutionDecisionAttributes(in.GetFailWorkflowExecutionDecisionAttributes()),
		RequestCancelActivityTaskDecisionAttributes:              toThriftRequestCancelActivityTaskDecisionAttributes(in.GetRequestCancelActivityTaskDecisionAttributes()),
		CancelTimerDecisionAttributes:                            toThriftCancelTimerDecisionAttributes(in.GetCancelTimerDecisionAttributes()),
		CancelWorkflowExecutionDecisionAttributes:                toThriftCancelWorkflowExecutionDecisionAttributes(in.GetCancelWorkflowExecutionDecisionAttributes()),
		RequestCancelExternalWorkflowExecutionDecisionAttributes: toThriftRequestCancelExternalWorkflowExecutionDecisionAttributes(in.GetRequestCancelExternalWorkflowExecutionDecisionAttributes()),
		RecordMarkerDecisionAttributes:                           toThriftRecordMarkerDecisionAttributes(in.GetRecordMarkerDecisionAttributes()),
		ContinueAsNewWorkflowExecutionDecisionAttributes:         toThriftContinueAsNewWorkflowExecutionDecisionAttributes(in.GetContinueAsNewWorkflowExecutionDecisionAttributes()),
		StartChildWorkflowExecutionDecisionAttributes:            toThriftStartChildWorkflowExecutionDecisionAttributes(in.GetStartChildWorkflowExecutionDecisionAttributes()),
		SignalExternalWorkflowExecutionDecisionAttributes:        toThriftSignalExternalWorkflowExecutionDecisionAttributes(in.GetSignalExternalWorkflowExecutionDecisionAttributes()),
		UpsertWorkflowSearchAttributesDecisionAttributes:         toThriftUpsertWorkflowSearchAttributesDecisionAttributes(in.GetUpsertWorkflowSearchAttributesDecisionAttributes()),
	}
}

// toThriftScheduleActivityTaskDecisionAttributes ...
func toThriftScheduleActivityTaskDecisionAttributes(in *common.ScheduleActivityTaskDecisionAttributes) *shared.ScheduleActivityTaskDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.ScheduleActivityTaskDecisionAttributes{
		ActivityId:                    &in.ActivityId,
		ActivityType:                  toThriftActivityType(in.ActivityType),
		Domain:                        &in.Domain,
		TaskList:                      toThriftTaskList(in.TaskList),
		Input:                         in.Input,
		ScheduleToCloseTimeoutSeconds: &in.ScheduleToCloseTimeoutSeconds,
		ScheduleToStartTimeoutSeconds: &in.ScheduleToStartTimeoutSeconds,
		StartToCloseTimeoutSeconds:    &in.StartToCloseTimeoutSeconds,
		HeartbeatTimeoutSeconds:       &in.HeartbeatTimeoutSeconds,
		RetryPolicy:                   toThriftRetryPolicy(in.RetryPolicy),
		Header:                        toThriftHeader(in.Header),
	}
}

// toThriftRequestCancelActivityTaskDecisionAttributes ...
func toThriftRequestCancelActivityTaskDecisionAttributes(in *common.RequestCancelActivityTaskDecisionAttributes) *shared.RequestCancelActivityTaskDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.RequestCancelActivityTaskDecisionAttributes{
		ActivityId: &in.ActivityId,
	}
}

// toThriftStartTimerDecisionAttributes ...
func toThriftStartTimerDecisionAttributes(in *common.StartTimerDecisionAttributes) *shared.StartTimerDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.StartTimerDecisionAttributes{
		TimerId:                   &in.TimerId,
		StartToFireTimeoutSeconds: &in.StartToFireTimeoutSeconds,
	}
}

// toThriftCompleteWorkflowExecutionDecisionAttributes ...
func toThriftCompleteWorkflowExecutionDecisionAttributes(in *common.CompleteWorkflowExecutionDecisionAttributes) *shared.CompleteWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.CompleteWorkflowExecutionDecisionAttributes{
		Result: in.Result,
	}
}

// toThriftFailWorkflowExecutionDecisionAttributes ...
func toThriftFailWorkflowExecutionDecisionAttributes(in *common.FailWorkflowExecutionDecisionAttributes) *shared.FailWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.FailWorkflowExecutionDecisionAttributes{
		Reason:  &in.Reason,
		Details: in.Details,
	}
}

// toThriftCancelTimerDecisionAttributes ...
func toThriftCancelTimerDecisionAttributes(in *common.CancelTimerDecisionAttributes) *shared.CancelTimerDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.CancelTimerDecisionAttributes{
		TimerId: &in.TimerId,
	}
}

// toThriftCancelWorkflowExecutionDecisionAttributes ...
func toThriftCancelWorkflowExecutionDecisionAttributes(in *common.CancelWorkflowExecutionDecisionAttributes) *shared.CancelWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.CancelWorkflowExecutionDecisionAttributes{
		Details: in.Details,
	}
}

// toThriftRequestCancelExternalWorkflowExecutionDecisionAttributes ...
func toThriftRequestCancelExternalWorkflowExecutionDecisionAttributes(in *common.RequestCancelExternalWorkflowExecutionDecisionAttributes) *shared.RequestCancelExternalWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.RequestCancelExternalWorkflowExecutionDecisionAttributes{
		Domain:            &in.Domain,
		WorkflowId:        &in.WorkflowId,
		RunId:             &in.RunId,
		Control:           in.Control,
		ChildWorkflowOnly: &in.ChildWorkflowOnly,
	}
}

// toThriftSignalExternalWorkflowExecutionDecisionAttributes ...
func toThriftSignalExternalWorkflowExecutionDecisionAttributes(in *common.SignalExternalWorkflowExecutionDecisionAttributes) *shared.SignalExternalWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.SignalExternalWorkflowExecutionDecisionAttributes{
		Domain:            &in.Domain,
		Execution:         toThriftWorkflowExecution(in.Execution),
		SignalName:        &in.SignalName,
		Input:             in.Input,
		Control:           in.Control,
		ChildWorkflowOnly: &in.ChildWorkflowOnly,
	}
}

// toThriftUpsertWorkflowSearchAttributesDecisionAttributes ...
func toThriftUpsertWorkflowSearchAttributesDecisionAttributes(in *common.UpsertWorkflowSearchAttributesDecisionAttributes) *shared.UpsertWorkflowSearchAttributesDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.UpsertWorkflowSearchAttributesDecisionAttributes{
		SearchAttributes: toThriftSearchAttributes(in.SearchAttributes),
	}
}

// toThriftRecordMarkerDecisionAttributes ...
func toThriftRecordMarkerDecisionAttributes(in *common.RecordMarkerDecisionAttributes) *shared.RecordMarkerDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.RecordMarkerDecisionAttributes{
		MarkerName: &in.MarkerName,
		Details:    in.Details,
		Header:     toThriftHeader(in.Header),
	}
}

// toThriftContinueAsNewWorkflowExecutionDecisionAttributes ...
func toThriftContinueAsNewWorkflowExecutionDecisionAttributes(in *common.ContinueAsNewWorkflowExecutionDecisionAttributes) *shared.ContinueAsNewWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.ContinueAsNewWorkflowExecutionDecisionAttributes{
		WorkflowType:                        toThriftWorkflowType(in.WorkflowType),
		TaskList:                            toThriftTaskList(in.TaskList),
		Input:                               in.Input,
		ExecutionStartToCloseTimeoutSeconds: &in.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      &in.TaskStartToCloseTimeoutSeconds,
		BackoffStartIntervalInSeconds:       &in.BackoffStartIntervalInSeconds,
		RetryPolicy:                         toThriftRetryPolicy(in.RetryPolicy),
		Initiator:                           toThriftContinueAsNewInitiator(in.Initiator),
		FailureReason:                       &in.FailureReason,
		FailureDetails:                      in.FailureDetails,
		LastCompletionResult:                in.LastCompletionResult,
		CronSchedule:                        &in.CronSchedule,
		Header:                              toThriftHeader(in.Header),
		Memo:                                toThriftMemo(in.Memo),
		SearchAttributes:                    toThriftSearchAttributes(in.SearchAttributes),
	}
}

// toThriftStartChildWorkflowExecutionDecisionAttributes ...
func toThriftStartChildWorkflowExecutionDecisionAttributes(in *common.StartChildWorkflowExecutionDecisionAttributes) *shared.StartChildWorkflowExecutionDecisionAttributes {
	if in == nil {
		return nil
	}
	return &shared.StartChildWorkflowExecutionDecisionAttributes{
		Domain:                              &in.Domain,
		WorkflowId:                          &in.WorkflowId,
		WorkflowType:                        toThriftWorkflowType(in.WorkflowType),
		TaskList:                            toThriftTaskList(in.TaskList),
		Input:                               in.Input,
		ExecutionStartToCloseTimeoutSeconds: &in.ExecutionStartToCloseTimeoutSeconds,
		TaskStartToCloseTimeoutSeconds:      &in.TaskStartToCloseTimeoutSeconds,
		ParentClosePolicy:                   toThriftParentClosePolicy(in.ParentClosePolicy),
		Control:                             in.Control,
		WorkflowIdReusePolicy:               toThriftWorkflowIDReusePolicy(in.WorkflowIdReusePolicy),
		RetryPolicy:                         toThriftRetryPolicy(in.RetryPolicy),
		CronSchedule:                        &in.CronSchedule,
		Header:                              toThriftHeader(in.Header),
		Memo:                                toThriftMemo(in.Memo),
		SearchAttributes:                    toThriftSearchAttributes(in.SearchAttributes),
	}
}
