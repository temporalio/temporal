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

package parentclosepolicy

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

const (
	processorContextKey = "processorContext"
	// processorTaskQueueName is the taskqueue name
	processorTaskQueueName = "temporal-sys-processor-parent-close-policy"
	// processorWFTypeName is the workflow type
	processorWFTypeName   = "temporal-sys-parent-close-policy-workflow"
	processorActivityName = "temporal-sys-parent-close-policy-activity"
	infiniteDuration      = 20 * 365 * 24 * time.Hour
	processorChannelName  = "ParentClosePolicyProcessorChannelName"
)

type (
	// RequestDetail defines detail of each workflow to process
	RequestDetail struct {
		WorkflowID string
		RunID      string
		Policy     enumspb.ParentClosePolicy
	}

	// Request defines the request for parent close policy
	Request struct {
		Executions  []RequestDetail
		Namespace   string
		NamespaceID string
	}
)

var (
	retryPolicy = temporal.RetryPolicy{
		InitialInterval:    10 * time.Second,
		BackoffCoefficient: 1.7,
		MaximumInterval:    5 * time.Minute,
	}

	activityOptions = workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    5 * time.Minute,
		RetryPolicy:            &retryPolicy,
	}
)

// ProcessorWorkflow is the workflow that performs actions for ParentClosePolicy
func ProcessorWorkflow(ctx workflow.Context) error {
	requestCh := workflow.GetSignalChannel(ctx, processorChannelName)
	for {
		var request Request
		if !requestCh.ReceiveAsync(&request) {
			// no more request
			break
		}

		opt := workflow.WithActivityOptions(ctx, activityOptions)
		_ = workflow.ExecuteActivity(opt, processorActivityName, request).Get(ctx, nil)
	}
	return nil
}

// ProcessorActivity is activity for processing batch operation
func ProcessorActivity(ctx context.Context, request Request) error {
	processor := ctx.Value(processorContextKey).(*Processor)
	client := processor.clientBean.GetHistoryClient()
	for _, execution := range request.Executions {
		var err error
		switch execution.Policy {
		case enumspb.PARENT_CLOSE_POLICY_ABANDON:
			//no-op
			continue
		case enumspb.PARENT_CLOSE_POLICY_TERMINATE:
			_, err = client.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
				NamespaceId: request.NamespaceID,
				TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
					Namespace: request.Namespace,
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: execution.WorkflowID,
					},
					Reason:              "by parent close policy",
					Identity:            processorWFTypeName,
					FirstExecutionRunId: execution.RunID,
				},
			})
		case enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL:
			_, err = client.RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{
				NamespaceId: request.NamespaceID,
				CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
					Namespace: request.Namespace,
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: execution.WorkflowID,
					},
					Identity:            processorWFTypeName,
					FirstExecutionRunId: execution.RunID,
				},
			})
		}

		if err != nil {
			if _, ok := err.(*serviceerror.NotFound); ok {
				err = nil
			}
		}

		if err != nil {
			processor.metricsClient.IncCounter(metrics.ParentClosePolicyProcessorScope, metrics.ParentClosePolicyProcessorFailures)
			getActivityLogger(ctx).Error("failed to process parent close policy", tag.Error(err))
			return err
		}
		processor.metricsClient.IncCounter(metrics.ParentClosePolicyProcessorScope, metrics.ParentClosePolicyProcessorSuccess)
	}
	return nil
}

func getActivityLogger(ctx context.Context) log.Logger {
	processor := ctx.Value(processorContextKey).(*Processor)
	wfInfo := activity.GetInfo(ctx)
	return log.With(
		processor.logger,
		tag.WorkflowID(wfInfo.WorkflowExecution.ID),
		tag.WorkflowRunID(wfInfo.WorkflowExecution.RunID),
		tag.WorkflowNamespace(wfInfo.WorkflowNamespace),
	)
}
