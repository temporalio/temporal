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

package deleteexecutions

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/sdk"
)

const (
	HeartbeatEveryWorkflowExecutions = 100
)

type (
	Activities struct {
		sdkClientFactory sdk.ClientFactory
		historyClient    historyservice.HistoryServiceClient
		metricsClient    metrics.Client
		logger           log.Logger
	}

	GetNextPageTokenParams struct {
		Namespace     namespace.Name
		PageSize      int32
		NextPageToken []byte
	}
	GetNextPageTokenResult struct {
		NextPageToken []byte
	}

	DeleteExecutionsActivityParams struct {
		Namespace     namespace.Name
		NamespaceID   namespace.ID
		DeleteRPS     int
		ListPageSize  int32
		NextPageToken []byte
	}
	DeleteExecutionsActivityResult struct {
		ErrorCount   int
		SuccessCount int
	}
)

var (
	getNextPageTokenActivityOptions = workflow.LocalActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
		},
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Minute,
	}

	deleteWorkflowExecutionsActivityOptions = workflow.ActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 5 * time.Second,
		},
		StartToCloseTimeout:    60 * time.Minute,
		ScheduleToCloseTimeout: 6 * time.Hour,
		HeartbeatTimeout:       5 * time.Minute,
	}
)

func NewActivities(
	clientFactory sdk.ClientFactory,
	historyClient historyservice.HistoryServiceClient,
	metricsClient metrics.Client,
	logger log.Logger,
) *Activities {
	return &Activities{
		sdkClientFactory: clientFactory,
		historyClient:    historyClient,
		metricsClient:    metricsClient,
		logger:           logger,
	}
}

func (a *Activities) GetNextPageTokenActivity(ctx context.Context, params GetNextPageTokenParams) (GetNextPageTokenResult, error) {
	sdkClient, err := a.sdkClientFactory.NewClient(params.Namespace.String(), a.logger)
	if err != nil {
		return GetNextPageTokenResult{}, err
	}

	resp, err := sdkClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		NextPageToken: params.NextPageToken,
		PageSize:      params.PageSize,
	})
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.DeleteNamespaceFailuresCount)
		a.logger.Error("Unable to list all workflows.", tag.WorkflowNamespace(params.Namespace.String()), tag.Error(err))
		return GetNextPageTokenResult{}, err
	}

	return GetNextPageTokenResult{
		NextPageToken: resp.GetNextPageToken(),
	}, nil
}

func (a *Activities) DeleteExecutionsActivity(ctx context.Context, params DeleteExecutionsActivityParams) (DeleteExecutionsActivityResult, error) {
	sdkClient, err := a.sdkClientFactory.NewClient(params.Namespace.String(), a.logger)
	if err != nil {
		return DeleteExecutionsActivityResult{}, err
	}
	rateLimiter := quotas.NewRateLimiter(float64(params.DeleteRPS), params.DeleteRPS)

	var result DeleteExecutionsActivityResult
	resp, err := sdkClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		NextPageToken: params.NextPageToken,
		PageSize:      params.ListPageSize,
	})
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.DeleteNamespaceFailuresCount)
		a.logger.Error("Unable to list all workflows.", tag.WorkflowNamespace(params.Namespace.String()), tag.Error(err))
		return DeleteExecutionsActivityResult{}, err
	}
	for _, execution := range resp.Executions {
		_ = rateLimiter.Wait(ctx)
		if execution.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			_, err := a.historyClient.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
				NamespaceId: params.NamespaceID.String(),
				TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
					Namespace:         params.Namespace.String(),
					WorkflowExecution: execution.Execution,
					Reason:            "Delete namespace",
				},
			})
			if err != nil {
				a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.DeleteNamespaceFailuresCount)
				a.logger.Error("Unable to terminate workflow execution.", tag.WorkflowNamespace(params.Namespace.String()), tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()), tag.Error(err))
				result.ErrorCount++
				continue
			}
		}
		_, err = a.historyClient.DeleteWorkflowExecution(ctx, &historyservice.DeleteWorkflowExecutionRequest{
			NamespaceId:       params.NamespaceID.String(),
			WorkflowExecution: execution.Execution,
		})
		if err != nil {
			result.ErrorCount++
			a.metricsClient.IncCounter(metrics.DeleteNamespaceWorkflowScope, metrics.DeleteNamespaceFailuresCount)
			a.logger.Error("Unable to delete workflow execution.", tag.WorkflowNamespace(params.Namespace.String()), tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()), tag.Error(err))
		} else {
			result.SuccessCount++
		}
		if (result.SuccessCount+result.ErrorCount)%HeartbeatEveryWorkflowExecutions == 0 {
			activity.RecordHeartbeat(ctx, result)
		}
	}
	return result, nil
}
