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

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
)

type (
	Activities struct {
		visibilityManager manager.VisibilityManager
		historyClient     historyservice.HistoryServiceClient
		metricsClient     metrics.Client
		logger            log.Logger
	}

	GetNextPageTokenParams struct {
		Namespace     namespace.Name
		NamespaceID   namespace.ID
		PageSize      int
		NextPageToken []byte
	}

	DeleteExecutionsActivityParams struct {
		Namespace     namespace.Name
		NamespaceID   namespace.ID
		RPS           int
		ListPageSize  int
		NextPageToken []byte
	}

	DeleteExecutionsActivityResult struct {
		ErrorCount   int
		SuccessCount int
	}
)

func NewActivities(
	visibilityManager manager.VisibilityManager,
	historyClient historyservice.HistoryServiceClient,
	metricsClient metrics.Client,
	logger log.Logger,
) *Activities {
	return &Activities{
		visibilityManager: visibilityManager,
		historyClient:     historyClient,
		metricsClient:     metricsClient,
		logger:            logger,
	}
}

func (a *Activities) GetNextPageTokenActivity(ctx context.Context, params GetNextPageTokenParams) ([]byte, error) {
	req := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   params.NamespaceID,
		Namespace:     params.Namespace,
		PageSize:      params.PageSize,
		NextPageToken: params.NextPageToken,
	}

	resp, err := a.visibilityManager.ListWorkflowExecutions(ctx, req)
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteExecutionsWorkflowScope, metrics.ListExecutionsFailuresCount)
		a.logger.Error("Unable to list all workflows to get next page token.", tag.WorkflowNamespace(params.Namespace.String()), tag.Error(err))
		return nil, err
	}

	return resp.NextPageToken, nil
}

func (a *Activities) DeleteExecutionsActivity(ctx context.Context, params DeleteExecutionsActivityParams) (DeleteExecutionsActivityResult, error) {
	rateLimiter := quotas.NewRateLimiter(float64(params.RPS), params.RPS)

	var result DeleteExecutionsActivityResult

	req := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   params.NamespaceID,
		Namespace:     params.Namespace,
		PageSize:      params.ListPageSize,
		NextPageToken: params.NextPageToken,
	}
	resp, err := a.visibilityManager.ListWorkflowExecutions(ctx, req)
	if err != nil {
		a.metricsClient.IncCounter(metrics.DeleteExecutionsWorkflowScope, metrics.ListExecutionsFailuresCount)
		a.logger.Error("Unable to list all workflow executions.", tag.WorkflowNamespace(params.Namespace.String()), tag.Error(err))
		return result, err
	}
	for _, execution := range resp.Executions {
		err = rateLimiter.Wait(ctx)
		if err != nil {
			a.metricsClient.IncCounter(metrics.DeleteExecutionsWorkflowScope, metrics.RateLimiterFailuresCount)
			a.logger.Error("Workflow executions delete rate limiter error.", tag.WorkflowNamespace(params.Namespace.String()), tag.Error(err))
			return result, err
		}
		_, err = a.historyClient.DeleteWorkflowExecution(ctx, &historyservice.DeleteWorkflowExecutionRequest{
			NamespaceId:       params.NamespaceID.String(),
			WorkflowExecution: execution.Execution,
		})
		switch err.(type) {
		case nil:
			result.SuccessCount++
			a.metricsClient.IncCounter(metrics.DeleteExecutionsWorkflowScope, metrics.DeleteExecutionsSuccessCount)
		case *serviceerror.NotFound: // Workflow execution doesn't exist. Do nothing.
			a.metricsClient.IncCounter(metrics.DeleteExecutionsWorkflowScope, metrics.DeleteExecutionNotFoundCount)
			a.logger.Info("Workflow execution is not found.", tag.WorkflowNamespace(params.Namespace.String()), tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()))
		default:
			result.ErrorCount++
			a.metricsClient.IncCounter(metrics.DeleteExecutionsWorkflowScope, metrics.DeleteExecutionFailuresCount)
			a.logger.Error("Unable to delete workflow execution.", tag.WorkflowNamespace(params.Namespace.String()), tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()), tag.Error(err))
		}
		activity.RecordHeartbeat(ctx, result)
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}
	}
	return result, nil
}
