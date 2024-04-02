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
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/activity"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/searchattribute"
)

type (
	Activities struct {
		visibilityManager manager.VisibilityManager
		historyClient     historyservice.HistoryServiceClient
		metricsHandler    metrics.Handler
		logger            log.Logger
	}

	LocalActivities struct {
		visibilityManager manager.VisibilityManager
		metricsHandler    metrics.Handler
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
	metricsHandler metrics.Handler,
	logger log.Logger,
) *Activities {
	return &Activities{
		visibilityManager: visibilityManager,
		historyClient:     historyClient,
		metricsHandler:    metricsHandler.WithTags(metrics.OperationTag(metrics.DeleteExecutionsWorkflowScope)),
		logger:            logger,
	}
}

func NewLocalActivities(
	visibilityManager manager.VisibilityManager,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *LocalActivities {
	return &LocalActivities{
		visibilityManager: visibilityManager,
		metricsHandler:    metricsHandler.WithTags(metrics.OperationTag(metrics.DeleteExecutionsWorkflowScope)),
		logger:            logger,
	}
}

func (a *LocalActivities) GetNextPageTokenActivity(ctx context.Context, params GetNextPageTokenParams) ([]byte, error) {
	ctx = headers.SetCallerName(ctx, params.Namespace.String())

	req := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   params.NamespaceID,
		Namespace:     params.Namespace,
		PageSize:      params.PageSize,
		NextPageToken: params.NextPageToken,
		Query:         searchattribute.QueryWithAnyNamespaceDivision(""),
	}

	resp, err := a.visibilityManager.ListWorkflowExecutions(ctx, req)
	if err != nil {
		metrics.ListExecutionsFailuresCount.With(a.metricsHandler).Record(1)
		a.logger.Error("Unable to list all workflows to get next page token.", tag.WorkflowNamespace(params.Namespace.String()), tag.Error(err))
		return nil, err
	}

	return resp.NextPageToken, nil
}

func (a *Activities) DeleteExecutionsActivity(ctx context.Context, params DeleteExecutionsActivityParams) (DeleteExecutionsActivityResult, error) {
	ctx = headers.SetCallerName(ctx, params.Namespace.String())

	rateLimiter := quotas.NewRateLimiter(float64(params.RPS), params.RPS)

	var result DeleteExecutionsActivityResult

	req := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   params.NamespaceID,
		Namespace:     params.Namespace,
		PageSize:      params.ListPageSize,
		NextPageToken: params.NextPageToken,
		Query:         searchattribute.QueryWithAnyNamespaceDivision(""),
	}
	resp, err := a.visibilityManager.ListWorkflowExecutions(ctx, req)
	if err != nil {
		metrics.ListExecutionsFailuresCount.With(a.metricsHandler).Record(1)
		a.logger.Error("Unable to list all workflow executions.", tag.WorkflowNamespace(params.Namespace.String()), tag.Error(err))
		return result, err
	}
	for _, execution := range resp.Executions {
		err = rateLimiter.Wait(ctx)
		if err != nil {
			metrics.RateLimiterFailuresCount.With(a.metricsHandler).Record(1)
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
			metrics.DeleteExecutionsSuccessCount.With(a.metricsHandler).Record(1)

		case *serviceerror.NotFound:
			metrics.DeleteExecutionNotFoundCount.With(a.metricsHandler).Record(1)
			a.logger.Info("Workflow execution is not found in history service.", tag.WorkflowNamespace(params.Namespace.String()), tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()))
			// The reasons why workflow execution doesn't exist in history service, but exists in visibility store might be:
			// 1. The workflow execution was deleted by someone else after last ListWorkflowExecutions call but before historyClient.DeleteWorkflowExecution call.
			// 2. Database is in inconsistent state: workflow execution was manually deleted from history store, but not from visibility store.
			// To avoid continuously getting this workflow execution from visibility store, it needs to be deleted directly from visibility store.
			s, e := a.deleteWorkflowExecutionFromVisibility(ctx, params.NamespaceID, execution)
			result.SuccessCount += s
			result.ErrorCount += e

		default:
			result.ErrorCount++
			metrics.DeleteExecutionFailuresCount.With(a.metricsHandler).Record(1)
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

func (a *Activities) deleteWorkflowExecutionFromVisibility(
	ctx context.Context,
	namespaceID namespace.ID,
	execution *workflowpb.WorkflowExecutionInfo,
) (successCount int, errorCount int) {

	a.logger.Info("Deleting workflow execution from visibility.", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()))
	_, err := a.historyClient.DeleteWorkflowVisibilityRecord(ctx, &historyservice.DeleteWorkflowVisibilityRecordRequest{
		NamespaceId: namespaceID.String(),
		Execution:   execution.GetExecution(),
	})
	switch err.(type) {
	case nil:
		// Indicates that main and visibility stores were in inconsistent state.
		metrics.DeleteExecutionsSuccessCount.With(a.metricsHandler).Record(1)
		a.logger.Info("Workflow execution deleted from visibility.", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()))
		return 1, 0
	case *serviceerror.NotFound:
		// Indicates that workflow execution was deleted by someone else.
		a.logger.Error("Workflow execution is not found in visibility store.", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()))
		return 0, 0
	default:
		metrics.DeleteExecutionFailuresCount.With(a.metricsHandler).Record(1)
		a.logger.Error("Unable to delete workflow execution from visibility store.", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()), tag.Error(err))
		return 0, 1
	}
}
