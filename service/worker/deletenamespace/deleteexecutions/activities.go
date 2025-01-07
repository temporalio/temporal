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
	"fmt"
	"time"

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
		metricsHandler:    metricsHandler,
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
		metricsHandler:    metricsHandler,
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
		a.logger.Error("Unable to list all workflows to get next page token.", tag.WorkflowNamespace(params.Namespace.String()), tag.WorkflowNamespaceID(params.NamespaceID.String()), tag.Error(err))
		return nil, err
	}

	return resp.NextPageToken, nil
}

func (a *Activities) DeleteExecutionsActivity(ctx context.Context, params DeleteExecutionsActivityParams) (DeleteExecutionsActivityResult, error) {
	ctx = headers.SetCallerName(ctx, params.Namespace.String())
	logger := log.With(a.logger,
		tag.WorkflowNamespace(params.Namespace.String()),
		tag.WorkflowNamespaceID(params.NamespaceID.String()))

	progressCh := make(chan DeleteExecutionsActivityResult, 1)
	defer func() { close(progressCh) }()

	var result DeleteExecutionsActivityResult
	if activity.HasHeartbeatDetails(ctx) {
		var previousAttemptResult DeleteExecutionsActivityResult
		if err := activity.GetHeartbeatDetails(ctx, &previousAttemptResult); err != nil {
			// If heartbeat details can't be read, just log the error and continue because they are not important.
			logger.Warn("Unable to get heartbeat details from previous attempt while deleting workflow executions.", tag.Error(err))
		} else {
			// Carry over only success count because executions which gave error before,
			// either will give an error again or will be successfully deleted.
			// Errors shouldn't be double counted.
			result.SuccessCount = previousAttemptResult.SuccessCount
			// Send an initial result to heartbeat go routine.
			progressCh <- result
		}
	}

	go func() {
		heartbeatTicker := time.NewTicker(deleteWorkflowExecutionsActivityOptions.HeartbeatTimeout / 2)
		defer heartbeatTicker.Stop()

		var lastKnownProgress DeleteExecutionsActivityResult
		for {
			select {
			case progress, chOpen := <-progressCh:
				if !chOpen {
					// Stop heartbeating when a channel is closed, i.e., activity is completed.
					return
				}
				lastKnownProgress = progress
			case <-heartbeatTicker.C:
				activity.RecordHeartbeat(ctx, lastKnownProgress)
			}
		}
	}()

	req := &manager.ListWorkflowExecutionsRequestV2{
		NamespaceID:   params.NamespaceID,
		Namespace:     params.Namespace,
		PageSize:      params.ListPageSize,
		NextPageToken: params.NextPageToken,
		Query:         searchattribute.QueryWithAnyNamespaceDivision(""),
	}
	resp, err := a.visibilityManager.ListWorkflowExecutions(ctx, req)
	if err != nil {
		logger.Error("Unable to list all workflow executions.", tag.Error(err))
		return result, err
	}
	rateLimiter := quotas.NewRateLimiter(float64(params.RPS), params.RPS)
	for _, execution := range resp.Executions {
		err = rateLimiter.Wait(ctx)
		if err != nil {
			logger.Error("Workflow executions delete rate limiter error.", tag.Error(err))
			return result, fmt.Errorf("rate limiter error: %w", err)
		}
		_, err = a.historyClient.DeleteWorkflowExecution(ctx, &historyservice.DeleteWorkflowExecutionRequest{
			NamespaceId:       params.NamespaceID.String(),
			WorkflowExecution: execution.Execution,
		})
		switch err.(type) {
		case nil:
			result.SuccessCount++
			metrics.DeleteExecutionsSuccessCount.With(a.metricsHandler.WithTags(metrics.NamespaceTag(params.Namespace.String()))).Record(1)

		case *serviceerror.NotFound:
			metrics.DeleteExecutionsNotFoundCount.With(a.metricsHandler.WithTags(metrics.NamespaceTag(params.Namespace.String()))).Record(1)
			logger.Info("Workflow execution exists in the visibility store but not in the main store.", tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()))
			// The reasons why workflow execution doesn't exist in the main store, but exists in the visibility store might be:
			// 1. Someone else deleted the workflow execution after the last ListWorkflowExecutions call but before historyClient.DeleteWorkflowExecution call.
			// 2. Database is in inconsistent state: workflow execution was manually deleted from history store, but not from visibility store.
			// To avoid continuously getting this workflow execution from visibility store, it needs to be deleted directly from visibility store.
			s, e := a.deleteWorkflowExecutionFromVisibility(ctx, params.NamespaceID, params.Namespace, execution, logger)
			result.SuccessCount += s
			result.ErrorCount += e

		default:
			result.ErrorCount++
			metrics.DeleteExecutionsFailureCount.With(a.metricsHandler.WithTags(metrics.NamespaceTag(params.Namespace.String()))).Record(1)
			logger.Error("Unable to delete workflow execution.", tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()), tag.Error(err))
		}
		select {
		case progressCh <- result:
			// Send the current result to heartbeat go routine.
		case <-ctx.Done():
			// Stop deletion on cancellation.
			return result, ctx.Err()
		default:
			// Don't block deletion if a progress channel is full.
		}
	}
	return result, nil
}

func (a *Activities) deleteWorkflowExecutionFromVisibility(
	ctx context.Context,
	nsID namespace.ID,
	nsName namespace.Name,
	execution *workflowpb.WorkflowExecutionInfo,
	logger log.Logger,
) (successCount int, errorCount int) {

	logger = log.With(logger, tag.WorkflowID(execution.Execution.GetWorkflowId()), tag.WorkflowRunID(execution.Execution.GetRunId()))

	logger.Info("Deleting workflow execution from visibility.")
	_, err := a.historyClient.DeleteWorkflowVisibilityRecord(ctx, &historyservice.DeleteWorkflowVisibilityRecordRequest{
		NamespaceId: nsID.String(),
		Execution:   execution.GetExecution(),
	})
	switch err.(type) {
	case nil:
		// Indicates that main and visibility stores were in inconsistent state.
		metrics.DeleteExecutionsSuccessCount.With(a.metricsHandler.WithTags(metrics.NamespaceTag(nsName.String()))).Record(1)
		logger.Info("Workflow execution deleted from visibility.")
		return 1, 0
	case *serviceerror.NotFound:
		// Indicates that someone else deleted workflow execution.
		logger.Error("Workflow execution is not found in visibility store.")
		return 0, 0
	default:
		metrics.DeleteExecutionsFailureCount.With(a.metricsHandler.WithTags(metrics.NamespaceTag(nsName.String()))).Record(1)
		logger.Error("Unable to delete workflow execution from visibility store.", tag.Error(err))
		return 0, 1
	}
}
