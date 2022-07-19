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

package reclaimresources

import (
	stderrors "errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
)

const (
	WorkflowName = "temporal-sys-reclaim-namespace-resources-workflow"

	namespaceCacheRefreshDelay = 11 * time.Second
)

type (
	ReclaimResourcesParams struct {
		deleteexecutions.DeleteExecutionsParams
	}

	ReclaimResourcesResult struct {
		DeleteSuccessCount int
		DeleteErrorCount   int
		NamespaceDeleted   bool
	}
)

var (
	retryPolicy = &temporal.RetryPolicy{
		InitialInterval: 1 * time.Second,
		MaximumInterval: 10 * time.Second,
	}

	localActivityOptions = workflow.LocalActivityOptions{
		RetryPolicy:            retryPolicy,
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 5 * time.Minute,
	}

	deleteExecutionsWorkflowOptions = workflow.ChildWorkflowOptions{
		RetryPolicy:        retryPolicy,
		WorkflowRunTimeout: 60 * time.Minute,
	}

	ensureNoExecutionsActivityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    1 * time.Second,
		MaximumInterval:    2 * time.Minute,
		BackoffCoefficient: 2,
	}

	ensureNoExecutionsStdVisibilityOptionsActivity = workflow.ActivityOptions{
		RetryPolicy:            ensureNoExecutionsActivityRetryPolicy,
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 30 * time.Minute, // ~20 attempts
	}

	ensureNoExecutionsAdvVisibilityActivityOptions = workflow.ActivityOptions{
		RetryPolicy:            ensureNoExecutionsActivityRetryPolicy,
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Hour, // Sanity check, advanced visibility can control the progress of activity.
	}
)

func validateParams(params *ReclaimResourcesParams) error {
	if params.NamespaceID.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("namespace ID is required", "", nil)
	}

	if params.Namespace.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("namespace is required", "", nil)
	}

	params.Config.ApplyDefaults()

	return nil
}

func ReclaimResourcesWorkflow(ctx workflow.Context, params ReclaimResourcesParams) (ReclaimResourcesResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow started.", tag.WorkflowType(WorkflowName))

	if err := validateParams(&params); err != nil {
		return ReclaimResourcesResult{}, err
	}

	var a *Activities

	// Step 0. This workflow is started right after namespace is marked as DELETED and renamed.
	// Wait for namespace cache refresh to make sure no new executions are created.
	err := workflow.Sleep(ctx, namespaceCacheRefreshDelay)
	if err != nil {
		return ReclaimResourcesResult{}, err
	}

	// Step 1. Delete workflow executions.
	result, err := deleteWorkflowExecutions(ctx, params)
	if err != nil {
		return result, err
	}

	// Step 2. Delete namespace.
	ctx5 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	err = workflow.ExecuteLocalActivity(ctx5, a.DeleteNamespaceActivity, params.NamespaceID, params.Namespace).Get(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("%w: DeleteNamespaceActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	result.NamespaceDeleted = true
	logger.Info("Workflow finished successfully.", tag.WorkflowType(WorkflowName))
	return result, nil
}

func deleteWorkflowExecutions(ctx workflow.Context, params ReclaimResourcesParams) (ReclaimResourcesResult, error) {
	var a *Activities
	logger := workflow.GetLogger(ctx)
	var result ReclaimResourcesResult

	ctx1 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	var isAdvancedVisibility bool
	err := workflow.ExecuteLocalActivity(ctx1, a.IsAdvancedVisibilityActivity).Get(ctx, &isAdvancedVisibility)
	if err != nil {
		return result, fmt.Errorf("%w: IsAdvancedVisibilityActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	if isAdvancedVisibility {
		ctx4 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
		var executionsCount int64
		err = workflow.ExecuteLocalActivity(ctx4, a.CountExecutionsAdvVisibilityActivity, params.NamespaceID, params.Namespace).Get(ctx, &executionsCount)
		if err != nil {
			return result, fmt.Errorf("%w: CountExecutionsAdvVisibilityActivity: %v", errors.ErrUnableToExecuteActivity, err)
		}
		if executionsCount == 0 {
			return result, nil
		}
	}

	ctx2 := workflow.WithChildOptions(ctx, deleteExecutionsWorkflowOptions)
	ctx2 = workflow.WithWorkflowID(ctx2, fmt.Sprintf("%s/%s", deleteexecutions.WorkflowName, params.Namespace))
	var der deleteexecutions.DeleteExecutionsResult
	err = workflow.ExecuteChildWorkflow(ctx2, deleteexecutions.DeleteExecutionsWorkflow, params.DeleteExecutionsParams).Get(ctx, &der)
	if err != nil {
		logger.Error("Unable to execute child workflow.", tag.WorkflowType(deleteexecutions.WorkflowName), tag.Error(err))
		return result, fmt.Errorf("%w: %s: %v", errors.ErrUnableToExecuteChildWorkflow, deleteexecutions.WorkflowName, err)
	}
	result.DeleteSuccessCount = der.SuccessCount
	result.DeleteErrorCount = der.ErrorCount

	if isAdvancedVisibility {
		ctx3 := workflow.WithActivityOptions(ctx, ensureNoExecutionsAdvVisibilityActivityOptions)
		err = workflow.ExecuteActivity(ctx3, a.EnsureNoExecutionsAdvVisibilityActivity, params.NamespaceID, params.Namespace, der.ErrorCount).Get(ctx, nil)
	} else {
		ctx3 := workflow.WithActivityOptions(ctx, ensureNoExecutionsStdVisibilityOptionsActivity)
		err = workflow.ExecuteActivity(ctx3, a.EnsureNoExecutionsStdVisibilityActivity, params.NamespaceID, params.Namespace).Get(ctx, nil)
	}
	if err != nil {
		var appErr *temporal.ApplicationError
		if stderrors.As(err, &appErr) {
			switch appErr.Type() {
			case errors.ExecutionsStillExistErrType, errors.NoProgressErrType, errors.NotDeletedExecutionsStillExistErrType:
				var notDeletedCount int
				var counterTag tag.ZapTag
				if appErr.HasDetails() {
					_ = appErr.Details(&notDeletedCount)
					counterTag = tag.Counter(notDeletedCount)
				}
				logger.Info("Unable to delete workflow executions.", tag.WorkflowNamespace(params.Namespace.String()), counterTag)
				// appErr is not retryable. Convert it to retryable for the server to retry.
				return result, temporal.NewApplicationError(appErr.Message(), appErr.Type(), notDeletedCount)
			}
		}
		return result, fmt.Errorf("%w: EnsureNoExecutionsActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	return result, nil
}
