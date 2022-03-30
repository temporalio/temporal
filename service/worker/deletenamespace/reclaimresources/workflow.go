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
	// WorkflowName is the workflow name.
	WorkflowName = "temporal-sys-reclaim-namespace-resources-workflow"

	namespaceCacheRefreshDelay = 11 * time.Second
)

type (
	ReclaimResourcesParams struct {
		deleteexecutions.DeleteExecutionsParams
	}

	ReclaimResourcesResult struct {
		SuccessCount int
		ErrorCount   int
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

	logger.Info("Workflow finished successfully.", tag.WorkflowType(WorkflowName))
	return result, nil
}

func deleteWorkflowExecutions(ctx workflow.Context, params ReclaimResourcesParams) (ReclaimResourcesResult, error) {
	var a *Activities
	logger := workflow.GetLogger(ctx)
	deleteAttempt := int32(1)
	var result ReclaimResourcesResult

	for {
		ctx1 := workflow.WithChildOptions(ctx, deleteExecutionsWorkflowOptions)
		ctx1 = workflow.WithWorkflowID(ctx1, fmt.Sprintf("%s/%s", deleteexecutions.WorkflowName, params.Namespace))
		var der deleteexecutions.DeleteExecutionsResult
		err := workflow.ExecuteChildWorkflow(ctx1, deleteexecutions.DeleteExecutionsWorkflow, params.DeleteExecutionsParams).Get(ctx, &der)
		if err != nil {
			logger.Error("Unable to execute child workflow.", tag.WorkflowType(deleteexecutions.WorkflowName), tag.Error(err))
			return result, fmt.Errorf("%w: %s: %v", errors.ErrUnableToExecuteChildWorkflow, deleteexecutions.WorkflowName, err)
		}
		result.SuccessCount += der.SuccessCount
		result.ErrorCount += der.ErrorCount

		ensureNoExecutionsActivityOptions := workflow.ActivityOptions{
			// 445 seconds of total retry intervals.
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    1 * time.Second,
				MaximumInterval:    200 * time.Second,
				BackoffCoefficient: 1.8,
				MaximumAttempts:    10,
			},
			StartToCloseTimeout:    30 * time.Second,
			ScheduleToCloseTimeout: 600 * time.Second,
		}
		ctx2 := workflow.WithActivityOptions(ctx, ensureNoExecutionsActivityOptions)
		err = workflow.ExecuteActivity(ctx2, a.EnsureNoExecutionsActivity, params.NamespaceID, params.Namespace).Get(ctx, nil)
		if err == nil {
			break
		}
		var appErr *temporal.ApplicationError
		if stderrors.As(err, &appErr) {
			switch appErr.Type() {
			case errors.ExecutionsStillExistErrType:
				logger.Info("Unable to delete workflow executions. Will try again.", tag.WorkflowNamespace(params.Namespace.String()), tag.Counter(der.ErrorCount), tag.Attempt(deleteAttempt))
				deleteAttempt++
				continue
			}
		}
		return result, fmt.Errorf("%w: EnsureNoExecutionsActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	logger.Info("All workflow executions has been deleted successfully.", tag.WorkflowNamespace(params.Namespace.String()), tag.DeletedExecutionsCount(result.SuccessCount), tag.DeletedExecutionsErrorCount(result.ErrorCount))
	return result, nil
}
