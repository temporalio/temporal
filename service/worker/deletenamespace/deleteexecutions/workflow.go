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
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/worker/deletenamespace/defaults"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
)

const (
	WorkflowName = "temporal-sys-delete-executions-workflow"

	heartbeatEveryExecutions = 1000
)

type (
	DeleteExecutionsParams struct {
		Namespace   namespace.Name
		NamespaceID namespace.ID

		DeleteRPS                                 int
		ListPageSize                              int
		ConcurrentDeleteExecutionsActivitiesCount int

		PreviousSuccessCount int
		PreviousErrorCount   int
	}

	DeleteExecutionsResult struct {
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
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: 5 * time.Minute,
	}

	deleteWorkflowExecutionsActivityOptions = workflow.ActivityOptions{
		RetryPolicy:            retryPolicy,
		StartToCloseTimeout:    60 * time.Minute,
		ScheduleToCloseTimeout: 6 * time.Hour,
	}
)

func validateParams(params *DeleteExecutionsParams) error {
	if params.NamespaceID.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("namespace ID is empty", "", nil)
	}

	if params.Namespace.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("namespace is empty", "", nil)
	}

	if params.DeleteRPS <= 0 {
		params.DeleteRPS = defaults.DeleteRPS
	}
	if params.ListPageSize <= 0 {
		params.ListPageSize = defaults.ListPageSize
	}
	if params.ConcurrentDeleteExecutionsActivitiesCount <= 0 {
		params.ConcurrentDeleteExecutionsActivitiesCount = defaults.ConcurrentDeleteExecutionsActivitiesCount
	}
	if params.ConcurrentDeleteExecutionsActivitiesCount > defaults.MaxConcurrentDeleteExecutionsActivitiesCount {
		params.ConcurrentDeleteExecutionsActivitiesCount = defaults.MaxConcurrentDeleteExecutionsActivitiesCount
	}
	return nil
}

func DeleteExecutionsWorkflow(ctx workflow.Context, params DeleteExecutionsParams) (DeleteExecutionsResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Child workflow started.", tag.WorkflowType(WorkflowName))
	result := DeleteExecutionsResult{
		SuccessCount: params.PreviousSuccessCount,
		ErrorCount:   params.PreviousErrorCount,
	}

	if err := validateParams(&params); err != nil {
		return result, err
	}

	// Minimum value is heartbeatEveryExecutions/params.DeleteRPS. 2 is "make sure" multiplicator.
	deleteWorkflowExecutionsActivityOptions.HeartbeatTimeout = time.Duration(heartbeatEveryExecutions/params.DeleteRPS*2) * time.Second

	var a *Activities
	var nextPageToken []byte
	runningDeleteExecutionsActivityCount := 0
	runningDeleteExecutionsSelector := workflow.NewSelector(ctx)
	var lastDeleteExecutionsActivityErr error

	for i := 0; i < params.ConcurrentDeleteExecutionsActivitiesCount; i++ {
		ctx1 := workflow.WithActivityOptions(ctx, deleteWorkflowExecutionsActivityOptions)
		deleteExecutionsFuture := workflow.ExecuteActivity(ctx1, a.DeleteExecutionsActivity, &DeleteExecutionsActivityParams{
			Namespace:     params.Namespace,
			NamespaceID:   params.NamespaceID,
			DeleteRPS:     params.DeleteRPS,
			ListPageSize:  params.ListPageSize,
			NextPageToken: nextPageToken,
		})
		runningDeleteExecutionsActivityCount++
		runningDeleteExecutionsSelector.AddFuture(deleteExecutionsFuture, func(f workflow.Future) {
			runningDeleteExecutionsActivityCount--
			var der DeleteExecutionsActivityResult
			deErr := f.Get(ctx, &der)
			if deErr != nil {
				lastDeleteExecutionsActivityErr = deErr
				return
			}
			result.SuccessCount += der.SuccessCount
			result.ErrorCount += der.ErrorCount
		})

		if runningDeleteExecutionsActivityCount >= params.ConcurrentDeleteExecutionsActivitiesCount {
			// Wait for one of running activities to complete.
			runningDeleteExecutionsSelector.Select(ctx)
			if lastDeleteExecutionsActivityErr != nil {
				return result, fmt.Errorf("%w: DeleteExecutionsActivity: %v", errors.ErrUnableToExecuteActivity, lastDeleteExecutionsActivityErr)
			}
		}

		ctx2 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
		err := workflow.ExecuteLocalActivity(ctx2, a.GetNextPageTokenActivity, GetNextPageTokenParams{
			NamespaceID:   params.NamespaceID,
			Namespace:     params.Namespace,
			PageSize:      params.ListPageSize,
			NextPageToken: nextPageToken,
		}).Get(ctx, &nextPageToken)
		if err != nil {
			return result, fmt.Errorf("%w: GetNextPageTokenActivity: %v", errors.ErrUnableToExecuteActivity, err)
		}
		if nextPageToken == nil {
			break
		}
	}

	// Wait for all running activities to complete.
	for runningDeleteExecutionsActivityCount > 0 {
		runningDeleteExecutionsSelector.Select(ctx)
		if lastDeleteExecutionsActivityErr != nil {
			return result, fmt.Errorf("%w: DeleteExecutionsActivity: %v", errors.ErrUnableToExecuteActivity, lastDeleteExecutionsActivityErr)
		}
	}

	if nextPageToken == nil {
		logger.Info("Finish deleting workflow executions.", tag.WorkflowNamespace(params.Namespace.String()), tag.DeletedExecutionsCount(result.SuccessCount), tag.DeletedExecutionsErrorCount(result.ErrorCount))
		return result, nil
	}

	params.PreviousSuccessCount = result.SuccessCount
	params.PreviousErrorCount = result.ErrorCount

	logger.Info("There are more workflows to delete. Continuing workflow as new.", tag.WorkflowType(WorkflowName), tag.WorkflowNamespace(params.Namespace.String()), tag.DeletedExecutionsCount(result.SuccessCount), tag.DeletedExecutionsErrorCount(result.ErrorCount))
	// Too many workflow executions, and ConcurrentDeleteExecutionsActivitiesCount activities has been started already.
	// Continue as new to prevent workflow history size explosion.
	return result, workflow.NewContinueAsNewError(ctx, DeleteExecutionsWorkflow, params)
}
