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
	"go.temporal.io/server/service/worker/deletenamespace/errors"
)

const (
	WorkflowName = "temporal-sys-delete-executions-workflow"
)

type (
	DeleteExecutionsParams struct {
		Namespace   namespace.Name
		NamespaceID namespace.ID
		Config      DeleteExecutionsConfig

		// To carry over progress results with ContinueAsNew.
		PreviousSuccessCount int
		PreviousErrorCount   int
		ContinueAsNewCount   int
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
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 5 * time.Minute,
	}

	deleteWorkflowExecutionsActivityOptions = workflow.ActivityOptions{
		RetryPolicy:         retryPolicy,
		StartToCloseTimeout: 60 * time.Minute,
		HeartbeatTimeout:    10 * time.Second,
	}
)

func validateParams(params *DeleteExecutionsParams) error {
	if params.NamespaceID.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("namespace ID is required", "", nil)
	}

	if params.Namespace.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("namespace is required", "", nil)
	}

	params.Config.ApplyDefaults()

	return nil
}

func DeleteExecutionsWorkflow(ctx workflow.Context, params DeleteExecutionsParams) (DeleteExecutionsResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow started.", tag.WorkflowType(WorkflowName))
	result := DeleteExecutionsResult{
		SuccessCount: params.PreviousSuccessCount,
		ErrorCount:   params.PreviousErrorCount,
	}

	if err := validateParams(&params); err != nil {
		return result, err
	}
	logger.Info("Effective config.", tag.Value(params.Config.String()))

	var a *Activities
	var nextPageToken []byte
	runningDeleteExecutionsActivityCount := 0
	runningDeleteExecutionsSelector := workflow.NewSelector(ctx)
	var lastDeleteExecutionsActivityErr error

	// Two activities DeleteExecutionsActivity and GetNextPageTokenActivity are executed here essentially in reverse order
	// because Get is called immediately for GetNextPageTokenActivity but not for DeleteExecutionsActivity.
	// These activities scan visibility storage independently but GetNextPageTokenActivity considered to be quick and can be done synchronously.
	// It reads nextPageToken and pass it DeleteExecutionsActivity. This allocates block of workflow executions to delete for
	// DeleteExecutionsActivity which takes much longer to complete. This is why this workflow starts
	// ConcurrentDeleteExecutionsActivities number of them and executes them concurrently on available workers.
	for i := 0; i < params.Config.PagesPerExecutionCount; i++ {
		ctx1 := workflow.WithActivityOptions(ctx, deleteWorkflowExecutionsActivityOptions)
		deleteExecutionsFuture := workflow.ExecuteActivity(ctx1, a.DeleteExecutionsActivity, &DeleteExecutionsActivityParams{
			Namespace:     params.Namespace,
			NamespaceID:   params.NamespaceID,
			RPS:           params.Config.DeleteActivityRPS,
			ListPageSize:  params.Config.PageSize,
			NextPageToken: nextPageToken,
		})

		ctx2 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
		err := workflow.ExecuteLocalActivity(ctx2, a.GetNextPageTokenActivity, GetNextPageTokenParams{
			NamespaceID:   params.NamespaceID,
			Namespace:     params.Namespace,
			PageSize:      params.Config.PageSize,
			NextPageToken: nextPageToken,
		}).Get(ctx, &nextPageToken)
		if err != nil {
			return result, fmt.Errorf("%w: GetNextPageTokenActivity: %v", errors.ErrUnableToExecuteActivity, err)
		}

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

		if runningDeleteExecutionsActivityCount >= params.Config.ConcurrentDeleteExecutionsActivities {
			// Wait for one of running activities to complete.
			runningDeleteExecutionsSelector.Select(ctx)
			if lastDeleteExecutionsActivityErr != nil {
				return result, fmt.Errorf("%w: DeleteExecutionsActivity: %v", errors.ErrUnableToExecuteActivity, lastDeleteExecutionsActivityErr)
			}
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
		if result.ErrorCount == 0 {
			logger.Info("Successfully deleted workflow executions.", tag.WorkflowNamespace(params.Namespace.String()), tag.DeletedExecutionsCount(result.SuccessCount))
		} else {
			logger.Error("Finish deleting workflow executions with some errors.", tag.WorkflowNamespace(params.Namespace.String()), tag.DeletedExecutionsCount(result.SuccessCount), tag.DeletedExecutionsErrorCount(result.ErrorCount))
		}
		return result, nil
	}

	// Too many workflow executions, and ConcurrentDeleteExecutionsActivities activities has been started already.
	// Continue as new to prevent workflow history size explosion.

	params.PreviousSuccessCount = result.SuccessCount
	params.PreviousErrorCount = result.ErrorCount
	params.ContinueAsNewCount++

	logger.Info("There are more workflows to delete. Continuing workflow as new.", tag.WorkflowType(WorkflowName), tag.WorkflowNamespace(params.Namespace.String()), tag.DeletedExecutionsCount(result.SuccessCount), tag.DeletedExecutionsErrorCount(result.ErrorCount), tag.Counter(params.ContinueAsNewCount))
	return result, workflow.NewContinueAsNewError(ctx, DeleteExecutionsWorkflow, params)
}
