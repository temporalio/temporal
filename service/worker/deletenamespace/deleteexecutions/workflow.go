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
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/worker/deletenamespace/defaults"
)

type (
	// WorkflowParams is the parameters for add search attributes workflow.
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
	DeleteExecutionsWorkflowOptions = workflow.ChildWorkflowOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 5 * time.Second,
		},
		WorkflowRunTimeout:       60 * time.Minute,
		WorkflowExecutionTimeout: 6 * time.Hour,
	}

	ErrUnableToExecuteActivity = errors.New("unable to execute activity")
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
	logger.Info("Child workflow started.", tag.WorkflowType("DeleteExecutionsWorkflow"))
	result := DeleteExecutionsResult{
		SuccessCount: params.PreviousSuccessCount,
		ErrorCount:   params.PreviousErrorCount,
	}

	err := validateParams(&params)
	if err != nil {
		return result, err
	}

	var a *Activities

	var nextPageToken []byte
	runningDeleteExecutionsActivityCount := 0
	runningDeleteExecutionsSelector := workflow.NewSelector(ctx)
	var lastDeleteExecutionsActivityErr error

	for i := 0; i < params.ConcurrentDeleteExecutionsActivitiesCount; i++ {
		ctx1 := workflow.WithLocalActivityOptions(ctx, getNextPageTokenActivityOptions)
		var nptr GetNextPageTokenResult
		err := workflow.ExecuteLocalActivity(ctx1, a.GetNextPageTokenActivity, GetNextPageTokenParams{
			NamespaceID:   params.NamespaceID,
			Namespace:     params.Namespace,
			PageSize:      params.ListPageSize,
			NextPageToken: nextPageToken,
		}).Get(ctx, &nptr)
		if err != nil {
			return result, fmt.Errorf("%w: GetNextPageTokenActivity: %v", ErrUnableToExecuteActivity, err)
		}

		ctx2 := workflow.WithActivityOptions(ctx, deleteWorkflowExecutionsActivityOptions)
		deleteExecutionsFuture := workflow.ExecuteActivity(ctx2, a.DeleteExecutionsActivity, &DeleteExecutionsActivityParams{
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
			err := f.Get(ctx, &der)
			if err != nil {
				lastDeleteExecutionsActivityErr = err
				return
			}
			result.SuccessCount += der.SuccessCount
			result.ErrorCount += der.ErrorCount
		})

		if runningDeleteExecutionsActivityCount >= params.ConcurrentDeleteExecutionsActivitiesCount {
			// Wait for one of running activities to complete.
			runningDeleteExecutionsSelector.Select(ctx)
			if lastDeleteExecutionsActivityErr != nil {
				return result, fmt.Errorf("%w: DeleteExecutionsActivity: %v", ErrUnableToExecuteActivity, lastDeleteExecutionsActivityErr)
			}
		}

		nextPageToken = nptr.NextPageToken
		if nextPageToken == nil {
			break
		}
	}

	// Wait for all running activities to complete.
	for runningDeleteExecutionsActivityCount > 0 {
		runningDeleteExecutionsSelector.Select(ctx)
		if lastDeleteExecutionsActivityErr != nil {
			return result, fmt.Errorf("%w: DeleteExecutionsActivity: %v", ErrUnableToExecuteActivity, lastDeleteExecutionsActivityErr)
		}
	}

	if nextPageToken == nil {
		logger.Info("Finish deleting workflow executions.", tag.WorkflowNamespace(params.Namespace.String()), tag.NewInt("deleted-executions-count", result.SuccessCount), tag.NewInt("delete-executions-error-count", result.ErrorCount))
		return result, nil
	}

	params.PreviousSuccessCount = result.SuccessCount
	params.PreviousErrorCount = result.ErrorCount

	logger.Info("There are more workflows to delete. Continuing DeleteExecutionsWorkflow as new.", tag.WorkflowNamespace(params.Namespace.String()), tag.NewInt("deleted-executions-count", result.SuccessCount), tag.NewInt("delete-executions-error-count", result.ErrorCount))
	// too many pages, and we exceed PageCountPerExecution, so move on to next execution
	return result, workflow.NewContinueAsNewError(ctx, DeleteExecutionsWorkflow, params)
}
