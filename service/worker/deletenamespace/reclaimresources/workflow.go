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
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
)

const (
	// WorkflowName is the workflow name.
	WorkflowName = "temporal-sys-reclaim-namespace-resources-workflow"

	checkMaxAttempts = 10
)

type (
	// ReclaimResourcesParams is the parameters for add search attributes workflow.
	ReclaimResourcesParams struct {
		deleteexecutions.DeleteExecutionsParams
	}

	ReclaimResourcesResult struct {
		SuccessCount int
		ErrorCount   int
	}
)

var (
	checkExecutionsExistActivityOptions = workflow.LocalActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
		},
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Minute,
	}

	deleteNamespaceActivityOptions = workflow.LocalActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
		},
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Minute,
	}

	deleteExecutionsWorkflowOptions = workflow.ChildWorkflowOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 5 * time.Second,
		},
		WorkflowRunTimeout:       60 * time.Minute,
		WorkflowExecutionTimeout: 6 * time.Hour,
	}

	ErrUnableToExecuteActivity      = errors.New("unable to execute activity")
	ErrUnableToExecuteChildWorkflow = errors.New("unable to execute child workflow")
)

func validateParams(params *ReclaimResourcesParams) error {
	if params.NamespaceID.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("namespace ID is empty", "", nil)
	}

	if params.Namespace.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("namespace is empty", "", nil)
	}

	return nil
}

func ReclaimResourcesWorkflow(ctx workflow.Context, params ReclaimResourcesParams) (ReclaimResourcesResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Child workflow started.", tag.WorkflowType(WorkflowName))

	err := validateParams(&params)
	if err != nil {
		return ReclaimResourcesResult{}, err
	}

	var a *Activities

	// Step 1. Delete workflow executions.
	result, err := deleteWorkflowExecutions(ctx, params, a, logger)
	if err != nil {
		return result, err
	}

	// Step 2. Delete namespace.
	ctx5 := workflow.WithLocalActivityOptions(ctx, deleteNamespaceActivityOptions)
	err = workflow.ExecuteLocalActivity(ctx5, a.DeleteNamespaceActivity, params.Namespace, params.NamespaceID).Get(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("%w: DeleteNamespaceActivity: %v", ErrUnableToExecuteActivity, err)
	}

	logger.Info("Child workflow finished successfully.", tag.WorkflowType(WorkflowName))
	return result, nil
}

func deleteWorkflowExecutions(ctx workflow.Context, params ReclaimResourcesParams, a *Activities, logger log.Logger) (ReclaimResourcesResult, error) {
	deleteAttempt := int32(1)
	executionsExist := true
	var result ReclaimResourcesResult
	for executionsExist {
		ctx1 := workflow.WithChildOptions(ctx, deleteExecutionsWorkflowOptions)
		var der deleteexecutions.DeleteExecutionsResult
		err := workflow.ExecuteChildWorkflow(ctx1, deleteexecutions.DeleteExecutionsWorkflow, params.DeleteExecutionsParams).Get(ctx, &der)

		if err != nil {
			return result, fmt.Errorf("%w: DeleteExecutionsWorkflow: %v", ErrUnableToExecuteChildWorkflow, err)
		}
		result.SuccessCount += der.SuccessCount
		result.ErrorCount += der.ErrorCount

		for checkAttempt := int32(1); checkAttempt <= checkMaxAttempts; checkAttempt++ {
			err = workflow.Sleep(ctx, visibilityDelay(der.SuccessCount))
			if err != nil {
				return result, err
			}

			ctx2 := workflow.WithLocalActivityOptions(ctx, checkExecutionsExistActivityOptions)
			err = workflow.ExecuteLocalActivity(ctx2, a.CheckExecutionsExistActivity, params.NamespaceID, params.Namespace).Get(ctx, &executionsExist)
			if err != nil {
				return result, fmt.Errorf("%w: CheckExecutionsExistActivity: %v", ErrUnableToExecuteActivity, err)
			}

			if !executionsExist {
				break
			}
			logger.Info("Workflow executions are still not deleted.", tag.WorkflowNamespace(params.Namespace.String()), tag.Attempt(checkAttempt))
		}
		if executionsExist {
			logger.Info("Unable to delete workflow executions. Will try again.", tag.WorkflowNamespace(params.Namespace.String()), tag.Counter(der.ErrorCount), tag.Attempt(deleteAttempt))
			deleteAttempt++
		}
	}

	logger.Info("All workflow executions has been deleted successfully.", tag.WorkflowNamespace(params.Namespace.String()), tag.NewInt("deleted-executions-count", result.SuccessCount), tag.NewInt("delete-executions-error-count", result.ErrorCount))
	return result, nil
}

// visibilityDelay returns approximate delay for workflow to sleep and wait for internal tasks to be processed.
func visibilityDelay(successCount int) time.Duration {
	const delayPerWorkflowExecution = 10 * time.Millisecond
	vd := time.Duration(successCount) * delayPerWorkflowExecution
	if vd < 1*time.Second {
		vd = 1 * time.Second
	}
	if vd > 10*time.Second {
		vd = 10 * time.Second
	}
	return vd
}
