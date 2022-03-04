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

package deletenamespace

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/worker/deletenamespace/defaults"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
)

const (
	// WorkflowName is the workflow name.
	WorkflowName = "temporal-sys-delete-namespace-workflow"

	deleteMaxAttempts = 3
	checkMaxAttempts  = 5
)

type (
	// DeleteNamespaceWorkflowParams is the parameters for add search attributes workflow.
	DeleteNamespaceWorkflowParams struct {
		NamespaceID namespace.ID
		Namespace   namespace.Name

		DeleteRPS                                 int
		ListPageSize                              int32
		ConcurrentDeleteExecutionsActivitiesCount int
	}
)

var (
	getNamespaceIDActivityOptions = workflow.ActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
		},
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Minute,
	}

	markNamespaceAsDeletedActivityOptions = workflow.ActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
		},
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Minute,
	}

	checkExecutionsExistActivityOptions = workflow.ActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
		},
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Minute,
	}

	deleteNamespaceActivityOptions = workflow.ActivityOptions{
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
		},
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Minute,
	}

	ErrUnableToExecuteActivity = errors.New("unable to execute activity")
)

func validateParams(params *DeleteNamespaceWorkflowParams) error {
	if params.Namespace.IsEmpty() && params.NamespaceID.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("both namespace name and namespace ID are empty", "", nil)
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

func DeleteNamespaceWorkflow(ctx workflow.Context, params DeleteNamespaceWorkflowParams) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow started.", tag.WorkflowType(WorkflowName))

	err := validateParams(&params)
	if err != nil {
		return err
	}

	var a *activities

	// Step 1. Get namespaceID.
	if params.NamespaceID.IsEmpty() {
		ctx0 := workflow.WithActivityOptions(ctx, getNamespaceIDActivityOptions)
		err = workflow.ExecuteActivity(ctx0, a.GetNamespaceIDActivity, params.Namespace).Get(ctx, &params.NamespaceID)
		if err != nil || params.NamespaceID.IsEmpty() {
			return temporal.NewNonRetryableApplicationError(fmt.Sprintf("namespace %s is not found", params.Namespace), "", err)
		}
	}

	// Step 2. Mark namespace as deleted.
	ctx1 := workflow.WithActivityOptions(ctx, markNamespaceAsDeletedActivityOptions)
	err = workflow.ExecuteActivity(ctx1, a.MarkNamespaceDeletedActivity, params.Namespace).Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: MarkNamespaceDeletedActivity: %v", ErrUnableToExecuteActivity, err)
	}

	// Step 3. Wait for namespace cache to be updated.
	const namespaceCacheRefreshDelay = 11 * time.Second
	err = workflow.Sleep(ctx, namespaceCacheRefreshDelay)
	if err != nil {
		return err
	}

	// Step 4. Delete workflow executions.
	err = deleteWorkflowExecutions(ctx, params, a, logger)
	if err != nil {
		return err
	}

	// Step 5. Delete namespace.
	ctx2 := workflow.WithActivityOptions(ctx, deleteNamespaceActivityOptions)
	err = workflow.ExecuteActivity(ctx2, a.DeleteNamespaceActivity, params.Namespace, params.NamespaceID).Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: DeleteNamespaceActivity: %v", ErrUnableToExecuteActivity, err)
	}

	logger.Info("Workflow finished successfully.", tag.WorkflowType(WorkflowName))
	return nil
}

func deleteWorkflowExecutions(ctx workflow.Context, params DeleteNamespaceWorkflowParams, a *activities, logger log.Logger) error {
	totalDeletedCount := 0
	var executionsExist bool
	for deleteAttempt := int32(1); deleteAttempt <= deleteMaxAttempts; deleteAttempt++ {
		ctx1 := workflow.WithChildOptions(ctx, deleteexecutions.DeleteExecutionsWorkflowOptions)
		var der deleteexecutions.DeleteExecutionsResult
		err := workflow.ExecuteChildWorkflow(ctx1, deleteexecutions.DeleteExecutionsWorkflow, deleteexecutions.DeleteExecutionsParams{
			Namespace:    params.Namespace,
			NamespaceID:  params.NamespaceID,
			DeleteRPS:    params.DeleteRPS,
			ListPageSize: params.ListPageSize,
			ConcurrentDeleteExecutionsActivitiesCount: params.ConcurrentDeleteExecutionsActivitiesCount,
		}).Get(ctx, &der)

		if err != nil {
			return fmt.Errorf("%w: DeleteWorkflowExecutionsActivity: %v", ErrUnableToExecuteActivity, err)
		}
		totalDeletedCount += der.SuccessCount

		for checkAttempt := int32(1); checkAttempt <= checkMaxAttempts; checkAttempt++ {
			err = workflow.Sleep(ctx, visibilityDelay(der.SuccessCount))
			if err != nil {
				return err
			}

			ctx2 := workflow.WithActivityOptions(ctx, checkExecutionsExistActivityOptions)
			err = workflow.ExecuteActivity(ctx2, a.CheckExecutionsExistActivity, params.Namespace).Get(ctx, &executionsExist)
			if err != nil {
				return fmt.Errorf("%w: CountWorkflowActivity: %v", ErrUnableToExecuteActivity, err)
			}

			if !executionsExist {
				break
			}
			logger.Info("Workflow executions are still not deleted.", tag.WorkflowNamespace(params.Namespace.String()), tag.Attempt(checkAttempt))
		}
		if !executionsExist {
			break
		}
		logger.Info("Unable to delete workflow executions.", tag.WorkflowNamespace(params.Namespace.String()), tag.Counter(der.ErrorCount), tag.Attempt(deleteAttempt))
	}

	if executionsExist {
		logger.Info("Workflow executions are left in deleted namespace after all attempts.", tag.WorkflowNamespace(params.Namespace.String()), tag.Attempt(deleteMaxAttempts))
		return nil
	}

	logger.Info("All workflow executions has been deleted successfully.", tag.WorkflowNamespace(params.Namespace.String()), tag.NewInt("deleted-executions-count", totalDeletedCount))
	return nil
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
