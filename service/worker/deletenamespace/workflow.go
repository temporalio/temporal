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
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/worker/deletenamespace/defaults"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
	"go.temporal.io/server/service/worker/deletenamespace/reclaimresources"
)

const (
	// WorkflowName is the workflow name.
	WorkflowName = "temporal-sys-delete-namespace-workflow"

	namespaceCacheRefreshDelay = 11 * time.Second
)

type (
	DeleteNamespaceWorkflowParams struct {
		NamespaceID namespace.ID
		Namespace   namespace.Name

		DeleteRPS                                 int
		ListPageSize                              int
		ConcurrentDeleteExecutionsActivitiesCount int
	}

	DeleteNamespaceWorkflowResult struct {
		DeletedID   namespace.ID
		DeletedName namespace.Name
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

	reclaimResourcesWorkflowOptions = workflow.ChildWorkflowOptions{
		RetryPolicy:              retryPolicy,
		WorkflowExecutionTimeout: 24 * time.Hour,
		// Important: this is required to make sure the child workflow is not terminated when delete namespace workflow is completed.
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	}
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

func DeleteNamespaceWorkflow(ctx workflow.Context, params DeleteNamespaceWorkflowParams) (DeleteNamespaceWorkflowResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Workflow started.", tag.WorkflowType(WorkflowName))

	var result DeleteNamespaceWorkflowResult

	if err := validateParams(&params); err != nil {
		return result, err
	}

	var a *activities

	// Step 1. Get namespaceID.
	if params.NamespaceID.IsEmpty() {
		ctx1 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
		err := workflow.ExecuteLocalActivity(ctx1, a.GetNamespaceIDActivity, params.Namespace).Get(ctx, &params.NamespaceID)
		if err != nil || params.NamespaceID.IsEmpty() {
			return result, temporal.NewNonRetryableApplicationError(fmt.Sprintf("namespace %s is not found", params.Namespace), "", err)
		}
	}
	result.DeletedID = params.NamespaceID

	// Step 2. Mark namespace as deleted.
	ctx2 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	err := workflow.ExecuteLocalActivity(ctx2, a.MarkNamespaceDeletedActivity, params.Namespace).Get(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("%w: MarkNamespaceDeletedActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	// Step 3. Rename namespace.
	ctx3 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	err = workflow.ExecuteLocalActivity(ctx3, a.GenerateDeletedNamespaceNameActivity, params.Namespace).Get(ctx, &result.DeletedName)
	if err != nil {
		return result, fmt.Errorf("%w: GenerateDeletedNamespaceNameActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	ctx31 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	err = workflow.ExecuteLocalActivity(ctx31, a.RenameNamespaceActivity, params.Namespace, result.DeletedName).Get(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("%w: RenameNamespaceActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	err = workflow.Sleep(ctx, namespaceCacheRefreshDelay)
	if err != nil {
		return result, err
	}

	// Step 4. Reclaim workflow resources asynchronously.
	ctx4 := workflow.WithChildOptions(ctx, reclaimResourcesWorkflowOptions)
	reclaimResourcesFuture := workflow.ExecuteChildWorkflow(ctx4, reclaimresources.ReclaimResourcesWorkflow, reclaimresources.ReclaimResourcesParams{
		DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
			Namespace:    result.DeletedName,
			NamespaceID:  params.NamespaceID,
			DeleteRPS:    params.DeleteRPS,
			ListPageSize: params.ListPageSize,
			ConcurrentDeleteExecutionsActivitiesCount: params.ConcurrentDeleteExecutionsActivitiesCount,
		}})
	var reclaimResourcesExecution workflow.Execution
	if err := reclaimResourcesFuture.GetChildWorkflowExecution().Get(ctx, &reclaimResourcesExecution); err != nil {
		logger.Error("Unable to execute child workflow.", tag.WorkflowType(reclaimresources.WorkflowName), tag.Error(err))
		return result, fmt.Errorf("%w: %s: %v", errors.ErrUnableToExecuteChildWorkflow, reclaimresources.WorkflowName, err)
	}
	logger.Info("Child workflow executed successfully.", tag.WorkflowType(reclaimresources.WorkflowName))

	logger.Info("Workflow finished successfully.", tag.WorkflowType(WorkflowName))
	return result, nil
}
