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
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
	"go.temporal.io/server/service/worker/deletenamespace/errors"
	"go.temporal.io/server/service/worker/deletenamespace/reclaimresources"
)

const (
	WorkflowName = "temporal-sys-delete-namespace-workflow"
)

type (
	DeleteNamespaceWorkflowParams struct {
		// One of NamespaceID or Namespace must be provided.
		NamespaceID namespace.ID
		Namespace   namespace.Name

		DeleteExecutionsConfig deleteexecutions.DeleteExecutionsConfig
	}

	DeleteNamespaceWorkflowResult struct {
		DeletedNamespaceID namespace.ID
		DeletedNamespace   namespace.Name
	}
)

var (
	localRetryPolicy = &temporal.RetryPolicy{
		InitialInterval: 1 * time.Second,
		MaximumInterval: 10 * time.Second,
	}

	reclaimResourcesWorkflowRetryPolicy = &temporal.RetryPolicy{
		InitialInterval: 60 * time.Second,
		// ReclaimResourcesWorkflow will try to delete workflow executions (call `DeleteWorkflowExecution` and wait for all executions to be deleted) 3 times.
		// If there are still executions left, ReclaimResourcesWorkflow fails, and needs to be restarted manually (this indicates some serious problems with transfer/visibility task processing).
		MaximumAttempts: 3,
	}

	localActivityOptions = workflow.LocalActivityOptions{
		RetryPolicy:            localRetryPolicy,
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToCloseTimeout: 5 * time.Minute,
	}

	reclaimResourcesWorkflowOptions = workflow.ChildWorkflowOptions{
		RetryPolicy: reclaimResourcesWorkflowRetryPolicy,
		// Important: this is required to make sure the child workflow is not terminated when delete namespace workflow is completed.
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	}
)

func validateParams(params *DeleteNamespaceWorkflowParams) error {
	if params.Namespace.IsEmpty() && params.NamespaceID.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("namespace or namespace ID is required", "", nil)
	}

	if !params.Namespace.IsEmpty() && !params.NamespaceID.IsEmpty() {
		return temporal.NewNonRetryableApplicationError("only one of namespace or namespace ID must be set", "", nil)
	}

	params.DeleteExecutionsConfig.ApplyDefaults()

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

	// Step 1. Get namespace info.
	ctx1 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	var namespaceInfo getNamespaceInfoResult
	err := workflow.ExecuteLocalActivity(ctx1, a.GetNamespaceInfoActivity, params.NamespaceID, params.Namespace).Get(ctx, &namespaceInfo)
	if err != nil {
		return result, temporal.NewNonRetryableApplicationError(fmt.Sprintf("namespace %s is not found", params.Namespace), "", err)
	}
	params.Namespace = namespaceInfo.Namespace
	params.NamespaceID = namespaceInfo.NamespaceID

	// Step 2. Mark namespace as deleted.
	ctx2 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	err = workflow.ExecuteLocalActivity(ctx2, a.MarkNamespaceDeletedActivity, params.Namespace).Get(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("%w: MarkNamespaceDeletedActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	result.DeletedNamespaceID = params.NamespaceID

	// Step 3. Rename namespace.
	ctx3 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	err = workflow.ExecuteLocalActivity(ctx3, a.GenerateDeletedNamespaceNameActivity, params.NamespaceID, params.Namespace).Get(ctx, &result.DeletedNamespace)
	if err != nil {
		return result, fmt.Errorf("%w: GenerateDeletedNamespaceNameActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	ctx31 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	err = workflow.ExecuteLocalActivity(ctx31, a.RenameNamespaceActivity, params.Namespace, result.DeletedNamespace).Get(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("%w: RenameNamespaceActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	// Step 4. Reclaim workflow resources asynchronously.
	ctx4 := workflow.WithChildOptions(ctx, reclaimResourcesWorkflowOptions)
	ctx4 = workflow.WithWorkflowID(ctx4, fmt.Sprintf("%s/%s", reclaimresources.WorkflowName, result.DeletedNamespace))
	reclaimResourcesFuture := workflow.ExecuteChildWorkflow(ctx4, reclaimresources.ReclaimResourcesWorkflow, reclaimresources.ReclaimResourcesParams{
		DeleteExecutionsParams: deleteexecutions.DeleteExecutionsParams{
			Namespace:   result.DeletedNamespace,
			NamespaceID: params.NamespaceID,
			Config:      params.DeleteExecutionsConfig,
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
