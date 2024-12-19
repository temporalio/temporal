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
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
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

		// NamespaceDeleteDelay indicates duration for how long namespace stays in database
		// after all namespace resources (i.e. workflow executions) are deleted.
		// Default is 0, means, namespace will be deleted immediately.
		NamespaceDeleteDelay time.Duration

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
		// If the client is calling DeleteNamespace API again while ReclaimResourcesWorkflow is running, it might want to terminate existing run and start a new run.
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
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

func validateNamespace(ctx workflow.Context, nsID namespace.ID, nsName namespace.Name, nsClusters []string) error {

	if nsName == primitives.SystemLocalNamespace || nsID == primitives.SystemNamespaceID {
		return temporal.NewNonRetryableApplicationError("unable to delete system namespace", errors.ValidationErrorErrType, nil, nil)
	}

	// Disable namespace deletion if namespace is replicate because:
	//  - If namespace is passive in the current cluster, then WF executions will keep coming from
	//    the active cluster and namespace will never be deleted (ReclaimResourcesWorkflow will fail).
	//  - If namespace is active in the current cluster, then it technically can be deleted (and
	//    in this case it will be deleted from this cluster only because delete operation is not replicated),
	//    but this is confusing for the users, as they might expect that namespace is deleted from all clusters.
	if len(nsClusters) > 1 {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("namespace %s is replicated in several clusters [%s]: remove all other cluster and retry", nsName, strings.Join(nsClusters, ",")), errors.ValidationErrorErrType, nil)
	}

	// NOTE: there is very little chance that another cluster is added after the check above,
	// but before namespace is marked as deleted below.

	var la *localActivities

	ctx1 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	var bns blockedNamespaces
	err := workflow.ExecuteLocalActivity(ctx1, la.GetBlockedNamespacesActivity).Get(ctx, &bns)
	if err != nil {
		return fmt.Errorf("%w: GetBlockedNamespacesActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}
	for _, blockedNsName := range bns.Names {
		if blockedNsName == nsName.String() {
			return temporal.NewNonRetryableApplicationError(fmt.Sprintf("namespace %s is blocked from deletion", nsName), errors.ValidationErrorErrType, nil, nil)
		}
	}

	return nil
}

func DeleteNamespaceWorkflow(ctx workflow.Context, params DeleteNamespaceWorkflowParams) (DeleteNamespaceWorkflowResult, error) {
	logger := log.With(
		workflow.GetLogger(ctx),
		tag.WorkflowType(WorkflowName),
		tag.WorkflowNamespace(params.Namespace.String()),
		tag.WorkflowNamespaceID(params.NamespaceID.String()))

	logger.Info("Workflow started.")

	var result DeleteNamespaceWorkflowResult

	if err := validateParams(&params); err != nil {
		return result, err
	}

	ctx = workflow.WithTaskQueue(ctx, primitives.DeleteNamespaceActivityTQ)

	var la *localActivities

	// Step 1. Get namespace info.
	ctx1 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	var namespaceInfo getNamespaceInfoResult
	err := workflow.ExecuteLocalActivity(ctx1, la.GetNamespaceInfoActivity, params.NamespaceID, params.Namespace).Get(ctx, &namespaceInfo)
	if err != nil {
		ns := params.Namespace.String()
		if ns == "" {
			ns = params.NamespaceID.String()
		}
		return result, temporal.NewNonRetryableApplicationError(fmt.Sprintf("namespace %s is not found", ns), errors.ValidationErrorErrType, err)
	}
	params.Namespace = namespaceInfo.Namespace
	params.NamespaceID = namespaceInfo.NamespaceID

	// Step 1.1. Validate namespace.
	if err := validateNamespace(ctx, params.NamespaceID, params.Namespace, namespaceInfo.Clusters); err != nil {
		return result, err
	}

	// Step 2. Mark namespace as deleted.
	ctx2 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	err = workflow.ExecuteLocalActivity(ctx2, la.MarkNamespaceDeletedActivity, params.Namespace).Get(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("%w: MarkNamespaceDeletedActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	result.DeletedNamespaceID = params.NamespaceID

	// Step 3. Rename namespace.
	ctx3 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	err = workflow.ExecuteLocalActivity(ctx3, la.GenerateDeletedNamespaceNameActivity, params.NamespaceID, params.Namespace).Get(ctx, &result.DeletedNamespace)
	if err != nil {
		return result, fmt.Errorf("%w: GenerateDeletedNamespaceNameActivity: %v", errors.ErrUnableToExecuteActivity, err)
	}

	ctx31 := workflow.WithLocalActivityOptions(ctx, localActivityOptions)
	err = workflow.ExecuteLocalActivity(ctx31, la.RenameNamespaceActivity, params.Namespace, result.DeletedNamespace).Get(ctx, nil)
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
		},
		NamespaceDeleteDelay: params.NamespaceDeleteDelay,
	})
	var reclaimResourcesExecution workflow.Execution
	if err := reclaimResourcesFuture.GetChildWorkflowExecution().Get(ctx, &reclaimResourcesExecution); err != nil {
		logger.Error("Unable to execute child workflow.", tag.Error(err))
		return result, fmt.Errorf("%w: %s: %v", errors.ErrUnableToExecuteChildWorkflow, reclaimresources.WorkflowName, err)
	}
	logger.Info("Child workflow executed successfully.", tag.NewStringTag("wf-child-type", reclaimresources.WorkflowName))

	logger.Info("Workflow finished successfully.")
	return result, nil
}
