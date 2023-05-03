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

package migration

import (
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type (
	TaskQueueUserDataReplicationParams struct {
		// PageSize for the SeedReplicationQueueWithUserDataEntries activity
		PageSize int
		// RPS for enqueuing of replication tasks
		RPS float64
	}

	// TaskQueueUserDataReplicationParamsWithNamespace is used for child workflow / activity input
	TaskQueueUserDataReplicationParamsWithNamespace struct {
		TaskQueueUserDataReplicationParams
		// Namespace name
		Namespace string
	}

	ForceReplicationParams struct {
		Namespace               string
		Query                   string // query to list workflows for replication
		ConcurrentActivityCount int
		OverallRps              float64 // RPS for enqueuing of replication tasks
		ListWorkflowsPageSize   int     // PageSize of ListWorkflow, will paginate through results.
		PageCountPerExecution   int     // number of pages to be processed before continue as new, max is 1000.
		NextPageToken           []byte  // used by continue as new

		// Used by query handler to indicate overall progress of replication
		LastCloseTime                      time.Time
		LastStartTime                      time.Time
		ContinuedAsNewCount                int
		TaskQueueUserDataReplicationParams TaskQueueUserDataReplicationParams

		// Carry over the replication status after continue-as-new.
		TaskQueueUserDataReplicationStatus TaskQueueUserDataReplicationStatus
	}

	TaskQueueUserDataReplicationStatus struct {
		Done           bool
		FailureMessage string
	}

	ForceReplicationStatus struct {
		LastCloseTime                      time.Time
		LastStartTime                      time.Time
		TaskQueueUserDataReplicationStatus TaskQueueUserDataReplicationStatus
		ContinuedAsNewCount                int
	}

	listWorkflowsResponse struct {
		Executions    []commonpb.WorkflowExecution
		NextPageToken []byte
		Error         error

		// These can be used to help report progress of the force-replication scan
		LastCloseTime time.Time
		LastStartTime time.Time
	}

	generateReplicationTasksRequest struct {
		NamespaceID string
		Executions  []commonpb.WorkflowExecution
		RPS         float64
	}

	metadataRequest struct {
		Namespace string
	}

	metadataResponse struct {
		ShardCount  int32
		NamespaceID string
	}
)

var (
	forceReplicationActivityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval: time.Second,
		MaximumInterval: time.Second * 10,
	}
)

const (
	forceReplicationStatusQueryType            = "force-replication-status"
	taskQueueUserDataReplicationDoneSignalType = "task-queue-user-data-replication-done"
	taskQueueUserDataReplicationVersionMarker  = "replicate-task-queue-user-data"
)

func ForceReplicationWorkflow(ctx workflow.Context, params ForceReplicationParams) error {
	workflow.SetQueryHandler(ctx, forceReplicationStatusQueryType, func() (ForceReplicationStatus, error) {
		return ForceReplicationStatus{
			LastCloseTime:                      params.LastCloseTime,
			LastStartTime:                      params.LastStartTime,
			ContinuedAsNewCount:                params.ContinuedAsNewCount,
			TaskQueueUserDataReplicationStatus: params.TaskQueueUserDataReplicationStatus,
		}, nil
	})

	if err := validateAndSetForceReplicationParams(&params); err != nil {
		return err
	}

	metadataResp, err := getClusterMetadata(ctx, params)
	if err != nil {
		return err
	}

	if !params.TaskQueueUserDataReplicationStatus.Done {
		err = maybeKickoffTaskQueueUserDataReplication(ctx, params, func(failureReason string) {
			params.TaskQueueUserDataReplicationStatus.FailureMessage = failureReason
			params.TaskQueueUserDataReplicationStatus.Done = true
		})
		if err != nil {
			return err
		}
	}

	workflowExecutionsCh := workflow.NewBufferedChannel(ctx, params.PageCountPerExecution)
	var listWorkflowsErr error
	workflow.Go(ctx, func(ctx workflow.Context) {
		listWorkflowsErr = listWorkflowsForReplication(ctx, workflowExecutionsCh, &params)

		// enqueueReplicationTasks only returns when workflowExecutionsCh is closed (or if it encounters an error).
		// Therefore, listWorkflowsErr will be set prior to their use and params will be updated.
		workflowExecutionsCh.Close()
	})

	if err := enqueueReplicationTasks(ctx, workflowExecutionsCh, metadataResp.NamespaceID, params); err != nil {
		return err
	}

	if listWorkflowsErr != nil {
		return listWorkflowsErr
	}

	if params.NextPageToken == nil {
		if workflow.GetVersion(ctx, taskQueueUserDataReplicationVersionMarker, workflow.DefaultVersion, 1) > workflow.DefaultVersion {
			err := workflow.Await(ctx, func() bool { return params.TaskQueueUserDataReplicationStatus.Done })
			if err != nil {
				return err
			}
			if params.TaskQueueUserDataReplicationStatus.FailureMessage != "" {
				return fmt.Errorf("task queue user data replication failed: %v", params.TaskQueueUserDataReplicationStatus.FailureMessage)
			}
		}
		return nil
	}

	params.ContinuedAsNewCount++

	// There are still more workflows to replicate. Continue-as-new to process on a new run.
	// This prevents history size from exceeding the server-defined limit
	return workflow.NewContinueAsNewError(ctx, ForceReplicationWorkflow, params)
}

func maybeKickoffTaskQueueUserDataReplication(ctx workflow.Context, params ForceReplicationParams, onDone func(failureReason string)) error {
	if workflow.GetVersion(ctx, taskQueueUserDataReplicationVersionMarker, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
		return nil
	}

	workflow.Go(ctx, func(ctx workflow.Context) {
		taskQueueUserDataReplicationDoneCh := workflow.GetSignalChannel(ctx, taskQueueUserDataReplicationDoneSignalType)
		var errStr string
		// We don't care if there's more data to receive
		_ = taskQueueUserDataReplicationDoneCh.Receive(ctx, &errStr)
		onDone(errStr)
	})

	// We only start the child workflow before we continue as new to avoid starting the child workflow more than once.
	if params.ContinuedAsNewCount > 0 {
		return nil
	}

	options := workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("%s-task-queue-user-data-replicator", workflow.GetInfo(ctx).WorkflowExecution.ID),
		// We're going to continue-as-new, and cannot wait for this child to complete, instead child will notify of
		// its completion via signal.
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	}
	childCtx := workflow.WithChildOptions(ctx, options)
	input := TaskQueueUserDataReplicationParamsWithNamespace{
		TaskQueueUserDataReplicationParams: params.TaskQueueUserDataReplicationParams,
		Namespace:                          params.Namespace,
	}

	child := workflow.ExecuteChildWorkflow(childCtx, ForceTaskQueueUserDataReplicationWorkflow, input)
	var childExecution workflow.Execution
	// Wait for the child workflow to be started.
	err := child.GetChildWorkflowExecution().Get(ctx, &childExecution)
	return err
}

func ForceTaskQueueUserDataReplicationWorkflow(ctx workflow.Context, params TaskQueueUserDataReplicationParamsWithNamespace) error {
	var a *activities
	ao := workflow.ActivityOptions{
		// This shouldn't take "too long", just set an arbitrary long timeout here and rely on heartbeats for liveness detection.
		StartToCloseTimeout: time.Hour * 24 * 7,
		HeartbeatTimeout:    time.Second * 30,
		// Give the system some time to recover before the next attempt.
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second * 10,
			MaximumInterval: time.Minute * 5,
		},
	}

	actx := workflow.WithActivityOptions(ctx, ao)

	err := workflow.ExecuteActivity(actx, a.SeedReplicationQueueWithUserDataEntries, params).Get(ctx, nil)
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	err = workflow.SignalExternalWorkflow(ctx, workflow.GetInfo(ctx).ParentWorkflowExecution.ID, "", taskQueueUserDataReplicationDoneSignalType, errStr).Get(ctx, nil)
	return err
}

func validateAndSetForceReplicationParams(params *ForceReplicationParams) error {
	if len(params.Namespace) == 0 {
		return errors.New("InvalidArgument: Namespace is required")
	}
	if params.ConcurrentActivityCount <= 0 {
		params.ConcurrentActivityCount = 1
	}
	if params.OverallRps <= 0 {
		params.OverallRps = float64(params.ConcurrentActivityCount)
	}
	if params.ListWorkflowsPageSize <= 0 {
		params.ListWorkflowsPageSize = defaultListWorkflowsPageSize
	}
	if params.PageCountPerExecution <= 0 {
		params.PageCountPerExecution = defaultPageCountPerExecution
	}
	if params.PageCountPerExecution > maxPageCountPerExecution {
		params.PageCountPerExecution = maxPageCountPerExecution
	}

	return nil
}

func getClusterMetadata(ctx workflow.Context, params ForceReplicationParams) (metadataResponse, error) {
	var a *activities

	// Get cluster metadata, we need namespace ID for history API call.
	// TODO: remove this step.
	lao := workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}

	actx := workflow.WithLocalActivityOptions(ctx, lao)
	var metadataResp metadataResponse
	metadataRequest := metadataRequest{Namespace: params.Namespace}
	err := workflow.ExecuteLocalActivity(actx, a.GetMetadata, metadataRequest).Get(ctx, &metadataResp)
	return metadataResp, err
}

func listWorkflowsForReplication(ctx workflow.Context, workflowExecutionsCh workflow.Channel, params *ForceReplicationParams) error {
	var a *activities

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 30,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}

	actx := workflow.WithActivityOptions(ctx, ao)

	for i := 0; i < params.PageCountPerExecution; i++ {
		listFuture := workflow.ExecuteActivity(actx, a.ListWorkflows, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     params.Namespace,
			PageSize:      int32(params.ListWorkflowsPageSize),
			NextPageToken: params.NextPageToken,
			Query:         params.Query,
		})

		var listResp listWorkflowsResponse
		if err := listFuture.Get(ctx, &listResp); err != nil {
			return err
		}

		workflowExecutionsCh.Send(ctx, listResp.Executions)

		params.NextPageToken = listResp.NextPageToken
		params.LastCloseTime = listResp.LastCloseTime
		params.LastStartTime = listResp.LastStartTime

		if params.NextPageToken == nil {
			break
		}
	}

	return nil
}

func enqueueReplicationTasks(ctx workflow.Context, workflowExecutionsCh workflow.Channel, namespaceID string, params ForceReplicationParams) error {
	selector := workflow.NewSelector(ctx)
	pendingActivities := 0

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 30,
		RetryPolicy:         forceReplicationActivityRetryPolicy,
	}

	actx := workflow.WithActivityOptions(ctx, ao)
	var a *activities
	var futures []workflow.Future
	var workflowExecutions []commonpb.WorkflowExecution

	for workflowExecutionsCh.Receive(ctx, &workflowExecutions) {
		replicationTaskFuture := workflow.ExecuteActivity(actx, a.GenerateReplicationTasks, &generateReplicationTasksRequest{
			NamespaceID: namespaceID,
			Executions:  workflowExecutions,
			RPS:         params.OverallRps / float64(params.ConcurrentActivityCount),
		})

		pendingActivities++
		selector.AddFuture(replicationTaskFuture, func(f workflow.Future) {
			pendingActivities--
		})

		if pendingActivities == params.ConcurrentActivityCount {
			selector.Select(ctx) // this will block until one of the in-flight activities completes
		}

		futures = append(futures, replicationTaskFuture)
	}

	for _, future := range futures {
		if err := future.Get(ctx, nil); err != nil {
			return err
		}
	}

	return nil
}
