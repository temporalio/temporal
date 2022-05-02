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
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
)

const (
	forceReplicationWorkflowName  = "force-replication"
	namespaceHandoverWorkflowName = "namespace-handover"

	defaultListWorkflowsPageSize = 1000
	defaultPageCountPerExecution = 200
	maxPageCountPerExecution     = 1000

	minimumAllowedLaggingSeconds  = 5
	minimumHandoverTimeoutSeconds = 30
)

type (
	ForceReplicationParams struct {
		Namespace               string
		Query                   string // query to list workflows for replication
		ConcurrentActivityCount int
		RpsPerActivity          int    // RPS per each activity
		ListWorkflowsPageSize   int    // PageSize of ListWorkflow, will paginate through results.
		PageCountPerExecution   int    // number of pages to be processed before continue as new, max is 1000.
		NextPageToken           []byte // used by continue as new
	}

	NamespaceHandoverParams struct {
		Namespace     string
		RemoteCluster string

		// how far behind on replication is allowed for remote cluster before handover is initiated
		AllowedLaggingSeconds int
		// how long to wait for handover to complete before rollback
		HandoverTimeoutSeconds int
	}

	activities struct {
		historyShardCount int32
		executionManager  persistence.ExecutionManager
		namespaceRegistry namespace.Registry
		historyClient     historyservice.HistoryServiceClient
		frontendClient    workflowservice.WorkflowServiceClient
		logger            log.Logger
		metricsClient     metrics.Client
	}

	listWorkflowsResponse struct {
		Executions    []commonpb.WorkflowExecution
		NextPageToken []byte
	}

	generateReplicationTasksRequest struct {
		NamespaceID string
		Executions  []commonpb.WorkflowExecution
		RPS         int
	}

	metadataRequest struct {
		Namespace string
	}

	metadataResponse struct {
		ShardCount  int32
		NamespaceID string
	}

	replicationStatus struct {
		MaxReplicationTaskIds map[int32]int64 // max replication task id for each shard.
	}

	waitReplicationRequest struct {
		ShardCount     int32
		RemoteCluster  string          // remote cluster name
		WaitForTaskIds map[int32]int64 // remote acked replication task needs to pass this id
		AllowedLagging time.Duration   // allowed remote acked lagging
	}

	updateStateRequest struct {
		Namespace string // move this namespace into Handover state
		NewState  enumspb.ReplicationState
	}

	updateActiveClusterRequest struct {
		Namespace     string // move this namespace into Handover state
		ActiveCluster string
	}

	waitHandoverRequest struct {
		ShardCount    int32
		Namespace     string
		RemoteCluster string // remote cluster name
	}
)

var (
	historyServiceRetryPolicy = common.CreateHistoryServiceRetryPolicy()
)

func ForceReplicationWorkflow(ctx workflow.Context, params ForceReplicationParams) error {
	if len(params.Namespace) == 0 {
		return errors.New("InvalidArgument: Namespace is required")
	}
	if params.ConcurrentActivityCount <= 0 {
		params.ConcurrentActivityCount = 1
	}
	if params.RpsPerActivity <= 0 {
		params.RpsPerActivity = 1
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

	retryPolicy := &temporal.RetryPolicy{
		InitialInterval: time.Second,
		MaximumInterval: time.Second * 10,
	}

	// Get cluster metadata, we need namespace ID for history API call.
	// TODO: remove this step.
	lao := workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	ctx1 := workflow.WithLocalActivityOptions(ctx, lao)
	var a *activities
	var metadataResp metadataResponse
	metadataRequest := metadataRequest{Namespace: params.Namespace}
	err := workflow.ExecuteLocalActivity(ctx1, a.GetMetadata, metadataRequest).Get(ctx1, &metadataResp)
	if err != nil {
		return err
	}

	selector := workflow.NewSelector(ctx)
	pendingActivities := 0

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 30,
		RetryPolicy:         retryPolicy,
	}
	ctx2 := workflow.WithActivityOptions(ctx, ao)

	for i := 0; i < params.PageCountPerExecution; i++ {
		listFuture := workflow.ExecuteLocalActivity(ctx1, a.ListWorkflows, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     params.Namespace,
			PageSize:      int32(params.ListWorkflowsPageSize),
			NextPageToken: params.NextPageToken,
			Query:         params.Query,
		})
		var listResp listWorkflowsResponse
		err = listFuture.Get(ctx1, &listResp)
		if err != nil {
			return err
		}

		workerFuture := workflow.ExecuteActivity(ctx2, a.GenerateReplicationTasks, &generateReplicationTasksRequest{
			NamespaceID: metadataResp.NamespaceID,
			Executions:  listResp.Executions,
			RPS:         params.RpsPerActivity,
		})
		pendingActivities++
		selector.AddFuture(workerFuture, func(f workflow.Future) {
			pendingActivities--
		})

		if pendingActivities >= params.ConcurrentActivityCount {
			selector.Select(ctx) // this will block until one of the pending activities complete
		}

		params.NextPageToken = listResp.NextPageToken
		if params.NextPageToken == nil {
			break
		}
	}
	// wait until all pending activities are done
	for pendingActivities > 0 {
		selector.Select(ctx)
	}

	if params.NextPageToken == nil {
		// we are all done
		return nil
	}

	// too many pages, and we exceed PageCountPerExecution, so move on to next execution
	return workflow.NewContinueAsNewError(ctx, ForceReplicationWorkflow, params)
}

func NamespaceHandoverWorkflow(ctx workflow.Context, params NamespaceHandoverParams) error {
	// validate input params
	if len(params.Namespace) == 0 {
		return errors.New("InvalidArgument: Namespace is required")
	}
	if len(params.RemoteCluster) == 0 {
		return errors.New("InvalidArgument: RemoteCluster is required")
	}
	if params.AllowedLaggingSeconds <= minimumAllowedLaggingSeconds {
		params.AllowedLaggingSeconds = minimumAllowedLaggingSeconds
	}
	if params.HandoverTimeoutSeconds <= minimumHandoverTimeoutSeconds {
		params.HandoverTimeoutSeconds = minimumHandoverTimeoutSeconds
	}

	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:    time.Second,
		MaximumInterval:    time.Second,
		BackoffCoefficient: 1,
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	var a *activities

	// ** Step 1, Get cluster metadata **
	var metadataResp metadataResponse
	metadataRequest := metadataRequest{Namespace: params.Namespace}
	err := workflow.ExecuteActivity(ctx, a.GetMetadata, metadataRequest).Get(ctx, &metadataResp)
	if err != nil {
		return err
	}

	// ** Step 2, get current replication status **
	var repStatus replicationStatus
	err = workflow.ExecuteActivity(ctx, a.GetMaxReplicationTaskIDs).Get(ctx, &repStatus)
	if err != nil {
		return err
	}

	// ** Step 3, wait remote cluster to catch up on replication tasks
	ao3 := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	ctx3 := workflow.WithActivityOptions(ctx, ao3)
	waitRequest := waitReplicationRequest{
		ShardCount:     metadataResp.ShardCount,
		RemoteCluster:  params.RemoteCluster,
		AllowedLagging: time.Duration(params.AllowedLaggingSeconds) * time.Second,
		WaitForTaskIds: repStatus.MaxReplicationTaskIds,
	}
	err = workflow.ExecuteActivity(ctx3, a.WaitReplication, waitRequest).Get(ctx3, nil)
	if err != nil {
		return err
	}

	// ** Step 4, initiate handover
	handoverRequest := updateStateRequest{
		Namespace: params.Namespace,
		NewState:  enumspb.REPLICATION_STATE_HANDOVER,
	}
	err = workflow.ExecuteActivity(ctx, a.UpdateNamespaceState, handoverRequest).Get(ctx, nil)
	if err != nil {
		return err
	}

	// ** Step 5, wait remote to ack handover task id
	ao5 := workflow.ActivityOptions{
		StartToCloseTimeout:    time.Second * 30,
		HeartbeatTimeout:       time.Second * 10,
		ScheduleToCloseTimeout: time.Second * time.Duration(params.HandoverTimeoutSeconds),
		RetryPolicy:            retryPolicy,
	}
	ctx5 := workflow.WithActivityOptions(ctx, ao5)
	waitHandover := waitHandoverRequest{
		ShardCount:    metadataResp.ShardCount,
		Namespace:     params.Namespace,
		RemoteCluster: params.RemoteCluster,
	}
	err5 := workflow.ExecuteActivity(ctx5, a.WaitHandover, waitHandover).Get(ctx5, nil)
	if err5 == nil {
		// ** Step 6, remote cluster is ready to take over, update Namespace to use remote cluster as active
		updateRequest := updateActiveClusterRequest{
			Namespace:     params.Namespace,
			ActiveCluster: params.RemoteCluster,
		}
		err = workflow.ExecuteActivity(ctx, a.UpdateActiveCluster, updateRequest).Get(ctx, nil)
		if err != nil {
			return err
		}
	}

	// ** Step 7, reset namespace state from Handover -> Registered
	resetStateRequest := updateStateRequest{
		Namespace: params.Namespace,
		NewState:  enumspb.REPLICATION_STATE_NORMAL,
	}
	err = workflow.ExecuteActivity(ctx, a.UpdateNamespaceState, resetStateRequest).Get(ctx, nil)
	if err != nil {
		return err
	}

	return err5
}

// GetMetadata returns history shard count and namespaceID for requested namespace.
func (a *activities) GetMetadata(ctx context.Context, request metadataRequest) (*metadataResponse, error) {
	nsEntry, err := a.namespaceRegistry.GetNamespace(namespace.Name(request.Namespace))
	if err != nil {
		return nil, err
	}

	return &metadataResponse{
		ShardCount:  a.historyShardCount,
		NamespaceID: string(nsEntry.ID()),
	}, nil
}

func (a *activities) ListWorkflows(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
	resp, err := a.frontendClient.ListWorkflowExecutions(ctx, request)
	if err != nil {
		return nil, err
	}
	executions := make([]commonpb.WorkflowExecution, len(resp.Executions))
	for i, e := range resp.Executions {
		executions[i] = *e.Execution
	}
	return &listWorkflowsResponse{Executions: executions, NextPageToken: resp.NextPageToken}, nil
}

func (a *activities) GenerateReplicationTasks(ctx context.Context, request *generateReplicationTasksRequest) error {
	rateLimiter := quotas.NewRateLimiter(float64(request.RPS), request.RPS)

	startIndex := 0
	if activity.HasHeartbeatDetails(ctx) {
		var finishedIndex int
		if err := activity.GetHeartbeatDetails(ctx, &finishedIndex); err == nil {
			startIndex = finishedIndex + 1 // start from next one
		}
	}

	for i := startIndex; i < len(request.Executions); i++ {
		rateLimiter.Wait(ctx)
		we := request.Executions[i]
		err := a.generateWorkflowReplicationTask(ctx, definition.NewWorkflowKey(request.NamespaceID, we.WorkflowId, we.RunId))
		if err != nil {
			return err
		}
		activity.RecordHeartbeat(ctx, i)
	}

	return nil
}

// GetMaxReplicationTaskIDs returns max replication task id per shard
func (a *activities) GetMaxReplicationTaskIDs(ctx context.Context) (*replicationStatus, error) {
	resp, err := a.historyClient.GetReplicationStatus(ctx, &historyservice.GetReplicationStatusRequest{})
	if err != nil {
		return nil, err
	}
	result := &replicationStatus{MaxReplicationTaskIds: make(map[int32]int64)}
	for _, shard := range resp.Shards {
		result.MaxReplicationTaskIds[shard.ShardId] = shard.MaxReplicationTaskId
	}
	return result, nil
}

func (a *activities) WaitReplication(ctx context.Context, waitRequest waitReplicationRequest) error {
	for {
		done, err := a.checkReplicationOnce(ctx, waitRequest)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		// keep waiting and check again
		time.Sleep(time.Second)
		activity.RecordHeartbeat(ctx, nil)
	}
}

// Check if remote cluster has caught up on all shards on replication tasks
func (a *activities) checkReplicationOnce(ctx context.Context, waitRequest waitReplicationRequest) (bool, error) {

	resp, err := a.historyClient.GetReplicationStatus(ctx, &historyservice.GetReplicationStatusRequest{
		RemoteClusters: []string{waitRequest.RemoteCluster},
	})
	if err != nil {
		return false, err
	}
	if int(waitRequest.ShardCount) != len(resp.Shards) {
		return false, fmt.Errorf("GetReplicationStatus returns %d shards, expecting %d", len(resp.Shards), waitRequest.ShardCount)
	}

	// check that every shard has caught up
	readyShardCount := 0
	logged := false
	for _, shard := range resp.Shards {
		clusterInfo, hasClusterInfo := shard.RemoteClusters[waitRequest.RemoteCluster]
		if hasClusterInfo {
			if clusterInfo.AckedTaskId == shard.MaxReplicationTaskId ||
				(clusterInfo.AckedTaskId >= waitRequest.WaitForTaskIds[shard.ShardId] &&
					shard.ShardLocalTime.Sub(*clusterInfo.AckedTaskVisibilityTime) <= waitRequest.AllowedLagging) {
				readyShardCount++
				continue
			}
		}
		// shard is not ready, log first non-ready shard
		if !logged {
			logged = true
			if !hasClusterInfo {
				a.logger.Info("Wait catchup missing remote cluster info", tag.ShardID(shard.ShardId), tag.ClusterName(waitRequest.RemoteCluster))
				// this is not expected, so fail activity to surface the error, but retryPolicy will keep retrying.
				return false, fmt.Errorf("GetReplicationStatus response for shard %d does not contains remote cluster %s", shard.ShardId, waitRequest.RemoteCluster)
			}
			a.logger.Info("Wait catchup not ready",
				tag.NewInt32("ShardId", shard.ShardId),
				tag.NewInt64("AckedTaskId", clusterInfo.AckedTaskId),
				tag.NewInt64("WaitForTaskId", waitRequest.WaitForTaskIds[shard.ShardId]),
				tag.NewDurationTag("AllowedLagging", waitRequest.AllowedLagging),
				tag.NewDurationTag("ActualLagging", shard.ShardLocalTime.Sub(*clusterInfo.AckedTaskVisibilityTime)),
				tag.NewStringTag("RemoteCluster", waitRequest.RemoteCluster),
			)
		}
	}

	// emit metrics about how many shards are ready
	a.metricsClient.Scope(
		metrics.MigrationWorkflowScope,
		metrics.TargetClusterTag(waitRequest.RemoteCluster),
	).UpdateGauge(metrics.CatchUpReadyShardCountGauge, float64(readyShardCount))

	return readyShardCount == len(resp.Shards), nil
}

func (a *activities) WaitHandover(ctx context.Context, waitRequest waitHandoverRequest) error {
	for {
		done, err := a.checkHandoverOnce(ctx, waitRequest)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		// keep waiting and check again
		time.Sleep(time.Second)
		activity.RecordHeartbeat(ctx, nil)
	}
}

// Check if remote cluster has caught up on all shards on replication tasks
func (a *activities) checkHandoverOnce(ctx context.Context, waitRequest waitHandoverRequest) (bool, error) {

	resp, err := a.historyClient.GetReplicationStatus(ctx, &historyservice.GetReplicationStatusRequest{
		RemoteClusters: []string{waitRequest.RemoteCluster},
	})
	if err != nil {
		return false, err
	}
	if int(waitRequest.ShardCount) != len(resp.Shards) {
		return false, fmt.Errorf("GetReplicationStatus returns %d shards, expecting %d", len(resp.Shards), waitRequest.ShardCount)
	}

	readyShardCount := 0
	logged := false
	// check that every shard is ready to handover
	for _, shard := range resp.Shards {
		clusterInfo, hasClusterInfo := shard.RemoteClusters[waitRequest.RemoteCluster]
		handoverInfo, hasHandoverInfo := shard.HandoverNamespaces[waitRequest.Namespace]
		if hasClusterInfo && hasHandoverInfo {
			if clusterInfo.AckedTaskId == shard.MaxReplicationTaskId || clusterInfo.AckedTaskId >= handoverInfo.HandoverReplicationTaskId {
				readyShardCount++
				continue
			}
		}
		// shard is not ready, log first non-ready shard
		if !logged {
			logged = true
			if !hasClusterInfo {
				a.logger.Info("Wait handover missing remote cluster info", tag.ShardID(shard.ShardId), tag.ClusterName(waitRequest.RemoteCluster))
				// this is not expected, so fail activity to surface the error, but retryPolicy will keep retrying.
				return false, fmt.Errorf("GetReplicationStatus response for shard %d does not contains remote cluster %s", shard.ShardId, waitRequest.RemoteCluster)
			}

			if !hasHandoverInfo {
				// this could happen before namespace cache refresh
				a.logger.Info("Wait handover missing handover namespace info", tag.ShardID(shard.ShardId), tag.ClusterName(waitRequest.RemoteCluster), tag.WorkflowNamespace(waitRequest.Namespace))
			} else {
				a.logger.Info("Wait handover not ready",
					tag.NewInt32("ShardId", shard.ShardId),
					tag.NewInt64("AckedTaskId", clusterInfo.AckedTaskId),
					tag.NewInt64("HandoverTaskId", handoverInfo.HandoverReplicationTaskId),
					tag.NewStringTag("Namespace", waitRequest.Namespace),
					tag.NewStringTag("RemoteCluster", waitRequest.RemoteCluster),
				)
			}
		}
	}

	// emit metrics about how many shards are ready
	a.metricsClient.Scope(
		metrics.MigrationWorkflowScope,
		metrics.TargetClusterTag(waitRequest.RemoteCluster),
		metrics.NamespaceTag(waitRequest.Namespace),
	).UpdateGauge(metrics.HandoverReadyShardCountGauge, float64(readyShardCount))

	return readyShardCount == len(resp.Shards), nil
}

func (a *activities) generateWorkflowReplicationTask(ctx context.Context, wKey definition.WorkflowKey) error {
	// will generate replication task
	op := func(ctx context.Context) error {
		var err error
		ctx1, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()
		_, err = a.historyClient.GenerateLastHistoryReplicationTasks(ctx1, &historyservice.GenerateLastHistoryReplicationTasksRequest{
			NamespaceId: wKey.NamespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: wKey.WorkflowID,
				RunId:      wKey.RunID,
			},
		})
		return err
	}

	err := backoff.RetryContext(ctx, op, historyServiceRetryPolicy, common.IsServiceTransientError)
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			// ignore NotFound error
			return nil
		}
	}

	return err
}

func (a *activities) UpdateNamespaceState(ctx context.Context, req updateStateRequest) error {
	descResp, err := a.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: req.Namespace,
	})
	if err != nil {
		return err
	}
	if descResp.ReplicationConfig.State == req.NewState {
		return nil
	}

	_, err = a.frontendClient.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: req.Namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			State: req.NewState,
		},
	})

	return err
}

func (a *activities) UpdateActiveCluster(ctx context.Context, req updateActiveClusterRequest) error {
	descResp, err := a.frontendClient.DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: req.Namespace,
	})
	if err != nil {
		return err
	}
	if descResp.ReplicationConfig.GetActiveClusterName() == req.ActiveCluster {
		return nil
	}

	_, err = a.frontendClient.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: req.Namespace,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: req.ActiveCluster,
		},
	})

	return err
}
