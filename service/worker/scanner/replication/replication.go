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

package replication

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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

const (
	forceReplicationWorkflowName  = "force-replication"
	namespaceHandoverWorkflowName = "namespace-handover"
	listExecutionPageSize         = 1000

	minimumAllowedLaggingSeconds  = 5
	minimumHandoverTimeoutSeconds = 30
)

type (
	ForceReplicationParams struct {
		Namespace               string
		SkipAfterTime           time.Time // skip workflows that are updated after this time
		ConcurrentActivityCount int32
		RemoteCluster           string // remote cluster name
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
	}

	genReplicationForShardRange struct {
		BeginShardID  int32     // inclusive
		EndShardID    int32     // inclusive
		NamespaceID   string    // only generate replication tasks for workflows in this namespace
		SkipAfterTime time.Time // skip workflows whose LastUpdateTime is after this time
	}

	genReplicationForShard struct {
		ShardID       int32
		NamespaceID   string
		SkipAfterTime time.Time
		PageToken     []byte
		Index         int
	}

	heartbeatProgress struct {
		ShardID   int32
		PageToken []byte
		Index     int
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
	persistenceRetryPolicy    = common.CreateHistoryServiceRetryPolicy()
)

func ForceReplicationWorkflow(ctx workflow.Context, params ForceReplicationParams) error {
	if len(params.Namespace) == 0 {
		return errors.New("InvalidArgument: Namespace is required")
	}
	if len(params.RemoteCluster) == 0 {
		return errors.New("InvalidArgument: RemoteCluster is required")
	}
	if params.ConcurrentActivityCount <= 0 {
		params.ConcurrentActivityCount = 1
	}

	retryPolicy := &temporal.RetryPolicy{
		InitialInterval: time.Second,
		MaximumInterval: time.Second * 10,
	}

	// ** Step 1, Get cluster metadata **
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	ctx1 := workflow.WithActivityOptions(ctx, ao)
	var a *activities
	var metadataResp metadataResponse
	metadataRequest := metadataRequest{Namespace: params.Namespace}
	err := workflow.ExecuteActivity(ctx1, a.GetMetadata, metadataRequest).Get(ctx1, &metadataResp)
	if err != nil {
		return err
	}

	// ** Step 2, Force replication **
	ao2 := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour * 10,
		HeartbeatTimeout:    time.Second * 30,
		RetryPolicy:         retryPolicy,
	}
	ctx2 := workflow.WithActivityOptions(ctx, ao2)

	concurrentCount := params.ConcurrentActivityCount
	shardCount := metadataResp.ShardCount
	skipAfter := params.SkipAfterTime
	if skipAfter.IsZero() {
		skipAfter = workflow.Now(ctx2)
	}
	var futures []workflow.Future
	batchSize := (shardCount + concurrentCount - 1) / concurrentCount
	for beginShardID := int32(1); beginShardID <= shardCount; beginShardID += batchSize {
		endShardID := beginShardID + batchSize - 1
		if endShardID > shardCount {
			endShardID = shardCount
		}
		rangeRequest := genReplicationForShardRange{
			BeginShardID:  beginShardID,
			EndShardID:    endShardID,
			NamespaceID:   metadataResp.NamespaceID,
			SkipAfterTime: skipAfter,
		}
		future := workflow.ExecuteActivity(ctx2, a.GenerateReplicationTasks, rangeRequest)
		futures = append(futures, future)
	}

	for _, f := range futures {
		if err := f.Get(ctx2, nil); err != nil {
			return err
		}
	}

	return nil
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

// GenerateReplicationTasks generates replication task for last history event for each workflow.
func (a *activities) GenerateReplicationTasks(ctx context.Context, request genReplicationForShardRange) error {
	perShard := genReplicationForShard{
		ShardID:       request.BeginShardID,
		NamespaceID:   request.NamespaceID,
		SkipAfterTime: request.SkipAfterTime,
	}
	var progress heartbeatProgress
	if activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &progress); err == nil {
			perShard.ShardID = progress.ShardID
			perShard.PageToken = progress.PageToken
			perShard.Index = progress.Index
		}
	}
	for ; perShard.ShardID <= request.EndShardID; perShard.ShardID++ {
		if err := a.genReplicationTasks(ctx, perShard); err != nil {
			return err
		}
		// heartbeat progress only apply for first shard
		perShard.PageToken = nil
		perShard.Index = 0
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
	for _, shard := range resp.Shards {
		clusterInfo, ok := shard.RemoteClusters[waitRequest.RemoteCluster]
		if !ok {
			return false, fmt.Errorf("GetReplicationStatus response for shard %d does not contains remote cluster %s", shard.ShardId, waitRequest.RemoteCluster)
		}
		if clusterInfo.AckedTaskId == shard.MaxReplicationTaskId {
			continue // already caught up, continue to check next shard.
		}

		if clusterInfo.AckedTaskId < waitRequest.WaitForTaskIds[shard.ShardId] ||
			shard.ShardLocalTime.Sub(*clusterInfo.AckedTaskVisibilityTime) > waitRequest.AllowedLagging {
			a.logger.Info("Wait for remote ack",
				tag.NewInt32("ShardId", shard.ShardId),
				tag.NewInt64("AckedTaskId", clusterInfo.AckedTaskId),
				tag.NewInt64("WaitForTaskId", waitRequest.WaitForTaskIds[shard.ShardId]),
				tag.NewDurationTag("AllowedLagging", waitRequest.AllowedLagging),
				tag.NewDurationTag("ActualLagging", shard.ShardLocalTime.Sub(*clusterInfo.AckedTaskVisibilityTime)),
				tag.NewStringTag("RemoteCluster", waitRequest.RemoteCluster),
			)
			return false, nil
		}
	}

	return true, nil
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

	// check that every shard is ready to handover
	for _, shard := range resp.Shards {
		clusterInfo, ok := shard.RemoteClusters[waitRequest.RemoteCluster]
		if !ok {
			return false, fmt.Errorf("GetReplicationStatus response for shard %d does not contains remote cluster %s", shard.ShardId, waitRequest.RemoteCluster)
		}
		handoverInfo, ok := shard.HandoverNamespaces[waitRequest.Namespace]
		if !ok {
			return false, fmt.Errorf("namespace %s on shard %d is not in handover state", waitRequest.Namespace, shard.ShardId)
		}

		if clusterInfo.AckedTaskId == shard.MaxReplicationTaskId && clusterInfo.AckedTaskId >= handoverInfo.HandoverReplicationTaskId {
			continue // already caught up, continue to check next shard.
		}

		a.logger.Info("Wait for handover to be ready",
			tag.NewInt32("ShardId", shard.ShardId),
			tag.NewInt64("AckedTaskId", clusterInfo.AckedTaskId),
			tag.NewInt64("HandoverTaskId", handoverInfo.HandoverReplicationTaskId),
			tag.NewStringTag("Namespace", waitRequest.Namespace),
			tag.NewStringTag("RemoteCluster", waitRequest.RemoteCluster),
		)
		return false, nil
	}

	return true, nil
}

func (a *activities) genReplicationTasks(ctx context.Context, request genReplicationForShard) error {
	pageToken := request.PageToken
	startIndex := request.Index
	for {
		var listResult *persistence.ListConcreteExecutionsResponse
		op := func(ctx context.Context) error {
			var err error
			listResult, err = a.executionManager.ListConcreteExecutions(&persistence.ListConcreteExecutionsRequest{
				ShardID:   request.ShardID,
				PageSize:  listExecutionPageSize,
				PageToken: pageToken,
			})
			return err
		}

		err := backoff.RetryContext(ctx, op, persistenceRetryPolicy, common.IsPersistenceTransientError)
		if err != nil {
			return err
		}

		for i := startIndex; i < len(listResult.States); i++ {
			activity.RecordHeartbeat(ctx, heartbeatProgress{
				ShardID:   request.ShardID,
				PageToken: pageToken,
				Index:     i,
			})

			ms := listResult.States[i]
			if ms.ExecutionInfo.LastUpdateTime != nil && ms.ExecutionInfo.LastUpdateTime.After(request.SkipAfterTime) {
				// workflow was updated after SkipAfterTime, no need to generate replication task
				continue
			}
			if ms.ExecutionInfo.NamespaceId != request.NamespaceID {
				// skip if not target namespace
				continue
			}
			err := a.genReplicationTaskForOneWorkflow(ctx, definition.NewWorkflowKey(request.NamespaceID, ms.ExecutionInfo.WorkflowId, ms.ExecutionState.RunId))
			if err != nil {
				return err
			}
		}

		pageToken = listResult.PageToken
		startIndex = 0
		if pageToken == nil {
			break
		}
	}

	return nil
}

func (a *activities) genReplicationTaskForOneWorkflow(ctx context.Context, wKey definition.WorkflowKey) error {
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
		if _, ok := err.(*serviceerror.NotFound); ok {
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
