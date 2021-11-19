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
	"fmt"
	"time"

	apicommon "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
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
	forceReplicationWorkflowName = "force-replication"
	listExecutionPageSize        = 1000
)

type (
	ForceReplicationOptions struct {
		Namespace               string
		SkipAfterTime           time.Time // skip workflows that are updated after this time
		ConcurrentActivityCount int32
		RemoteCluster           string // remote cluster name
		AllowedLaggingSeconds   int    // If not zero, will wait until remote's acked time is within this gap.
	}

	activities struct {
		historyShardCount int32
		executionManager  persistence.ExecutionManager
		namespaceRegistry namespace.Registry
		historyClient     historyservice.HistoryServiceClient
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
)

var (
	historyServiceRetryPolicy = common.CreateHistoryServiceRetryPolicy()
	persistenceRetryPolicy    = common.CreateHistoryServiceRetryPolicy()
)

func GenReplicationWorkflow(ctx workflow.Context, options ForceReplicationOptions) error {
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
	metadataRequest := metadataRequest{Namespace: options.Namespace}
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

	concurrentCount := options.ConcurrentActivityCount
	if options.ConcurrentActivityCount <= 0 {
		concurrentCount = 1
	}

	shardCount := metadataResp.ShardCount
	skipAfter := options.SkipAfterTime
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

	if options.AllowedLaggingSeconds <= 0 {
		return nil // no need to wait
	}
	// ** Step 3, get replication status **
	ao3 := workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 30,
		RetryPolicy:         retryPolicy,
	}
	ctx3 := workflow.WithActivityOptions(ctx, ao3)
	var repStatus replicationStatus
	err = workflow.ExecuteActivity(ctx3, a.GetMaxReplicationTaskIDs).Get(ctx3, &repStatus)
	if err != nil {
		return err
	}

	// ** Step 4, wait remote replication ack to catch up **
	ao4 := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		HeartbeatTimeout:    time.Second * 10,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumInterval: time.Second,
		},
	}
	ctx4 := workflow.WithActivityOptions(ctx, ao4)
	waitRequest := waitReplicationRequest{
		ShardCount:     metadataResp.ShardCount,
		RemoteCluster:  options.RemoteCluster,
		AllowedLagging: time.Duration(options.AllowedLaggingSeconds) * time.Second,
		WaitForTaskIds: repStatus.MaxReplicationTaskIds,
	}
	err = workflow.ExecuteActivity(ctx4, a.WaitReplication, waitRequest).Get(ctx4, nil)
	if err != nil {
		return err
	}

	return nil
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
			return nil
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
			Execution: &apicommon.WorkflowExecution{
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
