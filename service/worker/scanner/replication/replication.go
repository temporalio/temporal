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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

const (
	genReplicationWorkflowName = "gen-replication"
	listExecutionPageSize      = 1000
)

type (
	GenReplicationOptions struct {
		Namespace               string
		SkipAfterTime           time.Time // skip workflows that are updated after this time
		ConcurrentActivityCount int32
	}

	activities struct {
		historyShardCount int32
		executionManager  persistence.ExecutionManager
		namespaceRegistry namespace.Registry
		historyClient     historyservice.HistoryServiceClient
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
)

var (
	historyServiceRetryPolicy = common.CreateHistoryServiceRetryPolicy()
	persistenceRetryPolicy    = common.CreateHistoryServiceRetryPolicy()
)

func GenReplicationWorkflow(ctx workflow.Context, options GenReplicationOptions) error {
	retryPolicy := &temporal.RetryPolicy{
		InitialInterval: time.Second,
		MaximumInterval: time.Second * 10,
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Second * 10,
		RetryPolicy:         retryPolicy,
	}
	ctx1 := workflow.WithActivityOptions(ctx, ao)

	var a *activities
	var metadataResp metadataResponse
	metadataRequest := metadataRequest{Namespace: options.Namespace}
	err := workflow.ExecuteActivity(ctx1, a.GetMetadata, metadataRequest).Get(ctx, &metadataResp)
	if err != nil {
		return err
	}

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
