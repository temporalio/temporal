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
	"fmt"
	"math"
	"sort"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
)

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

	sort.SliceStable(resp.Shards, func(i, j int) bool {
		return resp.Shards[i].ShardId < resp.Shards[j].ShardId
	})

	for _, shard := range resp.Shards {
		clusterInfo, hasClusterInfo := shard.RemoteClusters[waitRequest.RemoteCluster]
		if hasClusterInfo {
			// WE are all caught up
			if shard.MaxReplicationTaskId == clusterInfo.AckedTaskId {
				readyShardCount++
				continue
			}

			// Caught up to the last checked IDs, and within allowed lagging range
			if clusterInfo.AckedTaskId >= waitRequest.WaitForTaskIds[shard.ShardId] &&
				(shard.MaxReplicationTaskId-clusterInfo.AckedTaskId <= waitRequest.AllowedLaggingTasks ||
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
				tag.NewStringTag("RemoteCluster", waitRequest.RemoteCluster),
				tag.NewInt64("AckedTaskId", clusterInfo.AckedTaskId),
				tag.NewInt64("WaitForTaskId", waitRequest.WaitForTaskIds[shard.ShardId]),
				tag.NewDurationTag("AllowedLagging", waitRequest.AllowedLagging),
				tag.NewDurationTag("ActualLagging", shard.ShardLocalTime.Sub(*clusterInfo.AckedTaskVisibilityTime)),
				tag.NewInt64("MaxReplicationTaskId", shard.MaxReplicationTaskId),
				tag.NewTimeTag("ShardLocalTime", *shard.ShardLocalTime),
				tag.NewTimeTag("AckedTaskVisibilityTime", *clusterInfo.AckedTaskVisibilityTime),
				tag.NewInt64("AllowedLaggingTasks", waitRequest.AllowedLaggingTasks),
				tag.NewInt64("ActualLaggingTasks", shard.MaxReplicationTaskId-clusterInfo.AckedTaskId),
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
					tag.NewInt64("MaxReplicationTaskId", shard.MaxReplicationTaskId),
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
	a.logger.Info("Wait handover ready shard count.",
		tag.NewInt("ReadyShards", readyShardCount),
		tag.NewStringTag("Namespace", waitRequest.Namespace),
		tag.NewStringTag("RemoteCluster", waitRequest.RemoteCluster))

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

	err := backoff.ThrottleRetryContext(ctx, op, historyServiceRetryPolicy, common.IsServiceTransientError)
	if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
		// ignore NotFound error
		return nil
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

func (a *activities) ListWorkflows(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*listWorkflowsResponse, error) {
	resp, err := a.frontendClient.ListWorkflowExecutions(ctx, request)
	if err != nil {
		return nil, err
	}
	var lastCloseTime, lastStartTime time.Time

	executions := make([]commonpb.WorkflowExecution, len(resp.Executions))
	for i, e := range resp.Executions {
		executions[i] = *e.Execution

		if e.CloseTime != nil {
			lastCloseTime = *e.CloseTime
		}

		if e.StartTime != nil {
			lastStartTime = *e.StartTime
		}
	}
	return &listWorkflowsResponse{Executions: executions, NextPageToken: resp.NextPageToken, LastCloseTime: lastCloseTime, LastStartTime: lastStartTime}, nil
}

func (a *activities) GenerateReplicationTasks(ctx context.Context, request *generateReplicationTasksRequest) error {
	rateLimiter := quotas.NewRateLimiter(request.RPS, int(math.Ceil(request.RPS)))

	startIndex := 0
	if activity.HasHeartbeatDetails(ctx) {
		var finishedIndex int
		if err := activity.GetHeartbeatDetails(ctx, &finishedIndex); err == nil {
			startIndex = finishedIndex + 1 // start from next one
		}
	}

	for i := startIndex; i < len(request.Executions); i++ {
		if err := rateLimiter.Wait(ctx); err != nil {
			return err
		}
		we := request.Executions[i]
		err := a.generateWorkflowReplicationTask(ctx, definition.NewWorkflowKey(request.NamespaceID, we.WorkflowId, we.RunId))
		if err != nil {
			return err
		}
		activity.RecordHeartbeat(ctx, i)
	}

	return nil
}
