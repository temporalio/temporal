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

	"go.temporal.io/api/serviceerror"
	"golang.org/x/sync/errgroup"

	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	historyclient "go.temporal.io/server/client/history"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

func StreamReplicationTasks(
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	sourceClusterShardID historyclient.ClusterShardID,
	targetClusterShardID historyclient.ClusterShardID,
) error {
	errGroup, ctx := errgroup.WithContext(server.Context())
	errGroup.Go(func() error {
		return recvLoop(ctx, server, shardContext, sourceClusterShardID)
	})
	errGroup.Go(func() error {
		return sendLoop(ctx, server, shardContext, sourceClusterShardID, targetClusterShardID)
	})
	return errGroup.Wait()
}

func recvLoop(
	ctx context.Context,
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	sourceClusterShardID historyclient.ClusterShardID,
) error {
	for ctx.Err() == nil {
		req, err := server.Recv()
		if err != nil {
			return err
		}
		switch attr := req.GetAttributes().(type) {
		case *historyservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState:
			if err := recvSyncReplicationState(
				shardContext,
				attr.SyncReplicationState,
				sourceClusterShardID,
			); err != nil {
				shardContext.GetLogger().Error(
					"StreamWorkflowReplication unable to handle SyncReplicationState",
					tag.Error(err),
					tag.ShardID(shardContext.GetShardID()),
				)
				return err
			}
		default:
			return serviceerror.NewInternal(fmt.Sprintf(
				"StreamReplicationMessages encountered unknown type: %T %v", attr, attr,
			))
		}
	}
	return ctx.Err()
}

func recvSyncReplicationState(
	shardContext shard.Context,
	attr *replicationspb.SyncReplicationState,
	sourceClusterShardID historyclient.ClusterShardID,
) error {
	lastProcessedMessageID := attr.GetLastProcessedMessageId()
	lastProcessedMessageIDTime := attr.GetLastProcessedMessageTime()
	if lastProcessedMessageID != persistence.EmptyQueueMessageID {
		if err := shardContext.UpdateQueueClusterAckLevel(
			tasks.CategoryReplication,
			sourceClusterShardID.ClusterName,
			tasks.NewImmediateKey(lastProcessedMessageID),
		); err != nil {
			shardContext.GetLogger().Error(
				"error updating replication level for shard",
				tag.Error(err),
				tag.OperationFailed,
			)
		}
		shardContext.UpdateRemoteClusterInfo(
			sourceClusterShardID.ClusterName,
			lastProcessedMessageID,
			*lastProcessedMessageIDTime,
		)
	}
	return nil
}

func sendLoop(
	ctx context.Context,
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	sourceClusterShardID historyclient.ClusterShardID,
	targetClusterShardID historyclient.ClusterShardID,
) error {
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	newTaskNotificationChan, subscriberID := engine.SubscribeReplicationNotification()
	defer engine.UnsubscribeReplicationNotification(subscriberID)

	catchupEndExclusiveWatermark, err := sendCatchUp(
		ctx,
		server,
		shardContext,
		sourceClusterShardID,
		targetClusterShardID,
	)
	if err != nil {
		shardContext.GetLogger().Error(
			"StreamWorkflowReplication unable to catch up replication tasks",
			tag.Error(err),
			tag.ShardID(shardContext.GetShardID()),
		)
		return err
	}
	if err := sendLive(
		ctx,
		server,
		shardContext,
		sourceClusterShardID,
		targetClusterShardID,
		newTaskNotificationChan,
		catchupEndExclusiveWatermark,
	); err != nil {
		shardContext.GetLogger().Error(
			"StreamWorkflowReplication unable to stream replication tasks",
			tag.Error(err),
			tag.ShardID(shardContext.GetShardID()),
		)
		return err
	}
	shardContext.GetLogger().Info("StreamWorkflowReplication finish", tag.ShardID(shardContext.GetShardID()))
	return nil
}

func sendCatchUp(
	ctx context.Context,
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	sourceClusterShardID historyclient.ClusterShardID,
	targetClusterShardID historyclient.ClusterShardID,
) (int64, error) {
	catchupBeginInclusiveWatermark := shardContext.GetQueueClusterAckLevel(
		tasks.CategoryReplication,
		sourceClusterShardID.ClusterName,
	)
	catchupEndExclusiveWatermark := shardContext.GetImmediateQueueExclusiveHighReadWatermark()
	if err := sendTasks(
		ctx,
		server,
		shardContext,
		sourceClusterShardID,
		targetClusterShardID,
		catchupBeginInclusiveWatermark.TaskID,
		catchupEndExclusiveWatermark.TaskID,
	); err != nil {
		return 0, err
	}
	return catchupEndExclusiveWatermark.TaskID, nil
}

func sendLive(
	ctx context.Context,
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	sourceClusterShardID historyclient.ClusterShardID,
	targetClusterShardID historyclient.ClusterShardID,
	newTaskNotificationChan <-chan struct{},
	beginInclusiveWatermark int64,
) error {
	for {
		select {
		case <-newTaskNotificationChan:
			endExclusiveWatermark := shardContext.GetImmediateQueueExclusiveHighReadWatermark().TaskID
			if err := sendTasks(
				ctx,
				server,
				shardContext,
				sourceClusterShardID,
				targetClusterShardID,
				beginInclusiveWatermark,
				endExclusiveWatermark,
			); err != nil {
				return err
			}
			beginInclusiveWatermark = endExclusiveWatermark
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func sendTasks(
	ctx context.Context,
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	sourceClusterShardID historyclient.ClusterShardID,
	targetClusterShardID historyclient.ClusterShardID,
	beginInclusiveWatermark int64,
	endExclusiveWatermark int64,
) error {
	if beginInclusiveWatermark >= endExclusiveWatermark {
		return nil
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	iter, err := engine.GetReplicationTasksIter(
		ctx,
		sourceClusterShardID.ClusterName,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	if err != nil {
		return err
	}
Loop:
	for iter.HasNext() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		item, err := iter.Next()
		if err != nil {
			return err
		}
		task, err := engine.ConvertReplicationTask(ctx, item)
		if err != nil {
			return err
		}
		if task == nil {
			continue Loop
		}
		if err := server.Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks: []*replicationspb.ReplicationTask{task},
					LastTaskId:       task.SourceTaskId,
					LastTaskTime:     task.VisibilityTime,
				},
			},
		}); err != nil {
			return err
		}
	}
	return server.Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks: nil,
				LastTaskId:       endExclusiveWatermark - 1,
				LastTaskTime:     timestamp.TimeNowPtrUtc(),
			},
		},
	})
}
