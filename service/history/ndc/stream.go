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

package ndc

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"golang.org/x/sync/errgroup"

	"go.temporal.io/server/api/historyservice/v1"
	historyclient "go.temporal.io/server/client/history"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
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
		return sendLoop(ctx, server, shardContext, targetClusterShardID)
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
			lastProcessedMessageID := attr.SyncReplicationState.GetLastProcessedMessageId()
			lastProcessedMessageIDTime := attr.SyncReplicationState.GetLastProcessedMessageTime()
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
		default:
			return serviceerror.NewInternal(fmt.Sprintf(
				"StreamReplicationMessages encountered unknown type: %T %v", attr, attr,
			))
		}
	}
	return ctx.Err()
}

func sendLoop(
	ctx context.Context,
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	targetClusterShardID historyclient.ClusterShardID,
) error {
	for ctx.Err() == nil {
		// TODO push replication tasks to target
		panic(targetClusterShardID)
	}
	return ctx.Err()
}
