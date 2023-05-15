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

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination stream_mock.go

package replication

import (
	"context"
	"fmt"
	"math"

	"go.temporal.io/api/serviceerror"
	"golang.org/x/sync/errgroup"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	historyclient "go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	TaskConvertorImpl struct {
		Ctx                     context.Context
		Engine                  shard.Engine
		NamespaceCache          namespace.Registry
		ClientClusterShardCount int32
		ClientClusterName       string
		ClientClusterShardID    historyclient.ClusterShardID
	}
	TaskConvertor interface {
		Convert(task tasks.Task) (*replicationspb.ReplicationTask, error)
	}
)

func StreamReplicationTasks(
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	clientClusterShardID historyclient.ClusterShardID,
	serverClusterShardID historyclient.ClusterShardID,
) error {
	allClusterInfo := shardContext.GetClusterMetadata().GetAllClusterInfo()
	clientClusterName, clientShardCount, err := clusterIDToClusterNameShardCount(allClusterInfo, clientClusterShardID.ClusterID)
	if err != nil {
		return err
	}
	_, serverShardCount, err := clusterIDToClusterNameShardCount(allClusterInfo, int32(shardContext.GetClusterMetadata().GetClusterID()))
	if err != nil {
		return err
	}
	err = common.VerifyShardIDMapping(clientShardCount, serverShardCount, clientClusterShardID.ShardID, serverClusterShardID.ShardID)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(server.Context())
	if err != nil {
		return err
	}
	filter := &TaskConvertorImpl{
		Ctx:                     server.Context(),
		Engine:                  engine,
		NamespaceCache:          shardContext.GetNamespaceRegistry(),
		ClientClusterShardCount: clientShardCount,
		ClientClusterName:       clientClusterName,
		ClientClusterShardID:    clientClusterShardID,
	}

	errGroup, ctx := errgroup.WithContext(server.Context())
	errGroup.Go(func() error {
		return recvLoop(ctx, server, shardContext, clientClusterShardID, serverClusterShardID)
	})
	errGroup.Go(func() error {
		return sendLoop(ctx, server, shardContext, filter, clientClusterShardID, serverClusterShardID)
	})
	return errGroup.Wait()
}

func recvLoop(
	ctx context.Context,
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	clientClusterShardID historyclient.ClusterShardID,
	serverClusterShardID historyclient.ClusterShardID,
) error {
	for ctx.Err() == nil {
		req, err := server.Recv()
		if err != nil {
			return err
		}
		shardContext.GetLogger().Debug(fmt.Sprintf(
			"cluster shard ID %v/%v <- cluster shard ID %v/%v",
			serverClusterShardID.ClusterID, serverClusterShardID.ShardID,
			clientClusterShardID.ClusterID, clientClusterShardID.ShardID,
		))
		switch attr := req.GetAttributes().(type) {
		case *historyservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState:
			if err := recvSyncReplicationState(
				shardContext,
				attr.SyncReplicationState,
				clientClusterShardID,
			); err != nil {
				shardContext.GetLogger().Error(
					"StreamWorkflowReplication unable to handle SyncReplicationState",
					tag.Error(err),
					tag.ShardID(shardContext.GetShardID()),
				)
				return err
			}
			shardContext.GetMetricsHandler().Counter(metrics.ReplicationTasksRecv.GetMetricName()).Record(
				int64(1),
				metrics.FromClusterIDTag(clientClusterShardID.ClusterID),
				metrics.ToClusterIDTag(serverClusterShardID.ClusterID),
				metrics.OperationTag(metrics.SyncWatermarkScope),
			)
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
	clientClusterShardID historyclient.ClusterShardID,
) error {
	lastProcessedMessageID := attr.GetLastProcessedMessageId()
	lastProcessedMessageIDTime := attr.GetLastProcessedMessageTime()
	if lastProcessedMessageID == persistence.EmptyQueueMessageID {
		return nil
	}

	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(clientClusterShardID.ClusterID),
		clientClusterShardID.ShardID,
	)
	readerState := &persistencespb.QueueReaderState{
		Scopes: []*persistencespb.QueueSliceScope{{
			Range: &persistencespb.QueueSliceRange{
				InclusiveMin: shard.ConvertToPersistenceTaskKey(
					tasks.NewImmediateKey(lastProcessedMessageID + 1),
				),
				ExclusiveMax: shard.ConvertToPersistenceTaskKey(
					tasks.NewImmediateKey(math.MaxInt64),
				),
			},
			Predicate: &persistencespb.Predicate{
				PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
				Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
			},
		}},
	}
	if err := shardContext.UpdateReplicationQueueReaderState(
		readerID,
		readerState,
	); err != nil {
		return err
	}
	shardContext.UpdateRemoteClusterInfo(
		string(clientClusterShardID.ClusterID),
		lastProcessedMessageID,
		*lastProcessedMessageIDTime,
	)
	return nil
}

func sendLoop(
	ctx context.Context,
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	taskConvertor TaskConvertor,
	clientClusterShardID historyclient.ClusterShardID,
	serverClusterShardID historyclient.ClusterShardID,
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
		taskConvertor,
		clientClusterShardID,
		serverClusterShardID,
	)
	if err != nil {
		shardContext.GetLogger().Error(
			"StreamWorkflowReplication unable to catch up replication tasks",
			tag.Error(err),
		)
		return err
	}
	if err := sendLive(
		ctx,
		server,
		shardContext,
		taskConvertor,
		clientClusterShardID,
		serverClusterShardID,
		newTaskNotificationChan,
		catchupEndExclusiveWatermark,
	); err != nil {
		shardContext.GetLogger().Error(
			"StreamWorkflowReplication unable to stream replication tasks",
			tag.Error(err),
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
	taskConvertor TaskConvertor,
	clientClusterShardID historyclient.ClusterShardID,
	serverClusterShardID historyclient.ClusterShardID,
) (int64, error) {

	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(clientClusterShardID.ClusterID),
		clientClusterShardID.ShardID,
	)

	var catchupBeginInclusiveWatermark int64
	queueState, ok := shardContext.GetQueueState(
		tasks.CategoryReplication,
	)
	if !ok {
		catchupBeginInclusiveWatermark = 0
	} else {
		readerState, ok := queueState.ReaderStates[readerID]
		if !ok {
			catchupBeginInclusiveWatermark = 0
		} else {
			catchupBeginInclusiveWatermark = readerState.Scopes[0].Range.InclusiveMin.TaskId
		}
	}
	catchupEndExclusiveWatermark := shardContext.GetImmediateQueueExclusiveHighReadWatermark().TaskID
	if err := sendTasks(
		ctx,
		server,
		shardContext,
		taskConvertor,
		clientClusterShardID,
		serverClusterShardID,
		catchupBeginInclusiveWatermark,
		catchupEndExclusiveWatermark,
	); err != nil {
		return 0, err
	}
	return catchupEndExclusiveWatermark, nil
}

func sendLive(
	ctx context.Context,
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	taskConvertor TaskConvertor,
	clientClusterShardID historyclient.ClusterShardID,
	serverClusterShardID historyclient.ClusterShardID,
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
				taskConvertor,
				clientClusterShardID,
				serverClusterShardID,
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
	taskConvertor TaskConvertor,
	clientClusterShardID historyclient.ClusterShardID,
	serverClusterShardID historyclient.ClusterShardID,
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
		string(clientClusterShardID.ClusterID),
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
		task, err := taskConvertor.Convert(item)
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
		shardContext.GetMetricsHandler().Counter(metrics.ReplicationTasksSend.GetMetricName()).Record(
			int64(1),
			metrics.FromClusterIDTag(serverClusterShardID.ClusterID),
			metrics.ToClusterIDTag(clientClusterShardID.ClusterID),
			metrics.OperationTag(replication.TaskOperationTag(task)),
		)
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

func (f *TaskConvertorImpl) Convert(
	task tasks.Task,
) (*replicationspb.ReplicationTask, error) {
	if namespaceEntry, err := f.NamespaceCache.GetNamespaceByID(
		namespace.ID(task.GetNamespaceID()),
	); err == nil {
		shouldProcessTask := false
	FilterLoop:
		for _, targetCluster := range namespaceEntry.ClusterNames() {
			if f.ClientClusterName == targetCluster {
				shouldProcessTask = true
				break FilterLoop
			}
		}
		if !shouldProcessTask {
			return nil, nil
		}
	}
	// if there is error, then blindly send the task, better safe than sorry

	sourceShardID := common.WorkflowIDToHistoryShard(task.GetNamespaceID(), task.GetWorkflowID(), f.ClientClusterShardCount)
	if sourceShardID != f.ClientClusterShardID.ShardID {
		return nil, nil
	}

	replicationTask, err := f.Engine.ConvertReplicationTask(f.Ctx, task)
	if err != nil {
		return nil, err
	}
	return replicationTask, nil
}

func clusterIDToClusterNameShardCount(
	allClusterInfo map[string]cluster.ClusterInformation,
	clusterID int32,
) (string, int32, error) {
	for clusterName, clusterInfo := range allClusterInfo {
		if int32(clusterInfo.InitialFailoverVersion) == clusterID {
			return clusterName, clusterInfo.ShardCount, nil
		}
	}
	return "", 0, serviceerror.NewInternal(fmt.Sprintf("unknown cluster ID: %v", clusterID))
}
