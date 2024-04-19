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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination stream_sender_mock.go

package replication

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"

	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	StreamSender interface {
		IsValid() bool
		Key() ClusterShardKeyPair
		Stop()
	}
	StreamSenderImpl struct {
		server        historyservice.HistoryService_StreamWorkflowReplicationMessagesServer
		shardContext  shard.Context
		historyEngine shard.Engine
		taskConverter SourceTaskConverter
		metrics       metrics.Handler
		logger        log.Logger

		status         int32
		clientShardKey ClusterShardKey
		serverShardKey ClusterShardKey
		shutdownChan   channel.ShutdownOnce
	}
)

func NewStreamSender(
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	historyEngine shard.Engine,
	taskConverter SourceTaskConverter,
	clientClusterName string,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) *StreamSenderImpl {
	logger := log.With(
		shardContext.GetLogger(),
		tag.TargetCluster(clientClusterName), // client is the target cluster (passive cluster)
		tag.TargetShardID(clientShardKey.ShardID),
	)
	return &StreamSenderImpl{
		server:        server,
		shardContext:  shardContext,
		historyEngine: historyEngine,
		taskConverter: taskConverter,
		metrics:       shardContext.GetMetricsHandler(),
		logger:        logger,

		status:         common.DaemonStatusInitialized,
		clientShardKey: clientShardKey,
		serverShardKey: serverShardKey,
		shutdownChan:   channel.NewShutdownOnce(),
	}
}

func (s *StreamSenderImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go WrapEventLoop(s.sendEventLoop, s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, streamReceiverMonitorInterval)
	go WrapEventLoop(s.recvEventLoop, s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, streamReceiverMonitorInterval)

	s.logger.Info("StreamSender started.")
}

func (s *StreamSenderImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&s.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	s.shutdownChan.Shutdown()
	s.logger.Info("StreamSender stopped.")
}

func (s *StreamSenderImpl) IsValid() bool {
	return atomic.LoadInt32(&s.status) == common.DaemonStatusStarted
}

func (s *StreamSenderImpl) Wait() {
	<-s.shutdownChan.Channel()
}

func (s *StreamSenderImpl) Key() ClusterShardKeyPair {
	return ClusterShardKeyPair{
		Client: s.clientShardKey,
		Server: s.serverShardKey,
	}
}

func (s *StreamSenderImpl) recvEventLoop() (retErr error) {
	var panicErr error
	defer func() {
		if panicErr != nil {
			retErr = panicErr
			metrics.ReplicationStreamPanic.With(s.metrics).Record(1)
		}
	}()

	defer log.CapturePanic(s.logger, &panicErr)

	for !s.shutdownChan.IsShutdown() {
		req, err := s.server.Recv()
		if err != nil {
			s.logger.Error("GrpcStreamError StreamSender failed to receive", tag.Error(err))
			return NewStreamError("GrpcStreamError recv error", err)
		}
		switch attr := req.GetAttributes().(type) {
		case *historyservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState:
			if err := s.recvSyncReplicationState(attr.SyncReplicationState); err != nil {
				s.logger.Error("ReplicationServiceError StreamSender unable to handle SyncReplicationState", tag.Error(err))
				return err
			}
			metrics.ReplicationTasksRecv.With(s.metrics).Record(
				int64(1),
				metrics.FromClusterIDTag(s.clientShardKey.ClusterID),
				metrics.ToClusterIDTag(s.serverShardKey.ClusterID),
				metrics.OperationTag(metrics.SyncWatermarkScope),
			)
		default:
			err := serviceerror.NewInternal(fmt.Sprintf(
				"StreamReplicationMessages encountered unknown type: %T %v", attr, attr,
			))
			s.logger.Error("ReplicationServiceError StreamSender unable to handle request", tag.Error(err))
			return err
		}
	}
	return nil
}

func (s *StreamSenderImpl) sendEventLoop() (retErr error) {
	var panicErr error
	defer func() {
		if panicErr != nil {
			retErr = panicErr
			metrics.ReplicationStreamPanic.With(s.metrics).Record(1)
		}
	}()

	defer log.CapturePanic(s.logger, &panicErr)

	newTaskNotificationChan, subscriberID := s.historyEngine.SubscribeReplicationNotification()
	defer s.historyEngine.UnsubscribeReplicationNotification(subscriberID)

	catchupEndExclusiveWatermark, err := s.sendCatchUp()
	if err != nil {
		s.logger.Error("ReplicationServiceError StreamSender unable to catch up replication tasks", tag.Error(err))
		return err
	}
	if err := s.sendLive(
		newTaskNotificationChan,
		catchupEndExclusiveWatermark,
	); err != nil {
		s.logger.Error("ReplicationServiceError StreamSender unable to stream replication tasks", tag.Error(err))
		return err
	}
	return nil
}

func (s *StreamSenderImpl) recvSyncReplicationState(
	attr *replicationspb.SyncReplicationState,
) error {
	inclusiveLowWatermark := attr.GetInclusiveLowWatermark()
	inclusiveLowWatermarkTime := attr.GetInclusiveLowWatermarkTime()

	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	readerState := &persistencespb.QueueReaderState{
		Scopes: []*persistencespb.QueueSliceScope{{
			Range: &persistencespb.QueueSliceRange{
				InclusiveMin: shard.ConvertToPersistenceTaskKey(
					tasks.NewImmediateKey(inclusiveLowWatermark),
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
	if err := s.shardContext.UpdateReplicationQueueReaderState(
		readerID,
		readerState,
	); err != nil {
		return err
	}
	return s.shardContext.UpdateRemoteReaderInfo(
		readerID,
		inclusiveLowWatermark-1,
		inclusiveLowWatermarkTime.AsTime(),
	)
}

func (s *StreamSenderImpl) sendCatchUp() (int64, error) {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)

	var catchupBeginInclusiveWatermark int64
	queueState, ok := s.shardContext.GetQueueState(
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
	catchupEndExclusiveWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
	if err := s.sendTasks(
		catchupBeginInclusiveWatermark,
		catchupEndExclusiveWatermark,
	); err != nil {
		return 0, err
	}
	return catchupEndExclusiveWatermark, nil
}

func (s *StreamSenderImpl) sendLive(
	newTaskNotificationChan <-chan struct{},
	beginInclusiveWatermark int64,
) error {
	for {
		select {
		case <-newTaskNotificationChan:
			endExclusiveWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
			if err := s.sendTasks(
				beginInclusiveWatermark,
				endExclusiveWatermark,
			); err != nil {
				return err
			}
			beginInclusiveWatermark = endExclusiveWatermark
		case <-s.shutdownChan.Channel():
			return nil
		}
	}
}

func (s *StreamSenderImpl) sendTasks(
	beginInclusiveWatermark int64,
	endExclusiveWatermark int64,
) error {
	if beginInclusiveWatermark > endExclusiveWatermark {
		err := serviceerror.NewInternal(fmt.Sprintf("StreamWorkflowReplication encountered invalid task range [%v, %v)",
			beginInclusiveWatermark,
			endExclusiveWatermark,
		))
		s.logger.Error("ReplicationServiceError StreamSender unable to send tasks", tag.Error(err))
		return err
	}
	if beginInclusiveWatermark == endExclusiveWatermark {
		return s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           nil,
					ExclusiveHighWatermark:     endExclusiveWatermark,
					ExclusiveHighWatermarkTime: timestamp.TimeNowPtrUtc(),
				},
			},
		})
	}

	ctx := headers.SetCallerInfo(context.Background(), headers.SystemPreemptableCallerInfo)
	ctx, cancel := context.WithTimeout(ctx, replicationTimeout)
	defer cancel()
	iter, err := s.historyEngine.GetReplicationTasksIter(
		ctx,
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	if err != nil {
		return err
	}
Loop:
	for iter.HasNext() {
		if s.shutdownChan.IsShutdown() {
			return nil
		}

		item, err := iter.Next()
		if err != nil {
			return err
		}
		task, err := s.taskConverter.Convert(item)
		if err != nil {
			return err
		}
		if task == nil {
			continue Loop
		}
		if err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task},
					ExclusiveHighWatermark:     task.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task.VisibilityTime,
				},
			},
		}); err != nil {
			return err
		}
		metrics.ReplicationTasksSend.With(s.metrics).Record(
			int64(1),
			metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
			metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTag(task)),
		)
	}
	return s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks:           nil,
				ExclusiveHighWatermark:     endExclusiveWatermark,
				ExclusiveHighWatermarkTime: timestamp.TimeNowPtrUtc(),
			},
		},
	})
}

func (s *StreamSenderImpl) sendToStream(payload *historyservice.StreamWorkflowReplicationMessagesResponse) error {
	err := s.server.Send(payload)
	if err != nil {
		s.logger.Error("ReplicationStreamError Stream Sender unable to send", tag.Error(err))
		return NewStreamError("ReplicationStreamError send failed", err)
	}
	return nil
}
