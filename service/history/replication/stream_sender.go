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
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

const TaskMaxSkipCount int = 1000

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

		status                  int32
		clientClusterName       string
		clientShardKey          ClusterShardKey
		serverShardKey          ClusterShardKey
		clientClusterShardCount int32
		shutdownChan            channel.ShutdownOnce
		config                  *configs.Config
		isTieredStackEnabled    bool
	}
)

func NewStreamSender(
	server historyservice.HistoryService_StreamWorkflowReplicationMessagesServer,
	shardContext shard.Context,
	historyEngine shard.Engine,
	taskConverter SourceTaskConverter,
	clientClusterName string,
	clientClusterShardCount int32,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
	config *configs.Config,
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

		status:                  common.DaemonStatusInitialized,
		clientClusterName:       clientClusterName,
		clientShardKey:          clientShardKey,
		serverShardKey:          serverShardKey,
		clientClusterShardCount: clientClusterShardCount,
		shutdownChan:            channel.NewShutdownOnce(),
		config:                  config,
		isTieredStackEnabled:    config.EnableReplicationTaskTieredProcessing(),
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
	getSenderEventLoop := func(priority enumsspb.TaskPriority) func() error {
		return func() error {
			return s.sendEventLoop(priority)
		}
	}

	if s.isTieredStackEnabled {
		go WrapEventLoop(getSenderEventLoop(enumsspb.TASK_PRIORITY_HIGH), s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, streamReceiverMonitorInterval)
		go WrapEventLoop(getSenderEventLoop(enumsspb.TASK_PRIORITY_LOW), s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, streamReceiverMonitorInterval)
	} else {
		go WrapEventLoop(getSenderEventLoop(enumsspb.TASK_PRIORITY_UNSPECIFIED), s.Stop, s.logger, s.metrics, s.clientShardKey, s.serverShardKey, streamReceiverMonitorInterval)
	}

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

	var inclusiveLowWatermark int64

	for !s.shutdownChan.IsShutdown() {
		if s.isTieredStackEnabled != s.config.EnableReplicationTaskTieredProcessing() {
			s.logger.Warn("ReplicationStreamError StreamSender detected tiered stack change, restart the stream")
			return NewStreamError("StreamError tiered stack change, reconnect stream", serviceerror.NewInternal("tiered stack change"))
		}
		req, err := s.server.Recv()
		if err != nil {
			s.logger.Error("ReplicationStreamError StreamSender failed to receive", tag.Error(err))
			return NewStreamError("StreamError recv error", err)
		}
		switch attr := req.GetAttributes().(type) {
		case *historyservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState:
			if attr.SyncReplicationState.GetInclusiveLowWatermark() != inclusiveLowWatermark {
				inclusiveLowWatermark = attr.SyncReplicationState.GetInclusiveLowWatermark()
				s.logger.Debug(fmt.Sprintf("StreamSender received new inclusiveLowWatermark: %v", inclusiveLowWatermark))
			}
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

func (s *StreamSenderImpl) sendEventLoop(priority enumsspb.TaskPriority) (retErr error) {
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

	catchupEndExclusiveWatermark, err := s.sendCatchUp(priority)
	if err != nil {
		s.logger.Error("ReplicationServiceError StreamSender unable to catch up replication tasks", tag.Error(err))
		return err
	}
	s.logger.Debug(fmt.Sprintf("StreamSender sendCatchUp finished with catchupEndExclusiveWatermark %v", catchupEndExclusiveWatermark))
	if err := s.sendLive(
		priority,
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
	var readerState *persistencespb.QueueReaderState
	switch s.isTieredStackEnabled {
	case true:
		if attr.HighPriorityState == nil || attr.LowPriorityState == nil {
			return NewStreamError("ReplicationServiceError StreamSender encountered unsupported SyncReplicationState", nil)
		}
		readerState = &persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{
				// index 0 is for overall low watermark. In tiered stack it is Min(LowWatermark-high priority, LowWatermark-low priority)
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(attr.GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				// index 1 is for high priority
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(attr.GetHighPriorityState().GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				// index 2 is for low priority
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(attr.GetLowPriorityState().GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
			},
		}
	case false:
		if attr.HighPriorityState != nil || attr.LowPriorityState != nil {
			return NewStreamError("ReplicationServiceError StreamSender encountered unsupported SyncReplicationState", nil)
		}
		readerState = &persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{
				// in single stack, index 0 is for overall low watermark
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(attr.GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
			},
		}
	}

	inclusiveLowWatermark := attr.GetInclusiveLowWatermark()
	inclusiveLowWatermarkTime := attr.GetInclusiveLowWatermarkTime()

	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)

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

func (s *StreamSenderImpl) sendCatchUp(priority enumsspb.TaskPriority) (int64, error) {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)

	catchupEndExclusiveWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID

	var catchupBeginInclusiveWatermark int64
	queueState, ok := s.shardContext.GetQueueState(
		tasks.CategoryReplication,
	)
	if !ok {
		s.logger.Debug("StreamSender queueState not found")
		catchupBeginInclusiveWatermark = catchupEndExclusiveWatermark
	} else {
		readerState, ok := queueState.ReaderStates[readerID]
		if !ok {
			s.logger.Debug(fmt.Sprintf("StreamSender readerState not found, readerID %v", readerID))
			catchupBeginInclusiveWatermark = catchupEndExclusiveWatermark
		} else {
			catchupBeginInclusiveWatermark = s.getSendCatchupBeginInclusiveWatermark(readerState, priority)
		}
	}
	if err := s.sendTasks(
		priority,
		catchupBeginInclusiveWatermark,
		catchupEndExclusiveWatermark,
	); err != nil {
		return 0, err
	}
	return catchupEndExclusiveWatermark, nil
}

func (s *StreamSenderImpl) getSendCatchupBeginInclusiveWatermark(readerState *persistencespb.QueueReaderState, priority enumsspb.TaskPriority) int64 {
	getReaderScopesIndex := func(priority enumsspb.TaskPriority) int {
		switch priority {
		case enumsspb.TASK_PRIORITY_HIGH:
			/*
				this is to handle the case when switch from single stack to tiered stack, the reader state is still in old format.
				In this case, it is safe to use the overall low watermark as the beginInclusiveWatermark, as long as we always guarantee
				the overall low watermark is Min(lowPriorityLowWatermark, highPriorityLowWatermark)
			*/
			if len(readerState.Scopes) != 3 {
				return 0
			}
			return 1
		case enumsspb.TASK_PRIORITY_LOW:
			if len(readerState.Scopes) != 3 {
				return 0
			}
			return 2
		case enumsspb.TASK_PRIORITY_UNSPECIFIED:
			return 0
		default:
			return 0
		}
	}
	return readerState.Scopes[getReaderScopesIndex(priority)].Range.InclusiveMin.TaskId
}

func (s *StreamSenderImpl) sendLive(
	priority enumsspb.TaskPriority,
	newTaskNotificationChan <-chan struct{},
	beginInclusiveWatermark int64,
) error {
	for {
		select {
		case <-newTaskNotificationChan:
			endExclusiveWatermark := s.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).TaskID
			if err := s.sendTasks(
				priority,
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
	priority enumsspb.TaskPriority,
	beginInclusiveWatermark int64,
	endExclusiveWatermark int64,
) error {
	s.logger.Debug(fmt.Sprintf("StreamSender sendTasks [%v, %v)", beginInclusiveWatermark, endExclusiveWatermark))
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
					Priority:                   priority,
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
	skipCount := 0
Loop:
	for iter.HasNext() {
		if s.shutdownChan.IsShutdown() {
			return nil
		}

		item, err := iter.Next()
		if err != nil {
			return err
		}

		if !s.shouldProcessTask(item) {
			continue
		}

		skipCount++
		// To avoid a situation: we are skipping a lot of tasks and never send any task, receiver side will not have updated high watermark,
		// so it will not ACK back to sender, sender will not update the ACK level.
		// i.e. in tiered stack, if no low priority task in queue, we should still send watermark info to receiver to let it update ACK level.
		if skipCount > TaskMaxSkipCount {
			if err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
				Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
					Messages: &replicationspb.WorkflowReplicationMessages{
						ExclusiveHighWatermark:     item.GetTaskID(),
						ExclusiveHighWatermarkTime: timestamppb.New(item.GetVisibilityTime()),
						Priority:                   priority,
					},
				},
			}); err != nil {
				return err
			}
		}
		if priority != enumsspb.TASK_PRIORITY_UNSPECIFIED && // case: skip priority check. When priority is unspecified, send all tasks
			priority != s.getTaskPriority(item) { // case: skip task with different priority than this loop
			continue Loop
		}

		operation := func() error {
			task, err := s.taskConverter.Convert(item)
			if err != nil {
				return err
			}
			if task == nil {
				return nil
			}
			task.Priority = priority
			s.logger.Debug("StreamSender send replication task", tag.TaskID(task.SourceTaskId))
			if err := s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
				Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
					Messages: &replicationspb.WorkflowReplicationMessages{
						ReplicationTasks:           []*replicationspb.ReplicationTask{task},
						ExclusiveHighWatermark:     task.SourceTaskId + 1,
						ExclusiveHighWatermarkTime: task.VisibilityTime,
						Priority:                   priority,
					},
				},
			}); err != nil {
				return err
			}
			skipCount = 0
			metrics.ReplicationTasksSend.With(s.metrics).Record(
				int64(1),
				metrics.FromClusterIDTag(s.serverShardKey.ClusterID),
				metrics.ToClusterIDTag(s.clientShardKey.ClusterID),
				metrics.OperationTag(TaskOperationTag(task)),
			)
			return nil
		}

		retryPolicy := backoff.NewExponentialRetryPolicy(1 * time.Second).
			WithBackoffCoefficient(1.2).
			WithMaximumInterval(3 * time.Second).
			WithMaximumAttempts(80).
			WithExpirationInterval(3 * time.Minute)

		if err := backoff.ThrottleRetry(operation, retryPolicy, IsRetryableError); err != nil {
			return err
		}
	}
	return s.sendToStream(&historyservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks:           nil,
				ExclusiveHighWatermark:     endExclusiveWatermark,
				ExclusiveHighWatermarkTime: timestamp.TimeNowPtrUtc(),
				Priority:                   priority,
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

func (s *StreamSenderImpl) shouldProcessTask(item tasks.Task) bool {
	clientShardID := common.WorkflowIDToHistoryShard(item.GetNamespaceID(), item.GetWorkflowID(), s.clientClusterShardCount)
	if clientShardID != s.clientShardKey.ShardID {
		return false
	}

	var shouldProcessTask bool
	namespaceEntry, err := s.shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(item.GetNamespaceID()),
	)
	if err != nil {
		// if there is error, then blindly send the task, better safe than sorry
		shouldProcessTask = true
	}

	if namespaceEntry != nil {
	FilterLoop:
		for _, targetCluster := range namespaceEntry.ClusterNames() {
			if s.clientClusterName == targetCluster {
				shouldProcessTask = true
				break FilterLoop
			}
		}
	}

	return shouldProcessTask
}

func (s *StreamSenderImpl) getTaskPriority(task tasks.Task) enumsspb.TaskPriority {
	switch t := task.(type) {
	case *tasks.SyncWorkflowStateTask:
		if t.Priority == enumsspb.TASK_PRIORITY_UNSPECIFIED {
			return enumsspb.TASK_PRIORITY_LOW
		}
		return t.Priority
	default:
		return enumsspb.TASK_PRIORITY_HIGH
	}
}
