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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination stream_receiver_mock.go

package replication

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/enums/v1"
	ctasks "go.temporal.io/server/common/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"

	replicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	ReceiverModeUnset       ReceiverMode = 0
	ReceiverModeSingleStack ReceiverMode = 1 // default mode. It only uses High Priority Task Tracker for processing tasks.
	ReceiverModeTieredStack ReceiverMode = 2
)

type (
	ReceiverMode   int32
	StreamReceiver interface {
		IsValid() bool
		Key() ClusterShardKeyPair
		Stop()
	}
	StreamReceiverImpl struct {
		ProcessToolBox

		status                  int32
		clientShardKey          ClusterShardKey
		serverShardKey          ClusterShardKey
		highPriorityTaskTracker ExecutableTaskTracker
		lowPriorityTaskTracker  ExecutableTaskTracker
		shutdownChan            channel.ShutdownOnce
		logger                  log.Logger
		stream                  Stream
		taskConverter           ExecutableTaskConverter
		receiverMode            ReceiverMode
	}
)

func NewClusterShardKey(
	ClusterID int32,
	ClusterShardID int32,
) ClusterShardKey {
	return ClusterShardKey{
		ClusterID: ClusterID,
		ShardID:   ClusterShardID,
	}
}

func NewStreamReceiver(
	processToolBox ProcessToolBox,
	taskConverter ExecutableTaskConverter,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) *StreamReceiverImpl {
	logger := log.With(
		processToolBox.Logger,
		tag.SourceCluster(processToolBox.ClusterMetadata.ClusterNameForFailoverVersion(true, int64(serverShardKey.ClusterID))),
		tag.SourceShardID(serverShardKey.ShardID),
		tag.ShardID(clientShardKey.ShardID), // client is the local cluster (target cluster, passive cluster)
	)
	return &StreamReceiverImpl{
		ProcessToolBox: processToolBox,

		status:                  common.DaemonStatusInitialized,
		clientShardKey:          clientShardKey,
		serverShardKey:          serverShardKey,
		highPriorityTaskTracker: NewExecutableTaskTracker(logger, processToolBox.MetricsHandler),
		lowPriorityTaskTracker:  NewExecutableTaskTracker(logger, processToolBox.MetricsHandler),
		shutdownChan:            channel.NewShutdownOnce(),
		logger:                  logger,
		stream: newStream(
			processToolBox,
			clientShardKey,
			serverShardKey,
		),
		taskConverter: taskConverter,
		receiverMode:  ReceiverModeUnset,
	}
}

// Start starts the processor
func (r *StreamReceiverImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go WrapEventLoop(r.sendEventLoop, r.Stop, r.logger, r.MetricsHandler, r.clientShardKey, r.serverShardKey, streamReceiverMonitorInterval)
	go WrapEventLoop(r.recvEventLoop, r.Stop, r.logger, r.MetricsHandler, r.clientShardKey, r.serverShardKey, streamReceiverMonitorInterval)

	r.logger.Info("StreamReceiver started.")
}

// Stop stops the processor
func (r *StreamReceiverImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	r.shutdownChan.Shutdown()
	r.stream.Close()
	r.highPriorityTaskTracker.Cancel()
	r.lowPriorityTaskTracker.Cancel()

	r.logger.Info("StreamReceiver shutting down.")
}

func (r *StreamReceiverImpl) IsValid() bool {
	return atomic.LoadInt32(&r.status) == common.DaemonStatusStarted
}

func (r *StreamReceiverImpl) Key() ClusterShardKeyPair {
	return ClusterShardKeyPair{
		Client: r.clientShardKey,
		Server: r.serverShardKey,
	}
}

func (r *StreamReceiverImpl) sendEventLoop() error {
	var panicErr error
	defer func() {
		if panicErr != nil {
			metrics.ReplicationStreamPanic.With(r.MetricsHandler).Record(1)
		}
	}()
	defer log.CapturePanic(r.logger, &panicErr)

	timer := time.NewTicker(r.Config.ReplicationStreamSyncStatusDuration())
	defer timer.Stop()

	var inclusiveLowWatermark int64

	for {
		select {
		case <-timer.C:
			timer.Reset(r.Config.ReplicationStreamSyncStatusDuration())
			watermark, err := r.ackMessage(r.stream)
			if err != nil {
				if IsStreamError(err) {
					r.logger.Error("ReplicationStreamError StreamReceiver exit send loop", tag.Error(err))
				} else {
					r.logger.Error("ReplicationServiceError StreamReceiver exit send loop", tag.Error(err))
				}
				return err
			}
			if watermark != inclusiveLowWatermark {
				inclusiveLowWatermark = watermark
				r.logger.Debug(fmt.Sprintf("StreamReceiver acked inclusiveLowWatermark %d", inclusiveLowWatermark))
			}
		case <-r.shutdownChan.Channel():
			return nil
		}
	}
}

func (r *StreamReceiverImpl) recvEventLoop() error {
	var panicErr error
	defer func() {
		if panicErr != nil {
			metrics.ReplicationStreamPanic.With(r.MetricsHandler).Record(1)
		}
	}()
	defer log.CapturePanic(r.logger, &panicErr)

	err := r.processMessages(r.stream)
	if err == nil {
		return nil
	}
	if IsStreamError(err) {
		r.logger.Error("ReplicationStreamError StreamReceiver exit recv loop", tag.Error(err))
	} else {
		r.logger.Error("ReplicationServiceError StreamReceiver exit recv loop", tag.Error(err))
	}
	return err
}

// ackMessage returns the inclusive low watermark if present.
func (r *StreamReceiverImpl) ackMessage(
	stream Stream,
) (int64, error) {
	highPriorityWaterMarkInfo := r.highPriorityTaskTracker.LowWatermark()
	lowPriorityWaterMarkInfo := r.lowPriorityTaskTracker.LowWatermark()
	size := r.highPriorityTaskTracker.Size() + r.lowPriorityTaskTracker.Size()

	var highPriorityWatermark, lowPriorityWatermark *replicationpb.ReplicationState
	inclusiveLowWaterMark := int64(-1)
	var inclusiveLowWaterMarkTime time.Time

	receiverMode := ReceiverMode(atomic.LoadInt32((*int32)(&r.receiverMode)))
	switch receiverMode {
	case ReceiverModeUnset:
		return 0, nil
	case ReceiverModeTieredStack:
		if highPriorityWaterMarkInfo == nil || lowPriorityWaterMarkInfo == nil {
			// This is to prevent the case: high priority task tracker received a batch of tasks, but low priority task tracker did not get any task yet.
			// i.e. HighPriorityTracker received watermark 10 and LowPriorityTracker did not receive any tasks yet.
			// we should avoid ack with {high: 10, low: nil}. If we do, sender will not able to correctly interpret the overall low watermark of the queue,
			// because it is possible that low priority tracker might receive a batch of tasks with watermark 5 later.
			// It is also true for the opposite case.
			r.logger.Warn("Tiered stack mode. Have to wait for both high and low priority tracker received at least one batch of tasks before acking.")
			return 0, nil
		}
		highPriorityWatermark = &replicationpb.ReplicationState{
			InclusiveLowWatermark:     highPriorityWaterMarkInfo.Watermark,
			InclusiveLowWatermarkTime: timestamppb.New(highPriorityWaterMarkInfo.Timestamp),
		}
		lowPriorityWatermark = &replicationpb.ReplicationState{
			InclusiveLowWatermark:     lowPriorityWaterMarkInfo.Watermark,
			InclusiveLowWatermarkTime: timestamppb.New(lowPriorityWaterMarkInfo.Timestamp),
		}
		if highPriorityWaterMarkInfo.Watermark <= lowPriorityWaterMarkInfo.Watermark {
			inclusiveLowWaterMark = highPriorityWaterMarkInfo.Watermark
			inclusiveLowWaterMarkTime = highPriorityWaterMarkInfo.Timestamp
		} else {
			inclusiveLowWaterMark = lowPriorityWaterMarkInfo.Watermark
			inclusiveLowWaterMarkTime = lowPriorityWaterMarkInfo.Timestamp
		}
	case ReceiverModeSingleStack:
		if highPriorityWaterMarkInfo == nil { // This should not happen, more for a safety check
			return 0, NewStreamError("Single stack mode. High priority tracker does not have low watermark info", serviceerror.NewInternal("Invalid tracker state"))
		}
		if lowPriorityWaterMarkInfo != nil {
			return 0, NewStreamError("Single stack mode. Should not receive low priority task", serviceerror.NewInternal("Invalid tracker state"))
		}
		inclusiveLowWaterMark = highPriorityWaterMarkInfo.Watermark
		inclusiveLowWaterMarkTime = highPriorityWaterMarkInfo.Timestamp
	}
	if inclusiveLowWaterMark == -1 {
		return 0, NewStreamError("InclusiveLowWaterMark is not set", serviceerror.NewInternal("Invalid inclusive low watermark"))
	}

	if err := stream.Send(&adminservice.StreamWorkflowReplicationMessagesRequest{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &replicationpb.SyncReplicationState{
				InclusiveLowWatermark:     inclusiveLowWaterMark,
				InclusiveLowWatermarkTime: timestamppb.New(inclusiveLowWaterMarkTime),
				HighPriorityState:         highPriorityWatermark,
				LowPriorityState:          lowPriorityWatermark,
			},
		},
	}); err != nil {
		return 0, err
	}
	metrics.ReplicationTasksRecvBacklog.With(r.MetricsHandler).Record(
		int64(size),
		metrics.FromClusterIDTag(r.serverShardKey.ClusterID),
		metrics.ToClusterIDTag(r.clientShardKey.ClusterID),
	)
	metrics.ReplicationTasksSend.With(r.MetricsHandler).Record(
		int64(1),
		metrics.FromClusterIDTag(r.clientShardKey.ClusterID),
		metrics.ToClusterIDTag(r.serverShardKey.ClusterID),
		metrics.OperationTag(metrics.SyncWatermarkScope),
	)
	return inclusiveLowWaterMark, nil
}

func (r *StreamReceiverImpl) processMessages(
	stream Stream,
) error {
	allClusterInfo := r.ClusterMetadata.GetAllClusterInfo()
	clusterName, _, err := ClusterIDToClusterNameShardCount(allClusterInfo, r.serverShardKey.ClusterID)
	if err != nil {
		return err
	}

	streamRespChen, err := stream.Recv()
	if err != nil {
		return err
	}
	for streamResp := range streamRespChen {
		if streamResp.Err != nil {
			return streamResp.Err
		}
		if err := r.validateAndSetReceiverMode(streamResp.Resp.GetMessages().Priority); err != nil {
			// sender mode changed, exit loop and let stream reconnect to retry
			return NewStreamError("ReplicationTask wrong receiver mode", err)
		}

		if err = ValidateTasksHaveSamePriority(streamResp.Resp.GetMessages().Priority, streamResp.Resp.GetMessages().ReplicationTasks...); err != nil {
			// This should not happen because source side is sending task 1 by 1. Validate here just in case.
			return NewStreamError("ReplicationTask priority check failed", err)
		}
		convertedTasks := r.taskConverter.Convert(
			clusterName,
			r.clientShardKey,
			r.serverShardKey,
			streamResp.Resp.GetMessages().ReplicationTasks...,
		)
		exclusiveHighWatermark := streamResp.Resp.GetMessages().ExclusiveHighWatermark
		exclusiveHighWatermarkTime := timestamp.TimeValue(streamResp.Resp.GetMessages().ExclusiveHighWatermarkTime)
		taskTracker, taskScheduler, err := r.getTrackerAndSchedulerByPriority(streamResp.Resp.GetMessages().Priority)
		if err != nil {
			// Todo: Change to write Tasks to DLQ. As resend task will not help here
			return NewStreamError("ReplicationTask wrong priority", err)
		}
		for _, task := range taskTracker.TrackTasks(WatermarkInfo{
			Watermark: exclusiveHighWatermark,
			Timestamp: exclusiveHighWatermarkTime,
		}, convertedTasks...) {
			taskScheduler.Submit(task)
		}
	}
	r.logger.Error("StreamReceiver encountered channel close")
	return nil
}

func (r *StreamReceiverImpl) getTrackerAndSchedulerByPriority(priority enums.TaskPriority) (ExecutableTaskTracker, ctasks.Scheduler[TrackableExecutableTask], error) {
	switch priority {
	case enums.TASK_PRIORITY_UNSPECIFIED, enums.TASK_PRIORITY_HIGH:
		return r.highPriorityTaskTracker, r.ProcessToolBox.HighPriorityTaskScheduler, nil
	case enums.TASK_PRIORITY_LOW:
		return r.lowPriorityTaskTracker, r.ProcessToolBox.LowPriorityTaskScheduler, nil
	default:
		return nil, nil, serviceerror.NewInvalidArgument(fmt.Sprintf("Unknown task priority: %v", priority))
	}
}

// Receiver mode can only be set once for the lifetime of the receiver. Receiver mode is set when receiver receive the first task.
// If the first task is prioritized, receiver mode will be set to ReceiverModeTieredStack. If the first task is not prioritized, receiver mode will be set to ReceiverModeSingleStack.
// Receiver mode cannot be changed once it is set. If we enabled sender side to send tasks with different priority, we need to change the receiver mode by reconnecting the stream.
func (r *StreamReceiverImpl) validateAndSetReceiverMode(priority enums.TaskPriority) error {
	receiverMode := ReceiverMode(atomic.LoadInt32((*int32)(&r.receiverMode)))
	switch receiverMode {
	case ReceiverModeUnset:
		r.setReceiverMode(priority)
		return nil
	case ReceiverModeSingleStack:
		if priority != enums.TASK_PRIORITY_UNSPECIFIED {
			return serviceerror.NewInvalidArgument("ReceiverModeSingleStack cannot process prioritized task")
		}
	case ReceiverModeTieredStack:
		if priority == enums.TASK_PRIORITY_UNSPECIFIED {
			return serviceerror.NewInvalidArgument("ReceiverModeTieredStack cannot process non-prioritized task")
		}
	}
	return nil
}

func (r *StreamReceiverImpl) setReceiverMode(priority enums.TaskPriority) {
	if r.receiverMode != ReceiverModeUnset {
		return
	}
	switch priority {
	case enums.TASK_PRIORITY_UNSPECIFIED:
		atomic.StoreInt32((*int32)(&r.receiverMode), int32(ReceiverModeSingleStack))
	case enums.TASK_PRIORITY_HIGH, enums.TASK_PRIORITY_LOW:
		atomic.StoreInt32((*int32)(&r.receiverMode), int32(ReceiverModeTieredStack))
	}
}

func ValidateTasksHaveSamePriority(messageBatchPriority enums.TaskPriority, tasks ...*replicationpb.ReplicationTask) error {
	if len(tasks) == 0 || messageBatchPriority == enums.TASK_PRIORITY_UNSPECIFIED {
		return nil
	}
	for _, task := range tasks {
		if task.Priority != messageBatchPriority {
			return serviceerror.NewInvalidArgument(fmt.Sprintf("Task priority does not match batch priority: %v, %v", task.Priority, messageBatchPriority))
		}
	}
	return nil
}

func newStream(
	processToolBox ProcessToolBox,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) Stream {
	var clientProvider BiDirectionStreamClientProvider[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse] = &streamClientProvider{
		processToolBox: processToolBox,
		clientShardKey: clientShardKey,
		serverShardKey: serverShardKey,
	}
	return NewBiDirectionStream(
		clientProvider,
		processToolBox.MetricsHandler,
		log.With(processToolBox.Logger, tag.ShardID(clientShardKey.ShardID)),
	)
}

type streamClientProvider struct {
	processToolBox ProcessToolBox
	clientShardKey ClusterShardKey
	serverShardKey ClusterShardKey
}

var _ BiDirectionStreamClientProvider[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse] = (*streamClientProvider)(nil)

func (p *streamClientProvider) Get(
	ctx context.Context,
) (BiDirectionStreamClient[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse], error) {
	return NewStreamBiDirectionStreamClientProvider(
		p.processToolBox.ClusterMetadata,
		p.processToolBox.ClientBean,
	).Get(ctx, p.clientShardKey, p.serverShardKey)
}
