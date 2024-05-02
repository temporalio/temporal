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
	"math"
	"sync/atomic"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
	"golang.org/x/crypto/openpgp/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	replicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
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

	if highPriorityWaterMarkInfo == nil && lowPriorityWaterMarkInfo == nil {
		return 0, nil
	}

	inclusiveLowWaterMark := int64(math.MaxInt64)
	var inclusiveLowWaterMarkTime time.Time
	var highPriorityWatermark, lowPriorityWatermark *replicationpb.ReplicationState
	if highPriorityWaterMarkInfo != nil {
		highPriorityWatermark = &replicationpb.ReplicationState{
			InclusiveLowWatermark:     highPriorityWaterMarkInfo.Watermark,
			InclusiveLowWatermarkTime: timestamppb.New(highPriorityWaterMarkInfo.Timestamp),
		}
		if inclusiveLowWaterMark > highPriorityWaterMarkInfo.Watermark {
			inclusiveLowWaterMark = highPriorityWaterMarkInfo.Watermark
			inclusiveLowWaterMarkTime = highPriorityWaterMarkInfo.Timestamp
		}
	}
	if lowPriorityWaterMarkInfo != nil {
		lowPriorityWatermark = &replicationpb.ReplicationState{
			InclusiveLowWatermark:     lowPriorityWaterMarkInfo.Watermark,
			InclusiveLowWatermarkTime: timestamppb.New(lowPriorityWaterMarkInfo.Timestamp),
		}
		if inclusiveLowWaterMark > lowPriorityWaterMarkInfo.Watermark {
			inclusiveLowWaterMark = lowPriorityWaterMarkInfo.Watermark
			inclusiveLowWaterMarkTime = lowPriorityWaterMarkInfo.Timestamp
		}
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
		// This should not happen because source side is sending task 1 by 1. Validate here just in case.
		if err = ValidateTasksHaveSamePriority(streamResp.Resp.GetMessages().Priority, streamResp.Resp.GetMessages().ReplicationTasks...); err != nil {
			r.logger.Error("ReplicationTask priority check failed", tag.Error(err))
			break // exit loop and let stream reconnect to retry
		}
		convertedTasks := r.taskConverter.Convert(
			clusterName,
			r.clientShardKey,
			r.serverShardKey,
			streamResp.Resp.GetMessages().ReplicationTasks...,
		)
		exclusiveHighWatermark := streamResp.Resp.GetMessages().ExclusiveHighWatermark
		exclusiveHighWatermarkTime := timestamp.TimeValue(streamResp.Resp.GetMessages().ExclusiveHighWatermarkTime)
		taskTracker, taskScheduler := r.getTrackerAndSchedulerByPriority(tasks.ReplicationTaskPriority(streamResp.Resp.GetMessages().Priority))
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

func (r *StreamReceiverImpl) getTrackerAndSchedulerByPriority(priority tasks.ReplicationTaskPriority) (ExecutableTaskTracker, ctasks.Scheduler[TrackableExecutableTask]) {
	switch priority {
	case tasks.ReplicationTaskPriorityHigh:
		return r.highPriorityTaskTracker, r.ProcessToolBox.HighPriorityTaskScheduler
	case tasks.ReplicationTaskPriorityLow:
		return r.lowPriorityTaskTracker, r.ProcessToolBox.LowPriorityTaskScheduler
	default:
		panic(fmt.Sprintf("unknown priority %v", priority))
	}
}

func ValidateTasksHaveSamePriority(messageBatchPriority int32, tasks ...*replicationpb.ReplicationTask) error {
	if len(tasks) == 0 {
		return nil
	}
	for _, task := range tasks {
		if task.Priority != messageBatchPriority {
			return errors.InvalidArgumentError(fmt.Sprintf("Task priority does not match batch priority: %v, %v", task.Priority, messageBatchPriority))
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
