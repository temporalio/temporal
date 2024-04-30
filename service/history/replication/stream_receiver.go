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
	"fmt"
	"sync/atomic"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	repicationpb "go.temporal.io/server/api/replication/v1"
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

		status         int32
		clientShardKey ClusterShardKey
		serverShardKey ClusterShardKey
		taskTracker    ExecutableTaskTracker
		shutdownChan   channel.ShutdownOnce
		logger         log.Logger
		stream         Stream
		taskConverter  ExecutableTaskConverter
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
	taskTracker := NewExecutableTaskTracker(logger, processToolBox.MetricsHandler)
	return &StreamReceiverImpl{
		ProcessToolBox: processToolBox,

		status:         common.DaemonStatusInitialized,
		clientShardKey: clientShardKey,
		serverShardKey: serverShardKey,
		taskTracker:    taskTracker,
		shutdownChan:   channel.NewShutdownOnce(),
		logger:         logger,
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
	r.taskTracker.Cancel()

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
				r.logger.Debug(fmt.Sprintf("REMOVEME StreamReceiver acked inclusiveLowWatermark %d", inclusiveLowWatermark))
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
	watermarkInfo := r.taskTracker.LowWatermark()
	size := r.taskTracker.Size()
	if watermarkInfo == nil {
		return 0, nil
	}
	if err := stream.Send(&adminservice.StreamWorkflowReplicationMessagesRequest{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &repicationpb.SyncReplicationState{
				InclusiveLowWatermark:     watermarkInfo.Watermark,
				InclusiveLowWatermarkTime: timestamppb.New(watermarkInfo.Timestamp),
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
	return watermarkInfo.Watermark, nil
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
		tasks := r.taskConverter.Convert(
			clusterName,
			r.clientShardKey,
			r.serverShardKey,
			streamResp.Resp.GetMessages().ReplicationTasks...,
		)
		exclusiveHighWatermark := streamResp.Resp.GetMessages().ExclusiveHighWatermark
		exclusiveHighWatermarkTime := timestamp.TimeValue(streamResp.Resp.GetMessages().ExclusiveHighWatermarkTime)
		r.logger.Debug(fmt.Sprintf("StreamReceiver processMessages received %d tasks with exclusiveHighWatermark %d", len(tasks), exclusiveHighWatermark))
		for _, task := range r.taskTracker.TrackTasks(WatermarkInfo{
			Watermark: exclusiveHighWatermark,
			Timestamp: exclusiveHighWatermarkTime,
		}, tasks...) {
			r.logger.Debug(fmt.Sprintf("REMOVEME StreamReceiver processMessages submit task to scheduler, task %+v", task), tag.TaskID(task.TaskID()))
			r.ProcessToolBox.TaskScheduler.Submit(task)
		}
	}
	r.logger.Error("StreamReceiver encountered channel close")
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
