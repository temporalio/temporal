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
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	repicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ClusterShardKey struct {
		ClusterID int32
		ShardID   int32
	}
	ClusterShardKeyPair struct {
		Client ClusterShardKey
		Server ClusterShardKey
	}

	Stream         BiDirectionStream[*adminservice.StreamWorkflowReplicationMessagesRequest, *adminservice.StreamWorkflowReplicationMessagesResponse]
	StreamReceiver struct {
		ProcessToolBox

		status         int32
		clientShardKey ClusterShardKey
		serverShardKey ClusterShardKey
		taskTracker    ExecutableTaskTracker
		shutdownChan   channel.ShutdownOnce
		logger         log.Logger

		sync.Mutex
		streamCreationTime time.Time
		stream             Stream
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
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
) *StreamReceiver {
	logger := log.With(processToolBox.Logger, tag.ShardID(clientShardKey.ShardID))
	taskTracker := NewExecutableTaskTracker(logger)
	return &StreamReceiver{
		ProcessToolBox: processToolBox,

		status:         common.DaemonStatusInitialized,
		clientShardKey: clientShardKey,
		serverShardKey: serverShardKey,
		taskTracker:    taskTracker,
		shutdownChan:   channel.NewShutdownOnce(),
		logger:         logger,

		streamCreationTime: time.Now().UTC(),
		stream: newStream(
			processToolBox,
			clientShardKey,
			serverShardKey,
		),
	}
}

// Start starts the processor
func (r *StreamReceiver) Start() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go r.sendEventLoop()
	go r.recvEventLoop()

	r.logger.Info("StreamReceiver started.")
}

// Stop stops the processor
func (r *StreamReceiver) Stop() {
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

func (r *StreamReceiver) IsValid() bool {
	return atomic.LoadInt32(&r.status) == common.DaemonStatusStarted
}

func (r *StreamReceiver) sendEventLoop() {
	defer r.Stop()
	timer := time.NewTicker(r.Config.ReplicationStreamSyncStatusDuration())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(r.Config.ReplicationStreamSyncStatusDuration())
			streamCreationTime, stream := r.getStream()
			if err := r.ackMessage(stream); err != nil {
				r.recreateStream(streamCreationTime)
			}
		case <-r.shutdownChan.Channel():
			return
		}
	}
}

func (r *StreamReceiver) recvEventLoop() {
	defer r.Stop()

	for !r.shutdownChan.IsShutdown() {
		streamCreationTime, stream := r.getStream()
		_ = r.processMessages(stream)
		r.recreateStream(streamCreationTime)
	}
}

func (r *StreamReceiver) getStream() (time.Time, Stream) {
	r.Lock()
	defer r.Unlock()
	return r.streamCreationTime, r.stream
}

func (r *StreamReceiver) recreateStream(
	streamCreationTime time.Time,
) {
	delay := streamCreationTime.Add(r.Config.ReplicationStreamMinReconnectDuration()).Sub(time.Now().UTC())
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.shutdownChan.Channel():
		}
	}

	r.Lock()
	defer r.Unlock()
	r.streamCreationTime = time.Now().UTC()
	r.stream = newStream(
		r.ProcessToolBox,
		r.clientShardKey,
		r.serverShardKey,
	)
}

func (r *StreamReceiver) ackMessage(
	stream Stream,
) error {
	watermarkInfo := r.taskTracker.LowWatermark()
	size := r.taskTracker.Size()
	if watermarkInfo == nil {
		return nil
	}
	if err := stream.Send(&adminservice.StreamWorkflowReplicationMessagesRequest{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &repicationpb.SyncReplicationState{
				LastProcessedMessageId:   watermarkInfo.Watermark,
				LastProcessedMessageTime: timestamp.TimePtr(watermarkInfo.Timestamp),
			},
		},
	}); err != nil {
		r.logger.Error("StreamReceiver unable to send message, err", tag.Error(err))
		return err
	}
	r.MetricsHandler.Histogram(metrics.ReplicationTasksRecvBacklog.GetMetricName(), metrics.ReplicationTasksRecvBacklog.GetMetricUnit()).Record(
		int64(size),
		metrics.FromClusterIDTag(r.serverShardKey.ClusterID),
		metrics.ToClusterIDTag(r.clientShardKey.ClusterID),
	)
	r.MetricsHandler.Counter(metrics.ReplicationTasksSend.GetMetricName()).Record(
		int64(1),
		metrics.FromClusterIDTag(r.clientShardKey.ClusterID),
		metrics.ToClusterIDTag(r.serverShardKey.ClusterID),
		metrics.OperationTag(metrics.SyncWatermarkScope),
	)
	return nil
}

func (r *StreamReceiver) processMessages(
	stream Stream,
) error {
	allClusterInfo := r.ClusterMetadata.GetAllClusterInfo()
	clusterName, err := clusterIDToClusterName(allClusterInfo, r.serverShardKey.ClusterID)
	if err != nil {
		return err
	}

	streamRespChen, err := stream.Recv()
	if err != nil {
		r.logger.Error("StreamReceiver unable to recv message, err", tag.Error(err))
		return err
	}
	for streamResp := range streamRespChen {
		if streamResp.Err != nil {
			r.logger.Error("StreamReceiver recv stream encountered unexpected err", tag.Error(streamResp.Err))
			return streamResp.Err
		}
		tasks := r.ConvertTasks(
			clusterName,
			r.clientShardKey,
			r.serverShardKey,
			streamResp.Resp.GetMessages().ReplicationTasks...,
		)
		highWatermark := streamResp.Resp.GetMessages().LastTaskId
		highWatermarkTime := timestamp.TimeValue(streamResp.Resp.GetMessages().LastTaskTime)
		for _, task := range r.taskTracker.TrackTasks(WatermarkInfo{
			Watermark: highWatermark,
			Timestamp: highWatermarkTime,
		}, tasks...) {
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

type noopSchedulerMonitor struct {
}

func newNoopSchedulerMonitor() *noopSchedulerMonitor {
	return &noopSchedulerMonitor{}
}

func (m *noopSchedulerMonitor) Start()                    {}
func (m *noopSchedulerMonitor) Stop()                     {}
func (m *noopSchedulerMonitor) RecordStart(_ ctasks.Task) {}
