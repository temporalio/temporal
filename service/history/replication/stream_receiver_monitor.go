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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/cluster"
)

const (
	streamReceiverMonitorInterval = 2 * time.Second
)

type (
	StreamReceiverMonitor interface {
		RegisterInboundStream(streamSender StreamSender)
		Start()
		Stop()
	}
	StreamReceiverMonitorImpl struct {
		ProcessToolBox
		executableTaskConverter ExecutableTaskConverter
		enableStreaming         bool

		status       int32
		shutdownOnce channel.ShutdownOnce

		sync.Mutex
		inboundStreams  map[ClusterShardKeyPair]StreamSender
		outboundStreams map[ClusterShardKeyPair]StreamReceiver
	}
)

func NewStreamReceiverMonitor(
	processToolBox ProcessToolBox,
	executableTaskConverter ExecutableTaskConverter,
	enableStreaming bool,
) *StreamReceiverMonitorImpl {
	return &StreamReceiverMonitorImpl{
		ProcessToolBox:          processToolBox,
		executableTaskConverter: executableTaskConverter,
		enableStreaming:         enableStreaming,

		status:       streamStatusInitialized,
		shutdownOnce: channel.NewShutdownOnce(),

		inboundStreams:  make(map[ClusterShardKeyPair]StreamSender),
		outboundStreams: make(map[ClusterShardKeyPair]StreamReceiver),
	}
}

func (m *StreamReceiverMonitorImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&m.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}
	if !m.enableStreaming {
		return
	}

	go m.eventLoop()

	m.Logger.Info("StreamReceiverMonitor started.")
}

func (m *StreamReceiverMonitorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&m.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	if !m.enableStreaming {
		return
	}

	m.shutdownOnce.Shutdown()
	m.Lock()
	defer m.Unlock()
	for serverKey, stream := range m.outboundStreams {
		stream.Stop()
		delete(m.outboundStreams, serverKey)
	}
	m.Logger.Info("StreamReceiverMonitor stopped.")
}

func (m *StreamReceiverMonitorImpl) RegisterInboundStream(
	streamSender StreamSender,
) {
	streamKey := streamSender.Key()

	m.Lock()
	defer m.Unlock()

	if staleSender, ok := m.inboundStreams[streamKey]; ok {
		staleSender.Stop()
		delete(m.inboundStreams, streamKey)
	}
	m.inboundStreams[streamKey] = streamSender
}

func (m *StreamReceiverMonitorImpl) eventLoop() {
	defer m.Stop()
	ticker := time.NewTicker(streamReceiverMonitorInterval)
	defer ticker.Stop()

	clusterMetadataChangeChan := make(chan struct{}, 1)
	m.ClusterMetadata.RegisterMetadataChangeCallback(m, func(_ map[string]*cluster.ClusterInformation, _ map[string]*cluster.ClusterInformation) {
		select {
		case clusterMetadataChangeChan <- struct{}{}:
		default:
		}
	})
	defer m.ClusterMetadata.UnRegisterMetadataChangeCallback(m)
	m.reconcileOutboundStreams()

Loop:
	for !m.shutdownOnce.IsShutdown() {
		select {
		case <-clusterMetadataChangeChan:
			m.reconcileInboundStreams()
			m.reconcileOutboundStreams()
		case <-ticker.C:
			m.reconcileInboundStreams()
			m.reconcileOutboundStreams()
		case <-m.shutdownOnce.Channel():
			break Loop
		}
	}
}

func (m *StreamReceiverMonitorImpl) reconcileInboundStreams() {
	streamKeys := m.generateInboundStreamKeys()
	m.doReconcileInboundStreams(streamKeys)
}

func (m *StreamReceiverMonitorImpl) reconcileOutboundStreams() {
	streamKeys := m.generateOutboundStreamKeys()
	m.doReconcileOutboundStreams(streamKeys)
}

func (m *StreamReceiverMonitorImpl) generateInboundStreamKeys() map[ClusterShardKeyPair]struct{} {
	allClusterInfo := m.ClusterMetadata.GetAllClusterInfo()

	clientClusterIDs := make(map[int32]struct{})
	serverClusterID := int32(m.ClusterMetadata.GetClusterID())
	clusterIDToShardCount := make(map[int32]int32)
	for _, clusterInfo := range allClusterInfo {
		clusterIDToShardCount[int32(clusterInfo.InitialFailoverVersion)] = clusterInfo.ShardCount

		if !clusterInfo.Enabled || int32(clusterInfo.InitialFailoverVersion) == serverClusterID {
			continue
		}
		clientClusterIDs[int32(clusterInfo.InitialFailoverVersion)] = struct{}{}
	}
	streamKeys := make(map[ClusterShardKeyPair]struct{})
	for _, shardID := range m.ShardController.ShardIDs() {
		for clientClusterID := range clientClusterIDs {
			serverShardID := shardID
			for _, clientShardID := range common.MapShardID(
				clusterIDToShardCount[serverClusterID],
				clusterIDToShardCount[clientClusterID],
				serverShardID,
			) {
				m.Logger.Debug(fmt.Sprintf(
					"inbound cluster shard ID %v/%v -> cluster shard ID %v/%v",
					clientClusterID, clientShardID, serverClusterID, serverShardID,
				))
				streamKeys[ClusterShardKeyPair{
					Client: NewClusterShardKey(clientClusterID, clientShardID),
					Server: NewClusterShardKey(serverClusterID, serverShardID),
				}] = struct{}{}
			}
		}
	}
	return streamKeys
}

func (m *StreamReceiverMonitorImpl) generateOutboundStreamKeys() map[ClusterShardKeyPair]struct{} {
	allClusterInfo := m.ClusterMetadata.GetAllClusterInfo()

	clientClusterID := int32(m.ClusterMetadata.GetClusterID())
	serverClusterIDs := make(map[int32]struct{})
	clusterIDToShardCount := make(map[int32]int32)
	for _, clusterInfo := range allClusterInfo {
		clusterIDToShardCount[int32(clusterInfo.InitialFailoverVersion)] = clusterInfo.ShardCount

		if !clusterInfo.Enabled || int32(clusterInfo.InitialFailoverVersion) == clientClusterID {
			continue
		}
		serverClusterIDs[int32(clusterInfo.InitialFailoverVersion)] = struct{}{}
	}
	streamKeys := make(map[ClusterShardKeyPair]struct{})
	for _, shardID := range m.ShardController.ShardIDs() {
		for serverClusterID := range serverClusterIDs {
			clientShardID := shardID
			for _, serverShardID := range common.MapShardID(
				clusterIDToShardCount[clientClusterID],
				clusterIDToShardCount[serverClusterID],
				clientShardID,
			) {
				m.Logger.Debug(fmt.Sprintf(
					"outbound cluster shard ID %v/%v -> cluster shard ID %v/%v",
					clientClusterID, clientShardID, serverClusterID, serverShardID,
				))
				streamKeys[ClusterShardKeyPair{
					Client: NewClusterShardKey(clientClusterID, clientShardID),
					Server: NewClusterShardKey(serverClusterID, serverShardID),
				}] = struct{}{}
			}
		}
	}
	return streamKeys
}

func (m *StreamReceiverMonitorImpl) doReconcileInboundStreams(
	streamKeys map[ClusterShardKeyPair]struct{},
) {
	m.Lock()
	defer m.Unlock()
	if m.shutdownOnce.IsShutdown() {
		return
	}

	for streamKey, stream := range m.inboundStreams {
		if !stream.IsValid() {
			stream.Stop()
			delete(m.inboundStreams, streamKey)
		} else if _, ok := streamKeys[streamKey]; !ok {
			stream.Stop()
			delete(m.inboundStreams, streamKey)
		}
	}
}

func (m *StreamReceiverMonitorImpl) doReconcileOutboundStreams(
	streamKeys map[ClusterShardKeyPair]struct{},
) {
	m.Lock()
	defer m.Unlock()
	if m.shutdownOnce.IsShutdown() {
		return
	}

	for streamKey, stream := range m.outboundStreams {
		if !stream.IsValid() {
			stream.Stop()
			delete(m.outboundStreams, streamKey)
		} else if _, ok := streamKeys[streamKey]; !ok {
			stream.Stop()
			delete(m.outboundStreams, streamKey)
		}
	}
	for streamKey := range streamKeys {
		if _, ok := m.outboundStreams[streamKey]; !ok {
			stream := NewStreamReceiver(
				m.ProcessToolBox,
				m.executableTaskConverter,
				streamKey.Client,
				streamKey.Server,
			)
			stream.Start()
			m.outboundStreams[streamKey] = stream
		}
	}
}
