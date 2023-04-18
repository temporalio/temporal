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
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/cluster"
)

const (
	streamReceiverMonitorInterval = 5 * time.Second
)

type (
	StreamReceiverMonitor interface {
		common.Daemon
	}
	StreamReceiverMonitorImpl struct {
		ProcessToolBox
		enableStreaming bool

		status       int32
		shutdownOnce channel.ShutdownOnce

		sync.Mutex
		streams map[ClusterShardKeyPair]*StreamReceiver
	}
)

func NewStreamReceiverMonitor(
	processToolBox ProcessToolBox,
	enableStreaming bool,
) *StreamReceiverMonitorImpl {
	return &StreamReceiverMonitorImpl{
		ProcessToolBox:  processToolBox,
		enableStreaming: enableStreaming,

		status:       streamStatusInitialized,
		shutdownOnce: channel.NewShutdownOnce(),

		streams: make(map[ClusterShardKeyPair]*StreamReceiver),
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
	for targetKey, stream := range m.streams {
		stream.Stop()
		delete(m.streams, targetKey)
	}
	m.Logger.Info("StreamReceiverMonitor stopped.")
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
	m.reconcileStreams()

Loop:
	for !m.shutdownOnce.IsShutdown() {
		select {
		case <-clusterMetadataChangeChan:
			m.reconcileStreams()
		case <-ticker.C:
			m.reconcileStreams()
		case <-m.shutdownOnce.Channel():
			break Loop
		}
	}
}

func (m *StreamReceiverMonitorImpl) reconcileStreams() {
	streamKeys := m.generateStreamKeys()
	m.reconcileToTargetStreams(streamKeys)
}

func (m *StreamReceiverMonitorImpl) generateStreamKeys() map[ClusterShardKeyPair]struct{} {
	// NOTE: source / target are relative to stream itself, not replication data flow

	sourceClusterName := m.ClusterMetadata.GetCurrentClusterName()
	targetClusterNames := make(map[string]struct{})
	for clusterName, clusterInfo := range m.ClusterMetadata.GetAllClusterInfo() {
		if !clusterInfo.Enabled || clusterName == sourceClusterName {
			continue
		}
		targetClusterNames[clusterName] = struct{}{}
	}
	streamKeys := make(map[ClusterShardKeyPair]struct{})
	for _, shardID := range m.ShardController.ShardIDs() {
		for targetClusterName := range targetClusterNames {
			sourceShardID := shardID
			// TODO src shards !necessary= target shards, add conversion fn here
			targetShardID := shardID
			streamKeys[ClusterShardKeyPair{
				Source: NewClusterShardKey(sourceClusterName, sourceShardID),
				Target: NewClusterShardKey(targetClusterName, targetShardID),
			}] = struct{}{}
		}
	}
	return streamKeys
}

func (m *StreamReceiverMonitorImpl) reconcileToTargetStreams(
	streamKeys map[ClusterShardKeyPair]struct{},
) {
	m.Lock()
	defer m.Unlock()
	if m.shutdownOnce.IsShutdown() {
		return
	}

	for streamKey, stream := range m.streams {
		if !stream.IsValid() {
			stream.Stop()
			delete(m.streams, streamKey)
		}
		if _, ok := streamKeys[streamKey]; !ok {
			stream.Stop()
			delete(m.streams, streamKey)
		}
	}
	for streamKey := range streamKeys {
		if _, ok := m.streams[streamKey]; !ok {
			stream := NewStreamReceiver(
				m.ProcessToolBox,
				streamKey.Source,
				streamKey.Target,
			)
			stream.Start()
			m.streams[streamKey] = stream
		}
	}
}
