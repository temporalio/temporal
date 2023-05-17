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
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	repicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

type (
	streamReceiverMonitorSuite struct {
		suite.Suite
		*require.Assertions

		controller      *gomock.Controller
		clusterMetadata *cluster.MockMetadata
		clientBean      *client.MockBean
		shardController *shard.MockController

		clientClusterID int32
		serverClusterID int32

		streamReceiverMonitor *StreamReceiverMonitorImpl
	}
)

func TestStreamReceiverMonitorSuite(t *testing.T) {
	s := new(streamReceiverMonitorSuite)
	suite.Run(t, s)
}

func (s *streamReceiverMonitorSuite) SetupSuite() {

}

func (s *streamReceiverMonitorSuite) TearDownSuite() {

}

func (s *streamReceiverMonitorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)

	s.clientClusterID = int32(cluster.TestCurrentClusterInitialFailoverVersion)
	s.serverClusterID = int32(cluster.TestAlternativeClusterInitialFailoverVersion)

	s.streamReceiverMonitor = NewStreamReceiverMonitor(
		ProcessToolBox{
			Config: configs.NewConfig(
				dynamicconfig.NewNoopCollection(),
				1,
				true,
				false,
			),
			ClusterMetadata: s.clusterMetadata,
			ClientBean:      s.clientBean,
			ShardController: s.shardController,
			MetricsHandler:  metrics.NoopMetricsHandler,
			Logger:          log.NewNoopLogger(),
		},
		true,
	)
	streamClient := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesClient(s.controller)
	streamClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	streamClient.EXPECT().Recv().Return(&adminservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &repicationpb.WorkflowReplicationMessages{
				ReplicationTasks:           []*repicationpb.ReplicationTask{},
				ExclusiveHighWatermark:     100,
				ExclusiveHighWatermarkTime: timestamp.TimePtr(time.Unix(0, 100)),
			},
		},
	}, nil).AnyTimes()
	adminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	adminClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(streamClient, nil).AnyTimes()
	s.clientBean.EXPECT().GetRemoteAdminClient(cluster.TestAlternativeClusterName).Return(adminClient, nil).AnyTimes()
}

func (s *streamReceiverMonitorSuite) TearDownTest() {
	s.controller.Finish()

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	for serverKey, stream := range s.streamReceiverMonitor.streams {
		stream.Stop()
		delete(s.streamReceiverMonitor.streams, serverKey)
	}
}

func (s *streamReceiverMonitorSuite) TestGenerateStreamKeys_1To4() {
	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(s.clientClusterID)).AnyTimes()
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(s.clientClusterID),
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             1,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(s.serverClusterID),
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             4,
		},
	}).AnyTimes()
	s.shardController.EXPECT().ShardIDs().Return([]int32{1}).AnyTimes()

	streamKeys := s.streamReceiverMonitor.generateStreamKeys()
	s.Equal(map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(s.clientClusterID, 1),
			Server: NewClusterShardKey(s.serverClusterID, 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(s.clientClusterID, 1),
			Server: NewClusterShardKey(s.serverClusterID, 2),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(s.clientClusterID, 1),
			Server: NewClusterShardKey(s.serverClusterID, 3),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(s.clientClusterID, 1),
			Server: NewClusterShardKey(s.serverClusterID, 4),
		}: {},
	}, streamKeys)
}

func (s *streamReceiverMonitorSuite) TestGenerateStreamKeys_4To1() {
	s.clusterMetadata.EXPECT().GetClusterID().Return(int64(s.clientClusterID)).AnyTimes()
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(s.clientClusterID),
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             4,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: int64(s.serverClusterID),
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             1,
		},
	}).AnyTimes()
	s.shardController.EXPECT().ShardIDs().Return([]int32{1, 2, 3, 4}).AnyTimes()

	streamKeys := s.streamReceiverMonitor.generateStreamKeys()
	s.Equal(map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(s.clientClusterID, 1),
			Server: NewClusterShardKey(s.serverClusterID, 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(s.clientClusterID, 2),
			Server: NewClusterShardKey(s.serverClusterID, 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(s.clientClusterID, 3),
			Server: NewClusterShardKey(s.serverClusterID, 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(s.clientClusterID, 4),
			Server: NewClusterShardKey(s.serverClusterID, 1),
		}: {},
	}, streamKeys)
}

func (s *streamReceiverMonitorSuite) TestDoReconcileStreams_Add() {
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(s.clientClusterID, rand.Int31())
	serverKey := NewClusterShardKey(s.serverClusterID, rand.Int31())

	s.streamReceiverMonitor.Lock()
	s.Equal(0, len(s.streamReceiverMonitor.streams))
	s.streamReceiverMonitor.Unlock()

	streamKeys := map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: clientKey,
			Server: serverKey,
		}: {},
	}
	s.streamReceiverMonitor.doReconcileStreams(streamKeys)

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(1, len(s.streamReceiverMonitor.streams))
	stream, ok := s.streamReceiverMonitor.streams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}]
	s.True(ok)
	s.True(stream.IsValid())
}

func (s *streamReceiverMonitorSuite) TestDoReconcileStreams_Remove() {
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(s.clientClusterID, rand.Int31())
	serverKey := NewClusterShardKey(s.serverClusterID, rand.Int31())
	stream := NewStreamReceiver(s.streamReceiverMonitor.ProcessToolBox, clientKey, serverKey)

	s.streamReceiverMonitor.Lock()
	s.Equal(0, len(s.streamReceiverMonitor.streams))
	stream.Start()
	s.True(stream.IsValid())
	s.streamReceiverMonitor.streams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}] = stream
	s.streamReceiverMonitor.Unlock()

	s.streamReceiverMonitor.doReconcileStreams(map[ClusterShardKeyPair]struct{}{})

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(0, len(s.streamReceiverMonitor.streams))
	s.False(stream.IsValid())
}

func (s *streamReceiverMonitorSuite) TestDoReconcileStreams_Reactivate() {
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(s.clientClusterID, rand.Int31())
	serverKey := NewClusterShardKey(s.serverClusterID, rand.Int31())
	stream := NewStreamReceiver(s.streamReceiverMonitor.ProcessToolBox, clientKey, serverKey)

	s.streamReceiverMonitor.Lock()
	s.Equal(0, len(s.streamReceiverMonitor.streams))
	stream.Start()
	stream.Stop()
	s.False(stream.IsValid())
	s.streamReceiverMonitor.streams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}] = stream
	s.streamReceiverMonitor.Unlock()

	s.streamReceiverMonitor.doReconcileStreams(map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: clientKey,
			Server: serverKey,
		}: {},
	})

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(1, len(s.streamReceiverMonitor.streams))
	stream, ok := s.streamReceiverMonitor.streams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}]
	s.True(ok)
	s.True(stream.IsValid())
}
