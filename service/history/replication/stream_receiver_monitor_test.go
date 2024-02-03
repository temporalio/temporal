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
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	repicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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

	processToolBox := ProcessToolBox{
		Config: configs.NewConfig(
			dynamicconfig.NewNoopCollection(),
			1,
		),
		ClusterMetadata: s.clusterMetadata,
		ClientBean:      s.clientBean,
		ShardController: s.shardController,
		MetricsHandler:  metrics.NoopMetricsHandler,
		Logger:          log.NewNoopLogger(),
		DLQWriter:       NoopDLQWriter{},
	}
	s.streamReceiverMonitor = NewStreamReceiverMonitor(
		processToolBox,
		NewExecutableTaskConverter(processToolBox),
		true,
	)
	streamClient := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesClient(s.controller)
	streamClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	streamClient.EXPECT().Recv().Return(&adminservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &repicationpb.WorkflowReplicationMessages{
				ReplicationTasks:           []*repicationpb.ReplicationTask{},
				ExclusiveHighWatermark:     100,
				ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, 100)),
			},
		},
	}, nil).AnyTimes()
	streamClient.EXPECT().CloseSend().Return(nil).AnyTimes()
	adminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	adminClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(streamClient, nil).AnyTimes()
	s.clientBean.EXPECT().GetRemoteAdminClient(cluster.TestAlternativeClusterName).Return(adminClient, nil).AnyTimes()
}

func (s *streamReceiverMonitorSuite) TearDownTest() {
	s.controller.Finish()

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	for serverKey, stream := range s.streamReceiverMonitor.outboundStreams {
		stream.Stop()
		delete(s.streamReceiverMonitor.outboundStreams, serverKey)
	}
}

func (s *streamReceiverMonitorSuite) TestGenerateInboundStreamKeys_1From4() {
	s.clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestAlternativeClusterInitialFailoverVersion).AnyTimes()
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             1,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             4,
		},
	}).AnyTimes()
	s.shardController.EXPECT().ShardIDs().Return([]int32{1}).AnyTimes()

	streamKeys := s.streamReceiverMonitor.generateInboundStreamKeys()
	s.Equal(map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 2),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 3),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 4),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
	}, streamKeys)
}

func (s *streamReceiverMonitorSuite) TestGenerateInboundStreamKeys_4From1() {
	s.clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestAlternativeClusterInitialFailoverVersion).AnyTimes()
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             4,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             1,
		},
	}).AnyTimes()
	s.shardController.EXPECT().ShardIDs().Return([]int32{1, 2, 3, 4}).AnyTimes()

	streamKeys := s.streamReceiverMonitor.generateInboundStreamKeys()
	s.Equal(map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 2),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 3),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 4),
		}: {},
	}, streamKeys)
}

func (s *streamReceiverMonitorSuite) TestGenerateOutboundStreamKeys_1To4() {
	s.clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             1,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             4,
		},
	}).AnyTimes()
	s.shardController.EXPECT().ShardIDs().Return([]int32{1}).AnyTimes()

	streamKeys := s.streamReceiverMonitor.generateOutboundStreamKeys()
	s.Equal(map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 2),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 3),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 4),
		}: {},
	}, streamKeys)
}

func (s *streamReceiverMonitorSuite) TestGenerateOutboundStreamKeys_4To1() {
	s.clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             4,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             1,
		},
	}).AnyTimes()
	s.shardController.EXPECT().ShardIDs().Return([]int32{1, 2, 3, 4}).AnyTimes()

	streamKeys := s.streamReceiverMonitor.generateOutboundStreamKeys()
	s.Equal(map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 2),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 3),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 4),
			Server: NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), 1),
		}: {},
	}, streamKeys)
}

func (s *streamReceiverMonitorSuite) TestDoReconcileInboundStreams_Add() {
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())

	s.streamReceiverMonitor.Lock()
	s.Equal(0, len(s.streamReceiverMonitor.inboundStreams))
	s.streamReceiverMonitor.Unlock()

	streamKeys := map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: clientKey,
			Server: serverKey,
		}: {},
	}
	streamSender := NewMockStreamSender(s.controller)
	streamSender.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamSender.EXPECT().IsValid().Return(true)
	s.streamReceiverMonitor.RegisterInboundStream(streamSender)
	s.streamReceiverMonitor.doReconcileInboundStreams(streamKeys)

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(1, len(s.streamReceiverMonitor.inboundStreams))
	stream, ok := s.streamReceiverMonitor.inboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}]
	s.True(ok)
	s.Equal(streamSender, stream)
}

func (s *streamReceiverMonitorSuite) TestDoReconcileInboundStreams_Remove() {
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	streamSender := NewMockStreamSender(s.controller)
	streamSender.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamSender.EXPECT().IsValid().Return(false)
	streamSender.EXPECT().Stop()
	s.streamReceiverMonitor.RegisterInboundStream(streamSender)

	s.streamReceiverMonitor.Lock()
	s.Equal(1, len(s.streamReceiverMonitor.inboundStreams))
	s.streamReceiverMonitor.Unlock()

	s.streamReceiverMonitor.doReconcileInboundStreams(map[ClusterShardKeyPair]struct{}{})

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(0, len(s.streamReceiverMonitor.inboundStreams))
}

func (s *streamReceiverMonitorSuite) TestDoReconcileInboundStreams_Reactivate() {
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	streamSenderStale := NewMockStreamSender(s.controller)
	streamSenderStale.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamSenderStale.EXPECT().Stop()
	s.streamReceiverMonitor.RegisterInboundStream(streamSenderStale)

	s.streamReceiverMonitor.Lock()
	s.Equal(1, len(s.streamReceiverMonitor.inboundStreams))
	s.streamReceiverMonitor.Unlock()

	streamSenderValid := NewMockStreamSender(s.controller)
	streamSenderValid.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	s.streamReceiverMonitor.RegisterInboundStream(streamSenderValid)

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(1, len(s.streamReceiverMonitor.inboundStreams))
	stream, ok := s.streamReceiverMonitor.inboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}]
	s.True(ok)
	s.Equal(streamSenderValid, stream)
}

func (s *streamReceiverMonitorSuite) TestDoReconcileOutboundStreams_Add() {
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.clusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).Return("some cluster name").AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())

	s.streamReceiverMonitor.Lock()
	s.Equal(0, len(s.streamReceiverMonitor.outboundStreams))
	s.streamReceiverMonitor.Unlock()

	streamKeys := map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: clientKey,
			Server: serverKey,
		}: {},
	}
	s.streamReceiverMonitor.doReconcileOutboundStreams(streamKeys)

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(1, len(s.streamReceiverMonitor.outboundStreams))
	stream, ok := s.streamReceiverMonitor.outboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}]
	s.True(ok)
	s.True(stream.IsValid())
}

func (s *streamReceiverMonitorSuite) TestDoReconcileOutboundStreams_Remove() {
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	streamReceiver := NewMockStreamReceiver(s.controller)
	streamReceiver.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamReceiver.EXPECT().IsValid().Return(true)
	streamReceiver.EXPECT().Stop()

	s.streamReceiverMonitor.Lock()
	s.Equal(0, len(s.streamReceiverMonitor.outboundStreams))
	s.streamReceiverMonitor.outboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}] = streamReceiver
	s.streamReceiverMonitor.Unlock()

	s.streamReceiverMonitor.doReconcileOutboundStreams(map[ClusterShardKeyPair]struct{}{})

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(0, len(s.streamReceiverMonitor.outboundStreams))
}

func (s *streamReceiverMonitorSuite) TestDoReconcileOutboundStreams_Reactivate() {
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.clusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).Return("some cluster name").AnyTimes()

	clientKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	streamReceiverStale := NewMockStreamReceiver(s.controller)
	streamReceiverStale.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamReceiverStale.EXPECT().IsValid().Return(false)
	streamReceiverStale.EXPECT().Stop()

	s.streamReceiverMonitor.Lock()
	s.Equal(0, len(s.streamReceiverMonitor.outboundStreams))
	s.streamReceiverMonitor.outboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}] = streamReceiverStale
	s.streamReceiverMonitor.Unlock()

	s.streamReceiverMonitor.doReconcileOutboundStreams(map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Client: clientKey,
			Server: serverKey,
		}: {},
	})

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(1, len(s.streamReceiverMonitor.outboundStreams))
	stream, ok := s.streamReceiverMonitor.outboundStreams[ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}]
	s.True(ok)
	s.True(stream.IsValid())
}
