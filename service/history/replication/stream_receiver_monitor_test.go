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
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	repicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
)

type (
	streamReceiverMonitorSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		clientBean *client.MockBean

		sourceCluster string
		targetCluster string

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
	s.clientBean = client.NewMockBean(s.controller)

	s.sourceCluster = uuid.NewString()
	s.targetCluster = uuid.NewString()

	s.streamReceiverMonitor = NewStreamReceiverMonitor(
		ProcessToolBox{
			Config: configs.NewConfig(
				dynamicconfig.NewNoopCollection(),
				1,
				true,
				false,
			),
			ClientBean:     s.clientBean,
			MetricsHandler: metrics.NoopMetricsHandler,
			Logger:         log.NewNoopLogger(),
		},
		true,
	)
	streamClient := adminservicemock.NewMockAdminService_StreamWorkflowReplicationMessagesClient(s.controller)
	streamClient.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	streamClient.EXPECT().Recv().Return(&adminservice.StreamWorkflowReplicationMessagesResponse{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
			Messages: &repicationpb.WorkflowReplicationMessages{
				ReplicationTasks: []*repicationpb.ReplicationTask{},
				LastTaskId:       100,
				LastTaskTime:     timestamp.TimePtr(time.Unix(0, 100)),
			},
		},
	}, nil).AnyTimes()
	adminClient := adminservicemock.NewMockAdminServiceClient(s.controller)
	adminClient.EXPECT().StreamWorkflowReplicationMessages(gomock.Any()).Return(streamClient, nil).AnyTimes()
	s.clientBean.EXPECT().GetRemoteAdminClient(s.targetCluster).Return(adminClient, nil).AnyTimes()
}

func (s *streamReceiverMonitorSuite) TearDownTest() {
	s.controller.Finish()

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	for targetKey, stream := range s.streamReceiverMonitor.streams {
		stream.Stop()
		delete(s.streamReceiverMonitor.streams, targetKey)
	}
}

func (s *streamReceiverMonitorSuite) TestReconcileToTargetStreams_Add() {
	sourceKey := NewClusterShardKey(s.sourceCluster, rand.Int31())
	targetKey := NewClusterShardKey(s.targetCluster, rand.Int31())

	s.streamReceiverMonitor.Lock()
	s.Equal(0, len(s.streamReceiverMonitor.streams))
	s.streamReceiverMonitor.Unlock()

	streamKeys := map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Source: sourceKey,
			Target: targetKey,
		}: {},
	}
	s.streamReceiverMonitor.reconcileToTargetStreams(streamKeys)

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(1, len(s.streamReceiverMonitor.streams))
	stream, ok := s.streamReceiverMonitor.streams[ClusterShardKeyPair{
		Source: sourceKey,
		Target: targetKey,
	}]
	s.True(ok)
	s.True(stream.IsValid())
}

func (s *streamReceiverMonitorSuite) TestReconcileToTargetStreams_Remove() {
	sourceKey := NewClusterShardKey(s.sourceCluster, rand.Int31())
	targetKey := NewClusterShardKey(s.targetCluster, rand.Int31())
	stream := NewStreamReceiver(s.streamReceiverMonitor.ProcessToolBox, sourceKey, targetKey)

	s.streamReceiverMonitor.Lock()
	s.Equal(0, len(s.streamReceiverMonitor.streams))
	stream.Start()
	s.True(stream.IsValid())
	s.streamReceiverMonitor.streams[ClusterShardKeyPair{
		Source: sourceKey,
		Target: targetKey,
	}] = stream
	s.streamReceiverMonitor.Unlock()

	s.streamReceiverMonitor.reconcileToTargetStreams(map[ClusterShardKeyPair]struct{}{})

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(0, len(s.streamReceiverMonitor.streams))
	s.False(stream.IsValid())
}

func (s *streamReceiverMonitorSuite) TestReconcileToTargetStreams_Reactivate() {
	sourceKey := NewClusterShardKey(s.sourceCluster, rand.Int31())
	targetKey := NewClusterShardKey(s.targetCluster, rand.Int31())
	stream := NewStreamReceiver(s.streamReceiverMonitor.ProcessToolBox, sourceKey, targetKey)

	s.streamReceiverMonitor.Lock()
	s.Equal(0, len(s.streamReceiverMonitor.streams))
	stream.Start()
	stream.Stop()
	s.False(stream.IsValid())
	s.streamReceiverMonitor.streams[ClusterShardKeyPair{
		Source: sourceKey,
		Target: targetKey,
	}] = stream
	s.streamReceiverMonitor.Unlock()

	s.streamReceiverMonitor.reconcileToTargetStreams(map[ClusterShardKeyPair]struct{}{
		ClusterShardKeyPair{
			Source: sourceKey,
			Target: targetKey,
		}: {},
	})

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Equal(1, len(s.streamReceiverMonitor.streams))
	stream, ok := s.streamReceiverMonitor.streams[ClusterShardKeyPair{
		Source: sourceKey,
		Target: targetKey,
	}]
	s.True(ok)
	s.True(stream.IsValid())
}
