package replication

import (
	"errors"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			Messages: &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks:           []*replicationspb.ReplicationTask{},
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

func (s *streamReceiverMonitorSuite) TestStop_StopsInboundStreams() {
	clientKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	streamSender := NewMockStreamSender(s.controller)
	streamSender.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	streamSender.EXPECT().Stop()
	s.streamReceiverMonitor.RegisterInboundStream(streamSender)

	s.streamReceiverMonitor.Lock()
	s.Len(s.streamReceiverMonitor.inboundStreams, 1)
	s.streamReceiverMonitor.Unlock()

	// Transition to started state so Stop() proceeds past the CAS check.
	s.streamReceiverMonitor.status = common.DaemonStatusStarted
	s.streamReceiverMonitor.Stop()

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Empty(s.streamReceiverMonitor.inboundStreams)
}

func (s *streamReceiverMonitorSuite) TestStop_SkipsInboundStreamsWhenFlagDisabled() {
	col := dynamicconfig.NewCollection(
		dynamicconfig.StaticClient(map[dynamicconfig.Key]any{
			dynamicconfig.EnableCloseInboundReplicationStreamOnShutdown.Key(): false,
		}),
		log.NewNoopLogger(),
	)
	monitor := NewStreamReceiverMonitor(
		ProcessToolBox{
			Config:          configs.NewConfig(col, 1),
			ClusterMetadata: s.clusterMetadata,
			ClientBean:      s.clientBean,
			ShardController: s.shardController,
			MetricsHandler:  metrics.NoopMetricsHandler,
			Logger:          log.NewNoopLogger(),
			DLQWriter:       NoopDLQWriter{},
		},
		NewExecutableTaskConverter(ProcessToolBox{
			Config:          configs.NewConfig(col, 1),
			ClusterMetadata: s.clusterMetadata,
			ClientBean:      s.clientBean,
			ShardController: s.shardController,
			MetricsHandler:  metrics.NoopMetricsHandler,
			Logger:          log.NewNoopLogger(),
			DLQWriter:       NoopDLQWriter{},
		}),
		true,
	)

	clientKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	serverKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	streamSender := NewMockStreamSender(s.controller)
	streamSender.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: clientKey,
		Server: serverKey,
	}).AnyTimes()
	// Stop should NOT be called on inbound streams when the flag is disabled.
	monitor.RegisterInboundStream(streamSender)

	monitor.Lock()
	s.Len(monitor.inboundStreams, 1)
	monitor.Unlock()

	monitor.status = common.DaemonStatusStarted
	monitor.Stop()

	monitor.Lock()
	defer monitor.Unlock()
	s.Len(monitor.inboundStreams, 1, "inbound streams should not be closed when flag is disabled")
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

func (s *streamReceiverMonitorSuite) TestGenerateStatusMap_Success() {
	inboundKeys := make(map[ClusterShardKeyPair]struct{})
	key1 := ClusterShardKeyPair{
		Client: NewClusterShardKey(2, 1),
		Server: NewClusterShardKey(1, 1),
	}
	key2 := ClusterShardKeyPair{
		Client: NewClusterShardKey(3, 1),
		Server: NewClusterShardKey(1, 1),
	}
	key3 := ClusterShardKeyPair{
		Client: NewClusterShardKey(4, 2),
		Server: NewClusterShardKey(1, 2),
	}
	inboundKeys[key1] = struct{}{}
	inboundKeys[key2] = struct{}{}
	inboundKeys[key3] = struct{}{}
	ctx1 := historyi.NewMockShardContext(s.controller)
	ctx2 := historyi.NewMockShardContext(s.controller)
	engine1 := historyi.NewMockEngine(s.controller)
	engine2 := historyi.NewMockEngine(s.controller)
	engine1.EXPECT().GetMaxReplicationTaskInfo().Return(int64(1000), time.Now())
	engine2.EXPECT().GetMaxReplicationTaskInfo().Return(int64(2000), time.Now())
	readerId1 := shard.ReplicationReaderIDFromClusterShardID(int64(key1.Client.ClusterID), key1.Client.ShardID)
	ackLevel1 := int64(100)
	ackLevel2 := int64(200)
	ackLevel3 := int64(300)
	readerId2 := shard.ReplicationReaderIDFromClusterShardID(int64(key2.Client.ClusterID), key2.Client.ShardID)
	readerId3 := shard.ReplicationReaderIDFromClusterShardID(int64(key3.Client.ClusterID), key3.Client.ShardID)
	queueState1 := &persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerId1: {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackLevel1),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				}},
			},
			readerId2: {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackLevel2),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				}},
			},
		},
	}
	queueState2 := &persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerId3: {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackLevel3),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				}},
			},
		},
	}
	ctx1.EXPECT().GetQueueState(tasks.CategoryReplication).Return(queueState1, true)
	ctx2.EXPECT().GetQueueState(tasks.CategoryReplication).Return(queueState2, true)
	s.shardController.EXPECT().GetShardByID(int32(1)).Return(ctx1, nil)
	s.shardController.EXPECT().GetShardByID(int32(2)).Return(ctx2, nil)
	ctx1.EXPECT().GetEngine(gomock.Any()).Return(engine1, nil)
	ctx2.EXPECT().GetEngine(gomock.Any()).Return(engine2, nil)
	statusMap := s.streamReceiverMonitor.generateStatusMap(inboundKeys)
	s.Equal(map[ClusterShardKeyPair]*streamStatus{
		ClusterShardKeyPair{
			Client: NewClusterShardKey(2, 1),
			Server: NewClusterShardKey(1, 1),
		}: {
			defaultAckLevel:      ackLevel1,
			maxReplicationTaskId: int64(1000),
			isTieredStackEnabled: false,
		},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(3, 1),
			Server: NewClusterShardKey(1, 1),
		}: {
			defaultAckLevel:      ackLevel2,
			maxReplicationTaskId: int64(1000),
			isTieredStackEnabled: false,
		},
		ClusterShardKeyPair{
			Client: NewClusterShardKey(4, 2),
			Server: NewClusterShardKey(1, 2),
		}: {
			defaultAckLevel:      ackLevel3,
			maxReplicationTaskId: int64(2000),
			isTieredStackEnabled: false,
		},
	}, statusMap)
}

func (s *streamReceiverMonitorSuite) TestEvaluateStreamStatus() {
	keyPair := &ClusterShardKeyPair{
		Client: NewClusterShardKey(2, 1),
		Server: NewClusterShardKey(1, 1),
	}

	s.True(s.streamReceiverMonitor.evaluateSingleStreamConnection(keyPair,
		&streamStatus{
			defaultAckLevel:      100,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: false,
		},
		&streamStatus{
			defaultAckLevel:      50,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: false,
		},
	),
	)

	s.False(s.streamReceiverMonitor.evaluateSingleStreamConnection(keyPair,
		&streamStatus{
			defaultAckLevel:      50,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: false,
		},
		&streamStatus{
			defaultAckLevel:      50,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: false,
		},
	),
	)

	s.False(s.streamReceiverMonitor.evaluateSingleStreamConnection(keyPair,
		&streamStatus{
			highPriorityAckLevel: 100,
			lowPriorityAckLevel:  20,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: true,
		},
		&streamStatus{
			highPriorityAckLevel: 50,
			lowPriorityAckLevel:  20,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: true,
		},
	),
	)

	s.False(s.streamReceiverMonitor.evaluateSingleStreamConnection(keyPair,
		&streamStatus{
			highPriorityAckLevel: 20,
			lowPriorityAckLevel:  50,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: true,
		},
		&streamStatus{
			highPriorityAckLevel: 20,
			lowPriorityAckLevel:  20,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: true,
		},
	),
	)

	s.True(s.streamReceiverMonitor.evaluateSingleStreamConnection(keyPair,
		&streamStatus{
			highPriorityAckLevel: 51,
			lowPriorityAckLevel:  21,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: true,
		},
		&streamStatus{
			highPriorityAckLevel: 50,
			lowPriorityAckLevel:  20,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: true,
		},
	),
	)
}

func (s *streamReceiverMonitorSuite) TestEvaluateSingleStreamConnection_NilStatus() {
	srMonitorKeyPair := &ClusterShardKeyPair{
		Client: NewClusterShardKey(2, 1),
		Server: NewClusterShardKey(1, 1),
	}
	srMonitorCurrent := &streamStatus{
		defaultAckLevel:      50,
		maxReplicationTaskId: 1000,
		isTieredStackEnabled: false,
	}
	// previous is nil
	s.True(s.streamReceiverMonitor.evaluateSingleStreamConnection(srMonitorKeyPair, srMonitorCurrent, nil))
	// current is nil
	s.True(s.streamReceiverMonitor.evaluateSingleStreamConnection(srMonitorKeyPair, nil, srMonitorCurrent))
}

func (s *streamReceiverMonitorSuite) TestEvaluateSingleStreamConnection_TieredStackConfigChange() {
	srMonitorKeyPair := &ClusterShardKeyPair{
		Client: NewClusterShardKey(2, 1),
		Server: NewClusterShardKey(1, 1),
	}
	// tiered stack config changed between data points; should return true and skip progress evaluation.
	s.True(s.streamReceiverMonitor.evaluateSingleStreamConnection(srMonitorKeyPair,
		&streamStatus{
			defaultAckLevel:      50,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: true,
		},
		&streamStatus{
			defaultAckLevel:      50,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: false,
		},
	))
}

func (s *streamReceiverMonitorSuite) TestEvaluateStreamStatus_IteratesAllKeys() {
	srMonitorKey1 := ClusterShardKeyPair{
		Client: NewClusterShardKey(2, 1),
		Server: NewClusterShardKey(1, 1),
	}
	srMonitorKey2 := ClusterShardKeyPair{
		Client: NewClusterShardKey(3, 1),
		Server: NewClusterShardKey(1, 1),
	}
	s.streamReceiverMonitor.previousStatus = map[ClusterShardKeyPair]*streamStatus{
		srMonitorKey1: {
			defaultAckLevel:      50,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: false,
		},
		srMonitorKey2: {
			defaultAckLevel:      50,
			maxReplicationTaskId: 500,
			isTieredStackEnabled: false,
		},
	}
	// key1 makes progress, key2 stuck. Should not panic; just exercises iteration.
	s.streamReceiverMonitor.evaluateStreamStatus(map[ClusterShardKeyPair]*streamStatus{
		srMonitorKey1: {
			defaultAckLevel:      100,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: false,
		},
		srMonitorKey2: {
			defaultAckLevel:      50,
			maxReplicationTaskId: 1000,
			isTieredStackEnabled: false,
		},
	})
}

func (s *streamReceiverMonitorSuite) srMonitorSetupInboundKeysExpectations() {
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
			ShardCount:             1,
		},
	}).AnyTimes()
	s.shardController.EXPECT().ShardIDs().Return([]int32{1}).AnyTimes()
}

func (s *streamReceiverMonitorSuite) TestMonitorStreamStatus_FirstRunSeedsPreviousStatus() {
	s.srMonitorSetupInboundKeysExpectations()

	srMonitorCtx := historyi.NewMockShardContext(s.controller)
	srMonitorEngine := historyi.NewMockEngine(s.controller)
	srMonitorEngine.EXPECT().GetMaxReplicationTaskInfo().Return(int64(1000), time.Now()).AnyTimes()
	srMonitorClientKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), 1)
	srMonitorReaderID := shard.ReplicationReaderIDFromClusterShardID(int64(srMonitorClientKey.ClusterID), srMonitorClientKey.ShardID)
	srMonitorQueueState := &persistencespb.QueueState{
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			srMonitorReaderID: {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(100)),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(math.MaxInt64)),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				}},
			},
		},
	}
	srMonitorCtx.EXPECT().GetEngine(gomock.Any()).Return(srMonitorEngine, nil).AnyTimes()
	srMonitorCtx.EXPECT().GetQueueState(tasks.CategoryReplication).Return(srMonitorQueueState, true).AnyTimes()
	s.shardController.EXPECT().GetShardByID(int32(1)).Return(srMonitorCtx, nil).AnyTimes()

	s.Nil(s.streamReceiverMonitor.previousStatus)
	// First run seeds previousStatus and returns early.
	s.streamReceiverMonitor.monitorStreamStatus()
	s.NotNil(s.streamReceiverMonitor.previousStatus)
	s.Len(s.streamReceiverMonitor.previousStatus, 1)

	// Second run evaluates against previous status.
	s.streamReceiverMonitor.monitorStreamStatus()
	s.NotNil(s.streamReceiverMonitor.previousStatus)
}

func (s *streamReceiverMonitorSuite) TestMonitorStreamStatus_ShutdownNoop() {
	s.streamReceiverMonitor.shutdownOnce.Shutdown()
	s.streamReceiverMonitor.monitorStreamStatus()
	s.Nil(s.streamReceiverMonitor.previousStatus)
}

func (s *streamReceiverMonitorSuite) TestReconcileInboundStreams() {
	s.srMonitorSetupInboundKeysExpectations()
	// No inbound streams registered; should be a no-op that exercises the wrapper.
	s.streamReceiverMonitor.reconcileInboundStreams()
	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Empty(s.streamReceiverMonitor.inboundStreams)
}

func (s *streamReceiverMonitorSuite) TestReconcileOutboundStreams() {
	s.clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             1,
		},
	}).AnyTimes()
	s.shardController.EXPECT().ShardIDs().Return([]int32{1}).AnyTimes()
	// Only the current cluster present, so no outbound stream keys are generated.
	s.streamReceiverMonitor.reconcileOutboundStreams()
	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Empty(s.streamReceiverMonitor.outboundStreams)
}

func (s *streamReceiverMonitorSuite) TestDoReconcileInboundStreams_ShutdownNoop() {
	srMonitorClientKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	srMonitorServerKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	streamSender := NewMockStreamSender(s.controller)
	streamSender.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: srMonitorClientKey,
		Server: srMonitorServerKey,
	}).AnyTimes()
	s.streamReceiverMonitor.RegisterInboundStream(streamSender)

	s.streamReceiverMonitor.shutdownOnce.Shutdown()
	// After shutdown, doReconcile is a no-op: stream is not stopped or removed.
	s.streamReceiverMonitor.doReconcileInboundStreams(map[ClusterShardKeyPair]struct{}{})

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Len(s.streamReceiverMonitor.inboundStreams, 1)
}

func (s *streamReceiverMonitorSuite) TestDoReconcileInboundStreams_RemoveValidNotInKeys() {
	srMonitorClientKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	srMonitorServerKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	streamSender := NewMockStreamSender(s.controller)
	streamSender.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: srMonitorClientKey,
		Server: srMonitorServerKey,
	}).AnyTimes()
	streamSender.EXPECT().IsValid().Return(true)
	streamSender.EXPECT().Stop()
	s.streamReceiverMonitor.RegisterInboundStream(streamSender)

	// Valid stream but not in the desired key set: should be stopped and removed.
	s.streamReceiverMonitor.doReconcileInboundStreams(map[ClusterShardKeyPair]struct{}{})

	s.streamReceiverMonitor.Lock()
	defer s.streamReceiverMonitor.Unlock()
	s.Empty(s.streamReceiverMonitor.inboundStreams)
}

func (s *streamReceiverMonitorSuite) TestDoReconcileOutboundStreams_ShutdownNoop() {
	srMonitorClientKey := NewClusterShardKey(int32(cluster.TestCurrentClusterInitialFailoverVersion), rand.Int31())
	srMonitorServerKey := NewClusterShardKey(int32(cluster.TestAlternativeClusterInitialFailoverVersion), rand.Int31())
	streamReceiver := NewMockStreamReceiver(s.controller)
	streamReceiver.EXPECT().Key().Return(ClusterShardKeyPair{
		Client: srMonitorClientKey,
		Server: srMonitorServerKey,
	}).AnyTimes()

	s.streamReceiverMonitor.Lock()
	s.streamReceiverMonitor.outboundStreams[ClusterShardKeyPair{
		Client: srMonitorClientKey,
		Server: srMonitorServerKey,
	}] = streamReceiver
	s.streamReceiverMonitor.Unlock()

	s.streamReceiverMonitor.shutdownOnce.Shutdown()
	// After shutdown, doReconcile is a no-op: stream is not stopped, added, or removed.
	s.streamReceiverMonitor.doReconcileOutboundStreams(map[ClusterShardKeyPair]struct{}{})

	s.streamReceiverMonitor.Lock()
	s.Len(s.streamReceiverMonitor.outboundStreams, 1)
	// Remove the mock stream so TearDownTest does not call Stop on it.
	delete(s.streamReceiverMonitor.outboundStreams, ClusterShardKeyPair{
		Client: srMonitorClientKey,
		Server: srMonitorServerKey,
	})
	s.streamReceiverMonitor.Unlock()
}

func (s *streamReceiverMonitorSuite) TestFillStatusMap_GetShardByIDError() {
	srMonitorServerKey := NewClusterShardKey(1, 1)
	s.shardController.EXPECT().GetShardByID(int32(1)).Return(nil, errors.New("shard error"))
	srMonitorStatusMap := make(map[ClusterShardKeyPair]*streamStatus)
	s.streamReceiverMonitor.fillStatusMap(srMonitorStatusMap, srMonitorServerKey, []ClusterShardKey{NewClusterShardKey(2, 1)})
	s.Empty(srMonitorStatusMap)
}

func (s *streamReceiverMonitorSuite) TestFillStatusMap_GetEngineError() {
	srMonitorServerKey := NewClusterShardKey(1, 1)
	srMonitorCtx := historyi.NewMockShardContext(s.controller)
	srMonitorCtx.EXPECT().GetEngine(gomock.Any()).Return(nil, errors.New("engine error"))
	s.shardController.EXPECT().GetShardByID(int32(1)).Return(srMonitorCtx, nil)
	srMonitorStatusMap := make(map[ClusterShardKeyPair]*streamStatus)
	s.streamReceiverMonitor.fillStatusMap(srMonitorStatusMap, srMonitorServerKey, []ClusterShardKey{NewClusterShardKey(2, 1)})
	s.Empty(srMonitorStatusMap)
}

func (s *streamReceiverMonitorSuite) TestFillStatusMap_GetQueueStateMissing() {
	srMonitorServerKey := NewClusterShardKey(1, 1)
	srMonitorCtx := historyi.NewMockShardContext(s.controller)
	srMonitorEngine := historyi.NewMockEngine(s.controller)
	srMonitorEngine.EXPECT().GetMaxReplicationTaskInfo().Return(int64(1000), time.Now())
	srMonitorCtx.EXPECT().GetEngine(gomock.Any()).Return(srMonitorEngine, nil)
	srMonitorCtx.EXPECT().GetQueueState(tasks.CategoryReplication).Return(nil, false)
	s.shardController.EXPECT().GetShardByID(int32(1)).Return(srMonitorCtx, nil)
	srMonitorStatusMap := make(map[ClusterShardKeyPair]*streamStatus)
	s.streamReceiverMonitor.fillStatusMap(srMonitorStatusMap, srMonitorServerKey, []ClusterShardKey{NewClusterShardKey(2, 1)})
	s.Empty(srMonitorStatusMap)
}

func (s *streamReceiverMonitorSuite) TestFillStatusMap_ReaderStateMissing() {
	srMonitorServerKey := NewClusterShardKey(1, 1)
	srMonitorClientKey := NewClusterShardKey(2, 1)
	srMonitorCtx := historyi.NewMockShardContext(s.controller)
	srMonitorEngine := historyi.NewMockEngine(s.controller)
	srMonitorEngine.EXPECT().GetMaxReplicationTaskInfo().Return(int64(1000), time.Now())
	srMonitorCtx.EXPECT().GetEngine(gomock.Any()).Return(srMonitorEngine, nil)
	// Queue state present but with no matching reader state for the client key.
	srMonitorCtx.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ReaderStates: map[int64]*persistencespb.QueueReaderState{},
	}, true)
	s.shardController.EXPECT().GetShardByID(int32(1)).Return(srMonitorCtx, nil)
	srMonitorStatusMap := make(map[ClusterShardKeyPair]*streamStatus)
	s.streamReceiverMonitor.fillStatusMap(srMonitorStatusMap, srMonitorServerKey, []ClusterShardKey{srMonitorClientKey})
	s.Len(srMonitorStatusMap, 1)
	srMonitorStatus := srMonitorStatusMap[ClusterShardKeyPair{Client: srMonitorClientKey, Server: srMonitorServerKey}]
	s.NotNil(srMonitorStatus)
	s.Equal(int64(1000), srMonitorStatus.maxReplicationTaskId)
	s.False(srMonitorStatus.isTieredStackEnabled)
}

func (s *streamReceiverMonitorSuite) TestFillStatusMap_TieredStackThreeScopes() {
	srMonitorServerKey := NewClusterShardKey(1, 1)
	srMonitorClientKey := NewClusterShardKey(2, 1)
	srMonitorReaderID := shard.ReplicationReaderIDFromClusterShardID(int64(srMonitorClientKey.ClusterID), srMonitorClientKey.ShardID)
	srMonitorCtx := historyi.NewMockShardContext(s.controller)
	srMonitorEngine := historyi.NewMockEngine(s.controller)
	srMonitorEngine.EXPECT().GetMaxReplicationTaskInfo().Return(int64(5000), time.Now())
	srMonitorScope := func(ackLevel int64) *persistencespb.QueueSliceScope {
		return &persistencespb.QueueSliceScope{
			Range: &persistencespb.QueueSliceRange{
				InclusiveMin: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(ackLevel)),
				ExclusiveMax: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(math.MaxInt64)),
			},
			Predicate: &persistencespb.Predicate{
				PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
				Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
			},
		}
	}
	srMonitorCtx.EXPECT().GetEngine(gomock.Any()).Return(srMonitorEngine, nil)
	srMonitorCtx.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			srMonitorReaderID: {
				Scopes: []*persistencespb.QueueSliceScope{
					srMonitorScope(100),
					srMonitorScope(200),
					srMonitorScope(300),
				},
			},
		},
	}, true)
	s.shardController.EXPECT().GetShardByID(int32(1)).Return(srMonitorCtx, nil)
	srMonitorStatusMap := make(map[ClusterShardKeyPair]*streamStatus)
	s.streamReceiverMonitor.fillStatusMap(srMonitorStatusMap, srMonitorServerKey, []ClusterShardKey{srMonitorClientKey})
	s.Equal(map[ClusterShardKeyPair]*streamStatus{
		{Client: srMonitorClientKey, Server: srMonitorServerKey}: {
			defaultAckLevel:      100,
			highPriorityAckLevel: 200,
			lowPriorityAckLevel:  300,
			maxReplicationTaskId: 5000,
			isTieredStackEnabled: true,
		},
	}, srMonitorStatusMap)
}

func (s *streamReceiverMonitorSuite) TestStartStop_RunsEventLoop() {
	s.clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             1,
		},
	}).AnyTimes()
	s.shardController.EXPECT().ShardIDs().Return([]int32{1}).AnyTimes()

	srMonitorRegistered := make(chan struct{}, 1)
	s.clusterMetadata.EXPECT().RegisterMetadataChangeCallback(gomock.Any(), gomock.Any()).Do(
		func(_ any, _ func(map[string]*cluster.ClusterInformation, map[string]*cluster.ClusterInformation)) {
			select {
			case srMonitorRegistered <- struct{}{}:
			default:
			}
		},
	).AnyTimes()
	s.clusterMetadata.EXPECT().UnRegisterMetadataChangeCallback(gomock.Any()).AnyTimes()

	s.streamReceiverMonitor.Start()
	// Calling Start again is a no-op (CAS fails).
	s.streamReceiverMonitor.Start()

	// Wait until the event loop has registered its metadata change callback.
	select {
	case <-srMonitorRegistered:
	case <-time.After(5 * time.Second):
		s.Fail("event loop did not start")
	}

	s.streamReceiverMonitor.Stop()
	// Calling Stop again is a no-op (CAS fails).
	s.streamReceiverMonitor.Stop()
	s.True(s.streamReceiverMonitor.shutdownOnce.IsShutdown())
}

func (s *streamReceiverMonitorSuite) TestStartStop_StreamingDisabled() {
	srMonitorDisabled := NewStreamReceiverMonitor(
		ProcessToolBox{
			Config:          configs.NewConfig(dynamicconfig.NewNoopCollection(), 1),
			ClusterMetadata: s.clusterMetadata,
			ClientBean:      s.clientBean,
			ShardController: s.shardController,
			MetricsHandler:  metrics.NoopMetricsHandler,
			Logger:          log.NewNoopLogger(),
			DLQWriter:       NoopDLQWriter{},
		},
		nil,
		false,
	)
	// With streaming disabled, Start and Stop return early after the CAS transition.
	srMonitorDisabled.Start()
	s.Equal(int32(common.DaemonStatusStarted), srMonitorDisabled.status)
	srMonitorDisabled.Stop()
	s.Equal(int32(common.DaemonStatusStopped), srMonitorDisabled.status)
}
