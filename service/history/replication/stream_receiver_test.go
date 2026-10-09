package replication

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	streamReceiverSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		clusterMetadata         *cluster.MockMetadata
		highPriorityTaskTracker *MockExecutableTaskTracker
		lowPriorityTaskTracker  *MockExecutableTaskTracker
		stream                  *mockStream
		taskScheduler           *mockScheduler

		streamReceiver         *StreamReceiverImpl
		receiverFlowController *MockReceiverFlowController
	}

	mockStream struct {
		requests []*adminservice.StreamWorkflowReplicationMessagesRequest
		respChan chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]
		closed   bool
	}
	mockScheduler struct {
		tasks []TrackableExecutableTask
	}
	// fakeNamespaceThrottler returns configured throttled namespace IDs per shard
	// and records the shard ID it was queried with.
	fakeNamespaceThrottler struct {
		throttled      map[int32][]string
		queriedShardID int32
	}
)

func (f *fakeNamespaceThrottler) RecordTask(_ int32, _ string) {}

func (f *fakeNamespaceThrottler) ThrottledNamespaceIDs(shardID int32) []string {
	f.queriedShardID = shardID
	return f.throttled[shardID]
}

func TestStreamReceiverSuite(t *testing.T) {
	s := new(streamReceiverSuite)
	suite.Run(t, s)
}

func (s *streamReceiverSuite) SetupSuite() {

}

func (s *streamReceiverSuite) TearDownSuite() {

}

func (s *streamReceiverSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.highPriorityTaskTracker = NewMockExecutableTaskTracker(s.controller)
	s.lowPriorityTaskTracker = NewMockExecutableTaskTracker(s.controller)
	s.stream = &mockStream{
		requests: nil,
		respChan: make(chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse], 100),
	}
	s.taskScheduler = &mockScheduler{
		tasks: nil,
	}

	processToolBox := ProcessToolBox{
		ClusterMetadata:           s.clusterMetadata,
		Config:                    tests.NewDynamicConfig(),
		HighPriorityTaskScheduler: s.taskScheduler,
		LowPriorityTaskScheduler:  s.taskScheduler,
		MetricsHandler:            metrics.NoopMetricsHandler,
		Logger:                    log.NewTestLogger(),
		DLQWriter:                 NoopDLQWriter{},
		NamespaceThrottler:        NoopNamespaceThrottler{},
	}
	processToolBox.Config.ReplicationStreamSyncStatusDuration = dynamicconfig.GetDurationPropertyFn(5 * time.Millisecond)
	s.clusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, gomock.Any()).Return("some-cluster-name").AnyTimes()
	s.streamReceiver = NewStreamReceiver(
		processToolBox,
		NewExecutableTaskConverter(processToolBox),
		NewClusterShardKey(rand.Int31(), rand.Int31()),
		NewClusterShardKey(rand.Int31(), rand.Int31()),
	)
	s.clusterMetadata.EXPECT().GetAllClusterInfo().Return(
		map[string]cluster.ClusterInformation{
			uuid.New().String(): {
				Enabled:                true,
				InitialFailoverVersion: int64(s.streamReceiver.clientShardKey.ClusterID),
			},
			uuid.New().String(): {
				Enabled:                true,
				InitialFailoverVersion: int64(s.streamReceiver.serverShardKey.ClusterID),
			},
		},
	).AnyTimes()
	s.streamReceiver.laneRegistry.highPriorityTracker = s.highPriorityTaskTracker
	s.streamReceiver.laneRegistry.lowPriorityTracker = s.lowPriorityTaskTracker
	s.stream.requests = []*adminservice.StreamWorkflowReplicationMessagesRequest{}
	s.receiverFlowController = NewMockReceiverFlowController(s.controller)
	s.streamReceiver.flowController = s.receiverFlowController
}

func (s *streamReceiverSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *streamReceiverSuite) TestAckMessage_Noop() {
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	s.streamReceiver.ackMessage(s.stream)

	s.Empty(s.stream.requests)
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeUnset() {
	s.streamReceiver.receiverMode = ReceiverModeUnset // when stream receiver is in unset mode, means no task received yet, so no ACK should be sent
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Empty(s.stream.requests)
	s.NoError(err)
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeSingleStack() {
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	s.streamReceiver.receiverMode = ReceiverModeSingleStack
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)
	s.Equal([]*adminservice.StreamWorkflowReplicationMessagesRequest{{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &replicationspb.SyncReplicationState{
				InclusiveLowWatermark:     watermarkInfo.Watermark,
				InclusiveLowWatermarkTime: timestamppb.New(watermarkInfo.Timestamp),
			},
		},
	},
	}, s.stream.requests)
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeSingleStack_NoHighPriorityWatermark() {
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	s.streamReceiver.receiverMode = ReceiverModeSingleStack
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Error(err)
	s.Empty(s.stream.requests)
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeSingleStack_HasBothWatermark() {
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}

	s.streamReceiver.receiverMode = ReceiverModeSingleStack
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)

	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Error(err)
	s.Empty(s.stream.requests)
}

func (s *streamReceiverSuite) TestTrackBatch_DefaultRoutesByPriority() {
	watermark := WatermarkInfo{Watermark: 100, Timestamp: time.Now()}
	s.highPriorityTaskTracker.EXPECT().TrackTasks(watermark).Return(nil).Times(2)
	s.lowPriorityTaskTracker.EXPECT().TrackTasks(watermark).Return(nil)

	for _, priority := range []enumsspb.TaskPriority{
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		enumsspb.TASK_PRIORITY_HIGH,
		enumsspb.TASK_PRIORITY_LOW,
	} {
		tracked, err := s.streamReceiver.laneRegistry.TrackBatch(priority, nil, watermark)
		s.Require().NoError(err)
		s.Empty(tracked)
	}
	s.Empty(s.streamReceiver.laneRegistry.lanes)
}

func (s *streamReceiverSuite) TestTrackBatch_NamedLanesTrackIndependently() {
	watermark := WatermarkInfo{Watermark: 100, Timestamp: time.Now()}
	tracked, err := s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-a"), watermark)
	s.Require().NoError(err)
	s.Empty(tracked)
	laneA := s.streamReceiver.laneRegistry.lanes["ns-a"]
	s.Require().NotNil(laneA)

	nextWatermark := WatermarkInfo{Watermark: 200, Timestamp: time.Now()}
	tracked, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-a"), nextWatermark)
	s.Require().NoError(err)
	s.Empty(tracked)
	s.Same(laneA, s.streamReceiver.laneRegistry.lanes["ns-a"])
	s.Equal(nextWatermark, *laneA.tracker.LowWatermark())

	_, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-b"), watermark)
	s.Require().NoError(err)
	s.NotSame(laneA, s.streamReceiver.laneRegistry.lanes["ns-b"])
}

func (s *streamReceiverSuite) TestTrackBatch_ReturnsOnlyNewTasks() {
	watermark := WatermarkInfo{Watermark: 10, Timestamp: time.Now()}
	task := NewMockTrackableExecutableTask(s.controller)
	task.EXPECT().TaskID().Return(int64(1)).AnyTimes()

	tracked, err := s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-a"), watermark, task)
	s.Require().NoError(err)
	s.Equal([]TrackableExecutableTask{task}, tracked)

	tracked, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-a"), watermark, task)
	s.Require().NoError(err)
	s.Empty(tracked)
}

func (s *streamReceiverSuite) TestTrackBatch_RejectsInvalidLane() {
	watermark := WatermarkInfo{Watermark: 10, Timestamp: time.Now()}
	registry := s.streamReceiver.laneRegistry
	_, err := registry.TrackBatch(enumsspb.TaskPriority(-1), nil, watermark)
	s.Require().Error(err)
	_, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, &replicationspb.ReplicationLaneInfo{}, watermark)
	s.Require().Error(err)
	_, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_UNSPECIFIED, replicationLaneInfo("ns-a"), watermark)
	s.Require().Error(err)
	_, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TaskPriority(-1), replicationLaneInfo("ns-a"), watermark)
	s.Require().Error(err)
	s.Empty(registry.lanes)
}

func (s *streamReceiverSuite) TestTrackBatch_RejectsPriorityChange() {
	watermark := WatermarkInfo{Watermark: 10, Timestamp: time.Now()}
	_, err := s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_LOW, replicationLaneInfo("ns-a"), watermark)
	s.Require().NoError(err)

	_, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-a"), watermark)
	s.Require().Error(err)
	s.Equal(enumsspb.TASK_PRIORITY_LOW, s.streamReceiver.laneRegistry.lanes["ns-a"].priority)
}

func (s *streamReceiverSuite) TestReplicationLane_RejectsTrafficAfterRetirement() {
	watermark := WatermarkInfo{Watermark: 100, Timestamp: time.Now()}
	_, err := s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-a"), watermark)
	s.Require().NoError(err)

	retire := &replicationspb.ReplicationLaneInfo{LaneId: "ns-a", RetireLane: true}
	_, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, retire, WatermarkInfo{Watermark: 200, Timestamp: time.Now()})
	s.Require().NoError(err)
	_, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, retire, WatermarkInfo{Watermark: 201, Timestamp: time.Now()})
	s.Require().NoError(err)

	_, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-a"), WatermarkInfo{Watermark: 202, Timestamp: time.Now()})
	s.Require().Error(err)
	s.Equal(int64(201), s.streamReceiver.laneRegistry.lanes["ns-a"].tracker.LowWatermark().Watermark)
}

func (s *streamReceiverSuite) TestMemberLane_TrackBatchHoldsRegistryLock() {
	registry := s.streamReceiver.laneRegistry
	tracker := NewMockExecutableTaskTracker(s.controller)
	registry.lanes["ns-a"] = &receiverLane{tracker: tracker, priority: enumsspb.TASK_PRIORITY_HIGH}
	watermark := WatermarkInfo{Watermark: 100, Timestamp: time.Now()}
	tracker.EXPECT().TrackTasks(watermark).DoAndReturn(func(WatermarkInfo, ...TrackableExecutableTask) []TrackableExecutableTask {
		if registry.mu.TryLock() {
			registry.mu.Unlock()
			s.Fail("registry lock was released during batch tracking")
		}
		return nil
	})

	_, err := registry.TrackBatch(
		enumsspb.TASK_PRIORITY_HIGH,
		&replicationspb.ReplicationLaneInfo{LaneId: "ns-a", RetireLane: true},
		watermark,
	)
	s.Require().NoError(err)
	s.True(registry.lanes["ns-a"].retiring)
}

func (s *streamReceiverSuite) TestMemberLane_CreatedAfterStopIsCancelled() {
	s.highPriorityTaskTracker.EXPECT().Cancel()
	s.lowPriorityTaskTracker.EXPECT().Cancel()
	s.streamReceiver.laneRegistry.Close()
	task := NewMockTrackableExecutableTask(s.controller)
	task.EXPECT().TaskID().Return(int64(1)).AnyTimes()
	task.EXPECT().Cancel()

	tracked, err := s.streamReceiver.laneRegistry.TrackBatch(
		enumsspb.TASK_PRIORITY_HIGH,
		replicationLaneInfo("ns-late"),
		WatermarkInfo{Watermark: 2, Timestamp: time.Now()},
		task,
	)
	s.Require().NoError(err)
	s.Equal([]TrackableExecutableTask{task}, tracked)
}

func (s *streamReceiverSuite) TestMemberLane_RetirementWaitsForPendingTasks() {
	task := NewMockTrackableExecutableTask(s.controller)
	task.EXPECT().TaskID().Return(int64(1)).AnyTimes()
	task.EXPECT().TaskCreationTime().Return(time.Now()).AnyTimes()
	gomock.InOrder(
		task.EXPECT().State().Return(ctasks.TaskStatePending),
		task.EXPECT().State().Return(ctasks.TaskStateAcked),
	)

	retire := &replicationspb.ReplicationLaneInfo{LaneId: "ns-a", RetireLane: true}
	_, err := s.streamReceiver.laneRegistry.TrackBatch(
		enumsspb.TASK_PRIORITY_HIGH,
		retire,
		WatermarkInfo{Watermark: 2, Timestamp: time.Now()},
		task,
	)
	s.Require().NoError(err)
	s.Contains(s.streamReceiver.laneRegistry.Watermarks(), "ns-a")
	s.NotContains(s.streamReceiver.laneRegistry.Watermarks(), "ns-a")
}

func (s *streamReceiverSuite) TestMemberLane_RetiredLaneForgottenOnceDrained() {
	watermark := WatermarkInfo{Watermark: 100, Timestamp: time.Now()}
	_, err := s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-a"), watermark)
	s.Require().NoError(err)
	original := s.streamReceiver.laneRegistry.lanes["ns-a"]
	s.Equal(watermark, s.streamReceiver.laneRegistry.Watermarks()["ns-a"])

	_, err = s.streamReceiver.laneRegistry.TrackBatch(
		enumsspb.TASK_PRIORITY_HIGH,
		&replicationspb.ReplicationLaneInfo{LaneId: "ns-a", RetireLane: true},
		WatermarkInfo{Watermark: 200, Timestamp: time.Now()},
	)
	s.Require().NoError(err)
	s.NotContains(s.streamReceiver.laneRegistry.Watermarks(), "ns-a")

	_, err = s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-a"), WatermarkInfo{Watermark: 300, Timestamp: time.Now()})
	s.Require().NoError(err)
	s.NotSame(original, s.streamReceiver.laneRegistry.lanes["ns-a"])
}

func (s *streamReceiverSuite) TestMemberLane_WatermarkDoesNotHoldLaneLock() {
	tracker := NewMockExecutableTaskTracker(s.controller)
	lowWatermarkStarted := make(chan struct{})
	releaseLowWatermark := make(chan struct{})
	defer func() {
		select {
		case <-releaseLowWatermark:
		default:
			close(releaseLowWatermark)
		}
	}()
	tracker.EXPECT().LowWatermark().DoAndReturn(func() *WatermarkInfo {
		close(lowWatermarkStarted)
		<-releaseLowWatermark
		return nil
	})
	s.streamReceiver.laneRegistry.lanes["ns-a"] = &receiverLane{tracker: tracker, priority: enumsspb.TASK_PRIORITY_HIGH}

	snapshotDone := make(chan struct{})
	go func() {
		s.streamReceiver.laneRegistry.Watermarks()
		close(snapshotDone)
	}()
	<-lowWatermarkStarted

	laneLockAcquired := make(chan struct{})
	go func() {
		s.streamReceiver.laneRegistry.mu.Lock()
		close(laneLockAcquired)
		s.streamReceiver.laneRegistry.mu.Unlock()
	}()
	await.RequireTrue(s.T(), func() bool {
		select {
		case <-laneLockAcquired:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	close(releaseLowWatermark)
	<-snapshotDone
}

func (s *streamReceiverSuite) TestAckMessage_TieredStack_FoldsMemberLaneWatermarkIntoAck() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	highWatermarkInfo := &WatermarkInfo{Watermark: 200, Timestamp: time.Unix(0, 2000)}
	lowWatermarkInfo := &WatermarkInfo{Watermark: 300, Timestamp: time.Unix(0, 3000)}
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(highWatermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(lowWatermarkInfo)
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH).Return(FlowControlInfo{Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME})
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW).Return(FlowControlInfo{Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME})
	s.highPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()

	// An isolated lane lagging below both shared-lane watermarks.
	laneWatermark := WatermarkInfo{Watermark: 100, Timestamp: time.Unix(0, 1000)}
	_, err := s.streamReceiver.laneRegistry.TrackBatch(enumsspb.TASK_PRIORITY_HIGH, replicationLaneInfo("ns-a"), laneWatermark)
	s.Require().NoError(err)

	_, err = s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)
	s.Len(s.stream.requests, 1)
	state := s.stream.requests[0].GetSyncReplicationState()
	// The lane drags the overall min below both shared-lane watermarks, so the
	// sender's cleanup accounts for the lowest point in flight across every lane.
	s.Equal(laneWatermark.Watermark, state.InclusiveLowWatermark)
	s.Equal(timestamppb.New(laneWatermark.Timestamp), state.InclusiveLowWatermarkTime)
	// The shared-lane states keep their own watermarks.
	s.Equal(highWatermarkInfo.Watermark, state.HighPriorityState.InclusiveLowWatermark)
	s.Equal(lowWatermarkInfo.Watermark, state.LowPriorityState.InclusiveLowWatermark)
	// The lane reports its own applied watermark keyed by namespace.
	s.Len(state.LaneStates, 1)
	s.Equal(laneWatermark.Watermark, state.LaneStates["ns-a"].InclusiveLowWatermark)
	s.Equal(timestamppb.New(laneWatermark.Timestamp), state.LaneStates["ns-a"].InclusiveLowWatermarkTime)
}

func (s *streamReceiverSuite) TestAckMessage_TieredStack_ReportsThrottledNamespaces() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	throttler := &fakeNamespaceThrottler{throttled: map[int32][]string{
		s.streamReceiver.clientShardKey.ShardID:     {"ns-hot-a", "ns-hot-b"},
		s.streamReceiver.clientShardKey.ShardID + 1: {"ns-other"},
	}}
	s.streamReceiver.NamespaceThrottler = throttler
	watermarkInfo := &WatermarkInfo{Watermark: 10, Timestamp: time.Unix(0, 1000)}
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH).Return(FlowControlInfo{Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME})
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW).Return(FlowControlInfo{Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME})
	s.highPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()

	_, err := s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)
	s.Len(s.stream.requests, 1)
	state := s.stream.requests[0].GetSyncReplicationState()
	// Only the local shard's throttled set is reported.
	s.Equal([]string{"ns-hot-a", "ns-hot-b"}, state.ThrottleHighNamespaceIds)
	s.Equal(s.streamReceiver.clientShardKey.ShardID, throttler.queriedShardID)
}

func (s *streamReceiverSuite) TestTrackingCount_IncludesDefaultAndNamedLanes() {
	s.highPriorityTaskTracker.EXPECT().Size().Return(5)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(3)
	highTask := NewMockTrackableExecutableTask(s.controller)
	highTask.EXPECT().TaskID().Return(int64(1)).AnyTimes()
	_, err := s.streamReceiver.laneRegistry.TrackBatch(
		enumsspb.TASK_PRIORITY_HIGH,
		replicationLaneInfo("ns-a"),
		WatermarkInfo{Watermark: 10, Timestamp: time.Now()},
		highTask,
	)
	s.Require().NoError(err)
	lowTask := NewMockTrackableExecutableTask(s.controller)
	lowTask.EXPECT().TaskID().Return(int64(2)).AnyTimes()
	_, err = s.streamReceiver.laneRegistry.TrackBatch(
		enumsspb.TASK_PRIORITY_LOW,
		replicationLaneInfo("ns-b"),
		WatermarkInfo{Watermark: 10, Timestamp: time.Now()},
		lowTask,
	)
	s.Require().NoError(err)

	s.Equal(6, s.streamReceiver.laneRegistry.TrackingCount(enumsspb.TASK_PRIORITY_HIGH))
	s.Equal(4, s.streamReceiver.laneRegistry.TrackingCount(enumsspb.TASK_PRIORITY_LOW))
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeTieredStack_NoHighPriorityWatermark() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Empty(s.stream.requests)
	s.NoError(err)
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeTieredStack_NoLowPriorityWatermark() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	watermarkInfo := &WatermarkInfo{
		Watermark: rand.Int63(),
		Timestamp: time.Unix(0, rand.Int63()),
	}
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(watermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Empty(s.stream.requests)
	s.NoError(err)
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeTieredStack() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	highWatermarkInfo := &WatermarkInfo{
		Watermark: 10,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	lowWatermarkInfo := &WatermarkInfo{
		Watermark: 11,
		Timestamp: time.Unix(0, rand.Int63()),
	}
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(highWatermarkInfo)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(lowWatermarkInfo)
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_HIGH).Return(FlowControlInfo{Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME})
	s.receiverFlowController.EXPECT().GetFlowControlInfo(enumsspb.TASK_PRIORITY_LOW).Return(FlowControlInfo{Command: enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE, Cause: "test cause"})
	s.highPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0).AnyTimes()
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)
	s.Equal([]*adminservice.StreamWorkflowReplicationMessagesRequest{{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &replicationspb.SyncReplicationState{
				InclusiveLowWatermark:     highWatermarkInfo.Watermark,
				InclusiveLowWatermarkTime: timestamppb.New(highWatermarkInfo.Timestamp),
				HighPriorityState: &replicationspb.ReplicationState{
					InclusiveLowWatermark:     highWatermarkInfo.Watermark,
					InclusiveLowWatermarkTime: timestamppb.New(highWatermarkInfo.Timestamp),
					FlowControlCommand:        enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_RESUME,
				},
				LowPriorityState: &replicationspb.ReplicationState{
					InclusiveLowWatermark:     lowWatermarkInfo.Watermark,
					InclusiveLowWatermarkTime: timestamppb.New(lowWatermarkInfo.Timestamp),
					FlowControlCommand:        enumsspb.REPLICATION_FLOW_CONTROL_COMMAND_PAUSE,
				},
				SupportsReplicationLanes:       true,
				ReplicationLaneProtocolVersion: 1,
			},
		},
	},
	}, s.stream.requests)
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_SingleStack() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_LOW,
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
				},
			},
		},
		Err: nil,
	}
	s.stream.respChan <- streamResp
	close(s.stream.respChan)

	s.highPriorityTaskTracker.EXPECT().TrackTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask {
			s.Equal(streamResp.Resp.GetMessages().ExclusiveHighWatermark, highWatermarkInfo.Watermark)
			s.Equal(streamResp.Resp.GetMessages().ExclusiveHighWatermarkTime.AsTime(), highWatermarkInfo.Timestamp)
			s.Len(tasks, 1)
			s.IsType(&ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)

	err := s.streamReceiver.processMessages(s.stream)
	s.NoError(err)
	s.Len(s.taskScheduler.tasks, 1)
	s.IsType(&ExecutableUnknownTask{}, s.taskScheduler.tasks[0])
	s.Equal(ReceiverModeSingleStack, s.streamReceiver.receiverMode)
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_SingleStack_ReceivedPrioritizedTask() {
	s.streamReceiver.receiverMode = ReceiverModeSingleStack
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
				},
			},
		},
		Err: nil,
	}
	s.stream.respChan <- streamResp

	// no TrackTasks call should be made
	err := s.streamReceiver.processMessages(s.stream)
	s.ErrorAs(err, new(*StreamError))
	s.Empty(s.taskScheduler.tasks)
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_TieredStack_ReceivedNonPrioritizedTask() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
				},
			},
		},
		Err: nil,
	}
	s.stream.respChan <- streamResp

	// no TrackTasks call should be made
	err := s.streamReceiver.processMessages(s.stream)
	s.ErrorAs(err, new(*StreamError))
	s.Empty(s.taskScheduler.tasks)
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_TieredStack() {
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	streamResp1 := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{replicationTask},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
				},
			},
		},
		Err: nil,
	}
	streamResp2 := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks: []*replicationspb.ReplicationTask{
						{
							TaskType:       enumsspb.ReplicationTaskType(-1),
							SourceTaskId:   rand.Int63(),
							VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
							Priority:       enumsspb.TASK_PRIORITY_LOW,
						},
					},
					ExclusiveHighWatermark:     rand.Int63(),
					ExclusiveHighWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
					Priority:                   enumsspb.TASK_PRIORITY_LOW,
				},
			},
		},
		Err: nil,
	}
	s.stream.respChan <- streamResp1
	s.stream.respChan <- streamResp2
	close(s.stream.respChan)

	s.highPriorityTaskTracker.EXPECT().TrackTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask {
			s.Equal(streamResp1.Resp.GetMessages().ExclusiveHighWatermark, highWatermarkInfo.Watermark)
			s.Equal(streamResp1.Resp.GetMessages().ExclusiveHighWatermarkTime.AsTime(), highWatermarkInfo.Timestamp)
			s.Len(tasks, 1)
			s.IsType(&ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)
	s.lowPriorityTaskTracker.EXPECT().TrackTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask {
			s.Equal(streamResp2.Resp.GetMessages().ExclusiveHighWatermark, highWatermarkInfo.Watermark)
			s.Equal(streamResp2.Resp.GetMessages().ExclusiveHighWatermarkTime.AsTime(), highWatermarkInfo.Timestamp)
			s.Len(tasks, 1)
			s.IsType(&ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)

	err := s.streamReceiver.processMessages(s.stream)
	s.NoError(err)
	s.Len(s.taskScheduler.tasks, 2)
	s.Equal(ReceiverModeTieredStack, s.streamReceiver.receiverMode)
}

func (s *streamReceiverSuite) TestGetTaskScheduler() {
	tests := []struct {
		name         string
		priority     enumsspb.TaskPriority
		task         TrackableExecutableTask
		expected     enumsspb.TaskPriority
		expectErr    bool
		errorMessage string
	}{
		{
			name:     "Unspecified priority with ExecutableWorkflowStateTask",
			priority: enumsspb.TASK_PRIORITY_UNSPECIFIED,
			task:     &ExecutableWorkflowStateTask{},
			expected: enumsspb.TASK_PRIORITY_LOW,
		},
		{
			name:     "Unspecified priority with other task",
			priority: enumsspb.TASK_PRIORITY_UNSPECIFIED,
			task:     &ExecutableHistoryTask{},
			expected: enumsspb.TASK_PRIORITY_HIGH,
		},
		{
			name:     "High priority",
			priority: enumsspb.TASK_PRIORITY_HIGH,
			task:     &ExecutableHistoryTask{},
			expected: enumsspb.TASK_PRIORITY_HIGH,
		},
		{
			name:     "Low priority",
			priority: enumsspb.TASK_PRIORITY_LOW,
			task:     &ExecutableWorkflowStateTask{},
			expected: enumsspb.TASK_PRIORITY_LOW,
		},
		{
			name:         "Invalid priority",
			priority:     enumsspb.TaskPriority(999),
			task:         &ExecutableHistoryTask{},
			expectErr:    true,
			errorMessage: "InvalidArgument",
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			priority, err := s.streamReceiver.getTaskSchedulerPriority(tt.priority, tt.task)
			if tt.expectErr {
				s.Error(err)
			} else {
				s.NoError(err)
				s.Equal(tt.expected, priority, "Expected scheduler to match")
			}
		})
	}
}

func (s *streamReceiverSuite) TestProcessMessage_Err() {
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: nil,
		Err:  serviceerror.NewUnavailable("random recv error"),
	}
	s.stream.respChan <- streamResp
	close(s.stream.respChan)

	err := s.streamReceiver.processMessages(s.stream)
	s.Error(err)
}

func (s *streamReceiverSuite) TestSendEventLoop_Panic_Captured() {
	// This would never actually panic, but it's the quickest way to test that a later panic is captured.
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Do(func() {
		panic("panic")
	})

	s.streamReceiver.sendEventLoop() // should not cause panic
}

func (s *streamReceiverSuite) TestRecvEventLoop_Panic_Captured() {
	s.streamReceiver.recvEventLoop() // should not cause panic
}

func (s *streamReceiverSuite) TestLivenessMonitor() {
	s.streamReceiver.recvSignalChan <- struct{}{}
	livenessMonitor(
		s.streamReceiver.recvSignalChan,
		dynamicconfig.GetDurationPropertyFn(time.Second),
		dynamicconfig.GetIntPropertyFn(1),
		s.streamReceiver.shutdownChan,
		s.streamReceiver.Stop,
		s.streamReceiver.logger,
	)
	s.False(s.streamReceiver.IsValid())
}

func (s *mockStream) Send(
	req *adminservice.StreamWorkflowReplicationMessagesRequest,
) error {
	s.requests = append(s.requests, req)
	return nil
}

func (s *mockStream) Recv() (<-chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse], error) {
	return s.respChan, nil
}

func (s *mockStream) Close() {
	s.closed = true
}

func (s *mockStream) IsValid() bool {
	return !s.closed
}

func (s *mockScheduler) Submit(task TrackableExecutableTask) {
	s.tasks = append(s.tasks, task)
}

func (s *mockScheduler) TrySubmit(task TrackableExecutableTask) bool {
	s.tasks = append(s.tasks, task)
	return true
}

func (s *mockScheduler) Start() {}
func (s *mockScheduler) Stop()  {}
