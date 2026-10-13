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
	s.streamReceiver.highPriorityTaskTracker = s.highPriorityTaskTracker
	s.streamReceiver.lowPriorityTaskTracker = s.lowPriorityTaskTracker
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

func (s *streamReceiverSuite) TestGetTaskTrackerForLane_UnsetRoutesByPriority() {
	tracker, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "", false)
	s.NoError(err)
	s.Same(s.streamReceiver.highPriorityTaskTracker, tracker)

	tracker, err = s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_LOW, "", false)
	s.NoError(err)
	s.Same(s.streamReceiver.lowPriorityTaskTracker, tracker)
}

func (s *streamReceiverSuite) TestGetTaskTrackerForLane_PerMemberLanes() {
	trackerA, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", false)
	s.NoError(err)
	s.NotNil(trackerA)

	// Same namespace, same lane; different namespaces are independent lanes.
	trackerA2, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", false)
	s.NoError(err)
	s.Same(trackerA, trackerA2)
	trackerB, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-b", false)
	s.NoError(err)
	s.NotSame(trackerA, trackerB)
}

func (s *streamReceiverSuite) TestGetTaskTrackerForLane_RejectsNonHighPriority() {
	// Isolation splits the HIGH lane only: lane-tagged traffic at LOW or
	// single-stack UNSPECIFIED priority is a protocol violation, not a routing case.
	_, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_LOW, "ns-a", false)
	s.Error(err)
	_, err = s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_UNSPECIFIED, "ns-a", false)
	s.Error(err)
	s.Empty(s.streamReceiver.memberLanes)
}

func (s *streamReceiverSuite) TestMemberLane_NonRetireTrafficRevivesRetiringLane() {
	tracker, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", false)
	s.NoError(err)
	tracker.TrackTasks(WatermarkInfo{Watermark: 100, Timestamp: time.Now()})
	s.streamReceiver.finishLaneBatch("ns-a", false)

	// A retire marker arrives: the lane is now retiring.
	markerTracker, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", true)
	s.NoError(err)
	s.Same(tracker, markerTracker)
	markerTracker.TrackTasks(WatermarkInfo{Watermark: 200, Timestamp: time.Now()})
	s.streamReceiver.finishLaneBatch("ns-a", true)

	// The namespace is re-isolated before the lane drains: new traffic clears the
	// stale retiring flag so a transient drain can't delete the active lane.
	trackerAgain, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", false)
	s.NoError(err)
	s.Same(tracker, trackerAgain)
	wms := s.streamReceiver.memberLaneWatermarks()
	s.Equal(int64(200), wms["ns-a"].Watermark)
	s.Contains(s.streamReceiver.memberLanes, "ns-a")
	s.streamReceiver.finishLaneBatch("ns-a", false)
}

func (s *streamReceiverSuite) TestMemberLane_RetireBatchMidTrackSurvivesAckSnapshot() {
	// A lane whose retire batch has been resolved but not yet tracked must survive
	// the ack loop's snapshot; once the (watermark-only) retire batch is tracked
	// and finished, the drained lane is dropped.
	tracker, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", true)
	s.NoError(err)

	s.streamReceiver.memberLaneWatermarks()
	s.Contains(s.streamReceiver.memberLanes, "ns-a")

	tracker.TrackTasks(WatermarkInfo{Watermark: 50, Timestamp: time.Now()})
	s.streamReceiver.finishLaneBatch("ns-a", true)
	s.streamReceiver.memberLaneWatermarks()
	s.NotContains(s.streamReceiver.memberLanes, "ns-a")
}

func (s *streamReceiverSuite) TestMemberLane_RetireBatchOnRetiringLaneNotOrphaned() {
	// A lane that already retired and drained can receive a second retire-tagged
	// batch (e.g. a duplicate retirement marker after the namespace re-isolated and
	// merged back before its lane was dropped). The ack loop must not delete the
	// lane between the batch's lane resolution and its TrackTasks call, or the
	// batch would be tracked on an orphaned lane and never re-enter the ack fold.
	tracker, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", true)
	s.NoError(err)
	tracker.TrackTasks(WatermarkInfo{Watermark: 100, Timestamp: time.Now()})
	s.streamReceiver.finishLaneBatch("ns-a", true)

	// The lane is now retiring and drained. A second retire batch resolves to it...
	trackerAgain, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", true)
	s.NoError(err)
	s.Same(tracker, trackerAgain)

	// ...and the concurrent ack snapshot must find it undeletable mid-track.
	wms := s.streamReceiver.memberLaneWatermarks()
	s.Contains(s.streamReceiver.memberLanes, "ns-a")
	s.Equal(int64(100), wms["ns-a"].Watermark)

	// Once the batch is tracked and finished, the drained lane is dropped as before.
	trackerAgain.TrackTasks(WatermarkInfo{Watermark: 200, Timestamp: time.Now()})
	s.streamReceiver.finishLaneBatch("ns-a", true)
	s.streamReceiver.memberLaneWatermarks()
	s.NotContains(s.streamReceiver.memberLanes, "ns-a")
}

func (s *streamReceiverSuite) TestMemberLane_CreatedAfterStopIsCancelled() {
	s.streamReceiver.memberLaneMu.Lock()
	s.streamReceiver.memberLanesClosed = true
	s.streamReceiver.memberLaneMu.Unlock()

	tracker, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-late", false)
	s.NoError(err)
	// The tracker was pre-cancelled: tasks tracked into it are cancelled instead
	// of running to completion after shutdown.
	task := NewMockTrackableExecutableTask(s.controller)
	task.EXPECT().TaskID().Return(int64(1)).AnyTimes()
	task.EXPECT().Cancel()
	tracker.TrackTasks(WatermarkInfo{Watermark: 2, Timestamp: time.Now()}, task)
}

func (s *streamReceiverSuite) TestMemberLane_RetiredLaneDroppedOnceDrained() {
	tracker, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", false)
	s.NoError(err)
	tracker.TrackTasks(WatermarkInfo{Watermark: 100, Timestamp: time.Now()})
	s.streamReceiver.finishLaneBatch("ns-a", false)

	// Active lane reports its watermark.
	wms := s.streamReceiver.memberLaneWatermarks()
	s.Equal(int64(100), wms["ns-a"].Watermark)

	// A retire marker arrives and is tracked (watermark-only batch): retired and
	// drained, the lane is dropped on the next snapshot, so it can never pin the
	// overall ack minimum.
	markerTracker, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", true)
	s.NoError(err)
	s.Same(tracker, markerTracker)
	markerTracker.TrackTasks(WatermarkInfo{Watermark: 200, Timestamp: time.Now()})
	s.streamReceiver.finishLaneBatch("ns-a", true)
	wms = s.streamReceiver.memberLaneWatermarks()
	s.NotContains(wms, "ns-a")

	// A later message would lazily create a fresh lane.
	trackerNew, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", false)
	s.NoError(err)
	s.NotSame(tracker, trackerNew)
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
	s.streamReceiver.memberLanes["ns-a"] = &memberLane{tracker: tracker}

	snapshotDone := make(chan struct{})
	go func() {
		s.streamReceiver.memberLaneWatermarks()
		close(snapshotDone)
	}()
	<-lowWatermarkStarted

	laneLockAcquired := make(chan struct{})
	go func() {
		s.streamReceiver.memberLaneMu.Lock()
		close(laneLockAcquired)
		s.streamReceiver.memberLaneMu.Unlock()
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
	laneTracker, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", false)
	s.NoError(err)
	laneTracker.TrackTasks(laneWatermark)

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
	s.Len(state.IsolatedLaneStates, 1)
	s.Equal(laneWatermark.Watermark, state.IsolatedLaneStates["ns-a"].InclusiveLowWatermark)
	s.Equal(timestamppb.New(laneWatermark.Timestamp), state.IsolatedLaneStates["ns-a"].InclusiveLowWatermarkTime)
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

func (s *streamReceiverSuite) TestHighFamilyTrackingCount_IncludesMemberLanes() {
	s.highPriorityTaskTracker.EXPECT().Size().Return(5)
	laneTracker, err := s.streamReceiver.getTaskTrackerForLane(enumsspb.TASK_PRIORITY_HIGH, "ns-a", false)
	s.NoError(err)
	task := NewMockTrackableExecutableTask(s.controller)
	task.EXPECT().TaskID().Return(int64(1)).AnyTimes()
	laneTracker.TrackTasks(WatermarkInfo{Watermark: 10, Timestamp: time.Now()}, task)

	s.Equal(6, s.streamReceiver.highFamilyTrackingCount())
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
				SupportsNamespaceIsolation: true,
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
