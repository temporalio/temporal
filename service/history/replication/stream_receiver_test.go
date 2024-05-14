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
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	repicationpb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
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

		streamReceiver *StreamReceiverImpl
	}

	mockStream struct {
		requests []*adminservice.StreamWorkflowReplicationMessagesRequest
		respChan chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]
		closed   bool
	}
	mockScheduler struct {
		tasks []TrackableExecutableTask
	}
)

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
		HighPriorityTaskScheduler: s.taskScheduler,
		LowPriorityTaskScheduler:  s.taskScheduler,
		MetricsHandler:            metrics.NoopMetricsHandler,
		Logger:                    log.NewTestLogger(),
		DLQWriter:                 NoopDLQWriter{},
	}
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

	s.Equal(0, len(s.stream.requests))
}

func (s *streamReceiverSuite) TestAckMessage_SyncStatus_ReceiverModeUnset() {
	s.streamReceiver.receiverMode = ReceiverModeUnset // when stream receiver is in unset mode, means no task received yet, so no ACK should be sent
	s.highPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.lowPriorityTaskTracker.EXPECT().LowWatermark().Return(nil)
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.Equal(0, len(s.stream.requests))
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
			SyncReplicationState: &repicationpb.SyncReplicationState{
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
	s.Equal(0, len(s.stream.requests))
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
	s.Equal(0, len(s.stream.requests))
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
	s.Equal(0, len(s.stream.requests))
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
	s.Equal(0, len(s.stream.requests))
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
	s.highPriorityTaskTracker.EXPECT().Size().Return(0)
	s.lowPriorityTaskTracker.EXPECT().Size().Return(0)
	_, err := s.streamReceiver.ackMessage(s.stream)
	s.NoError(err)
	s.Equal([]*adminservice.StreamWorkflowReplicationMessagesRequest{{
		Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: &repicationpb.SyncReplicationState{
				InclusiveLowWatermark:     highWatermarkInfo.Watermark,
				InclusiveLowWatermarkTime: timestamppb.New(highWatermarkInfo.Timestamp),
				HighPriorityState: &repicationpb.ReplicationState{
					InclusiveLowWatermark:     highWatermarkInfo.Watermark,
					InclusiveLowWatermarkTime: timestamppb.New(highWatermarkInfo.Timestamp),
				},
				LowPriorityState: &repicationpb.ReplicationState{
					InclusiveLowWatermark:     lowWatermarkInfo.Watermark,
					InclusiveLowWatermarkTime: timestamppb.New(lowWatermarkInfo.Timestamp),
				},
			},
		},
	},
	}, s.stream.requests)
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_SingleStack() {
	replicationTask := &repicationpb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_LOW,
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &repicationpb.WorkflowReplicationMessages{
					ReplicationTasks:           []*repicationpb.ReplicationTask{replicationTask},
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
			s.Equal(1, len(tasks))
			s.IsType(&ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)

	err := s.streamReceiver.processMessages(s.stream)
	s.NoError(err)
	s.Equal(1, len(s.taskScheduler.tasks))
	s.IsType(&ExecutableUnknownTask{}, s.taskScheduler.tasks[0])
	s.Equal(ReceiverModeSingleStack, s.streamReceiver.receiverMode)
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_SingleStack_ReceivedPrioritizedTask() {
	s.streamReceiver.receiverMode = ReceiverModeSingleStack
	replicationTask := &repicationpb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &repicationpb.WorkflowReplicationMessages{
					ReplicationTasks:           []*repicationpb.ReplicationTask{replicationTask},
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
	s.IsType(&StreamError{}, err)
	s.Equal(0, len(s.taskScheduler.tasks))
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_TieredStack_ReceivedNonPrioritizedTask() {
	s.streamReceiver.receiverMode = ReceiverModeTieredStack
	replicationTask := &repicationpb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}
	streamResp := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &repicationpb.WorkflowReplicationMessages{
					ReplicationTasks:           []*repicationpb.ReplicationTask{replicationTask},
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
	s.IsType(&StreamError{}, err)
	s.Equal(0, len(s.taskScheduler.tasks))
}

func (s *streamReceiverSuite) TestProcessMessage_TrackSubmit_TieredStack() {
	replicationTask := &repicationpb.ReplicationTask{
		TaskType:       enumsspb.ReplicationTaskType(-1),
		SourceTaskId:   rand.Int63(),
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	streamResp1 := StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
		Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &repicationpb.WorkflowReplicationMessages{
					ReplicationTasks:           []*repicationpb.ReplicationTask{replicationTask},
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
				Messages: &repicationpb.WorkflowReplicationMessages{
					ReplicationTasks: []*repicationpb.ReplicationTask{
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
			s.Equal(1, len(tasks))
			s.IsType(&ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)
	s.lowPriorityTaskTracker.EXPECT().TrackTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask {
			s.Equal(streamResp2.Resp.GetMessages().ExclusiveHighWatermark, highWatermarkInfo.Watermark)
			s.Equal(streamResp2.Resp.GetMessages().ExclusiveHighWatermarkTime.AsTime(), highWatermarkInfo.Timestamp)
			s.Equal(1, len(tasks))
			s.IsType(&ExecutableUnknownTask{}, tasks[0])
			return []TrackableExecutableTask{tasks[0]}
		},
	)

	err := s.streamReceiver.processMessages(s.stream)
	s.NoError(err)
	s.Equal(2, len(s.taskScheduler.tasks))
	s.Equal(ReceiverModeTieredStack, s.streamReceiver.receiverMode)
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
	s.streamReceiver.sendEventLoop() // should not cause panic
}

func (s *streamReceiverSuite) TestRecvEventLoop_Panic_Captured() {
	s.streamReceiver.recvEventLoop() // should not cause panic
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
