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
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	streamSenderSuite struct {
		suite.Suite
		*require.Assertions

		controller    *gomock.Controller
		server        *historyservicemock.MockHistoryService_StreamWorkflowReplicationMessagesServer
		shardContext  *shard.MockContext
		historyEngine *shard.MockEngine
		taskConverter *MockSourceTaskConverter

		clientShardKey ClusterShardKey
		serverShardKey ClusterShardKey

		streamSender *StreamSenderImpl
	}
)

func TestStreamSenderSuite(t *testing.T) {
	s := new(streamSenderSuite)
	suite.Run(t, s)
}

func (s *streamSenderSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *streamSenderSuite) TearDownSuite() {

}

func (s *streamSenderSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.server = historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesServer(s.controller)
	s.shardContext = shard.NewMockContext(s.controller)
	s.historyEngine = shard.NewMockEngine(s.controller)
	s.taskConverter = NewMockSourceTaskConverter(s.controller)

	s.clientShardKey = NewClusterShardKey(rand.Int31(), rand.Int31())
	s.serverShardKey = NewClusterShardKey(rand.Int31(), rand.Int31())
	s.shardContext.EXPECT().GetEngine(gomock.Any()).Return(s.historyEngine, nil).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()

	s.streamSender = NewStreamSender(
		s.server,
		s.shardContext,
		s.historyEngine,
		s.taskConverter,
		s.clientShardKey,
		s.serverShardKey,
	)
}

func (s *streamSenderSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *streamSenderSuite) TestRecvSyncReplicationState_Success() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamp.TimePtr(time.Unix(0, rand.Int63())),
	}

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{{
				Range: &persistencespb.QueueSliceRange{
					InclusiveMin: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(replicationState.InclusiveLowWatermark),
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
	).Return(nil)
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(
		readerID,
		replicationState.InclusiveLowWatermark-1,
		*replicationState.InclusiveLowWatermarkTime,
	).Return(nil)

	err := s.streamSender.recvSyncReplicationState(replicationState)
	s.NoError(err)
}

func (s *streamSenderSuite) TestRecvSyncReplicationState_Error() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamp.TimePtr(time.Unix(0, rand.Int63())),
	}

	var ownershipLost error
	if rand.Float64() < 0.5 {
		ownershipLost = &persistence.ShardOwnershipLostError{}
	} else {
		ownershipLost = serviceerrors.NewShardOwnershipLost("", "")
	}

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{{
				Range: &persistencespb.QueueSliceRange{
					InclusiveMin: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(replicationState.InclusiveLowWatermark),
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
	).Return(ownershipLost)

	err := s.streamSender.recvSyncReplicationState(replicationState)
	s.Error(err)
	s.Equal(ownershipLost, err)
}

func (s *streamSenderSuite) TestSendCatchUp() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 1
	s.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(beginInclusiveWatermark),
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
	}, true)
	s.shardContext.EXPECT().GetImmediateQueueExclusiveHighReadWatermark().Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	)

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	taskID, err := s.streamSender.sendCatchUp()
	s.NoError(err)
	s.Equal(endExclusiveWatermark, taskID)
}

func (s *streamSenderSuite) TestSendLive() {
	channel := make(chan struct{})
	watermark0 := rand.Int63()
	watermark1 := watermark0 + 1 + rand.Int63n(100)
	watermark2 := watermark1 + 1 + rand.Int63n(100)

	gomock.InOrder(
		s.shardContext.EXPECT().GetImmediateQueueExclusiveHighReadWatermark().Return(
			tasks.NewImmediateKey(watermark1),
		),
		s.shardContext.EXPECT().GetImmediateQueueExclusiveHighReadWatermark().Return(
			tasks.NewImmediateKey(watermark2),
		),
	)
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	gomock.InOrder(
		s.historyEngine.EXPECT().GetReplicationTasksIter(
			gomock.Any(),
			string(s.clientShardKey.ClusterID),
			watermark0,
			watermark1,
		).Return(iter, nil),
		s.historyEngine.EXPECT().GetReplicationTasksIter(
			gomock.Any(),
			string(s.clientShardKey.ClusterID),
			watermark1,
			watermark2,
		).Return(iter, nil),
	)
	gomock.InOrder(
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(watermark1, resp.GetMessages().ExclusiveHighWatermark)
			s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(watermark2, resp.GetMessages().ExclusiveHighWatermark)
			s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
	)
	go func() {
		channel <- struct{}{}
		channel <- struct{}{}
		s.streamSender.shutdownChan.Shutdown()
	}()
	err := s.streamSender.sendLive(
		channel,
		watermark0,
	)
	s.Nil(err)
	s.True(!s.streamSender.IsValid())
}

func (s *streamSenderSuite) TestSendTasks_Noop() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	err := s.streamSender.sendTasks(
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSenderSuite) TestSendTasks_WithoutTasks() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	err := s.streamSender.sendTasks(
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSenderSuite) TestSendTasks_WithTasks() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item0 := tasks.NewMockTask(s.controller)
	item1 := tasks.NewMockTask(s.controller)
	item2 := tasks.NewMockTask(s.controller)
	task0 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamp.TimePtr(time.Unix(0, rand.Int63())),
	}
	task2 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark + 2,
		VisibilityTime: timestamp.TimePtr(time.Unix(0, rand.Int63())),
	}

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0, item1, item2}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConverter.EXPECT().Convert(item0).Return(task0, nil)
	s.taskConverter.EXPECT().Convert(item1).Return(nil, nil)
	s.taskConverter.EXPECT().Convert(item2).Return(task2, nil)
	gomock.InOrder(
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task0},
					ExclusiveHighWatermark:     task0.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task0.VisibilityTime,
				},
			},
		}).Return(nil),
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task2},
					ExclusiveHighWatermark:     task2.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task2.VisibilityTime,
				},
			},
		}).Return(nil),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
			s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
	)

	err := s.streamSender.sendTasks(
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}
