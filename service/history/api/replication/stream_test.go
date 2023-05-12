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
	"context"
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
	historyclient "go.temporal.io/server/client/history"
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
	streamSuite struct {
		suite.Suite
		*require.Assertions

		controller    *gomock.Controller
		server        *historyservicemock.MockHistoryService_StreamWorkflowReplicationMessagesServer
		shardContext  *shard.MockContext
		historyEngine *shard.MockEngine
		taskConvertor *MockTaskConvertor

		ctx                  context.Context
		cancel               context.CancelFunc
		clientClusterShardID historyclient.ClusterShardID
		serverClusterShardID historyclient.ClusterShardID
	}
)

func TestStreamSuite(t *testing.T) {
	s := new(streamSuite)
	suite.Run(t, s)
}

func (s *streamSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *streamSuite) TearDownSuite() {

}

func (s *streamSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.server = historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesServer(s.controller)
	s.shardContext = shard.NewMockContext(s.controller)
	s.historyEngine = shard.NewMockEngine(s.controller)
	s.taskConvertor = NewMockTaskConvertor(s.controller)

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.clientClusterShardID = historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	s.serverClusterShardID = historyclient.ClusterShardID{
		ClusterID: rand.Int31(),
		ShardID:   rand.Int31(),
	}
	s.shardContext.EXPECT().GetEngine(gomock.Any()).Return(s.historyEngine, nil).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
}

func (s *streamSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *streamSuite) TestRecvSyncReplicationState_Success() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientClusterShardID.ClusterID),
		s.clientClusterShardID.ShardID,
	)
	replicationState := &replicationspb.SyncReplicationState{
		LastProcessedMessageId:   rand.Int63(),
		LastProcessedMessageTime: timestamp.TimePtr(time.Unix(0, rand.Int63())),
	}

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{{
				Range: &persistencespb.QueueSliceRange{
					InclusiveMin: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(replicationState.LastProcessedMessageId + 1),
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
	s.shardContext.EXPECT().UpdateRemoteClusterInfo(
		string(s.clientClusterShardID.ClusterID),
		replicationState.LastProcessedMessageId,
		*replicationState.LastProcessedMessageTime,
	)

	err := recvSyncReplicationState(s.shardContext, replicationState, s.clientClusterShardID)
	s.NoError(err)
}

func (s *streamSuite) TestRecvSyncReplicationState_Error() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientClusterShardID.ClusterID),
		s.clientClusterShardID.ShardID,
	)
	replicationState := &replicationspb.SyncReplicationState{
		LastProcessedMessageId:   rand.Int63(),
		LastProcessedMessageTime: timestamp.TimePtr(time.Unix(0, rand.Int63())),
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
						tasks.NewImmediateKey(replicationState.LastProcessedMessageId + 1),
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

	err := recvSyncReplicationState(s.shardContext, replicationState, s.clientClusterShardID)
	s.Error(err)
	s.Equal(ownershipLost, err)
}

func (s *streamSuite) TestSendCatchUp() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientClusterShardID.ClusterID),
		s.clientClusterShardID.ShardID,
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
		s.ctx,
		string(s.clientClusterShardID.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark-1, resp.GetMessages().LastTaskId)
		s.NotNil(resp.GetMessages().LastTaskTime)
		return nil
	})

	taskID, err := sendCatchUp(
		s.ctx,
		s.server,
		s.shardContext,
		s.taskConvertor,
		s.clientClusterShardID,
		s.serverClusterShardID,
	)
	s.NoError(err)
	s.Equal(endExclusiveWatermark, taskID)
}

func (s *streamSuite) TestSendLive() {
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
			s.ctx,
			string(s.clientClusterShardID.ClusterID),
			watermark0,
			watermark1,
		).Return(iter, nil),
		s.historyEngine.EXPECT().GetReplicationTasksIter(
			s.ctx,
			string(s.clientClusterShardID.ClusterID),
			watermark1,
			watermark2,
		).Return(iter, nil),
	)
	gomock.InOrder(
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(watermark1-1, resp.GetMessages().LastTaskId)
			s.NotNil(resp.GetMessages().LastTaskTime)
			return nil
		}),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(watermark2-1, resp.GetMessages().LastTaskId)
			s.NotNil(resp.GetMessages().LastTaskTime)
			return nil
		}),
	)
	go func() {
		channel <- struct{}{}
		channel <- struct{}{}
		s.cancel()
	}()
	err := sendLive(
		s.ctx,
		s.server,
		s.shardContext,
		s.taskConvertor,
		s.clientClusterShardID,
		s.serverClusterShardID,
		channel,
		watermark0,
	)
	s.Equal(s.ctx.Err(), err)
}

func (s *streamSuite) TestSendTasks_Noop() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark

	err := sendTasks(
		s.ctx,
		s.server,
		s.shardContext,
		s.taskConvertor,
		s.clientClusterShardID,
		s.serverClusterShardID,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSuite) TestSendTasks_WithoutTasks() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		s.ctx,
		string(s.clientClusterShardID.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark-1, resp.GetMessages().LastTaskId)
		s.NotNil(resp.GetMessages().LastTaskTime)
		return nil
	})

	err := sendTasks(
		s.ctx,
		s.server,
		s.shardContext,
		s.taskConvertor,
		s.clientClusterShardID,
		s.serverClusterShardID,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSuite) TestSendTasks_WithTasks() {
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
		s.ctx,
		string(s.clientClusterShardID.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConvertor.EXPECT().Convert(item0).Return(task0, nil)
	s.taskConvertor.EXPECT().Convert(item1).Return(nil, nil)
	s.taskConvertor.EXPECT().Convert(item2).Return(task2, nil)
	gomock.InOrder(
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks: []*replicationspb.ReplicationTask{task0},
					LastTaskId:       task0.SourceTaskId,
					LastTaskTime:     task0.VisibilityTime,
				},
			},
		}).Return(nil),
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks: []*replicationspb.ReplicationTask{task2},
					LastTaskId:       task2.SourceTaskId,
					LastTaskTime:     task2.VisibilityTime,
				},
			},
		}).Return(nil),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(endExclusiveWatermark-1, resp.GetMessages().LastTaskId)
			s.NotNil(resp.GetMessages().LastTaskTime)
			return nil
		}),
	)

	err := sendTasks(
		s.ctx,
		s.server,
		s.shardContext,
		s.taskConvertor,
		s.clientClusterShardID,
		s.serverClusterShardID,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}
