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

package queues

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	queueBaseSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

		controller      *gomock.Controller
		mockScheduler   *MockScheduler
		mockRescheduler *MockRescheduler

		config         *configs.Config
		options        *Options
		rateLimiter    quotas.RequestRateLimiter
		logger         log.Logger
		metricsHandler metrics.Handler
	}
)

var testQueueOptions = &Options{
	ReaderOptions: ReaderOptions{
		BatchSize:            dynamicconfig.GetIntPropertyFn(10),
		MaxPendingTasksCount: dynamicconfig.GetIntPropertyFn(100),
		PollBackoffInterval:  dynamicconfig.GetDurationPropertyFn(200 * time.Millisecond),
	},
	MonitorOptions: MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	},
	MaxPollRPS:                          dynamicconfig.GetIntPropertyFn(20),
	MaxPollInterval:                     dynamicconfig.GetDurationPropertyFn(time.Minute * 5),
	MaxPollIntervalJitterCoefficient:    dynamicconfig.GetFloatPropertyFn(0.15),
	CheckpointInterval:                  dynamicconfig.GetDurationPropertyFn(100 * time.Millisecond),
	CheckpointIntervalJitterCoefficient: dynamicconfig.GetFloatPropertyFn(0.15),
	MaxReaderCount:                      dynamicconfig.GetIntPropertyFn(5),
}

func TestQueueBaseSuite(t *testing.T) {
	s := new(queueBaseSuite)
	suite.Run(t, s)
}

func (s *queueBaseSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockScheduler = NewMockScheduler(s.controller)
	s.mockRescheduler = NewMockRescheduler(s.controller)

	s.mockScheduler.EXPECT().TaskChannelKeyFn().Return(
		func(_ Executable) TaskChannelKey { return TaskChannelKey{} },
	).AnyTimes()

	s.config = tests.NewDynamicConfig()
	s.options = testQueueOptions
	s.rateLimiter = NewReaderPriorityRateLimiter(func() float64 { return 20 }, int64(s.options.MaxReaderCount()))
	s.logger = log.NewTestLogger()
	s.metricsHandler = metrics.NoopMetricsHandler
}

func (s *queueBaseSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *queueBaseSuite) TestNewProcessBase_NoPreviousState() {
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: int64(10),
		},
		s.config,
	)

	base := s.newQueueBase(mockShard, tasks.CategoryTransfer, nil)

	s.Len(base.readerGroup.Readers(), 0)
	s.Equal(int64(1), base.nonReadableScope.Range.InclusiveMin.TaskID)
}

func (s *queueBaseSuite) TestNewProcessBase_WithPreviousState_RestoreSucceed() {
	persistenceState := &persistencespb.QueueState{
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			DefaultReaderId: {
				Scopes: []*persistencespb.QueueSliceScope{
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: &persistencespb.TaskKey{FireTime: timestamppb.New(tasks.DefaultFireTime), TaskId: 1000},
							ExclusiveMax: &persistencespb.TaskKey{FireTime: timestamppb.New(tasks.DefaultFireTime), TaskId: 2000},
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
							Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
						},
					},
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: &persistencespb.TaskKey{FireTime: timestamppb.New(tasks.DefaultFireTime), TaskId: 2000},
							ExclusiveMax: &persistencespb.TaskKey{FireTime: timestamppb.New(tasks.DefaultFireTime), TaskId: 3000},
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_TASK_TYPE,
							Attributes: &persistencespb.Predicate_TaskTypePredicateAttributes{
								TaskTypePredicateAttributes: &persistencespb.TaskTypePredicateAttributes{
									TaskTypes: []enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER},
								},
							},
						},
					},
				},
			},
			DefaultReaderId + 1: {
				Scopes: []*persistencespb.QueueSliceScope{
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: &persistencespb.TaskKey{FireTime: timestamppb.New(tasks.DefaultFireTime), TaskId: 2000},
							ExclusiveMax: &persistencespb.TaskKey{FireTime: timestamppb.New(tasks.DefaultFireTime), TaskId: 3000},
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_NAMESPACE_ID,
							Attributes: &persistencespb.Predicate_NamespaceIdPredicateAttributes{
								NamespaceIdPredicateAttributes: &persistencespb.NamespaceIdPredicateAttributes{
									NamespaceIds: []string{uuid.New()},
								},
							},
						},
					},
				},
			},
		},
		ExclusiveReaderHighWatermark: &persistencespb.TaskKey{FireTime: timestamppb.New(tasks.DefaultFireTime), TaskId: 4000},
	}

	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 10,
			QueueStates: map[int32]*persistencespb.QueueState{
				int32(tasks.CategoryIDTransfer): persistenceState,
			},
		},
		s.config,
	)
	base := s.newQueueBase(mockShard, tasks.CategoryTransfer, nil)
	readerScopes := make(map[int64][]Scope)
	for id, reader := range base.readerGroup.Readers() {
		readerScopes[id] = reader.Scopes()
	}
	queueState := &queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: base.nonReadableScope.Range.InclusiveMin,
	}

	s.ProtoEqual(persistenceState, ToPersistenceQueueState(queueState))
}

func (s *queueBaseSuite) TestStartStop() {
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 10,
		},
		s.config,
	)
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	paginationFnProvider := func(paginationRange Range) collection.PaginationFn[tasks.Task] {
		return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			mockTask := tasks.NewMockTask(s.controller)
			key := NewRandomKeyInRange(paginationRange)
			mockTask.EXPECT().GetKey().Return(key).AnyTimes()
			mockTask.EXPECT().GetNamespaceID().Return(uuid.New()).AnyTimes()
			return []tasks.Task{mockTask}, nil, nil
		}
	}

	doneCh := make(chan struct{})
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(_ Executable) bool {
		close(doneCh)
		return true
	}).Times(1)
	s.mockRescheduler.EXPECT().Len().Return(0).AnyTimes()

	base := s.newQueueBase(mockShard, tasks.CategoryTransfer, paginationFnProvider)
	s.mockRescheduler.EXPECT().Start().Times(1)
	base.Start()
	base.processNewRange()

	<-doneCh
	<-base.checkpointTimer.C

	s.mockRescheduler.EXPECT().Stop().Times(1)
	base.Stop()
	s.False(base.checkpointTimer.Stop())
}

func (s *queueBaseSuite) TestProcessNewRange() {
	queueState := &queueState{
		readerScopes: map[int64][]Scope{
			DefaultReaderId: {},
		},
		exclusiveReaderHighWatermark: tasks.MinimumKey,
	}

	persistenceState := ToPersistenceQueueState(queueState)

	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 10,
			QueueStates: map[int32]*persistencespb.QueueState{
				int32(tasks.CategoryIDTimer): persistenceState,
			},
		},
		s.config,
	)
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	base := s.newQueueBase(mockShard, tasks.CategoryTimer, nil)
	s.True(base.nonReadableScope.Range.Equals(NewRange(tasks.MinimumKey, tasks.MaximumKey)))

	base.processNewRange()
	defaultReader, ok := base.readerGroup.ReaderByID(DefaultReaderId)
	s.True(ok)
	scopes := defaultReader.Scopes()
	s.Len(scopes, 1)
	s.True(scopes[0].Range.InclusiveMin.CompareTo(tasks.MinimumKey) == 0)
	s.True(scopes[0].Predicate.Equals(predicates.Universal[tasks.Task]()))
	s.True(time.Since(scopes[0].Range.ExclusiveMax.FireTime) <= time.Second)
	s.True(base.nonReadableScope.Range.Equals(NewRange(scopes[0].Range.ExclusiveMax, tasks.MaximumKey)))
}

func (s *queueBaseSuite) TestCheckPoint_WithPendingTasks_PerformRangeCompletion() {
	scopeMinKey := tasks.MaximumKey
	readerScopes := map[int64][]Scope{}
	readerIDs := []int64{DefaultReaderId, 2, 3}
	for _, readerID := range readerIDs {
		scopes := NewRandomScopes(10)
		readerScopes[readerID] = scopes
		if len(scopes) != 0 {
			scopeMinKey = tasks.MinKey(scopeMinKey, scopes[0].Range.InclusiveMin)
		}
	}
	queueState := &queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: tasks.MaximumKey,
	}
	persistenceState := ToPersistenceQueueState(queueState)

	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 10,
			QueueStates: map[int32]*persistencespb.QueueState{
				int32(tasks.CategoryIDTimer): persistenceState,
			},
		},
		s.config,
	)
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	base := s.newQueueBase(mockShard, tasks.CategoryTimer, nil)
	base.checkpointTimer = time.NewTimer(s.options.CheckpointInterval())

	s.True(scopeMinKey.CompareTo(base.exclusiveDeletionHighWatermark) == 0)

	// set to a smaller value so that delete will be triggered
	currentLowWatermark := tasks.MinimumKey
	base.exclusiveDeletionHighWatermark = currentLowWatermark

	gomock.InOrder(
		mockShard.Resource.ExecutionMgr.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, request *persistence.RangeCompleteHistoryTasksRequest) error {
				s.Equal(mockShard.GetShardID(), request.ShardID)
				s.Equal(base.category, request.TaskCategory)
				if base.category.Type() == tasks.CategoryTypeScheduled {
					s.True(request.InclusiveMinTaskKey.FireTime.Equal(currentLowWatermark.FireTime))
					s.True(request.ExclusiveMaxTaskKey.FireTime.Equal(scopeMinKey.FireTime))
				} else {
					s.True(request.InclusiveMinTaskKey.CompareTo(currentLowWatermark) == 0)
					s.True(request.ExclusiveMaxTaskKey.CompareTo(scopeMinKey) == 0)
				}

				return nil
			},
		).Times(1),
		mockShard.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request *persistence.UpdateShardRequest) error {
				s.QueueStateEqual(persistenceState, request.ShardInfo.QueueStates[int32(tasks.CategoryIDTimer)])
				return nil
			},
		).Times(1),
	)

	base.checkpoint()

	s.True(scopeMinKey.CompareTo(base.exclusiveDeletionHighWatermark) == 0)
}

func (s *queueBaseSuite) TestCheckPoint_WithPendingTasks_SkipRangeCompletion() {
	// task range completion should be skipped when there's no task to delete
	scopeMinKey := tasks.MinimumKey
	readerScopes := map[int64][]Scope{
		DefaultReaderId: {
			{
				Range:     NewRange(scopeMinKey, tasks.NewKey(time.Now(), rand.Int63())),
				Predicate: predicates.Universal[tasks.Task](),
			},
		},
	}
	queueState := &queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: tasks.MaximumKey,
	}
	persistenceState := ToPersistenceQueueState(queueState)

	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 10,
			QueueStates: map[int32]*persistencespb.QueueState{
				int32(tasks.CategoryIDTimer): persistenceState,
			},
		},
		s.config,
	)
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	base := s.newQueueBase(mockShard, tasks.CategoryTimer, nil)
	base.checkpointTimer = time.NewTimer(s.options.CheckpointInterval())

	s.True(scopeMinKey.CompareTo(base.exclusiveDeletionHighWatermark) == 0)

	// set to a smaller value so that delete will be triggered
	currentLowWatermark := tasks.MinimumKey
	base.exclusiveDeletionHighWatermark = currentLowWatermark

	mockShard.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpdateShardRequest) error {
			s.QueueStateEqual(persistenceState, request.ShardInfo.QueueStates[int32(tasks.CategoryIDTimer)])
			return nil
		},
	).Times(1)

	base.checkpoint()

	s.True(scopeMinKey.CompareTo(base.exclusiveDeletionHighWatermark) == 0)
}

func (s *queueBaseSuite) TestCheckPoint_NoPendingTasks() {
	exclusiveReaderHighWatermark := NewRandomKey()
	queueState := &queueState{
		readerScopes:                 map[int64][]Scope{},
		exclusiveReaderHighWatermark: exclusiveReaderHighWatermark,
	}
	persistenceState := ToPersistenceQueueState(queueState)

	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 10,
			QueueStates: map[int32]*persistencespb.QueueState{
				int32(tasks.CategoryIDTimer): persistenceState,
			},
		},
		s.config,
	)
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	base := s.newQueueBase(mockShard, tasks.CategoryTimer, nil)
	base.checkpointTimer = time.NewTimer(s.options.CheckpointInterval())

	s.True(exclusiveReaderHighWatermark.CompareTo(base.exclusiveDeletionHighWatermark) == 0)

	// set to a smaller value so that delete will be triggered
	currentLowWatermark := tasks.MinimumKey
	base.exclusiveDeletionHighWatermark = currentLowWatermark

	gomock.InOrder(
		mockShard.Resource.ExecutionMgr.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, request *persistence.RangeCompleteHistoryTasksRequest) error {
				s.Equal(mockShard.GetShardID(), request.ShardID)
				s.Equal(base.category, request.TaskCategory)
				s.True(request.InclusiveMinTaskKey.CompareTo(currentLowWatermark) == 0)
				if base.category.Type() == tasks.CategoryTypeScheduled {
					s.True(request.InclusiveMinTaskKey.FireTime.Equal(currentLowWatermark.FireTime))
					s.True(request.ExclusiveMaxTaskKey.FireTime.Equal(exclusiveReaderHighWatermark.FireTime))
				} else {
					s.True(request.InclusiveMinTaskKey.CompareTo(currentLowWatermark) == 0)
					s.True(request.ExclusiveMaxTaskKey.CompareTo(exclusiveReaderHighWatermark) == 0)
				}

				return nil
			},
		).Times(1),
		mockShard.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request *persistence.UpdateShardRequest) error {
				s.QueueStateEqual(persistenceState, request.ShardInfo.QueueStates[int32(tasks.CategoryIDTimer)])
				return nil
			},
		).Times(1),
	)

	base.checkpoint()

	s.True(exclusiveReaderHighWatermark.CompareTo(base.exclusiveDeletionHighWatermark) == 0)
}

func (s *queueBaseSuite) TestCheckPoint_MoveSlices() {
	exclusiveReaderHighWatermark := tasks.MaximumKey
	scopes := NewRandomScopes(3)
	scopes[0].Predicate = tasks.NewNamespacePredicate([]string{uuid.New()})
	scopes[2].Predicate = tasks.NewTypePredicate([]enumsspb.TaskType{enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER})
	initialQueueState := &queueState{
		readerScopes: map[int64][]Scope{
			DefaultReaderId: scopes,
		},
		exclusiveReaderHighWatermark: exclusiveReaderHighWatermark,
	}
	initialPersistenceState := ToPersistenceQueueState(initialQueueState)

	expectedQueueState := &queueState{
		readerScopes: map[int64][]Scope{
			DefaultReaderId:     {scopes[1]},
			DefaultReaderId + 1: {scopes[0], scopes[2]},
		},
		exclusiveReaderHighWatermark: exclusiveReaderHighWatermark,
	}
	expectedPersistenceState := ToPersistenceQueueState(expectedQueueState)

	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 10,
			QueueStates: map[int32]*persistencespb.QueueState{
				int32(tasks.CategoryIDTimer): initialPersistenceState,
			},
		},
		s.config,
	)
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	base := s.newQueueBase(mockShard, tasks.CategoryTimer, nil)
	base.checkpointTimer = time.NewTimer(s.options.CheckpointInterval())
	s.True(scopes[0].Range.InclusiveMin.CompareTo(base.exclusiveDeletionHighWatermark) == 0)

	// set to a smaller value so that delete will be triggered
	base.exclusiveDeletionHighWatermark = tasks.MinimumKey

	// manually set pending task count to trigger slice predicate action
	base.monitor.SetSlicePendingTaskCount(&SliceImpl{}, 2*moveSliceDefaultReaderMinPendingTaskCount)

	gomock.InOrder(
		mockShard.Resource.ExecutionMgr.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), gomock.Any()).Return(nil).Times(1),
		mockShard.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, request *persistence.UpdateShardRequest) error {
				s.QueueStateEqual(expectedPersistenceState, request.ShardInfo.QueueStates[int32(tasks.CategoryIDTimer)])
				return nil
			},
		).Times(1),
	)

	base.checkpoint()

	s.True(scopes[0].Range.InclusiveMin.CompareTo(base.exclusiveDeletionHighWatermark) == 0)
}

func (s *queueBaseSuite) QueueStateEqual(
	this *persistencespb.QueueState,
	that *persistencespb.QueueState,
) {
	// ser/de so to equal will not take timezone into consideration
	thisBlob, err := serialization.QueueStateToBlob(this)
	s.NoError(err)
	this, err = serialization.QueueStateFromBlob(thisBlob.Data, thisBlob.EncodingType.String())
	s.NoError(err)

	thatBlob, err := serialization.QueueStateToBlob(that)
	s.NoError(err)
	that, err = serialization.QueueStateFromBlob(thatBlob.Data, thatBlob.EncodingType.String())
	s.NoError(err)

	s.Equal(this, that)
}

func (s *queueBaseSuite) newQueueBase(
	mockShard *shard.ContextTest,
	category tasks.Category,
	paginationFnProvider PaginationFnProvider,
) *queueBase {
	factory := NewExecutableFactory(
		nil,
		s.mockScheduler,
		s.mockRescheduler,
		NewNoopPriorityAssigner(),
		mockShard.GetTimeSource(),
		mockShard.GetNamespaceRegistry(),
		mockShard.GetClusterMetadata(),
		s.logger,
		s.metricsHandler,
		nil,
		func() bool {
			return false
		},
		func() int {
			return math.MaxInt
		},
		func() bool {
			return false
		},
		func() string {
			return ""
		},
	)
	return newQueueBase(
		mockShard,
		category,
		paginationFnProvider,
		s.mockScheduler,
		s.mockRescheduler,
		factory,
		s.options,
		s.rateLimiter,
		NoopReaderCompletionFn,
		GrouperNamespaceID{},
		s.logger,
		s.metricsHandler,
	)
}
