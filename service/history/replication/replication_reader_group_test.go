package replication

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	replicationReaderGroupSuite struct {
		suite.Suite
		*require.Assertions

		controller   *gomock.Controller
		shardContext *historyi.MockShardContext

		clientShardKey ClusterShardKey
		readerID       int64
	}
)

func TestReplicationReaderGroupSuite(t *testing.T) {
	s := new(replicationReaderGroupSuite)
	suite.Run(t, s)
}

func (s *replicationReaderGroupSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.shardContext = historyi.NewMockShardContext(s.controller)
	s.clientShardKey = NewClusterShardKey(42, 7)
	s.readerID = shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
}

func (s *replicationReaderGroupSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *replicationReaderGroupSuite) newGroup(tiered bool) *ReplicationReaderGroup {
	return NewReplicationReaderGroup(s.shardContext, s.clientShardKey, tiered)
}

// ReaderID

func (s *replicationReaderGroupSuite) TestReaderID() {
	g := s.newGroup(false)
	s.Equal(s.readerID, g.ReaderID())
}

// CatchupBeginWatermark

func (s *replicationReaderGroupSuite) TestCatchupBeginWatermark_NoQueueState_FallsBack() {
	g := s.newGroup(false)
	s.shardContext.EXPECT().GetQueueState(tasks.CategoryReplication).Return(nil, false)
	s.Equal(int64(999), g.CatchupBeginWatermark(999, enumsspb.TASK_PRIORITY_HIGH))
}

func (s *replicationReaderGroupSuite) TestCatchupBeginWatermark_NoReaderState_FallsBack() {
	g := s.newGroup(false)
	s.shardContext.EXPECT().GetQueueState(tasks.CategoryReplication).Return(
		&persistencespb.QueueState{ReaderStates: map[int64]*persistencespb.QueueReaderState{}},
		true,
	)
	s.Equal(int64(500), g.CatchupBeginWatermark(500, enumsspb.TASK_PRIORITY_LOW))
}

func (s *replicationReaderGroupSuite) TestCatchupBeginWatermark_SingleScope_UsesIndex0ForAll() {
	g := s.newGroup(false)
	state := s.makeQueueState(100, 0, 0) // 1-scope
	s.shardContext.EXPECT().GetQueueState(tasks.CategoryReplication).Return(state, true).Times(2)

	s.Equal(int64(100), g.CatchupBeginWatermark(999, enumsspb.TASK_PRIORITY_HIGH))
	s.Equal(int64(100), g.CatchupBeginWatermark(999, enumsspb.TASK_PRIORITY_LOW))
}

func (s *replicationReaderGroupSuite) TestCatchupBeginWatermark_ThreeScopes_UsesCorrectIndex() {
	g := s.newGroup(true)
	state := s.makeQueueState(100, 200, 300)
	s.shardContext.EXPECT().GetQueueState(tasks.CategoryReplication).Return(state, true).Times(3)

	s.Equal(int64(200), g.CatchupBeginWatermark(999, enumsspb.TASK_PRIORITY_HIGH))
	s.Equal(int64(300), g.CatchupBeginWatermark(999, enumsspb.TASK_PRIORITY_LOW))
	s.Equal(int64(100), g.CatchupBeginWatermark(999, enumsspb.TASK_PRIORITY_UNSPECIFIED))
}

// BuildReaderState — tiered mode

func (s *replicationReaderGroupSuite) TestBuildReaderState_Tiered_Success() {
	g := s.newGroup(true)
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     50,
		HighPriorityState:         &replicationspb.ReplicationState{InclusiveLowWatermark: 100},
		LowPriorityState:          &replicationspb.ReplicationState{InclusiveLowWatermark: 200},
	}
	got, err := g.BuildReaderState(attr)
	s.NoError(err)
	s.Len(got.Scopes, 3)
	s.Equal(int64(50), got.Scopes[0].Range.InclusiveMin.TaskId)
	s.Equal(int64(100), got.Scopes[1].Range.InclusiveMin.TaskId)
	s.Equal(int64(200), got.Scopes[2].Range.InclusiveMin.TaskId)
	for _, sc := range got.Scopes {
		s.Equal(int64(math.MaxInt64), sc.Range.ExclusiveMax.TaskId)
		s.Equal(enumsspb.PREDICATE_TYPE_UNIVERSAL, sc.Predicate.PredicateType)
	}
}

func (s *replicationReaderGroupSuite) TestBuildReaderState_Tiered_MissingHighPriority_Error() {
	g := s.newGroup(true)
	attr := &replicationspb.SyncReplicationState{
		LowPriorityState: &replicationspb.ReplicationState{InclusiveLowWatermark: 200},
	}
	_, err := g.BuildReaderState(attr)
	s.Error(err)
}

func (s *replicationReaderGroupSuite) TestBuildReaderState_Tiered_MissingLowPriority_Error() {
	g := s.newGroup(true)
	attr := &replicationspb.SyncReplicationState{
		HighPriorityState: &replicationspb.ReplicationState{InclusiveLowWatermark: 100},
	}
	_, err := g.BuildReaderState(attr)
	s.Error(err)
}

// BuildReaderState — single-stack mode

func (s *replicationReaderGroupSuite) TestBuildReaderState_SingleStack_Success() {
	g := s.newGroup(false)
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     77,
		InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, 123)),
	}
	got, err := g.BuildReaderState(attr)
	s.NoError(err)
	s.Len(got.Scopes, 1)
	s.Equal(int64(77), got.Scopes[0].Range.InclusiveMin.TaskId)
	s.Equal(int64(math.MaxInt64), got.Scopes[0].Range.ExclusiveMax.TaskId)
	s.Equal(enumsspb.PREDICATE_TYPE_UNIVERSAL, got.Scopes[0].Predicate.PredicateType)
}

func (s *replicationReaderGroupSuite) TestBuildReaderState_SingleStack_UnexpectedHighPriority_Error() {
	g := s.newGroup(false)
	attr := &replicationspb.SyncReplicationState{
		HighPriorityState: &replicationspb.ReplicationState{InclusiveLowWatermark: 100},
	}
	_, err := g.BuildReaderState(attr)
	s.Error(err)
}

func (s *replicationReaderGroupSuite) TestBuildReaderState_SingleStack_UnexpectedLowPriority_Error() {
	g := s.newGroup(false)
	attr := &replicationspb.SyncReplicationState{
		LowPriorityState: &replicationspb.ReplicationState{InclusiveLowWatermark: 200},
	}
	_, err := g.BuildReaderState(attr)
	s.Error(err)
}

// FailoverWatermark

func (s *replicationReaderGroupSuite) TestFailoverWatermark_Tiered_UsesHighPriority() {
	g := s.newGroup(true)
	ts := time.Unix(0, 987654321).UTC()
	attr := &replicationspb.SyncReplicationState{
		HighPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     300,
			InclusiveLowWatermarkTime: timestamppb.New(ts),
		},
		LowPriorityState: &replicationspb.ReplicationState{InclusiveLowWatermark: 400},
	}
	taskID, gotTs := g.FailoverWatermark(attr)
	s.Equal(int64(299), taskID)
	s.Equal(ts, gotTs)
}

func (s *replicationReaderGroupSuite) TestFailoverWatermark_SingleStack_UsesOverall() {
	g := s.newGroup(false)
	ts := time.Unix(0, 111222333).UTC()
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     150,
		InclusiveLowWatermarkTime: timestamppb.New(ts),
	}
	taskID, gotTs := g.FailoverWatermark(attr)
	s.Equal(int64(149), taskID)
	s.Equal(ts, gotTs)
}

// priorityScopeIndex

func (s *replicationReaderGroupSuite) TestPriorityScopeIndex() {
	s.Equal(1, priorityScopeIndex(enumsspb.TASK_PRIORITY_HIGH, 3))
	s.Equal(0, priorityScopeIndex(enumsspb.TASK_PRIORITY_HIGH, 1))
	s.Equal(2, priorityScopeIndex(enumsspb.TASK_PRIORITY_LOW, 3))
	s.Equal(0, priorityScopeIndex(enumsspb.TASK_PRIORITY_LOW, 1))
	s.Equal(0, priorityScopeIndex(enumsspb.TASK_PRIORITY_UNSPECIFIED, 3))
	s.Equal(0, priorityScopeIndex(enumsspb.TASK_PRIORITY_UNSPECIFIED, 1))
}

// helpers

func (s *replicationReaderGroupSuite) makeQueueState(overall, high, low int64) *persistencespb.QueueState {
	makeScope := func(taskID int64) *persistencespb.QueueSliceScope {
		return &persistencespb.QueueSliceScope{
			Range: &persistencespb.QueueSliceRange{
				InclusiveMin: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(taskID)),
				ExclusiveMax: shard.ConvertToPersistenceTaskKey(tasks.NewImmediateKey(math.MaxInt64)),
			},
			Predicate: &persistencespb.Predicate{
				PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
				Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
			},
		}
	}

	var scopes []*persistencespb.QueueSliceScope
	if high == 0 && low == 0 {
		scopes = []*persistencespb.QueueSliceScope{makeScope(overall)}
	} else {
		scopes = []*persistencespb.QueueSliceScope{makeScope(overall), makeScope(high), makeScope(low)}
	}

	return &persistencespb.QueueState{
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			s.readerID: {Scopes: scopes},
		},
	}
}
