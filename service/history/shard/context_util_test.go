package shard

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	contextUtilSuite struct {
		suite.Suite
		*require.Assertions
	}
)

func TestContextUtilSuite(t *testing.T) {
	s := &contextUtilSuite{}
	suite.Run(t, s)
}

func (s *contextUtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *contextUtilSuite) TeardownTest() {
	s.Assertions = require.New(s.T())
}

func TestGetMinTaskKey(t *testing.T) {
	key := func(taskID int64) *persistencespb.TaskKey {
		return ConvertToPersistenceTaskKey(tasks.NewImmediateKey(taskID))
	}
	rangeFrom := func(minTaskID, maxTaskID int64) *persistencespb.QueueSliceRange {
		return &persistencespb.QueueSliceRange{InclusiveMin: key(minTaskID), ExclusiveMax: key(maxTaskID)}
	}
	scopeFrom := func(minTaskID, maxTaskID int64) *persistencespb.QueueSliceScope {
		return &persistencespb.QueueSliceScope{Range: rangeFrom(minTaskID, maxTaskID)}
	}

	tests := []struct {
		name  string
		state *persistencespb.QueueState
		want  tasks.Key
	}{
		{
			name: "scopes only",
			state: &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: key(9000),
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {Scopes: []*persistencespb.QueueSliceScope{scopeFrom(3000, 4000), scopeFrom(4000, 5000)}},
				},
			},
			want: tasks.NewImmediateKey(3000),
		},
		{
			// Replication readers hold one scope per priority, each with its own ack level; only the
			// first (default priority) feeds the lag figure.
			name: "first scope is used when later scopes are lower",
			state: &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: key(9000),
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {Scopes: []*persistencespb.QueueSliceScope{
						scopeFrom(5000, 9000),
						scopeFrom(3000, 9000),
						scopeFrom(4000, 9000),
					}},
				},
			},
			want: tasks.NewImmediateKey(5000),
		},
		{
			name: "groups only",
			state: &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: key(9000),
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {PredicateGroups: []*persistencespb.QueueSlicePredicateGroup{
						{Ranges: []*persistencespb.QueueSliceRange{rangeFrom(5000, 6000), rangeFrom(2000, 3000)}},
					}},
				},
			},
			want: tasks.NewImmediateKey(2000),
		},
		{
			name: "a group holds the minimum",
			state: &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: key(9000),
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {
						Scopes: []*persistencespb.QueueSliceScope{scopeFrom(4000, 5000)},
						PredicateGroups: []*persistencespb.QueueSlicePredicateGroup{
							{Ranges: []*persistencespb.QueueSliceRange{rangeFrom(1000, 2000)}},
						},
					},
				},
			},
			want: tasks.NewImmediateKey(1000),
		},
		{
			name: "a reader with no scopes and no groups is skipped",
			state: &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: key(9000),
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {},
					1: {Scopes: []*persistencespb.QueueSliceScope{scopeFrom(3000, 4000)}},
				},
			},
			want: tasks.NewImmediateKey(3000),
		},
		{
			name: "watermark below every reader",
			state: &persistencespb.QueueState{
				ExclusiveReaderHighWatermark: key(500),
				ReaderStates: map[int64]*persistencespb.QueueReaderState{
					0: {Scopes: []*persistencespb.QueueSliceScope{scopeFrom(3000, 4000)}},
				},
			},
			want: tasks.NewImmediateKey(500),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := getMinTaskKey(tc.state)

			require.NotNil(t, got)
			require.Equal(t, tc.want, *got)
		})
	}
}

func (s *contextUtilSuite) TestReplicationReaderIDConversion() {
	expectedClusterID := int64(rand.Int31())
	expectedShardID := rand.Int31()

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *contextUtilSuite) TestReplicationReaderIDConversion_1() {
	expectedClusterID := int64(1)
	expectedShardID := int32(1)

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *contextUtilSuite) TestReplicationReaderIDConversion_Int32Max() {
	expectedClusterID := int64(math.MaxInt32)
	expectedShardID := int32(math.MaxInt32)

	actualClusterID, actualShardID := ReplicationReaderIDToClusterShardID(
		ReplicationReaderIDFromClusterShardID(expectedClusterID, expectedShardID),
	)
	s.Equal(expectedClusterID, actualClusterID)
	s.Equal(expectedShardID, actualShardID)
}

func (s *contextUtilSuite) newMockContext() *historyi.MockShardContext {
	controller := gomock.NewController(s.T())
	mockContext := historyi.NewMockShardContext(controller)
	mockContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	mockContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	return mockContext
}
