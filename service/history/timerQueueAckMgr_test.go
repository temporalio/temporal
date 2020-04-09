package history

import (
	"testing"
	"time"

	"github.com/temporalio/temporal/common/primitives"

	"github.com/gogo/protobuf/types"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	timerQueueAckMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shardContextTest
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionMgr *mocks.ExecutionManager
		mockShardMgr     *mocks.ShardManager

		logger           log.Logger
		clusterName      string
		timerQueueAckMgr *timerQueueAckMgrImpl
	}

	timerQueueFailoverAckMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shardContextTest
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionMgr *mocks.ExecutionManager
		mockShardMgr     *mocks.ShardManager

		logger                   log.Logger
		namespaceID              string
		timerQueueFailoverAckMgr *timerQueueAckMgrImpl
		minLevel                 time.Time
		maxLevel                 time.Time
	}
)

var (
	TestNamespaceId = primitives.MustParseUUID("deadbeef-c001-face-0000-000000000000")
)

func TestTimerQueueAckMgrSuite(t *testing.T) {
	s := new(timerQueueAckMgrSuite)
	suite.Run(t, s)
}

func TestTimerQueueFailoverAckMgrSuite(t *testing.T) {
	s := new(timerQueueFailoverAckMgrSuite)
	suite.Run(t, s)
}

func (s *timerQueueAckMgrSuite) SetupSuite() {

}

func (s *timerQueueAckMgrSuite) TearDownSuite() {

}

func (s *timerQueueAckMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := NewDynamicConfigForTest()
	config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)

	s.controller = gomock.NewController(s.T())
	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId: 0,
				RangeId: 1,
				ClusterTimerAckLevel: map[string]*types.Timestamp{
					cluster.TestCurrentClusterName:     gogoProtoTimestampNowAddDuration(-8),
					cluster.TestAlternativeClusterName: gogoProtoTimestampNowAddDuration(-10),
				}},
		},
		config,
	)

	s.mockShardMgr = s.mockShard.resource.ShardMgr
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata

	s.logger = s.mockShard.GetLogger()

	// this is used by shard context, not relevant to this test, so we do not care how many times "GetCurrentClusterName" is called
	s.clusterName = cluster.TestCurrentClusterName
	s.timerQueueAckMgr = newTimerQueueAckMgr(
		0,
		s.mockShard,
		s.mockShard.GetMetricsClient(),
		s.mockShard.GetTimerClusterAckLevel(s.clusterName),
		func() time.Time {
			return s.mockShard.GetCurrentTime(s.clusterName)
		},
		func(ackLevel timerKey) error {
			return s.mockShard.UpdateTimerClusterAckLevel(s.clusterName, ackLevel.VisibilityTimestamp)
		},
		s.logger,
		s.clusterName,
	)
}

func (s *timerQueueAckMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

// Test for normal ack manager

func (s *timerQueueAckMgrSuite) TestIsProcessNow() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.True(s.timerQueueAckMgr.isProcessNow(time.Time{}))

	now := s.mockShard.GetCurrentTime(s.clusterName)
	s.True(s.timerQueueAckMgr.isProcessNow(now))

	timeBefore := now.Add(-10 * time.Second)
	s.True(s.timerQueueAckMgr.isProcessNow(timeBefore))

	timeAfter := now.Add(10 * time.Second)
	s.False(s.timerQueueAckMgr.isProcessNow(timeAfter))
}

func (s *timerQueueAckMgrSuite) TestGetTimerTasks_More() {
	minTimestamp := time.Now().Add(-10 * time.Second)
	maxTimestamp := time.Now().Add(10 * time.Second)
	batchSize := 10

	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp:  minTimestamp,
		MaxTimestamp:  maxTimestamp,
		BatchSize:     batchSize,
		NextPageToken: []byte("some random input next page token"),
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistenceblobs.TimerTaskInfo{
			{
				NamespaceId:         TestNamespaceId,
				WorkflowId:          "some random workflow ID",
				RunId:               uuid.NewRandom(),
				VisibilityTimestamp: gogoProtoTimestampNowAddDuration(-5),
				TaskId:              int64(59),
				TaskType:            1,
				TimeoutType:         2,
				EventId:             int64(28),
				ScheduleAttempt:     0,
			},
		},
		NextPageToken: []byte("some random output next page token"),
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()

	timers, token, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize, request.NextPageToken)
	s.Nil(err)
	s.Equal(response.Timers, timers)
	s.Equal(response.NextPageToken, token)
}

func (s *timerQueueAckMgrSuite) TestGetTimerTasks_NoMore() {
	minTimestamp := time.Now().Add(-10 * time.Second)
	maxTimestamp := time.Now().Add(10 * time.Second)
	batchSize := 10

	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp:  minTimestamp,
		MaxTimestamp:  maxTimestamp,
		BatchSize:     batchSize,
		NextPageToken: nil,
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistenceblobs.TimerTaskInfo{
			{
				NamespaceId:         TestNamespaceId,
				WorkflowId:          "some random workflow ID",
				RunId:               uuid.NewRandom(),
				VisibilityTimestamp: gogoProtoTimestampNowAddDuration(-5),
				TaskId:              int64(59),
				TaskType:            1,
				TimeoutType:         2,
				EventId:             int64(28),
				ScheduleAttempt:     0,
			},
		},
		NextPageToken: nil,
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()

	timers, token, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize, request.NextPageToken)
	s.Nil(err)
	s.Equal(response.Timers, timers)
	s.Empty(token)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_NoLookAhead_NoNextPage() {
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

	timer := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: gogoProtoTimestampNowAddDuration(-5),
		TaskId:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(28),
		ScheduleAttempt:     0,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistenceblobs.TimerTaskInfo{timer},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(&persistence.GetTimerIndexTasksResponse{}, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistenceblobs.TimerTaskInfo{timer}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	timerSequenceID := timerKeyFromGogoTime(timer.VisibilityTimestamp, timer.GetTaskId())
	s.Equal(map[timerKey]bool{*timerSequenceID: false}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Empty(s.timerQueueAckMgr.pageToken)
	s.Equal(s.timerQueueAckMgr.minQueryLevel, s.timerQueueAckMgr.maxQueryLevel)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_NoLookAhead_HasNextPage() {
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

	timer := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: gogoProtoTimestampNowAddDuration(-5),
		TaskId:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(28),
		ScheduleAttempt:     0,
		Version:             int64(79),
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistenceblobs.TimerTaskInfo{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	readTimestamp := time.Now() // the approximate time of calling readTimerTasks
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistenceblobs.TimerTaskInfo{timer}, filteredTasks)
	s.Nil(lookAheadTask)
	s.True(moreTasks)
	timerSequenceID := timerKeyFromGogoTime(timer.VisibilityTimestamp, timer.GetTaskId())
	s.Equal(map[timerKey]bool{*timerSequenceID: false}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal(minQueryLevel, s.timerQueueAckMgr.minQueryLevel)
	s.Equal(response.NextPageToken, s.timerQueueAckMgr.pageToken)
	s.True(s.timerQueueAckMgr.maxQueryLevel.After(readTimestamp))
	s.True(s.timerQueueAckMgr.maxQueryLevel.Before(readTimestamp.Add(s.mockShard.GetConfig().TimerProcessorMaxTimeShift()).Add(time.Second)))
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_HasLookAhead_NoNextPage() {
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

	timer := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: gogoProtoTimestampNowAddDuration(int(s.mockShard.GetConfig().TimerProcessorMaxTimeShift().Seconds())),
		TaskId:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(28),
		ScheduleAttempt:     0,
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistenceblobs.TimerTaskInfo{timer},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistenceblobs.TimerTaskInfo{}, filteredTasks)
	s.Equal(timer, lookAheadTask)
	s.False(moreTasks)

	s.Equal(map[timerKey]bool{}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal(s.timerQueueAckMgr.maxQueryLevel, s.timerQueueAckMgr.minQueryLevel)
	s.Empty(s.timerQueueAckMgr.pageToken)
	s.Equal(protoToNanos(timer.VisibilityTimestamp), s.timerQueueAckMgr.maxQueryLevel.UnixNano())
}

func protoToNanos(timestamp *types.Timestamp) int64 {
	return (timestamp.Seconds * 1e9) + int64(timestamp.Nanos)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_HasLookAhead_HasNextPage() {
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

	timer := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: gogoProtoTimestampNowAddDuration(int(s.mockShard.GetConfig().TimerProcessorMaxTimeShift().Seconds())),
		TaskId:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(28),
		ScheduleAttempt:     0,
		Version:             int64(79),
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistenceblobs.TimerTaskInfo{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistenceblobs.TimerTaskInfo{}, filteredTasks)
	s.Equal(timer, lookAheadTask)
	s.False(moreTasks)

	s.Equal(map[timerKey]bool{}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal(s.timerQueueAckMgr.maxQueryLevel, s.timerQueueAckMgr.minQueryLevel)
	s.Empty(s.timerQueueAckMgr.pageToken)
	s.Equal(protoToNanos(timer.VisibilityTimestamp), s.timerQueueAckMgr.maxQueryLevel.UnixNano())
}

func (s *timerQueueAckMgrSuite) TestReadCompleteUpdateTimerTasks() {
	// create 3 timers, timer1 < timer2 < timer3 < now
	timer1 := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: gogoProtoTimestampNowAddDuration(-5),
		TaskId:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(28),
		ScheduleAttempt:     0,
	}
	timer2 := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: timer1.VisibilityTimestamp,
		TaskId:              timer1.GetTaskId() + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(29),
		ScheduleAttempt:     0,
	}
	timer3 := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: &types.Timestamp{Seconds: timer1.VisibilityTimestamp.Seconds + 1, Nanos: timer1.VisibilityTimestamp.Nanos},
		TaskId:              timer2.GetTaskId() + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(30),
		ScheduleAttempt:     0,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistenceblobs.TimerTaskInfo{timer1, timer2, timer3},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(&persistence.GetTimerIndexTasksResponse{}, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistenceblobs.TimerTaskInfo{timer1, timer2, timer3}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	// we are not testing shard context
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	timerSequenceID1 := timerKeyFromGogoTime(timer1.VisibilityTimestamp, timer1.GetTaskId())
	s.timerQueueAckMgr.completeTimerTask(timer1)
	s.True(s.timerQueueAckMgr.outstandingTasks[*timerSequenceID1])
	s.timerQueueAckMgr.updateAckLevel()
	s.Equal(protoToNanos(timer1.VisibilityTimestamp), s.mockShard.GetTimerClusterAckLevel(s.clusterName).UnixNano())

	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	timerSequenceID3 := timerKeyFromGogoTime(timer3.VisibilityTimestamp, timer3.GetTaskId())
	s.timerQueueAckMgr.completeTimerTask(timer3)
	s.True(s.timerQueueAckMgr.outstandingTasks[*timerSequenceID3])
	s.timerQueueAckMgr.updateAckLevel()
	// ack level remains unchanged
	s.Equal(protoToNanos(timer1.VisibilityTimestamp), s.mockShard.GetTimerClusterAckLevel(s.clusterName).UnixNano())

	// we are not testing shard context
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	timerSequenceID2 := timerKeyFromGogoTime(timer2.VisibilityTimestamp, timer2.GetTaskId())
	s.timerQueueAckMgr.completeTimerTask(timer2)
	s.True(s.timerQueueAckMgr.outstandingTasks[*timerSequenceID2])
	s.timerQueueAckMgr.updateAckLevel()
	s.Equal(protoToNanos(timer3.VisibilityTimestamp), s.mockShard.GetTimerClusterAckLevel(s.clusterName).UnixNano())
}

func (s *timerQueueAckMgrSuite) TestReadLookAheadTask() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(s.clusterName).AnyTimes()
	level := s.mockShard.UpdateTimerMaxReadLevel(s.clusterName)
	protoLevel, err := types.TimestampProto(level)
	s.NoError(err)
	s.timerQueueAckMgr.minQueryLevel = level
	s.timerQueueAckMgr.maxQueryLevel = s.timerQueueAckMgr.minQueryLevel

	timer := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: protoLevel,
		TaskId:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(28),
		ScheduleAttempt:     0,
		Version:             int64(79),
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistenceblobs.TimerTaskInfo{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	lookAheadTask, err := s.timerQueueAckMgr.readLookAheadTask()
	s.Nil(err)
	s.Equal(timer, lookAheadTask)
}

// Tests for failover ack manager
func (s *timerQueueFailoverAckMgrSuite) SetupSuite() {

}

func (s *timerQueueFailoverAckMgrSuite) TearDownSuite() {

}

func gogoProtoTimestampNowAddDuration(seconds int) *types.Timestamp {
	t := types.TimestampNow()
	t.Seconds += int64(seconds)
	return t
}

func (s *timerQueueFailoverAckMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := NewDynamicConfigForTest()
	config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)

	s.controller = gomock.NewController(s.T())
	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId: 0,
				RangeId: 1,
				ClusterTimerAckLevel: map[string]*types.Timestamp{
					cluster.TestCurrentClusterName:     types.TimestampNow(),
					cluster.TestAlternativeClusterName: gogoProtoTimestampNowAddDuration(-10),
				}},
			TimerFailoverLevels: make(map[string]persistence.TimerFailoverLevel),
		},
		config,
	)

	s.mockShardMgr = s.mockShard.resource.ShardMgr
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.namespaceID = "some random failover namespace ID"
	s.minLevel = time.Now().Add(-10 * time.Minute)
	s.maxLevel = time.Now().Add(10 * time.Minute)
	s.timerQueueFailoverAckMgr = newTimerQueueFailoverAckMgr(
		s.mockShard,
		s.mockShard.GetMetricsClient(),
		s.minLevel,
		s.maxLevel,
		func() time.Time {
			return s.mockShard.GetCurrentTime(s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName())
		},
		func(ackLevel timerKey) error {
			return s.mockShard.UpdateTimerFailoverLevel(
				s.namespaceID,
				persistence.TimerFailoverLevel{
					MinLevel:     ackLevel.VisibilityTimestamp,
					MaxLevel:     ackLevel.VisibilityTimestamp,
					NamespaceIDs: map[string]struct{}{s.namespaceID: {}},
				},
			)
		},
		func() error {
			return s.mockShard.DeleteTimerFailoverLevel(s.namespaceID)
		},
		s.logger,
	)
}

func (s *timerQueueFailoverAckMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *timerQueueFailoverAckMgrSuite) TestIsProcessNow() {
	// failover test to process whether to process a timer is use the current cluster's time
	now := s.mockShard.GetCurrentTime(s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName())
	s.True(s.timerQueueFailoverAckMgr.isProcessNow(time.Time{}))
	s.True(s.timerQueueFailoverAckMgr.isProcessNow(now))

	timeBefore := now.Add(-5 * time.Second)
	s.True(s.timerQueueFailoverAckMgr.isProcessNow(timeBefore))

	timeAfter := now.Add(5 * time.Second)
	s.False(s.timerQueueFailoverAckMgr.isProcessNow(timeAfter))
}

func (s *timerQueueFailoverAckMgrSuite) TestReadTimerTasks_HasNextPage() {
	ackLevel := s.timerQueueFailoverAckMgr.ackLevel
	minQueryLevel := s.timerQueueFailoverAckMgr.minQueryLevel
	token := s.timerQueueFailoverAckMgr.pageToken
	maxQueryLevel := s.timerQueueFailoverAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.minLevel, ackLevel.VisibilityTimestamp)
	s.Equal(s.minLevel, minQueryLevel)
	s.Empty(token)
	s.Equal(s.maxLevel, maxQueryLevel)

	timer1 := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: gogoProtoTimestampNowAddDuration(-5),
		TaskId:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(28),
		ScheduleAttempt:     0,
	}

	timer2 := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: gogoProtoTimestampNowAddDuration(-5),
		TaskId:              int64(60),
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(28),
		ScheduleAttempt:     0,
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistenceblobs.TimerTaskInfo{timer1, timer2},
		NextPageToken: []byte("some random next page token"),
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	readTimestamp := time.Now() // the approximate time of calling readTimerTasks
	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistenceblobs.TimerTaskInfo{timer1, timer2}, timers)
	s.Nil(lookAheadTimer)
	s.True(more)
	s.Equal(ackLevel, s.timerQueueFailoverAckMgr.ackLevel)
	s.Equal(minQueryLevel, s.timerQueueFailoverAckMgr.minQueryLevel)
	s.Equal(response.NextPageToken, s.timerQueueFailoverAckMgr.pageToken)
	s.True(s.timerQueueFailoverAckMgr.maxQueryLevel.After(readTimestamp))
	s.Equal(maxQueryLevel, s.timerQueueFailoverAckMgr.maxQueryLevel)
}

func (s *timerQueueFailoverAckMgrSuite) TestReadTimerTasks_NoNextPage() {
	ackLevel := s.timerQueueFailoverAckMgr.ackLevel
	minQueryLevel := s.timerQueueFailoverAckMgr.minQueryLevel
	token := s.timerQueueFailoverAckMgr.pageToken
	maxQueryLevel := s.timerQueueFailoverAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.minLevel, ackLevel.VisibilityTimestamp)
	s.Equal(s.minLevel, minQueryLevel)
	s.Empty(token)
	s.Equal(s.maxLevel, maxQueryLevel)

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistenceblobs.TimerTaskInfo{},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()

	readTimestamp := time.Now() // the approximate time of calling readTimerTasks
	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistenceblobs.TimerTaskInfo{}, timers)
	s.Nil(lookAheadTimer)
	s.False(more)

	s.Equal(ackLevel, s.timerQueueFailoverAckMgr.ackLevel)
	s.Equal(maximumTime, s.timerQueueFailoverAckMgr.minQueryLevel)
	s.Empty(s.timerQueueFailoverAckMgr.pageToken)
	s.True(s.timerQueueFailoverAckMgr.maxQueryLevel.After(readTimestamp))
	s.Equal(maxQueryLevel, s.timerQueueFailoverAckMgr.maxQueryLevel)
}

func (s *timerQueueFailoverAckMgrSuite) TestReadTimerTasks_InTheFuture() {
	ackLevel := s.timerQueueFailoverAckMgr.ackLevel

	// when namespace failover happen, it is possible that remote cluster's time is after
	// current cluster's time
	maxQueryLevel := time.Now()
	s.timerQueueFailoverAckMgr.minQueryLevel = maxQueryLevel.Add(1 * time.Second)
	s.timerQueueFailoverAckMgr.maxQueryLevel = maxQueryLevel

	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal(0, len(timers))
	s.Nil(lookAheadTimer)
	s.False(more)

	s.Equal(ackLevel, s.timerQueueFailoverAckMgr.ackLevel)
	s.Equal(maximumTime, s.timerQueueFailoverAckMgr.minQueryLevel)
	s.Empty(s.timerQueueFailoverAckMgr.pageToken)
	s.Equal(maxQueryLevel, s.timerQueueFailoverAckMgr.maxQueryLevel)
}

func (s *timerQueueFailoverAckMgrSuite) TestReadCompleteUpdateTimerTasks() {
	from := time.Now().Add(-10 * time.Second)
	s.timerQueueFailoverAckMgr.minQueryLevel = from
	s.timerQueueFailoverAckMgr.maxQueryLevel = from
	s.timerQueueFailoverAckMgr.ackLevel = timerKey{VisibilityTimestamp: from}

	// create 3 timers, timer1 < timer2 < timer3 < now
	timer1 := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: gogoProtoTimestampNowAddDuration(-5),
		TaskId:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(28),
		ScheduleAttempt:     0,
	}
	timer2 := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: timer1.VisibilityTimestamp,
		TaskId:              timer1.GetTaskId() + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(29),
		ScheduleAttempt:     0,
	}
	timer3 := &persistenceblobs.TimerTaskInfo{
		NamespaceId:         TestNamespaceId,
		WorkflowId:          "some random workflow ID",
		RunId:               uuid.NewRandom(),
		VisibilityTimestamp: &types.Timestamp{Seconds: timer1.VisibilityTimestamp.Seconds + 1, Nanos: timer1.VisibilityTimestamp.Nanos},
		TaskId:              timer2.GetTaskId() + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventId:             int64(30),
		ScheduleAttempt:     0,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistenceblobs.TimerTaskInfo{timer1, timer2, timer3},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistenceblobs.TimerTaskInfo{timer1, timer2, timer3}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	timerSequenceID2 := timerKeyFromGogoTime(timer2.VisibilityTimestamp, timer2.GetTaskId())
	s.timerQueueFailoverAckMgr.completeTimerTask(timer2)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[*timerSequenceID2])
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
		s.Fail("timer queue ack mgr finished chan should not be fired")
	default:
	}

	timerSequenceID3 := timerKeyFromGogoTime(timer3.VisibilityTimestamp, timer3.GetTaskId())
	s.timerQueueFailoverAckMgr.completeTimerTask(timer3)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[*timerSequenceID3])
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
		s.Fail("timer queue ack mgr finished chan should not be fired")
	default:
	}

	timerSequenceID1 := timerKeyFromGogoTime(timer1.VisibilityTimestamp, timer1.GetTaskId())
	s.timerQueueFailoverAckMgr.completeTimerTask(timer1)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[*timerSequenceID1])
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
	default:
		s.Fail("timer queue ack mgr finished chan should be fired")
	}
}
