package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	taskRefresherSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockShard             *shard.ContextTest
		mockNamespaceRegistry *namespace.MockRegistry
		mockTaskGenerator     *MockTaskGenerator

		namespaceEntry       *namespace.Namespace
		mutableState         historyi.MutableState
		stateMachineRegistry *hsm.Registry

		taskRefresher *TaskRefresherImpl
	}
)

func TestTaskRefresherSuite(t *testing.T) {
	s := new(taskRefresherSuite)
	suite.Run(t, s)
}

func (s *taskRefresherSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := tests.NewDynamicConfig()
	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		persistencespb.ShardInfo_builder{ShardId: 1}.Build(),
		config,
	)
	s.mockNamespaceRegistry = s.mockShard.Resource.NamespaceCache

	s.stateMachineRegistry = hsm.NewRegistry()
	s.mockShard.SetStateMachineRegistry(s.stateMachineRegistry)
	s.NoError(RegisterStateMachine(s.stateMachineRegistry))

	s.namespaceEntry = tests.GlobalNamespaceEntry
	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(s.namespaceEntry, nil).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespace(tests.Namespace).Return(s.namespaceEntry, nil).AnyTimes()
	s.mockTaskGenerator = NewMockTaskGenerator(s.controller)
	s.mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, s.namespaceEntry.FailoverVersion(tests.WorkflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.mockShard.GetLogger(),
		s.namespaceEntry.FailoverVersion(tests.WorkflowID),
		tests.WorkflowID,
		tests.RunID,
	)

	s.taskRefresher = NewTaskRefresher(s.mockShard)
	s.taskRefresher.taskGeneratorProvider = newMockTaskGeneratorProvider(s.mockTaskGenerator)
}

func (s *taskRefresherSuite) TestRefreshWorkflowStartTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: historyspb.VersionHistories_builder{
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							historyspb.VersionHistoryItem_builder{EventId: 2, Version: common.EmptyVersion}.Build(),
						},
					}.Build(),
				},
			}.Build(),
			WorkflowExecutionTimerTaskStatus: TimerTaskStatusCreated,
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
				TransitionCount:          1,
				NamespaceFailoverVersion: common.EmptyVersion,
			}.Build(),
		}.Build(),
		NextEventId: int64(3),
	}.Build()
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	s.NoError(err)

	startEvent := historypb.HistoryEvent_builder{
		EventId:   common.FirstEventID,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
			FirstWorkflowTaskBackoff: durationpb.New(10 * time.Second),
		}.Build(),
	}.Build()
	s.mockShard.MockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		s.mockShard.GetShardID(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
			EventID:     common.FirstEventID,
			Version:     common.EmptyVersion,
		},
		common.FirstEventID,
		branchToken,
	).Return(startEvent, nil).Times(1)
	s.mockTaskGenerator.EXPECT().GenerateWorkflowStartTasks(startEvent).DoAndReturn(
		func(_ *historypb.HistoryEvent) (int32, error) {
			s.Equal(int32(TimerTaskStatusNone), mutableState.GetExecutionInfo().GetWorkflowExecutionTimerTaskStatus())
			return int32(TimerTaskStatusCreated), nil
		},
	)
	s.mockTaskGenerator.EXPECT().GenerateDelayedWorkflowTasks(startEvent).Return(nil).Times(1)

	err = RefreshTasksForWorkflowStart(context.Background(), mutableState, s.mockTaskGenerator, EmptyVersionedTransition)
	s.NoError(err)
	s.Equal(int32(TimerTaskStatusCreated), mutableState.GetExecutionInfo().GetWorkflowExecutionTimerTaskStatus())

	err = RefreshTasksForWorkflowStart(context.Background(), mutableState, s.mockTaskGenerator, persistencespb.VersionedTransition_builder{
		// TransitionCount is higher than workflow state's last update versioned transition,
		// no task should be generated and no call to task generator should be made.
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build())
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshRecordWorkflowStartedTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: historyspb.VersionHistories_builder{
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							historyspb.VersionHistoryItem_builder{EventId: 2, Version: common.EmptyVersion}.Build(),
						},
					}.Build(),
				},
			}.Build(),
			VisibilityLastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
				TransitionCount:          1,
				NamespaceFailoverVersion: common.EmptyVersion,
			}.Build(),
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(3),
	}.Build()
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	s.NoError(err)

	startEvent := historypb.HistoryEvent_builder{
		EventId:                                 common.FirstEventID,
		Version:                                 common.EmptyVersion,
		EventType:                               enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
	}.Build()
	s.mockShard.MockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		s.mockShard.GetShardID(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
			EventID:     common.FirstEventID,
			Version:     common.EmptyVersion,
		},
		common.FirstEventID,
		branchToken,
	).Return(startEvent, nil).Times(1)
	s.mockTaskGenerator.EXPECT().GenerateRecordWorkflowStartedTasks(startEvent).Return(nil).Times(1)

	err = s.taskRefresher.refreshTasksForRecordWorkflowStarted(context.Background(), mutableState, s.mockTaskGenerator, EmptyVersionedTransition)
	s.NoError(err)

	err = s.taskRefresher.refreshTasksForRecordWorkflowStarted(context.Background(), mutableState, s.mockTaskGenerator, persistencespb.VersionedTransition_builder{
		// TransitionCount is higher than workflow visibility's last update versioned transition,
		// no task should be generated and no call to task generator should be made.
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build())
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshWorkflowCloseTasks() {
	closeTime := timestamppb.Now()
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			CloseTime:   closeTime,
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
				TransitionCount:          2,
				NamespaceFailoverVersion: common.EmptyVersion,
			}.Build(),
		}.Build(),
		NextEventId: int64(3),
	}.Build()
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	s.NoError(err)

	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(closeTime.AsTime(), false, false).Return(nil).Times(1)

	err = s.taskRefresher.refreshTasksForWorkflowClose(context.Background(), mutableState, s.mockTaskGenerator, EmptyVersionedTransition, false)
	s.NoError(err)

	err = s.taskRefresher.refreshTasksForWorkflowClose(context.Background(), mutableState, s.mockTaskGenerator, persistencespb.VersionedTransition_builder{
		// TransitionCount is higher than workflow state's last update versioned transition,
		TransitionCount:          3,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build(), false)
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshWorkflowTaskTasks() {
	baseMutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: historyspb.VersionHistories_builder{
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{
						BranchToken: []byte("branchToken"),
						Items: []*historyspb.VersionHistoryItem{
							historyspb.VersionHistoryItem_builder{EventId: 3, Version: common.EmptyVersion}.Build(),
						},
					}.Build(),
				},
			}.Build(),
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(3),
	}.Build()

	testCase := []struct {
		name                   string
		msRecordProvider       func() *persistencespb.WorkflowMutableState
		setupMock              func()
		minVersionedTransition *persistencespb.VersionedTransition
	}{
		{
			name: "Refresh/NoWorkflowTask",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				return common.CloneProto(baseMutableStateRecord)
			},
			setupMock:              func() {},
			minVersionedTransition: EmptyVersionedTransition,
		},
		{
			name: "Refresh/SpeculativeWorkflowTask",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.GetExecutionInfo().SetWorkflowTaskScheduledEventId(2)
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskAttempt(1)
				record.GetExecutionInfo().SetWorkflowTaskType(enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
				return record
			},
			setupMock:              func() {},
			minVersionedTransition: EmptyVersionedTransition,
		},
		{
			name: "Refresh/WorkflowTaskScheduled",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.GetExecutionInfo().SetWorkflowTaskScheduledEventId(2)
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskAttempt(1)
				record.GetExecutionInfo().SetWorkflowTaskType(enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
				return record
			},
			setupMock: func() {
				s.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(int64(2)).Return(nil).Times(1)
			},
			minVersionedTransition: EmptyVersionedTransition,
		},
		{
			name: "Refresh/WorkflowTaskStarted",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.GetExecutionInfo().SetWorkflowTaskScheduledEventId(2)
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskAttempt(1)
				record.GetExecutionInfo().SetWorkflowTaskStartedEventId(3)
				record.GetExecutionInfo().SetWorkflowTaskStartedTime(timestamppb.New(time.Now().Add(time.Second)))
				record.GetExecutionInfo().SetWorkflowTaskRequestId(uuid.NewString())
				record.GetExecutionInfo().SetWorkflowTaskType(enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
				return record
			},
			setupMock: func() {
				s.mockTaskGenerator.EXPECT().GenerateStartWorkflowTaskTasks(int64(2)).Return(nil).Times(1)
			},
			minVersionedTransition: EmptyVersionedTransition,
		},
		{
			name: "PartialRefresh/Skipped",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.GetExecutionInfo().SetWorkflowTaskScheduledEventId(2)
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskAttempt(1)
				record.GetExecutionInfo().SetWorkflowTaskType(enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
				record.GetExecutionInfo().SetWorkflowTaskLastUpdateVersionedTransition(persistencespb.VersionedTransition_builder{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build())
				return record
			},
			setupMock: func() {},
			minVersionedTransition: persistencespb.VersionedTransition_builder{
				TransitionCount:          2,
				NamespaceFailoverVersion: common.EmptyVersion,
			}.Build(),
		},
		{
			name: "PartialRefresh/Refreshed",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.GetExecutionInfo().SetWorkflowTaskScheduledEventId(2)
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskAttempt(1)
				record.GetExecutionInfo().SetWorkflowTaskType(enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
				record.GetExecutionInfo().SetWorkflowTaskLastUpdateVersionedTransition(persistencespb.VersionedTransition_builder{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build())
				return record
			},
			setupMock: func() {
				s.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(int64(2)).Return(nil).Times(1)
			},
			minVersionedTransition: persistencespb.VersionedTransition_builder{
				TransitionCount:          1,
				NamespaceFailoverVersion: common.EmptyVersion,
			}.Build(),
		},
		{
			name: "PartialRefresh/UnknownLastUpdateVersionedTransition",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.GetExecutionInfo().SetWorkflowTaskScheduledEventId(2)
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskScheduledTime(timestamppb.Now())
				record.GetExecutionInfo().SetWorkflowTaskAttempt(1)
				record.GetExecutionInfo().SetWorkflowTaskType(enumsspb.WORKFLOW_TASK_TYPE_NORMAL)
				// WorkflowTaskLastUpdateVersionedTransition not specified.
				// This could happen for ms record persisted before versioned transition is enabled.
				// We do not refresh in this case unless the refresh request is a full refresh
				// (minVersionedTransition is EmptyVersionedTransition), because the fact that
				// lastUpdateVersionedTransition is unknown means it's updated before the given
				// minVersionedTransition.
				return record
			},
			setupMock: func() {},
			minVersionedTransition: persistencespb.VersionedTransition_builder{
				TransitionCount:          2,
				NamespaceFailoverVersion: common.EmptyVersion,
			}.Build(),
		},
	}

	for _, tc := range testCase {
		s.T().Run(tc.name, func(t *testing.T) {
			mutableState, err := NewMutableStateFromDB(
				s.mockShard,
				s.mockShard.GetEventsCache(),
				log.NewTestLogger(),
				tests.LocalNamespaceEntry,
				tc.msRecordProvider(),
				101,
			)
			s.NoError(err)

			tc.setupMock()
			err = s.taskRefresher.refreshWorkflowTaskTasks(mutableState, s.mockTaskGenerator, tc.minVersionedTransition)
			s.NoError(err)
		})
	}
}

// This test asserts that the workflow tasks tasks are not refreshed when the workflow status is paused.
func (s *taskRefresherSuite) TestRefreshWorkflowTaskTasks_WhenPaused() {
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: historyspb.VersionHistories_builder{
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{
						BranchToken: []byte("branchToken"),
						Items: []*historyspb.VersionHistoryItem{
							historyspb.VersionHistoryItem_builder{EventId: 3, Version: common.EmptyVersion}.Build(),
						},
					}.Build(),
				},
			}.Build(),
			WorkflowTaskScheduledEventId: 2,
			WorkflowTaskScheduledTime:    timestamppb.Now(),
			WorkflowTaskAttempt:          1,
			WorkflowTaskType:             enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, // Workflow is paused
		}.Build(),
		NextEventId: int64(3),
	}.Build()

	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		101,
	)
	s.NoError(err)

	// No task generator calls expected since workflow is paused
	err = s.taskRefresher.refreshWorkflowTaskTasks(mutableState, s.mockTaskGenerator, EmptyVersionedTransition)
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshActivityTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: historyspb.VersionHistories_builder{
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							historyspb.VersionHistoryItem_builder{EventId: 10, Version: common.EmptyVersion}.Build(),
						},
					}.Build(),
				},
			}.Build(),
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(11),
		ActivityInfos: map[int64]*persistencespb.ActivityInfo{
			5: persistencespb.ActivityInfo_builder{
				ActivityId:             "5",
				ScheduledEventId:       5,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedEventId:         common.EmptyEventID,
				TimerTaskStatus:        TimerTaskStatusCreatedScheduleToStart,
				ScheduleToStartTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(10 * time.Second),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          4,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			6: persistencespb.ActivityInfo_builder{
				ActivityId:             "6",
				ScheduledEventId:       6,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedTime:            timestamppb.New(time.Now().Add(time.Second)),
				StartedEventId:         8,
				RequestId:              uuid.NewString(),
				TimerTaskStatus:        TimerTaskStatusCreatedStartToClose,
				ScheduleToStartTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(10 * time.Second),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			7: persistencespb.ActivityInfo_builder{
				ActivityId:             "7",
				ScheduledEventId:       7,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedEventId:         common.EmptyEventID,
				TimerTaskStatus:        TimerTaskStatusCreatedScheduleToStart,
				ScheduleToStartTimeout: durationpb.New(1 * time.Second),
				StartToCloseTimeout:    durationpb.New(1 * time.Second),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
		},
	}.Build()

	testCase := []struct {
		name                         string
		minVersionedTransition       *persistencespb.VersionedTransition
		getActivityScheduledEventIDs []int64
		generateActivityTaskIDs      []int64
		expectedTimerTaskStatus      map[int64]int32
		expectedRefreshedTasks       []tasks.Task
	}{
		{
			name: "PartialRefresh",
			minVersionedTransition: persistencespb.VersionedTransition_builder{
				TransitionCount:          4,
				NamespaceFailoverVersion: common.EmptyVersion,
			}.Build(),
			getActivityScheduledEventIDs: []int64{5},
			generateActivityTaskIDs:      []int64{5},
			expectedTimerTaskStatus: map[int64]int32{
				5: TimerTaskStatusCreatedScheduleToStart,
				6: TimerTaskStatusCreatedStartToClose,
				7: TimerTaskStatusCreatedScheduleToStart,
			},
		},
		{
			name:                         "FullRefresh",
			minVersionedTransition:       EmptyVersionedTransition,
			getActivityScheduledEventIDs: []int64{5, 7},
			generateActivityTaskIDs:      []int64{5, 7},
			expectedTimerTaskStatus: map[int64]int32{
				5: TimerTaskStatusNone,
				6: TimerTaskStatusNone,
				7: TimerTaskStatusCreatedScheduleToStart,
			},
			expectedRefreshedTasks: []tasks.Task{
				&tasks.ActivityTimeoutTask{
					WorkflowKey:         s.mutableState.GetWorkflowKey(),
					VisibilityTimestamp: mutableStateRecord.GetActivityInfos()[7].GetScheduledTime().AsTime().Add(mutableStateRecord.GetActivityInfos()[7].GetScheduleToStartTimeout().AsDuration()),
					EventID:             7,
					TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
					Attempt:             0,
				},
			},
		},
	}

	for _, tc := range testCase {
		s.T().Run(tc.name, func(t *testing.T) {
			mutableState, err := NewMutableStateFromDB(
				s.mockShard,
				s.mockShard.GetEventsCache(),
				log.NewTestLogger(),
				tests.LocalNamespaceEntry,
				mutableStateRecord,
				10,
			)
			s.NoError(err)
			for _, eventID := range tc.generateActivityTaskIDs {
				s.mockTaskGenerator.EXPECT().GenerateActivityTasks(int64(eventID)).Return(nil).Times(1)
			}

			err = s.taskRefresher.refreshTasksForActivity(context.Background(), mutableState, s.mockTaskGenerator, tc.minVersionedTransition)
			s.NoError(err)

			pendingActivityInfos := mutableState.GetPendingActivityInfos()
			s.Len(pendingActivityInfos, 3)
			s.Equal(tc.expectedTimerTaskStatus[5], pendingActivityInfos[5].GetTimerTaskStatus())
			s.Equal(tc.expectedTimerTaskStatus[6], pendingActivityInfos[6].GetTimerTaskStatus())
			s.Equal(tc.expectedTimerTaskStatus[7], pendingActivityInfos[7].GetTimerTaskStatus())

			refreshedTasks := mutableState.PopTasks()
			s.Len(refreshedTasks[tasks.CategoryTimer], len(tc.expectedRefreshedTasks))
			for idx, task := range refreshedTasks[tasks.CategoryTimer] {
				if activityTimeoutTask, ok := task.(*tasks.ActivityTimeoutTask); ok {
					s.Equal(tc.expectedRefreshedTasks[idx], activityTimeoutTask)
				}
			}
		})
	}

}

// This test asserts that the activity tasks are not refreshed when the workflow status is paused.
func (s *taskRefresherSuite) TestRefreshActivityTasks_WhenPaused() {
	branchToken := []byte("branchToken")
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: historyspb.VersionHistories_builder{
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							historyspb.VersionHistoryItem_builder{EventId: 10, Version: common.EmptyVersion}.Build(),
						},
					}.Build(),
				},
			}.Build(),
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED, // Workflow is paused
		}.Build(),
		NextEventId: int64(11),
		ActivityInfos: map[int64]*persistencespb.ActivityInfo{
			5: persistencespb.ActivityInfo_builder{
				ActivityId:             "5",
				ScheduledEventId:       5,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedEventId:         common.EmptyEventID,
				TimerTaskStatus:        TimerTaskStatusCreatedScheduleToStart,
				ScheduleToStartTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(10 * time.Second),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          4,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			6: persistencespb.ActivityInfo_builder{
				ActivityId:             "6",
				ScheduledEventId:       6,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedTime:            timestamppb.New(time.Now().Add(time.Second)),
				StartedEventId:         8,
				RequestId:              uuid.NewString(),
				TimerTaskStatus:        TimerTaskStatusCreatedStartToClose,
				ScheduleToStartTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(10 * time.Second),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
		},
	}.Build()

	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	// No task generator calls expected since workflow is paused
	err = s.taskRefresher.refreshTasksForActivity(context.Background(), mutableState, s.mockTaskGenerator, EmptyVersionedTransition)
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshUserTimer() {
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(11),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			"5": persistencespb.TimerInfo_builder{
				TimerId:        "5",
				StartedEventId: 5,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(time.Now().Add(10 * time.Second)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			"6": persistencespb.TimerInfo_builder{
				TimerId:        "6",
				StartedEventId: 6,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(time.Now().Add(100 * time.Second)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
		},
	}.Build()
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	err = s.taskRefresher.refreshTasksForTimer(mutableState, persistencespb.VersionedTransition_builder{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build())
	s.NoError(err)

	pendingTimerInfos := mutableState.GetPendingTimerInfos()
	s.Len(pendingTimerInfos, 2)
	s.Equal(int64(TimerTaskStatusCreated), pendingTimerInfos["5"].GetTaskStatus())
	s.Equal(int64(TimerTaskStatusCreated), pendingTimerInfos["6"].GetTaskStatus())

	refreshedTasks := mutableState.PopTasks()
	s.Len(refreshedTasks[tasks.CategoryTimer], 1)
}

func (s *taskRefresherSuite) TestRefreshUserTimer_Partial_NoUpdatedTimers_MaskNone_GeneratesEarliest() {
	now := time.Now().UTC()
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(20),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			// Earliest timer has TaskStatus None (as on passive), lastUpdate older than minVersion
			"10": persistencespb.TimerInfo_builder{
				TimerId:        "10",
				StartedEventId: 10,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(5 * time.Minute)),
				TaskStatus:     TimerTaskStatusNone,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			// Later timer remains Created
			"15": persistencespb.TimerInfo_builder{
				TimerId:        "15",
				StartedEventId: 15,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(10 * time.Minute)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
		},
	}.Build()

	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	// minVersion is higher than both timers' lastUpdate; loop clears none, but CreateNextUserTimer should still create earliest
	err = s.taskRefresher.refreshTasksForTimer(mutableState, persistencespb.VersionedTransition_builder{
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build())
	s.NoError(err)

	// Earliest timer should now be marked Created and one task enqueued
	pendingTimerInfos := mutableState.GetPendingTimerInfos()
	s.Equal(int64(TimerTaskStatusCreated), pendingTimerInfos["10"].GetTaskStatus())
	s.Equal(int64(TimerTaskStatusCreated), pendingTimerInfos["15"].GetTaskStatus())

	refreshedTasks := mutableState.PopTasks()
	s.Len(refreshedTasks[tasks.CategoryTimer], 1)
}

func (s *taskRefresherSuite) TestRefreshUserTimer_Partial_NoUpdatedTimers_MaskCreated_NoTask() {
	now := time.Now().UTC()
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(20),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			// Both timers Created and older than minVersion; CreateNextUserTimer should no-op
			"10": persistencespb.TimerInfo_builder{
				TimerId:        "10",
				StartedEventId: 10,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(5 * time.Minute)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			"15": persistencespb.TimerInfo_builder{
				TimerId:        "15",
				StartedEventId: 15,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(10 * time.Minute)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
		},
	}.Build()

	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	err = s.taskRefresher.refreshTasksForTimer(mutableState, persistencespb.VersionedTransition_builder{
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build())
	s.NoError(err)

	// No new tasks since earliest already Created
	refreshedTasks := mutableState.PopTasks()
	s.Empty(refreshedTasks[tasks.CategoryTimer])
}

func (s *taskRefresherSuite) TestRefreshUserTimer_FullRefresh_ClearsMasks_EnqueuesEarliest() {
	now := time.Now().UTC()
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(20),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			"10": persistencespb.TimerInfo_builder{
				TimerId:        "10",
				StartedEventId: 10,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(5 * time.Minute)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			"15": persistencespb.TimerInfo_builder{
				TimerId:        "15",
				StartedEventId: 15,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(10 * time.Minute)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
		},
	}.Build()

	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	// Full refresh
	err = s.taskRefresher.refreshTasksForTimer(mutableState, EmptyVersionedTransition)
	s.NoError(err)

	pendingTimerInfos := mutableState.GetPendingTimerInfos()
	// Earliest should be Created again, later should be left as None
	s.Equal(int64(TimerTaskStatusCreated), pendingTimerInfos["10"].GetTaskStatus())
	s.Equal(int64(TimerTaskStatusNone), pendingTimerInfos["15"].GetTaskStatus())

	refreshedTasks := mutableState.PopTasks()
	s.Len(refreshedTasks[tasks.CategoryTimer], 1)
}

func (s *taskRefresherSuite) TestRefreshUserTimer_RunExpiration_SkipsTask() {
	now := time.Now().UTC()
	runExpiration := now.Add(3 * time.Minute)
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId:               tests.NamespaceID.String(),
			WorkflowId:                tests.WorkflowID,
			WorkflowRunExpirationTime: timestamppb.New(runExpiration),
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(20),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			// Earliest timer expires after run expiration; should be skipped by CreateNextUserTimer
			"10": persistencespb.TimerInfo_builder{
				TimerId:        "10",
				StartedEventId: 10,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(now.Add(10 * time.Minute)),
				TaskStatus:     TimerTaskStatusNone,
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          2,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
		},
	}.Build()

	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	err = s.taskRefresher.refreshTasksForTimer(mutableState, persistencespb.VersionedTransition_builder{
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build())
	s.NoError(err)

	// No task generated due to run-expiration guard
	refreshedTasks := mutableState.PopTasks()
	s.Empty(refreshedTasks[tasks.CategoryTimer])
}

func (s *taskRefresherSuite) TestRefreshChildWorkflowTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: historyspb.VersionHistories_builder{
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							historyspb.VersionHistoryItem_builder{EventId: 10, Version: common.EmptyVersion}.Build(),
						},
					}.Build(),
				},
			}.Build(),
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(11),
		ChildExecutionInfos: map[int64]*persistencespb.ChildExecutionInfo{
			5: persistencespb.ChildExecutionInfo_builder{
				InitiatedEventBatchId: 4,
				InitiatedEventId:      5,
				CreateRequestId:       uuid.NewString(),
				StartedWorkflowId:     "child-workflow-id-5",
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			6: persistencespb.ChildExecutionInfo_builder{
				InitiatedEventBatchId: 4,
				InitiatedEventId:      6,
				CreateRequestId:       uuid.NewString(),
				StartedWorkflowId:     "child-workflow-id-6",
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			7: persistencespb.ChildExecutionInfo_builder{
				InitiatedEventBatchId: 4,
				InitiatedEventId:      7,
				StartedEventId:        8,
				CreateRequestId:       uuid.NewString(),
				StartedWorkflowId:     "child-workflow-id-7",
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
		},
	}.Build()
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	testcases := []struct {
		name                   string
		hasPendingChildIds     bool
		expectedRefreshedTasks []int64
	}{
		{
			name:                   "has pending child ids",
			hasPendingChildIds:     true,
			expectedRefreshedTasks: []int64{6},
		},
		{
			name:                   "no pending child ids",
			hasPendingChildIds:     false,
			expectedRefreshedTasks: []int64{6, 7},
		},
	}
	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			for _, eventID := range tc.expectedRefreshedTasks {
				// only the second child workflow will refresh the child workflow task
				s.mockTaskGenerator.EXPECT().GenerateChildWorkflowTasks(eventID).Return(nil).Times(1)
			}

			var previousPendingChildIds map[int64]struct{}
			if tc.hasPendingChildIds {
				previousPendingChildIds = mutableState.GetPendingChildIds()
			}
			err = s.taskRefresher.refreshTasksForChildWorkflow(
				mutableState,
				s.mockTaskGenerator,
				persistencespb.VersionedTransition_builder{
					TransitionCount:          4,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
				previousPendingChildIds,
			)
			s.NoError(err)
		})
	}
}

func (s *taskRefresherSuite) TestRefreshRequestCancelExternalTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: historyspb.VersionHistories_builder{
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							historyspb.VersionHistoryItem_builder{EventId: 10, Version: common.EmptyVersion}.Build(),
						},
					}.Build(),
				},
			}.Build(),
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(11),
		RequestCancelInfos: map[int64]*persistencespb.RequestCancelInfo{
			5: persistencespb.RequestCancelInfo_builder{
				InitiatedEventBatchId: 4,
				InitiatedEventId:      5,
				CancelRequestId:       uuid.NewString(),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			6: persistencespb.RequestCancelInfo_builder{
				InitiatedEventBatchId: 4,
				InitiatedEventId:      6,
				CancelRequestId:       uuid.NewString(),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
		},
	}.Build()
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	// only the second request cancel external will refresh tasks
	initEvent := historypb.HistoryEvent_builder{
		EventId:   6,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{},
	}.Build()
	s.mockShard.MockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		s.mockShard.GetShardID(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
			EventID:     int64(6),
			Version:     common.EmptyVersion,
		},
		int64(4),
		branchToken,
	).Return(initEvent, nil).Times(1)

	s.mockTaskGenerator.EXPECT().GenerateRequestCancelExternalTasks(initEvent).Return(nil).Times(1)

	err = s.taskRefresher.refreshTasksForRequestCancelExternalWorkflow(context.Background(), mutableState, s.mockTaskGenerator, persistencespb.VersionedTransition_builder{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build())
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshSignalExternalTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: historyspb.VersionHistories_builder{
				Histories: []*historyspb.VersionHistory{
					historyspb.VersionHistory_builder{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							historyspb.VersionHistoryItem_builder{EventId: 10, Version: common.EmptyVersion}.Build(),
						},
					}.Build(),
				},
			}.Build(),
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(11),
		SignalInfos: map[int64]*persistencespb.SignalInfo{
			5: persistencespb.SignalInfo_builder{
				InitiatedEventBatchId: 4,
				InitiatedEventId:      5,
				RequestId:             uuid.NewString(),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
			6: persistencespb.SignalInfo_builder{
				InitiatedEventBatchId: 4,
				InitiatedEventId:      6,
				RequestId:             uuid.NewString(),
				LastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				}.Build(),
			}.Build(),
		},
	}.Build()
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	// only the second signal external will refresh tasks
	initEvent := historypb.HistoryEvent_builder{
		EventId:   6,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{},
	}.Build()
	s.mockShard.MockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		s.mockShard.GetShardID(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
			EventID:     int64(6),
			Version:     common.EmptyVersion,
		},
		int64(4),
		branchToken,
	).Return(initEvent, nil).Times(1)

	s.mockTaskGenerator.EXPECT().GenerateSignalExternalTasks(initEvent).Return(nil).Times(1)

	err = s.taskRefresher.refreshTasksForSignalExternalWorkflow(context.Background(), mutableState, s.mockTaskGenerator, persistencespb.VersionedTransition_builder{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build())
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshWorkflowSearchAttributesTasks() {
	mutableStateRecord := persistencespb.WorkflowMutableState_builder{
		ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VisibilityLastUpdateVersionedTransition: persistencespb.VersionedTransition_builder{
				TransitionCount:          3,
				NamespaceFailoverVersion: common.EmptyVersion,
			}.Build(),
		}.Build(),
		ExecutionState: persistencespb.WorkflowExecutionState_builder{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		}.Build(),
		NextEventId: int64(3),
	}.Build()
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	s.NoError(err)

	s.mockTaskGenerator.EXPECT().GenerateUpsertVisibilityTask().Return(nil).Times(1)

	err = s.taskRefresher.refreshTasksForWorkflowSearchAttr(mutableState, s.mockTaskGenerator, persistencespb.VersionedTransition_builder{
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build())
	s.NoError(err)

	err = s.taskRefresher.refreshTasksForWorkflowSearchAttr(mutableState, s.mockTaskGenerator, persistencespb.VersionedTransition_builder{
		TransitionCount:          5,
		NamespaceFailoverVersion: common.EmptyVersion,
	}.Build())
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshSubStateMachineTasks() {

	stateMachineDef := hsmtest.NewDefinition("test")
	err := s.stateMachineRegistry.RegisterTaskSerializer(hsmtest.TaskType, hsmtest.TaskSerializer{})
	s.NoError(err)
	err = s.stateMachineRegistry.RegisterMachine(stateMachineDef)
	s.NoError(err)

	versionedTransition := persistencespb.VersionedTransition_builder{
		NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(tests.WorkflowID),
		TransitionCount:          3,
	}.Build()
	s.mutableState.GetExecutionInfo().SetTransitionHistory([]*persistencespb.VersionedTransition{
		versionedTransition,
	})

	hsmRoot := s.mutableState.HSM()
	child1, err := hsmRoot.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"}, hsmtest.NewData(hsmtest.State1))
	s.NoError(err)
	_, err = child1.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1_1"}, hsmtest.NewData(hsmtest.State2))
	s.NoError(err)
	_, err = hsmRoot.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_2"}, hsmtest.NewData(hsmtest.State3))
	s.NoError(err)
	// Clear the dirty flag so we can test it later.
	hsmRoot.ClearTransactionState()

	// mark all nodes dirty for setting last updated versioned transition
	err = hsmRoot.Walk(func(node *hsm.Node) error {
		// Ignore the root, it is the entire mutable state.
		if node.Parent == nil {
			return nil
		}
		// After the transition, the LastUpdateVersionedTransition should have transition count 4.
		return hsm.MachineTransition(node, func(_ *hsmtest.Data) (hsm.TransitionOutput, error) {
			return hsm.TransitionOutput{}, nil
		})
	})
	s.NoError(err)
	hsmRoot.ClearTransactionState()

	err = s.taskRefresher.refreshTasksForSubStateMachines(s.mutableState, nil)
	s.NoError(err)
	refreshedTasks := s.mutableState.PopTasks()
	s.Len(refreshedTasks[tasks.CategoryOutbound], 3)
	s.Len(s.mutableState.GetExecutionInfo().GetStateMachineTimers(), 3)
	s.Len(refreshedTasks[tasks.CategoryTimer], 1)
	s.False(hsmRoot.Dirty())

	err = s.taskRefresher.refreshTasksForSubStateMachines(
		s.mutableState,
		persistencespb.VersionedTransition_builder{
			NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(tests.WorkflowID),
			TransitionCount:          4,
		}.Build(),
	)
	s.NoError(err)
	refreshedTasks = s.mutableState.PopTasks()
	s.Len(refreshedTasks[tasks.CategoryOutbound], 3)
	s.Len(s.mutableState.GetExecutionInfo().GetStateMachineTimers(), 3)
	s.Len(refreshedTasks[tasks.CategoryTimer], 1)
	s.False(hsmRoot.Dirty())

	err = s.taskRefresher.refreshTasksForSubStateMachines(
		s.mutableState,
		persistencespb.VersionedTransition_builder{
			NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(tests.WorkflowID),
			TransitionCount:          5,
		}.Build(),
	)
	s.NoError(err)
	refreshedTasks = s.mutableState.PopTasks()
	s.Empty(refreshedTasks)
	s.False(hsmRoot.Dirty())
}

type mockTaskGeneratorProvider struct {
	mockTaskGenerator *MockTaskGenerator
}

func newMockTaskGeneratorProvider(
	mockTaskGenerator *MockTaskGenerator,
) TaskGeneratorProvider {
	return &mockTaskGeneratorProvider{
		mockTaskGenerator: mockTaskGenerator,
	}
}

func (m *mockTaskGeneratorProvider) NewTaskGenerator(
	_ historyi.ShardContext,
	_ historyi.MutableState,
) TaskGenerator {
	return m.mockTaskGenerator
}
