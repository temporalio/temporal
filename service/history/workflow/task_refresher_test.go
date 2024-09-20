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

package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
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
		mutableState         MutableState
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
		&persistencespb.ShardInfo{ShardId: 1},
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
	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, s.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mutableState = TestGlobalMutableState(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.mockShard.GetLogger(),
		s.namespaceEntry.FailoverVersion(),
		tests.WorkflowID,
		tests.RunID,
	)

	s.taskRefresher = NewTaskRefresher(s.mockShard)
	s.taskRefresher.taskGeneratorProvider = newMockTaskGeneratorProvider(s.mockTaskGenerator)
}

func (s *taskRefresherSuite) TestRefreshWorkflowStartTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 2, Version: common.EmptyVersion},
						},
					},
				},
			},
			WorkflowExecutionTimerTaskStatus: TimerTaskStatusCreated,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          1,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		NextEventId: int64(3),
	}
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	s.NoError(err)

	startEvent := &historypb.HistoryEvent{
		EventId:   common.FirstEventID,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				FirstWorkflowTaskBackoff: durationpb.New(10 * time.Second),
			},
		},
	}
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
			s.Equal(int32(TimerTaskStatusNone), mutableState.GetExecutionInfo().WorkflowExecutionTimerTaskStatus)
			return int32(TimerTaskStatusCreated), nil
		},
	)
	s.mockTaskGenerator.EXPECT().GenerateDelayedWorkflowTasks(startEvent).Return(nil).Times(1)

	err = RefreshTasksForWorkflowStart(context.Background(), mutableState, s.mockTaskGenerator, EmptyVersionedTransition)
	s.NoError(err)
	s.Equal(int32(TimerTaskStatusCreated), mutableState.GetExecutionInfo().WorkflowExecutionTimerTaskStatus)

	err = RefreshTasksForWorkflowStart(context.Background(), mutableState, s.mockTaskGenerator, &persistencespb.VersionedTransition{
		// TransitionCount is higher than workflow state's last update versioned transition,
		// no task should be generated and no call to task generator should be made.
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshRecordWorkflowStartedTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 2, Version: common.EmptyVersion},
						},
					},
				},
			},
			VisibilityLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          1,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(3),
	}
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	s.NoError(err)

	startEvent := &historypb.HistoryEvent{
		EventId:    common.FirstEventID,
		Version:    common.EmptyVersion,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{},
	}
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

	err = s.taskRefresher.refreshTasksForRecordWorkflowStarted(context.Background(), mutableState, s.mockTaskGenerator, &persistencespb.VersionedTransition{
		// TransitionCount is higher than workflow visibility's last update versioned transition,
		// no task should be generated and no call to task generator should be made.
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshWorkflowCloseTasks() {
	closeTime := timestamppb.Now()
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			CloseTime:   closeTime,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          2,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		NextEventId: int64(3),
	}
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		2,
	)
	s.NoError(err)

	s.mockTaskGenerator.EXPECT().GenerateWorkflowCloseTasks(closeTime.AsTime(), false).Return(nil).Times(1)

	err = s.taskRefresher.refreshTasksForWorkflowClose(context.Background(), mutableState, s.mockTaskGenerator, EmptyVersionedTransition)
	s.NoError(err)

	err = s.taskRefresher.refreshTasksForWorkflowClose(context.Background(), mutableState, s.mockTaskGenerator, &persistencespb.VersionedTransition{
		// TransitionCount is higher than workflow state's last update versioned transition,
		TransitionCount:          3,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshWorkflowTaskTasks() {
	baseMutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("branchToken"),
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 3, Version: common.EmptyVersion},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(3),
	}

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
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE
				return record
			},
			setupMock:              func() {},
			minVersionedTransition: EmptyVersionedTransition,
		},
		{
			name: "Refresh/WorkflowTaskScheduled",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
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
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskStartedEventId = 3
				record.ExecutionInfo.WorkflowTaskStartedTime = timestamppb.New(time.Now().Add(time.Second))
				record.ExecutionInfo.WorkflowTaskRequestId = uuid.New()
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
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
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
				record.ExecutionInfo.WorkflowTaskLastUpdateVersionedTransition = &persistencespb.VersionedTransition{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}
				return record
			},
			setupMock: func() {},
			minVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          2,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		{
			name: "PartialRefresh/Refreshed",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
				record.ExecutionInfo.WorkflowTaskLastUpdateVersionedTransition = &persistencespb.VersionedTransition{
					TransitionCount:          1,
					NamespaceFailoverVersion: common.EmptyVersion,
				}
				return record
			},
			setupMock: func() {
				s.mockTaskGenerator.EXPECT().GenerateScheduleWorkflowTaskTasks(int64(2)).Return(nil).Times(1)
			},
			minVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          1,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		{
			name: "PartialRefresh/UnknownLastUpdateVersionedTransition",
			msRecordProvider: func() *persistencespb.WorkflowMutableState {
				record := common.CloneProto(baseMutableStateRecord)
				record.ExecutionInfo.WorkflowTaskScheduledEventId = 2
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskScheduledTime = timestamppb.Now()
				record.ExecutionInfo.WorkflowTaskAttempt = 1
				record.ExecutionInfo.WorkflowTaskType = enumsspb.WORKFLOW_TASK_TYPE_NORMAL
				// WorkflowTaskLastUpdateVersionedTransition not specified.
				// This could happen for ms record persisted before versioned transition is enabled.
				// We do not refresh in this case unless the refresh request is a full refresh
				// (minVersionedTransition is EmptyVersionedTransition), because the fact that
				// lastUpdateVersionedTransition is unknown means it's updated before the given
				// minVersionedTransition.
				return record
			},
			setupMock: func() {},
			minVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          2,
				NamespaceFailoverVersion: common.EmptyVersion,
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

func (s *taskRefresherSuite) TestRefreshActivityTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 10, Version: common.EmptyVersion},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(11),
		ActivityInfos: map[int64]*persistencespb.ActivityInfo{
			5: {
				ActivityId:             "5",
				ScheduledEventId:       5,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedEventId:         common.EmptyEventID,
				TimerTaskStatus:        TimerTaskStatusCreatedScheduleToStart,
				ScheduleToStartTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(10 * time.Second),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          4,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			6: {
				ActivityId:             "6",
				ScheduledEventId:       6,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedTime:            timestamppb.New(time.Now().Add(time.Second)),
				StartedEventId:         8,
				RequestId:              uuid.New(),
				TimerTaskStatus:        TimerTaskStatusCreatedStartToClose,
				ScheduleToStartTimeout: durationpb.New(10 * time.Second),
				StartToCloseTimeout:    durationpb.New(10 * time.Second),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			7: {
				ActivityId:             "7",
				ScheduledEventId:       7,
				ScheduledEventBatchId:  4,
				Version:                common.EmptyVersion,
				ScheduledTime:          timestamppb.Now(),
				StartedEventId:         common.EmptyEventID,
				TimerTaskStatus:        TimerTaskStatusCreatedScheduleToStart,
				ScheduleToStartTimeout: durationpb.New(1 * time.Second),
				StartToCloseTimeout:    durationpb.New(1 * time.Second),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	// only the first activity will actually refresh the transfer activity task
	scheduledEvent := &historypb.HistoryEvent{
		EventId:   5,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{},
		},
	}
	s.mockShard.MockEventsCache.EXPECT().GetEvent(
		gomock.Any(),
		s.mockShard.GetShardID(),
		events.EventKey{
			NamespaceID: tests.NamespaceID,
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
			EventID:     int64(5),
			Version:     common.EmptyVersion,
		},
		int64(4),
		branchToken,
	).Return(scheduledEvent, nil).Times(1)

	s.mockTaskGenerator.EXPECT().GenerateActivityTasks(int64(5)).Return(nil).Times(1)

	err = s.taskRefresher.refreshTasksForActivity(context.Background(), mutableState, s.mockTaskGenerator, &persistencespb.VersionedTransition{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	s.NoError(err)

	pendingActivityInfos := mutableState.GetPendingActivityInfos()
	s.Len(pendingActivityInfos, 3)
	s.Equal(int32(TimerTaskStatusNone), pendingActivityInfos[5].TimerTaskStatus)
	s.Equal(int32(TimerTaskStatusNone), pendingActivityInfos[6].TimerTaskStatus)
	s.Equal(int32(TimerTaskStatusCreatedScheduleToStart), pendingActivityInfos[7].TimerTaskStatus)

	refreshedTasks := mutableState.PopTasks()
	s.Empty(refreshedTasks[tasks.CategoryTimer])
}

func (s *taskRefresherSuite) TestRefreshUserTimer() {
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(11),
		TimerInfos: map[string]*persistencespb.TimerInfo{
			"5": {
				TimerId:        "5",
				StartedEventId: 5,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(time.Now().Add(10 * time.Second)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			"6": {
				TimerId:        "6",
				StartedEventId: 6,
				Version:        common.EmptyVersion,
				ExpiryTime:     timestamppb.New(time.Now().Add(100 * time.Second)),
				TaskStatus:     TimerTaskStatusCreated,
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	err = s.taskRefresher.refreshTasksForTimer(mutableState, &persistencespb.VersionedTransition{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	s.NoError(err)

	pendingTimerInfos := mutableState.GetPendingTimerInfos()
	s.Len(pendingTimerInfos, 2)
	s.Equal(int64(TimerTaskStatusCreated), pendingTimerInfos["5"].TaskStatus)
	s.Equal(int64(TimerTaskStatusCreated), pendingTimerInfos["6"].TaskStatus)

	refreshedTasks := mutableState.PopTasks()
	s.Len(refreshedTasks[tasks.CategoryTimer], 1)
}

func (s *taskRefresherSuite) TestRefreshChildWorkflowTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 10, Version: common.EmptyVersion},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(11),
		ChildExecutionInfos: map[int64]*persistencespb.ChildExecutionInfo{
			5: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      5,
				CreateRequestId:       uuid.New(),
				StartedWorkflowId:     "child-workflow-id-5",
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			6: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      6,
				CreateRequestId:       uuid.New(),
				StartedWorkflowId:     "child-workflow-id-6",
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}
	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		log.NewTestLogger(),
		tests.LocalNamespaceEntry,
		mutableStateRecord,
		10,
	)
	s.NoError(err)

	// only the second child workflow will refresh the child workflow task
	initEvent := &historypb.HistoryEvent{
		EventId:   6,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
			StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{},
		},
	}
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

	s.mockTaskGenerator.EXPECT().GenerateChildWorkflowTasks(initEvent).Return(nil).Times(1)

	err = s.taskRefresher.refreshTasksForChildWorkflow(context.Background(), mutableState, s.mockTaskGenerator, &persistencespb.VersionedTransition{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshRequestCancelExternalTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 10, Version: common.EmptyVersion},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(11),
		RequestCancelInfos: map[int64]*persistencespb.RequestCancelInfo{
			5: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      5,
				CancelRequestId:       uuid.New(),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			6: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      6,
				CancelRequestId:       uuid.New(),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}
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
	initEvent := &historypb.HistoryEvent{
		EventId:   6,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			RequestCancelExternalWorkflowExecutionInitiatedEventAttributes: &historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{},
		},
	}
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

	err = s.taskRefresher.refreshTasksForRequestCancelExternalWorkflow(context.Background(), mutableState, s.mockTaskGenerator, &persistencespb.VersionedTransition{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshSignalExternalTasks() {
	branchToken := []byte("branchToken")
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 10, Version: common.EmptyVersion},
						},
					},
				},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(11),
		SignalInfos: map[int64]*persistencespb.SignalInfo{
			5: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      5,
				RequestId:             uuid.New(),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          3,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
			6: {
				InitiatedEventBatchId: 4,
				InitiatedEventId:      6,
				RequestId:             uuid.New(),
				LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
					TransitionCount:          5,
					NamespaceFailoverVersion: common.EmptyVersion,
				},
			},
		},
	}
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
	initEvent := &historypb.HistoryEvent{
		EventId:   6,
		Version:   common.EmptyVersion,
		EventType: enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_SignalExternalWorkflowExecutionInitiatedEventAttributes{
			SignalExternalWorkflowExecutionInitiatedEventAttributes: &historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes{},
		},
	}
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

	err = s.taskRefresher.refreshTasksForSignalExternalWorkflow(context.Background(), mutableState, s.mockTaskGenerator, &persistencespb.VersionedTransition{
		TransitionCount:          4,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshWorkflowSearchAttributesTasks() {
	mutableStateRecord := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VisibilityLastUpdateVersionedTransition: &persistencespb.VersionedTransition{
				TransitionCount:          3,
				NamespaceFailoverVersion: common.EmptyVersion,
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  tests.RunID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: int64(3),
	}
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

	err = s.taskRefresher.refreshTasksForWorkflowSearchAttr(mutableState, s.mockTaskGenerator, &persistencespb.VersionedTransition{
		TransitionCount:          2,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	s.NoError(err)

	err = s.taskRefresher.refreshTasksForWorkflowSearchAttr(mutableState, s.mockTaskGenerator, &persistencespb.VersionedTransition{
		TransitionCount:          5,
		NamespaceFailoverVersion: common.EmptyVersion,
	})
	s.NoError(err)
}

func (s *taskRefresherSuite) TestRefreshSubStateMachineTasks() {

	stateMachineDef := hsmtest.NewDefinition("test")
	err := s.stateMachineRegistry.RegisterTaskSerializer(hsmtest.TaskType, hsmtest.TaskSerializer{})
	s.NoError(err)
	err = s.stateMachineRegistry.RegisterMachine(stateMachineDef)
	s.NoError(err)

	versionedTransition := &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
		TransitionCount:          3,
	}
	s.mutableState.GetExecutionInfo().TransitionHistory = []*persistencespb.VersionedTransition{
		versionedTransition,
	}

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
	s.Len(s.mutableState.GetExecutionInfo().StateMachineTimers, 3)
	s.Len(refreshedTasks[tasks.CategoryTimer], 1)
	s.False(hsmRoot.Dirty())

	err = s.taskRefresher.refreshTasksForSubStateMachines(
		s.mutableState,
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
			TransitionCount:          4,
		},
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
	_ shard.Context,
	_ MutableState,
) TaskGenerator {
	return m.mockTaskGenerator
}
