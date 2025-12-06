package replication

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	rawTaskConverterSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

		controller         *gomock.Controller
		shardContext       *shard.ContextTest
		workflowCache      *wcache.MockCache
		mockEngine         *historyi.MockEngine
		progressCache      *MockProgressCache
		executionManager   *persistence.MockExecutionManager
		syncStateRetriever *MockSyncStateRetriever
		logger             log.Logger

		namespaceID string
		workflowID  string

		runID           string
		workflowContext *historyi.MockWorkflowContext
		mutableState    *historyi.MockMutableState
		releaseFn       historyi.ReleaseWorkflowContextFunc
		lockReleased    bool

		newRunID           string
		newWorkflowContext *historyi.MockWorkflowContext
		newMutableState    *historyi.MockMutableState
		newReleaseFn       historyi.ReleaseWorkflowContextFunc

		replicationMultipleBatches bool
	}
)

func TestRawTaskConverterSuite(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &rawTaskConverterSuite{
				replicationMultipleBatches: tc.replicationMultipleBatches,
			}
			suite.Run(t, s)
		})
	}
}

func (s *rawTaskConverterSuite) SetupSuite() {

}

func (s *rawTaskConverterSuite) TearDownSuite() {

}

func (s *rawTaskConverterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ProtoAssertions = protorequire.New(s.T())

	config := tests.NewDynamicConfig()
	config.ReplicationMultipleBatches = dynamicconfig.GetBoolPropertyFn(s.replicationMultipleBatches)

	s.controller = gomock.NewController(s.T())
	s.shardContext = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			Owner:   "test-shard-owner",
		},
		config,
	)
	s.workflowCache = wcache.NewMockCache(s.controller)
	s.progressCache = NewMockProgressCache(s.controller)
	s.executionManager = s.shardContext.Resource.ExecutionMgr
	s.logger = s.shardContext.GetLogger()

	s.mockEngine = historyi.NewMockEngine(s.controller)
	s.mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockEngine.EXPECT().Stop().AnyTimes()
	s.shardContext.SetEngineForTesting(s.mockEngine)

	s.namespaceID = tests.NamespaceID.String()
	namespaceRegistry := s.shardContext.Resource.NamespaceCache
	namespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	s.workflowID = uuid.NewString()

	s.runID = uuid.NewString()
	s.workflowContext = historyi.NewMockWorkflowContext(s.controller)
	s.mutableState = historyi.NewMockMutableState(s.controller)
	s.releaseFn = func(error) { s.lockReleased = true }

	s.newRunID = uuid.NewString()
	s.newWorkflowContext = historyi.NewMockWorkflowContext(s.controller)
	s.newMutableState = historyi.NewMockMutableState(s.controller)
	s.newReleaseFn = func(error) { s.lockReleased = true }
	s.syncStateRetriever = NewMockSyncStateRetriever(s.controller)
}

func (s *rawTaskConverterSuite) TearDownTest() {
	s.controller.Finish()
	s.shardContext.StopForTest()
}

func (s *rawTaskConverterSuite) TestConvertActivityStateReplicationTask_WorkflowMissing() {
	ctx := context.Background()
	scheduledEventID := int64(144)
	version := int64(288)
	taskID := int64(1444)
	task := &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		ScheduledEventID:    scheduledEventID,
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(nil, serviceerror.NewNotFound(""))

	result, err := convertActivityStateReplicationTask(ctx, s.shardContext, task, s.workflowCache)
	s.NoError(err)
	s.Nil(result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertActivityStateReplicationTask_WorkflowCompleted() {
	ctx := context.Background()
	scheduledEventID := int64(144)
	version := int64(288)
	taskID := int64(1444)
	task := &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		ScheduledEventID:    scheduledEventID,
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil)
	s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()

	result, err := convertActivityStateReplicationTask(ctx, s.shardContext, task, s.workflowCache)
	s.NoError(err)
	s.Nil(result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertActivityStateReplicationTask_ActivityCompleted() {
	ctx := context.Background()
	scheduledEventID := int64(144)
	version := int64(288)
	taskID := int64(1444)
	task := &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		ScheduledEventID:    scheduledEventID,
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil)
	s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(nil, false).AnyTimes()

	result, err := convertActivityStateReplicationTask(ctx, s.shardContext, task, s.workflowCache)
	s.NoError(err)
	s.Nil(result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertActivityStateReplicationTask_ActivityScheduled() {
	ctx := context.Background()
	scheduledEventID := int64(144)
	version := int64(333)
	taskID := int64(1444)
	task := &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		ScheduledEventID:    scheduledEventID,
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)

	activityVersion := version
	activityScheduledEventID := scheduledEventID
	activityScheduledTime := time.Now().UTC()
	activityStartedEventID := common.EmptyEventID
	activityAttempt := int32(16384)
	activityDetails := payloads.EncodeString("some random activity progress")
	activityLastFailure := failure.NewServerFailure("some random reason", false)
	activityLastWorkerIdentity := "some random worker identity"
	baseWorkflowInfo := &workflowspb.BaseExecutionInfo{
		RunId:                            uuid.NewString(),
		LowestCommonAncestorEventId:      rand.Int63(),
		LowestCommonAncestorEventVersion: rand.Int63(),
	}
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			versionHistory,
		},
	}
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil)
	s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version:                 activityVersion,
		ScheduledEventId:        activityScheduledEventID,
		ScheduledTime:           timestamppb.New(activityScheduledTime),
		StartedEventId:          activityStartedEventID,
		StartedTime:             nil,
		LastHeartbeatUpdateTime: nil,
		LastHeartbeatDetails:    activityDetails,
		Attempt:                 activityAttempt,
		RetryLastFailure:        activityLastFailure,
		RetryLastWorkerIdentity: activityLastWorkerIdentity,
	}, true).AnyTimes()
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		BaseExecutionInfo: baseWorkflowInfo,
		VersionHistories:  versionHistories,
	}).AnyTimes()
	s.mutableState.EXPECT().GetBaseWorkflowInfo().Return(baseWorkflowInfo).AnyTimes()

	result, err := convertActivityStateReplicationTask(ctx, s.shardContext, task, s.workflowCache)
	s.NoError(err)
	s.NotNil(result)
	retryInitialInterval := &durationpb.Duration{
		Nanos: 0,
	}
	s.ProtoEqual(&replicationspb.ReplicationTask{
		SourceTaskId: taskID,
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:          s.namespaceID,
				WorkflowId:           s.workflowID,
				RunId:                s.runID,
				Version:              activityVersion,
				ScheduledEventId:     activityScheduledEventID,
				ScheduledTime:        timestamppb.New(activityScheduledTime),
				StartedEventId:       activityStartedEventID,
				StartedTime:          nil,
				LastHeartbeatTime:    nil,
				Details:              activityDetails,
				Attempt:              activityAttempt,
				LastFailure:          activityLastFailure,
				LastWorkerIdentity:   activityLastWorkerIdentity,
				BaseExecutionInfo:    baseWorkflowInfo,
				VersionHistory:       versionHistory,
				RetryInitialInterval: retryInitialInterval,
			},
		},
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
	}, result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertActivityStateReplicationTask_ActivityStarted() {
	ctx := context.Background()
	scheduledEventID := int64(144)
	version := int64(333)
	taskID := int64(1444)
	task := &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		ScheduledEventID:    scheduledEventID,
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)

	activityVersion := version
	activityScheduledEventID := scheduledEventID
	activityScheduledTime := time.Now().UTC()
	activityStartedEventID := activityScheduledEventID + 1
	activityStartedTime := activityScheduledTime.Add(time.Minute)
	activityHeartbeatTime := activityStartedTime.Add(time.Minute)
	activityAttempt := int32(16384)
	activityDetails := payloads.EncodeString("some random activity progress")
	activityLastFailure := failure.NewServerFailure("some random reason", false)
	activityLastWorkerIdentity := "some random worker identity"
	baseWorkflowInfo := &workflowspb.BaseExecutionInfo{
		RunId:                            uuid.NewString(),
		LowestCommonAncestorEventId:      rand.Int63(),
		LowestCommonAncestorEventVersion: rand.Int63(),
	}
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			versionHistory,
		},
	}
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil)
	s.mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version:                 activityVersion,
		ScheduledEventId:        activityScheduledEventID,
		ScheduledTime:           timestamppb.New(activityScheduledTime),
		StartedEventId:          activityStartedEventID,
		StartedTime:             timestamppb.New(activityStartedTime),
		LastHeartbeatUpdateTime: timestamppb.New(activityHeartbeatTime),
		LastHeartbeatDetails:    activityDetails,
		Attempt:                 activityAttempt,
		RetryLastFailure:        activityLastFailure,
		RetryLastWorkerIdentity: activityLastWorkerIdentity,
	}, true).AnyTimes()
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		BaseExecutionInfo: baseWorkflowInfo,
		VersionHistories:  versionHistories,
	}).AnyTimes()
	s.mutableState.EXPECT().GetBaseWorkflowInfo().Return(baseWorkflowInfo).AnyTimes()

	result, err := convertActivityStateReplicationTask(ctx, s.shardContext, task, s.workflowCache)
	s.NoError(err)
	retryInitialInterval := &durationpb.Duration{
		Nanos: 0,
	}
	s.ProtoEqual(&replicationspb.ReplicationTask{
		SourceTaskId: taskID,
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:          s.namespaceID,
				WorkflowId:           s.workflowID,
				RunId:                s.runID,
				Version:              activityVersion,
				ScheduledEventId:     activityScheduledEventID,
				ScheduledTime:        timestamppb.New(activityScheduledTime),
				StartedEventId:       activityStartedEventID,
				StartedTime:          timestamppb.New(activityStartedTime),
				LastHeartbeatTime:    timestamppb.New(activityHeartbeatTime),
				Details:              activityDetails,
				Attempt:              activityAttempt,
				LastFailure:          activityLastFailure,
				LastWorkerIdentity:   activityLastWorkerIdentity,
				BaseExecutionInfo:    baseWorkflowInfo,
				VersionHistory:       versionHistory,
				RetryInitialInterval: retryInitialInterval,
			},
		},
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
	}, result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertWorkflowStateReplicationTask_WorkflowOpen() {
	ctx := context.Background()
	version := int64(288)
	taskID := int64(1444)
	task := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil)
	s.mutableState.EXPECT().GetWorkflowStateStatus().Return(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING).AnyTimes()

	result, err := convertWorkflowStateReplicationTask(ctx, s.shardContext, task, s.workflowCache)
	s.NoError(err)
	s.Nil(result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertWorkflowStateReplicationTask_WorkflowClosed() {
	ctx := context.Background()
	version := int64(288)
	taskID := int64(1444)
	task := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil)
	s.mutableState.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:                       s.namespaceID,
			WorkflowId:                        s.workflowID,
			TaskGenerationShardClockTimestamp: 123,
			CloseVisibilityTaskId:             456,
			CloseTransferTaskId:               789,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  s.runID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
	}).AnyTimes()
	s.mutableState.EXPECT().GetWorkflowStateStatus().Return(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED).AnyTimes()
	// Mock for watermark check
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:                       s.namespaceID,
		WorkflowId:                        s.workflowID,
		TaskGenerationShardClockTimestamp: 123,
		CloseVisibilityTaskId:             456,
		CloseTransferTaskId:               789,
	}
	s.mutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.mutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(s.namespaceID, s.workflowID, s.runID)).AnyTimes()

	result, err := convertWorkflowStateReplicationTask(ctx, s.shardContext, task, s.workflowCache)
	s.NoError(err)

	sanitizedMutableState := s.mutableState.CloneToProto()
	workflow.SanitizeMutableState(sanitizedMutableState)
	s.ProtoEqual(&replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		SourceTaskId: task.TaskID,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
				WorkflowState:            sanitizedMutableState,
				IsForceReplication:       task.IsForceReplication,
				IsCloseTransferTaskAcked: false, // No queue state available
			},
		},
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
	}, result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertHistoryReplicationTask_WorkflowMissing() {
	ctx := context.Background()
	shardID := int32(12)
	firstEventID := int64(999)
	nextEventID := int64(1911)
	version := int64(288)
	taskID := int64(1444)
	task := &tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		FirstEventID:        firstEventID,
		NextEventID:         nextEventID,
		NewRunID:            s.newRunID,
	}

	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(nil, serviceerror.NewNotFound(""))

	result, err := convertHistoryReplicationTask(ctx, s.shardContext, task, shardID, s.workflowCache, nil, s.executionManager, s.logger, s.shardContext.GetConfig())
	s.NoError(err)
	s.Nil(result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertHistoryReplicationTask_WithNewRun() {
	ctx := context.Background()
	shardID := int32(12)
	firstEventID := int64(999)
	nextEventID := int64(1911)
	version := int64(288)
	taskID := int64(1444)
	task := &tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		FirstEventID:        firstEventID,
		NextEventID:         nextEventID,
		NewRunID:            s.newRunID,
	}
	baseWorkflowInfo := &workflowspb.BaseExecutionInfo{
		RunId:                            uuid.NewString(),
		LowestCommonAncestorEventId:      rand.Int63(),
		LowestCommonAncestorEventVersion: rand.Int63(),
	}
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte("branch token"),
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: nextEventID - 1,
				Version: version,
			},
		},
	}
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			versionHistory,
		},
	}
	events := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("data"),
	}
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil)
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		BaseExecutionInfo: baseWorkflowInfo,
		VersionHistories:  versionHistories,
	}).AnyTimes()
	s.mutableState.EXPECT().GetBaseWorkflowInfo().Return(baseWorkflowInfo).AnyTimes()
	s.executionManager.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   versionHistory.BranchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{events},
		NextPageToken:     nil,
	}, nil)

	newVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte("new branch token"),
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: 3,
				Version: version,
			},
		},
	}
	newVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			newVersionHistory,
		},
	}
	newEvents := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("new data"),
	}
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.newRunID,
		},
		locks.PriorityLow,
	).Return(s.newWorkflowContext, s.releaseFn, nil)
	s.newWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.newMutableState, nil)
	s.newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		BaseExecutionInfo: baseWorkflowInfo,
		VersionHistories:  newVersionHistories,
	}).AnyTimes()
	s.newMutableState.EXPECT().GetBaseWorkflowInfo().Return(nil).AnyTimes()
	s.executionManager.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   newVersionHistory.BranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    common.FirstEventID + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{newEvents},
		NextPageToken:     nil,
	}, nil)

	result, err := convertHistoryReplicationTask(ctx, s.shardContext, task, shardID, s.workflowCache, nil, s.executionManager, s.logger, s.shardContext.GetConfig())
	s.NoError(err)
	if s.replicationMultipleBatches {
		s.Equal(&replicationspb.ReplicationTask{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
			SourceTaskId: task.TaskID,
			Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
				HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
					NamespaceId:         task.NamespaceID,
					WorkflowId:          task.WorkflowID,
					RunId:               task.RunID,
					BaseExecutionInfo:   baseWorkflowInfo,
					VersionHistoryItems: versionHistory.Items,
					Events:              nil,
					EventsBatches:       []*commonpb.DataBlob{events},
					NewRunEvents:        newEvents,
					NewRunId:            s.newRunID,
				},
			},
			VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		}, result)
	} else {
		s.Equal(&replicationspb.ReplicationTask{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
			SourceTaskId: task.TaskID,
			Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
				HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
					NamespaceId:         task.NamespaceID,
					WorkflowId:          task.WorkflowID,
					RunId:               task.RunID,
					BaseExecutionInfo:   baseWorkflowInfo,
					VersionHistoryItems: versionHistory.Items,
					Events:              events,
					EventsBatches:       nil,
					NewRunEvents:        newEvents,
					NewRunId:            s.newRunID,
				},
			},
			VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		}, result)

	}
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertHistoryReplicationTask_WithoutNewRun() {
	ctx := context.Background()
	shardID := int32(12)
	firstEventID := int64(999)
	nextEventID := int64(1911)
	version := int64(288)
	taskID := int64(1444)
	task := &tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		Version:             version,
		FirstEventID:        firstEventID,
		NextEventID:         nextEventID,
		NewRunID:            "",
	}
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte("branch token"),
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: nextEventID - 1,
				Version: version,
			},
		},
	}
	baseWorkflowInfo := &workflowspb.BaseExecutionInfo{
		RunId:                            uuid.NewString(),
		LowestCommonAncestorEventId:      rand.Int63(),
		LowestCommonAncestorEventVersion: rand.Int63(),
	}
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			versionHistory,
		},
	}
	events := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("data"),
	}
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil)
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		BaseExecutionInfo: baseWorkflowInfo,
		VersionHistories:  versionHistories,
	}).AnyTimes()
	s.mutableState.EXPECT().GetBaseWorkflowInfo().Return(baseWorkflowInfo).AnyTimes()
	s.executionManager.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   versionHistory.BranchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{events},
		NextPageToken:     nil,
	}, nil)

	result, err := convertHistoryReplicationTask(ctx, s.shardContext, task, shardID, s.workflowCache, nil, s.executionManager, s.logger, s.shardContext.GetConfig())
	s.NoError(err)
	if s.replicationMultipleBatches {
		s.Equal(&replicationspb.ReplicationTask{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
			SourceTaskId: task.TaskID,
			Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
				HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
					NamespaceId:         task.NamespaceID,
					WorkflowId:          task.WorkflowID,
					RunId:               task.RunID,
					BaseExecutionInfo:   baseWorkflowInfo,
					VersionHistoryItems: versionHistory.Items,
					Events:              nil,
					EventsBatches:       []*commonpb.DataBlob{events},
					NewRunEvents:        nil,
					NewRunId:            "",
				},
			},
			VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		}, result)
	} else {
		s.Equal(&replicationspb.ReplicationTask{
			TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
			SourceTaskId: task.TaskID,
			Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
				HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
					NamespaceId:         task.NamespaceID,
					WorkflowId:          task.WorkflowID,
					RunId:               task.RunID,
					BaseExecutionInfo:   baseWorkflowInfo,
					VersionHistoryItems: versionHistory.Items,
					Events:              events,
					EventsBatches:       nil,
					NewRunEvents:        nil,
					NewRunId:            "",
				},
			},
			VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		}, result)
	}
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertSyncHSMTask_WorkflowMissing() {
	ctx := context.Background()
	taskID := int64(1444)
	task := &tasks.SyncHSMTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(nil, serviceerror.NewNotFound(""))

	result, err := convertSyncHSMReplicationTask(ctx, s.shardContext, task, s.workflowCache)
	s.NoError(err)
	s.Nil(result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertSyncHSMTask_WorkflowFound() {
	ctx := context.Background()
	taskID := int64(1444)
	version := int64(288)
	task := &tasks.SyncHSMTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil)

	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 1,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branch token 1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 5, Version: 10},
				},
			},
			{
				BranchToken: []byte("branch token 2"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 5, Version: 10},
					{EventId: 10, Version: 20},
				},
			},
		},
	}
	s.mutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mutableState.EXPECT().GetCurrentVersion().Return(version).AnyTimes()
	s.mutableState.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes()

	reg := s.shardContext.StateMachineRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	stateMachineDef := hsmtest.NewDefinition("test")
	err = reg.RegisterMachine(stateMachineDef)
	s.NoError(err)

	root, err := hsm.NewRoot(reg, workflow.StateMachineType, s.mutableState, make(map[string]*persistencespb.StateMachineMap), s.mutableState)
	s.NoError(err)
	_, err = root.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"}, hsmtest.NewData(hsmtest.State1))
	s.NoError(err)
	_, err = root.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_2"}, hsmtest.NewData(hsmtest.State3))
	s.NoError(err)
	s.mutableState.EXPECT().HSM().Return(root).AnyTimes()

	result, err := convertSyncHSMReplicationTask(ctx, s.shardContext, task, s.workflowCache)
	s.NoError(err)
	sanitizedRoot := common.CloneProto(root.InternalRepr())
	workflow.SanitizeStateMachineNode(sanitizedRoot)
	s.ProtoEqual(&replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK,
		SourceTaskId: task.TaskID,
		Attributes: &replicationspb.ReplicationTask_SyncHsmAttributes{
			SyncHsmAttributes: &replicationspb.SyncHSMAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				VersionHistory: &historyspb.VersionHistory{
					BranchToken: []byte("branch token 2"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 5, Version: 10},
						{EventId: 10, Version: 20},
					},
				},
				StateMachineNode: sanitizedRoot,
			},
		},
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
	}, result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertSyncHSMTask_BufferedEvents() {
	ctx := context.Background()
	taskID := int64(1444)
	version := int64(288)
	task := &tasks.SyncHSMTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil)

	s.mutableState.EXPECT().HasBufferedEvents().Return(true).AnyTimes()
	s.mutableState.EXPECT().GetCurrentVersion().Return(version).AnyTimes()
	s.mutableState.EXPECT().NextTransitionCount().Return(int64(0)).AnyTimes()

	reg := s.shardContext.StateMachineRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	stateMachineDef := hsmtest.NewDefinition("test")
	err = reg.RegisterMachine(stateMachineDef)
	s.NoError(err)

	root, err := hsm.NewRoot(reg, workflow.StateMachineType, s.mutableState, make(map[string]*persistencespb.StateMachineMap), s.mutableState)
	s.NoError(err)
	_, err = root.AddChild(hsm.Key{Type: stateMachineDef.Type(), ID: "child_1"}, hsmtest.NewData(hsmtest.State1))
	s.NoError(err)
	s.mutableState.EXPECT().HSM().Return(root).AnyTimes()

	result, err := convertSyncHSMReplicationTask(ctx, s.shardContext, task, s.workflowCache)
	s.NoError(err)
	s.Nil(result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertSyncVersionedTransitionTask_Backfill() {
	ctx := context.Background()
	shardID := int32(0)
	targetClusterID := int32(3)
	firstEventID := int64(999)
	nextEventID := int64(1911)
	version := int64(288)
	taskID := int64(1444)
	task := &tasks.SyncVersionedTransitionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		ArchetypeID:         chasm.WorkflowArchetypeID,
		FirstEventID:        firstEventID,
		FirstEventVersion:   version,
		NextEventID:         nextEventID,
		NewRunID:            s.newRunID,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: version,
			TransitionCount:          nextEventID - 1,
		},
	}

	versionHistoryItems := []*historyspb.VersionHistoryItem{
		{
			EventId: nextEventID,
			Version: version,
		},
	}
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte("branch token"),
		Items:       versionHistoryItems,
	}
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			versionHistory,
		},
	}
	events := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("data"),
	}

	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1, TransitionCount: 3},
		{NamespaceFailoverVersion: 3, TransitionCount: 6},
	}

	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil).Times(1)
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil).Times(1)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil).Times(2)
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:  versionHistories,
		TransitionHistory: transitionHistory,
	}).AnyTimes()
	s.executionManager.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   versionHistory.BranchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{events},
		NextPageToken:     nil,
	}, nil)

	newVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte("new branch token"),
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: 3,
				Version: version,
			},
		},
	}
	newVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			newVersionHistory,
		},
	}
	newEvents := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("new data"),
	}
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.newRunID,
		},
		locks.PriorityLow,
	).Return(s.newWorkflowContext, s.releaseFn, nil)
	s.newWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.newMutableState, nil)
	s.newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: newVersionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 3},
		},
	})
	s.executionManager.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   newVersionHistory.BranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    common.FirstEventID + 1,
		PageSize:      1,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{newEvents},
		NextPageToken:     nil,
	}, nil)
	s.progressCache.EXPECT().Get(
		s.runID,
		targetClusterID,
	).Return(nil)

	taskVersionHistoryItems := []*historyspb.VersionHistoryItem{
		{
			EventId: nextEventID - 1,
			Version: version,
		},
	}
	s.progressCache.EXPECT().Update(
		s.runID,
		targetClusterID,
		nil,
		taskVersionHistoryItems,
	).Return(nil)
	converter := newSyncVersionedTransitionTaskConverter(s.shardContext, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)
	result, err := convertSyncVersionedTransitionTask(ctx, task, targetClusterID, converter)
	s.NoError(err)
	s.Equal(&replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK,
		SourceTaskId: task.TaskID,
		Attributes: &replicationspb.ReplicationTask_BackfillHistoryTaskAttributes{
			BackfillHistoryTaskAttributes: &replicationspb.BackfillHistoryTaskAttributes{
				NamespaceId:         task.NamespaceID,
				WorkflowId:          task.WorkflowID,
				RunId:               task.RunID,
				EventVersionHistory: taskVersionHistoryItems,
				EventBatches:        []*commonpb.DataBlob{events},
				NewRunInfo: &replicationspb.NewRunInfo{
					EventBatch: newEvents,
					RunId:      s.newRunID,
				},
			},
		},
		VersionedTransition: task.VersionedTransition,
		VisibilityTime:      timestamppb.New(task.VisibilityTimestamp),
	}, result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertSyncVersionTransitionTask_ConvertTaskEquivalent() {
	ctx := context.Background()
	targetClusterID := int32(3)
	version := int64(288)
	taskID := int64(1444)
	visibilityTimestamp := time.Now().UTC()
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID,
		s.workflowID,
		s.runID,
	)
	task := &tasks.SyncVersionedTransitionTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: visibilityTimestamp,
		TaskID:              taskID,
		ArchetypeID:         chasm.WorkflowArchetypeID,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: version,
			TransitionCount:          165,
		},
		TaskEquivalents: []tasks.Task{
			&tasks.SyncActivityTask{
				WorkflowKey:      workflowKey,
				Version:          version,
				ScheduledEventID: 100,
			},
		},
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil).Times(1)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil).Times(1)
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		TransitionHistory: nil,
	}).Times(1)
	s.mutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	expectedReplicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:      task.NamespaceID,
				WorkflowId:       task.WorkflowID,
				RunId:            task.RunID,
				Version:          version,
				ScheduledEventId: 100,
			},
		},
		VersionedTransition: task.VersionedTransition,
		VisibilityTime:      timestamppb.New(task.VisibilityTimestamp),
	}
	s.mockEngine.EXPECT().ConvertReplicationTask(
		gomock.Any(),
		&tasks.SyncActivityTask{
			WorkflowKey:         workflowKey,
			TaskID:              taskID,
			VisibilityTimestamp: visibilityTimestamp,
			Version:             version,
			ScheduledEventID:    100,
		},
		targetClusterID,
	).Return(expectedReplicationTask, nil).Times(1)
	converter := newSyncVersionedTransitionTaskConverter(s.shardContext, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)
	result, err := convertSyncVersionedTransitionTask(ctx, task, targetClusterID, converter)
	s.NoError(err)
	s.Equal(expectedReplicationTask, result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertSyncVersionTransitionTask_AddTaskEquivalent() {
	ctx := context.Background()
	targetClusterID := int32(3)
	version := int64(288)
	taskID := int64(1444)
	visibilityTimestamp := time.Now().UTC()
	workflowKey := definition.NewWorkflowKey(
		s.namespaceID,
		s.workflowID,
		s.runID,
	)
	syncActivityTask := &tasks.SyncActivityTask{
		WorkflowKey:      workflowKey,
		Version:          version,
		ScheduledEventID: 100,
	}
	historyReplicationTask := &tasks.HistoryReplicationTask{
		WorkflowKey:  workflowKey,
		FirstEventID: 98,
		NextEventID:  101,
		Version:      version,
	}
	task := &tasks.SyncVersionedTransitionTask{
		WorkflowKey:         workflowKey,
		VisibilityTimestamp: visibilityTimestamp,
		TaskID:              taskID,
		ArchetypeID:         chasm.WorkflowArchetypeID,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: version,
			TransitionCount:          165,
		},
		TaskEquivalents: []tasks.Task{syncActivityTask, historyReplicationTask},
	}
	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil).Times(1)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil).Times(1)
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		TransitionHistory: nil,
	}).Times(1)
	s.mutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	mockExecutionManager := s.shardContext.Resource.ExecutionMgr
	mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.AddHistoryTasksRequest) error {
			s.Equal(s.namespaceID, request.NamespaceID)
			s.Equal(s.workflowID, request.WorkflowID)
			s.Len(request.Tasks, 1)
			s.Len(request.Tasks[tasks.CategoryReplication], 2)
			for _, replicationTask := range request.Tasks[tasks.CategoryReplication] {
				s.NotZero(replicationTask.GetTaskID())
				s.NotZero(replicationTask.GetVisibilityTime())
				replicationTask.SetTaskID(0)
				replicationTask.SetVisibilityTime(time.Time{})
			}
			s.Equal(syncActivityTask, request.Tasks[tasks.CategoryReplication][0])
			s.Equal(historyReplicationTask, request.Tasks[tasks.CategoryReplication][1])
			return nil
		},
	).Times(1)
	converter := newSyncVersionedTransitionTaskConverter(s.shardContext, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)
	result, err := convertSyncVersionedTransitionTask(ctx, task, targetClusterID, converter)
	s.NoError(err)
	s.Nil(result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertSyncVersionedTransitionTask_Mutation() {
	ctx := context.Background()
	targetClusterID := int32(3)
	firstEventID := int64(999)
	nextEventID := int64(1911)
	version := int64(1)
	taskID := int64(1444)
	task := &tasks.SyncVersionedTransitionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		ArchetypeID:         chasm.WorkflowArchetypeID,
		FirstEventID:        firstEventID,
		NextEventID:         nextEventID,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: version,
			TransitionCount:          3,
		},
	}

	versionHistoryItems := []*historyspb.VersionHistoryItem{
		{
			EventId: nextEventID,
			Version: version,
		},
	}
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte("branch token"),
		Items:       versionHistoryItems,
	}
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			versionHistory,
		},
	}

	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1, TransitionCount: 3},
		{NamespaceFailoverVersion: 3, TransitionCount: 6},
	}

	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil).Times(1)
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:    versionHistories,
		TransitionHistory:   transitionHistory,
		CloseTransferTaskId: 0,
	}).Times(2)
	s.mutableState.EXPECT().HasBufferedEvents().Return(false).Times(1)
	s.mutableState.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	}).Times(1)

	s.progressCache.EXPECT().Get(
		s.runID,
		targetClusterID,
	).Return(nil)

	s.progressCache.EXPECT().Update(
		s.runID,
		targetClusterID,
		transitionHistory,
		versionHistoryItems,
	).Return(nil)
	syncResult := &SyncStateResult{
		VersionedTransitionArtifact: &replicationspb.VersionedTransitionArtifact{
			StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
				SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
					State: &persistencespb.WorkflowMutableState{
						Checksum: &persistencespb.Checksum{
							Value: []byte("test-checksum"),
						},
					},
				},
			},
		},
		VersionedTransitionHistory: transitionHistory,
	}
	s.syncStateRetriever.EXPECT().GetSyncWorkflowStateArtifactFromMutableState(
		ctx,
		s.namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		s.mutableState,
		nil,
		nil,
		gomock.Any(),
	).Return(syncResult, nil)
	converter := newSyncVersionedTransitionTaskConverter(s.shardContext, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)
	result, err := convertSyncVersionedTransitionTask(ctx, task, targetClusterID, converter)
	s.NoError(err)
	s.Equal(&replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK,
		SourceTaskId: task.TaskID,
		Attributes: &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
			SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
				VersionedTransitionArtifact: &replicationspb.VersionedTransitionArtifact{
					StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
						SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
							State: syncResult.VersionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes().State,
						},
					},
				},
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: task.VersionedTransition,
		VisibilityTime:      timestamppb.New(task.VisibilityTimestamp),
	}, result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertSyncVersionedTransitionTask_FirstTask_Mutation() {
	ctx := context.Background()
	targetClusterID := int32(3)
	firstEventID := int64(999)
	nextEventID := int64(1911)
	version := int64(1)
	taskID := int64(1444)
	task := &tasks.SyncVersionedTransitionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		ArchetypeID:         chasm.WorkflowArchetypeID,
		FirstEventID:        firstEventID,
		NextEventID:         nextEventID,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: version,
			TransitionCount:          3,
		},
		IsFirstTask: true,
	}

	versionHistoryItems := []*historyspb.VersionHistoryItem{
		{
			EventId: nextEventID,
			Version: version,
		},
	}
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte("branch token"),
		Items:       versionHistoryItems,
	}
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			versionHistory,
		},
	}

	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1, TransitionCount: 3},
		{NamespaceFailoverVersion: 3, TransitionCount: 6},
	}

	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil).Times(1)
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:    versionHistories,
		TransitionHistory:   transitionHistory,
		CloseTransferTaskId: 0,
	}).Times(2)
	s.mutableState.EXPECT().HasBufferedEvents().Return(false).Times(1)
	s.mutableState.EXPECT().GetWorkflowKey().Return(definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	}).Times(1)

	s.progressCache.EXPECT().Get(
		s.runID,
		targetClusterID,
	).Return(nil)

	s.progressCache.EXPECT().Update(
		s.runID,
		targetClusterID,
		transitionHistory,
		versionHistoryItems,
	).Return(nil)
	syncResult := &SyncStateResult{
		VersionedTransitionArtifact: &replicationspb.VersionedTransitionArtifact{
			StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
				SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
					State: &persistencespb.WorkflowMutableState{
						Checksum: &persistencespb.Checksum{
							Value: []byte("test-checksum"),
						},
					},
				},
			},
		},
		VersionedTransitionHistory: transitionHistory,
	}
	s.syncStateRetriever.EXPECT().GetSyncWorkflowStateArtifactFromMutableStateForNewWorkflow(
		ctx,
		s.namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		s.mutableState,
		gomock.Any(),
		gomock.Any(),
	).Return(syncResult, nil)
	converter := newSyncVersionedTransitionTaskConverter(s.shardContext, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)
	result, err := convertSyncVersionedTransitionTask(ctx, task, targetClusterID, converter)
	s.NoError(err)
	s.Equal(&replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK,
		SourceTaskId: task.TaskID,
		Attributes: &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
			SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
				VersionedTransitionArtifact: &replicationspb.VersionedTransitionArtifact{
					StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
						SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
							State: syncResult.VersionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes().State,
						},
					},
				},
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				ArchetypeId: chasm.WorkflowArchetypeID,
			},
		},
		VersionedTransition: task.VersionedTransition,
		VisibilityTime:      timestamppb.New(task.VisibilityTimestamp),
	}, result)
	s.True(s.lockReleased)
}

func (s *rawTaskConverterSuite) TestConvertSyncVersionedTransitionTask_HasBufferedEvent_Nil() {
	ctx := context.Background()
	targetClusterID := int32(3)
	firstEventID := int64(999)
	nextEventID := int64(1911)
	version := int64(1)
	taskID := int64(1444)
	task := &tasks.SyncVersionedTransitionTask{
		WorkflowKey: definition.NewWorkflowKey(
			s.namespaceID,
			s.workflowID,
			s.runID,
		),
		VisibilityTimestamp: time.Now().UTC(),
		TaskID:              taskID,
		ArchetypeID:         chasm.WorkflowArchetypeID,
		FirstEventID:        firstEventID,
		NextEventID:         nextEventID,
		VersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: version,
			TransitionCount:          3,
		},
	}
	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1, TransitionCount: 3},
		{NamespaceFailoverVersion: 3, TransitionCount: 6},
	}

	s.workflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.shardContext,
		namespace.ID(s.namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityLow,
	).Return(s.workflowContext, s.releaseFn, nil)
	s.workflowContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.mutableState, nil).Times(1)
	s.mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		TransitionHistory: transitionHistory,
	}).Times(2)
	s.mutableState.EXPECT().HasBufferedEvents().Return(true).Times(1)
	s.progressCache.EXPECT().Get(
		s.runID,
		targetClusterID,
	).Return(nil)

	converter := newSyncVersionedTransitionTaskConverter(s.shardContext, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)
	result, err := convertSyncVersionedTransitionTask(ctx, task, targetClusterID, converter)
	s.NoError(err)
	s.Nil(result)
}

func (s *rawTaskConverterSuite) TestIsCloseTransferTaskAcked_ZeroTaskId() {
	testCloseTaskID := int64(0)
	workflowKey := definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	}
	closeTransferTask := &tasks.CloseExecutionTask{
		WorkflowKey: workflowKey,
		TaskID:      testCloseTaskID,
	}

	converter := newSyncVersionedTransitionTaskConverter(s.shardContext, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)
	result := converter.isCloseTransferTaskAcked(closeTransferTask)
	s.False(result)
}

func (s *rawTaskConverterSuite) TestIsCloseTransferTaskAcked_QueueStateNotAvailable() {
	testCloseTaskID := int64(12345)
	workflowKey := definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	}
	closeTransferTask := &tasks.CloseExecutionTask{
		WorkflowKey: workflowKey,
		TaskID:      testCloseTaskID,
	}

	// Queue state not set, so should return false
	converter := newSyncVersionedTransitionTaskConverter(s.shardContext, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)
	result := converter.isCloseTransferTaskAcked(closeTransferTask)
	s.False(result)
}

func (s *rawTaskConverterSuite) TestIsCloseTransferTaskAcked_TaskAcked() {
	testCloseTaskID := int64(12345)
	testReaderID := int64(1)
	testShardID := int32(0)
	testRangeID := int64(1)

	// Reader scopes that don't contain the close task (testCloseTaskID = 12345)
	// Scopes represent ranges being actively processed by readers
	scope1Min := int64(1000)
	scope1Max := int64(5000)
	scope2Min := int64(6000)
	scope2Max := int64(10000)

	// Create a new mock shard with queue state pre-configured
	// Queue state has exclusive reader high watermark past the close task,
	// meaning all readers have acknowledged past the task
	// Also add reader scopes that do NOT contain the task to ensure
	// the reader scope logic is exercised
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: testShardID,
			RangeId: testRangeID,
			QueueStates: map[int32]*persistencespb.QueueState{
				int32(tasks.CategoryTransfer.ID()): {
					ReaderStates: map[int64]*persistencespb.QueueReaderState{
						testReaderID: {
							Scopes: []*persistencespb.QueueSliceScope{
								{
									Range: &persistencespb.QueueSliceRange{
										InclusiveMin: shard.ConvertToPersistenceTaskKey(
											tasks.NewImmediateKey(scope1Min),
										),
										ExclusiveMax: shard.ConvertToPersistenceTaskKey(
											tasks.NewImmediateKey(scope1Max),
										),
									},
									Predicate: &persistencespb.Predicate{
										PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
									},
								},
								{
									Range: &persistencespb.QueueSliceRange{
										InclusiveMin: shard.ConvertToPersistenceTaskKey(
											tasks.NewImmediateKey(scope2Min),
										),
										ExclusiveMax: shard.ConvertToPersistenceTaskKey(
											tasks.NewImmediateKey(scope2Max),
										),
									},
									Predicate: &persistencespb.Predicate{
										PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
									},
								},
							},
						},
					},
					ExclusiveReaderHighWatermark: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(testCloseTaskID + 1),
					),
				},
			},
		},
		tests.NewDynamicConfig(),
	)
	defer mockShard.StopForTest()

	converter := newSyncVersionedTransitionTaskConverter(mockShard, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)

	workflowKey := definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	}
	closeTransferTask := &tasks.CloseExecutionTask{
		WorkflowKey: workflowKey,
		TaskID:      testCloseTaskID,
	}

	result := converter.isCloseTransferTaskAcked(closeTransferTask)
	s.True(result)
}

func (s *rawTaskConverterSuite) TestIsCloseTransferTaskAcked_TaskNotAcked() {
	testCloseTaskID := int64(12345)
	testShardID := int32(0)
	testRangeID := int64(1)

	// Create a new mock shard with queue state pre-configured
	// Queue state has exclusive reader high watermark before the close task,
	// meaning the task has not been acknowledged yet as it hasnt even been read yet
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: testShardID,
			RangeId: testRangeID,
			QueueStates: map[int32]*persistencespb.QueueState{
				int32(tasks.CategoryTransfer.ID()): {
					ReaderStates: map[int64]*persistencespb.QueueReaderState{},
					ExclusiveReaderHighWatermark: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(testCloseTaskID - 100),
					),
				},
			},
		},
		tests.NewDynamicConfig(),
	)
	defer mockShard.StopForTest()

	converter := newSyncVersionedTransitionTaskConverter(mockShard, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)

	workflowKey := definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	}
	closeTransferTask := &tasks.CloseExecutionTask{
		WorkflowKey: workflowKey,
		TaskID:      testCloseTaskID,
	}

	result := converter.isCloseTransferTaskAcked(closeTransferTask)
	s.False(result)
}

func (s *rawTaskConverterSuite) TestIsCloseTransferTaskAcked_TaskNotAcked_ContainedInReaderScope() {
	testCloseTaskID := int64(12345)
	testReaderID := int64(1)
	testShardID := int32(0)
	testRangeID := int64(1)

	// Reader scope that contains the close task
	scopeMin := int64(10000)
	scopeMax := int64(15000)
	taskID := int64(12000)

	// Create a new mock shard with queue state where:
	// - exclusive reader high watermark is past the task
	// - BUT a reader scope contains the task, meaning it has not been fully processed
	// This tests the reader scope check logic in util.go lines 18-31
	mockShard := shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: testShardID,
			RangeId: testRangeID,
			QueueStates: map[int32]*persistencespb.QueueState{
				int32(tasks.CategoryTransfer.ID()): {
					ReaderStates: map[int64]*persistencespb.QueueReaderState{
						testReaderID: {
							Scopes: []*persistencespb.QueueSliceScope{
								{
									Range: &persistencespb.QueueSliceRange{
										InclusiveMin: shard.ConvertToPersistenceTaskKey(
											tasks.NewImmediateKey(scopeMin),
										),
										ExclusiveMax: shard.ConvertToPersistenceTaskKey(
											tasks.NewImmediateKey(scopeMax),
										),
									},
									Predicate: &persistencespb.Predicate{
										PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
									},
								},
							},
						},
					},
					ExclusiveReaderHighWatermark: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(taskID),
					),
				},
			},
		},
		tests.NewDynamicConfig(),
	)
	defer mockShard.StopForTest()

	converter := newSyncVersionedTransitionTaskConverter(mockShard, s.workflowCache, nil, s.progressCache, s.executionManager, s.syncStateRetriever, s.logger)

	workflowKey := definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.workflowID,
		RunID:       s.runID,
	}
	closeTransferTask := &tasks.CloseExecutionTask{
		WorkflowKey: workflowKey,
		TaskID:      testCloseTaskID,
	}

	result := converter.isCloseTransferTaskAcked(closeTransferTask)
	s.False(result)
}
