package pauseactivity

import (
	"context"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type (
	pauseActivitySuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockEventsCache     *events.MockCache
		mockNamespaceCache  *namespace.MockRegistry
		mockTaskGenerator   *workflow.MockTaskGenerator
		mockMutableState    *historyi.MockMutableState
		mockClusterMetadata *cluster.MockMetadata

		executionInfo *persistencespb.WorkflowExecutionInfo

		validator *api.CommandAttrValidator

		logger log.Logger
	}
)

func TestActivityOptionsSuite(t *testing.T) {
	s := new(pauseActivitySuite)
	suite.Run(t, s)
}

func (s *pauseActivitySuite) SetupSuite() {
}

func (s *pauseActivitySuite) TearDownSuite() {
}

func (s *pauseActivitySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTaskGenerator = workflow.NewMockTaskGenerator(s.controller)
	s.mockMutableState = historyi.NewMockMutableState(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(&namespace.Namespace{}, nil).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	s.executionInfo = &persistencespb.WorkflowExecutionInfo{
		VersionHistories:                 versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		FirstExecutionRunId:              uuid.New(),
		WorkflowExecutionTimerTaskStatus: workflow.TimerTaskStatusCreated,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(s.executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	s.validator = api.NewCommandAttrValidator(
		s.mockShard.GetNamespaceRegistry(),
		s.mockShard.GetConfig(),
		nil,
	)
}

func (s *pauseActivitySuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *pauseActivitySuite) TestPauseActivityAcceptance() {
	activityId := "activity_id"
	activityInfo := &persistencespb.ActivityInfo{
		TaskQueue:  "task_queue_name",
		ActivityId: activityId,
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
	}

	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(activityInfo, true)
	s.mockMutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any())

	err := workflow.PauseActivity(s.mockMutableState, activityId, nil)
	s.NoError(err)
}

func (s *pauseActivitySuite) TestInvokeWithActivityID() {
	ctx := context.Background()
	workflowID := "test-workflow-id"
	runID := uuid.New()
	activityID := "test-activity-id"
	identity := "test-identity"
	reason := "test-reason"

	request := &historyservice.PauseActivityRequest{
		NamespaceId: tests.NamespaceID.String(),
		FrontendRequest: &workflowservice.PauseActivityRequest{
			Namespace: tests.Namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Activity: &workflowservice.PauseActivityRequest_Id{
				Id: activityID,
			},
			Identity: identity,
			Reason:   reason,
		},
	}

	// Create activity info that will be returned by the mocked mutable state
	activityInfo := &persistencespb.ActivityInfo{
		ScheduledEventId: 1,
		StartedEventId:   2,
		ActivityId:       activityID,
		ActivityType: &commonpb.ActivityType{
			Name: "test-activity-type",
		},
		TaskQueue: "test-task-queue",
	}

	// Mock additional mutable state expectations beyond the base setup
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		1: activityInfo,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityByActivityID(activityID).Return(activityInfo, true).AnyTimes()
	s.mockMutableState.EXPECT().UpdateActivity(activityInfo.ScheduledEventId, gomock.Any()).Return(nil).Times(1)

	// Mock workflow context
	mockWorkflowContext := historyi.NewMockWorkflowContext(s.controller)
	mockWorkflowContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// Mock workflow lease
	mockWorkflowLease := ndc.NewMockWorkflow(s.controller)
	mockWorkflowLease.EXPECT().GetMutableState().Return(s.mockMutableState).AnyTimes()
	mockWorkflowLease.EXPECT().GetReleaseFn().Return(func(_ error) {}).AnyTimes()
	mockWorkflowLease.EXPECT().GetContext().Return(mockWorkflowContext).AnyTimes()

	// Mock consistency checker
	mockConsistencyChecker := api.NewMockWorkflowConsistencyChecker(s.controller)
	workflowKey := definition.NewWorkflowKey(tests.NamespaceID.String(), workflowID, runID)
	mockConsistencyChecker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(), workflowKey, gomock.Any()).Return(mockWorkflowLease, nil).AnyTimes()

	// Call the Invoke function
	resp, err := Invoke(ctx, request, s.mockShard, mockConsistencyChecker)

	// Verify the response
	s.NoError(err)
	s.NotNil(resp)
	s.IsType(&historyservice.PauseActivityResponse{}, resp)
}

func (s *pauseActivitySuite) TestInvokeWithActivityType() {
	ctx := context.Background()
	workflowID := "test-workflow-id"
	runID := uuid.New()
	activityType := "test-activity-type"
	identity := "test-identity"
	reason := "test-reason"

	request := &historyservice.PauseActivityRequest{
		NamespaceId: tests.NamespaceID.String(),
		FrontendRequest: &workflowservice.PauseActivityRequest{
			Namespace: tests.Namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Activity: &workflowservice.PauseActivityRequest_Type{
				Type: activityType,
			},
			Identity: identity,
			Reason:   reason,
		},
	}

	// Create activity infos with the target activity type
	activityInfo1 := &persistencespb.ActivityInfo{
		ScheduledEventId: 1,
		StartedEventId:   2,
		ActivityId:       "activity-1",
		ActivityType: &commonpb.ActivityType{
			Name: activityType,
		},
		TaskQueue: "test-task-queue",
	}

	activityInfo2 := &persistencespb.ActivityInfo{
		ScheduledEventId: 3,
		StartedEventId:   4,
		ActivityId:       "activity-2",
		ActivityType: &commonpb.ActivityType{
			Name: activityType,
		},
		TaskQueue: "test-task-queue",
	}

	// Mock additional mutable state expectations beyond the base setup
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{
		1: activityInfo1,
		3: activityInfo2,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityByActivityID("activity-1").Return(activityInfo1, true).AnyTimes()
	s.mockMutableState.EXPECT().UpdateActivity(activityInfo1.ScheduledEventId, gomock.Any()).Return(nil).Times(1) // Expect activity-1 to be updated.
	s.mockMutableState.EXPECT().GetActivityByActivityID("activity-2").Return(activityInfo2, true).AnyTimes()
	s.mockMutableState.EXPECT().UpdateActivity(activityInfo2.ScheduledEventId, gomock.Any()).Return(nil).Times(1) // Expect activity-2 to be updated.
	s.mockMutableState.EXPECT().PauseActivityByType(activityType, identity, reason).Return(nil).Times(1)

	// Mock workflow context
	mockWorkflowContext := historyi.NewMockWorkflowContext(s.controller)
	mockWorkflowContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// Mock workflow lease
	mockWorkflowLease := ndc.NewMockWorkflow(s.controller)
	mockWorkflowLease.EXPECT().GetMutableState().Return(s.mockMutableState).AnyTimes()
	mockWorkflowLease.EXPECT().GetReleaseFn().Return(func(_ error) {}).AnyTimes()
	mockWorkflowLease.EXPECT().GetContext().Return(mockWorkflowContext).AnyTimes()

	// Mock consistency checker
	mockConsistencyChecker := api.NewMockWorkflowConsistencyChecker(s.controller)
	workflowKey := definition.NewWorkflowKey(tests.NamespaceID.String(), workflowID, runID)
	mockConsistencyChecker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(), workflowKey, gomock.Any()).Return(mockWorkflowLease, nil).AnyTimes()

	// Call the Invoke function
	resp, err := Invoke(ctx, request, s.mockShard, mockConsistencyChecker)

	// Verify the response
	s.NoError(err)
	s.NotNil(resp)
	s.IsType(&historyservice.PauseActivityResponse{}, resp)
}

func (s *pauseActivitySuite) TestInvokeActivityNotFound() {
	ctx := context.Background()
	workflowID := "test-workflow-id"
	runID := uuid.New()
	activityID := "non-existent-activity-id"
	identity := "test-identity"
	reason := "test-reason"

	request := &historyservice.PauseActivityRequest{
		NamespaceId: tests.NamespaceID.String(),
		FrontendRequest: &workflowservice.PauseActivityRequest{
			Namespace: tests.Namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Activity: &workflowservice.PauseActivityRequest_Id{
				Id: activityID,
			},
			Identity: identity,
			Reason:   reason,
		},
	}

	// Mock additional mutable state expectations for the error case
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{}).AnyTimes()
	// The workflow.PauseActivity function will call GetActivityByActivityID, but it won't find the activity
	s.mockMutableState.EXPECT().GetActivityByActivityID(activityID).Return(nil, false).AnyTimes()

	// Mock workflow lease
	mockWorkflowLease := ndc.NewMockWorkflow(s.controller)
	mockWorkflowLease.EXPECT().GetMutableState().Return(s.mockMutableState).AnyTimes()
	mockWorkflowLease.EXPECT().GetReleaseFn().Return(func(_ error) {}).AnyTimes()

	// Mock consistency checker
	mockConsistencyChecker := api.NewMockWorkflowConsistencyChecker(s.controller)
	workflowKey := definition.NewWorkflowKey(tests.NamespaceID.String(), workflowID, runID)
	mockConsistencyChecker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(), workflowKey, gomock.Any()).Return(mockWorkflowLease, nil).AnyTimes()

	// Call the Invoke function
	resp, err := Invoke(ctx, request, s.mockShard, mockConsistencyChecker)

	// Verify the error response
	s.ErrorIs(err, consts.ErrActivityNotFound)
	s.Nil(resp)
}
