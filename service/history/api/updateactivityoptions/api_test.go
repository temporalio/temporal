package updateactivityoptions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestApplyActivityOptionsAcceptance(t *testing.T) {
	updateOptions := activitypb.ActivityOptions_builder{
		TaskQueue:              taskqueuepb.TaskQueue_builder{Name: "task_queue_name"}.Build(),
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		Priority: commonpb.Priority_builder{
			PriorityKey:    42,
			FairnessKey:    "test_key",
			FairnessWeight: 5.0,
		}.Build(),
		RetryPolicy: commonpb.RetryPolicy_builder{
			MaximumInterval:    durationpb.New(time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(time.Second),
		}.Build(),
	}.Build()

	testCases := []struct {
		name      string
		mergeInto *activitypb.ActivityOptions
		mergeFrom *activitypb.ActivityOptions
		expected  *activitypb.ActivityOptions
		mask      *fieldmaskpb.FieldMask
	}{
		{
			name:      "Top-level fields with CamelCase",
			mergeFrom: updateOptions,
			mergeInto: &activitypb.ActivityOptions{},
			expected:  updateOptions,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"TaskQueue.Name",
					"ScheduleToCloseTimeout",
					"ScheduleToStartTimeout",
					"StartToCloseTimeout",
					"HeartbeatTimeout",
					"Priority",
					"RetryPolicy",
				},
			},
		},
		{
			name:      "Top-level fields with snake_case",
			mergeFrom: updateOptions,
			mergeInto: &activitypb.ActivityOptions{},
			expected:  updateOptions,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"task_queue.name",
					"schedule_to_close_timeout",
					"schedule_to_start_timeout",
					"start_to_close_timeout",
					"heartbeat_timeout",
					"priority",
					"retry_policy",
				},
			},
		},
		{
			name: "Sub-fields",
			mergeFrom: activitypb.ActivityOptions_builder{
				Priority: commonpb.Priority_builder{
					PriorityKey:    99,
					FairnessKey:    "newKey",
					FairnessWeight: 7.5,
				}.Build(),
				RetryPolicy: commonpb.RetryPolicy_builder{
					MaximumInterval:    durationpb.New(time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				}.Build(),
			}.Build(),
			mergeInto: activitypb.ActivityOptions_builder{
				Priority: commonpb.Priority_builder{
					PriorityKey:    10,
					FairnessKey:    "oldKey",
					FairnessWeight: 1.0,
				}.Build(),
				RetryPolicy: &commonpb.RetryPolicy{},
			}.Build(),
			expected: activitypb.ActivityOptions_builder{
				Priority: commonpb.Priority_builder{
					PriorityKey:    99,
					FairnessKey:    "newKey",
					FairnessWeight: 7.5,
				}.Build(),
				RetryPolicy: commonpb.RetryPolicy_builder{
					MaximumInterval:    durationpb.New(time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				}.Build(),
			}.Build(),
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"priority.priority_key",
					"priority.fairness_key",
					"priority.fairness_weight",
					"retry_policy.backoff_coefficient",
					"retry_policy.initial_interval",
					"retry_policy.maximum_interval",
					"retry_policy.maximum_attempts",
				},
			},
		},
	}
	for _, tc := range testCases {
		updateFields := util.ParseFieldMask(tc.mask)

		t.Run(tc.name, func(t *testing.T) {})
		err := mergeActivityOptions(tc.mergeInto, tc.mergeFrom, updateFields)
		assert.NoError(t, err)
		assert.Equal(t, tc.mergeInto.GetRetryPolicy().GetInitialInterval(), tc.expected.GetRetryPolicy().GetInitialInterval(), "RetryInitialInterval")
		assert.Equal(t, tc.mergeInto.GetRetryPolicy().GetMaximumInterval(), tc.expected.GetRetryPolicy().GetMaximumInterval(), "RetryMaximumInterval")
		assert.Equal(t, tc.mergeInto.GetRetryPolicy().GetBackoffCoefficient(), tc.expected.GetRetryPolicy().GetBackoffCoefficient(), "RetryBackoffCoefficient")
		assert.Equal(t, tc.mergeInto.GetRetryPolicy().GetMaximumAttempts(), tc.expected.GetRetryPolicy().GetMaximumAttempts(), "RetryMaximumAttempts")

		assert.Equal(t, tc.mergeInto.GetTaskQueue(), tc.expected.GetTaskQueue(), "TaskQueue")

		assert.Equal(t, tc.mergeInto.GetScheduleToCloseTimeout(), tc.expected.GetScheduleToCloseTimeout(), "ScheduleToCloseTimeout")
		assert.Equal(t, tc.mergeInto.GetScheduleToStartTimeout(), tc.expected.GetScheduleToStartTimeout(), "ScheduleToStartTimeout")
		assert.Equal(t, tc.mergeInto.GetStartToCloseTimeout(), tc.expected.GetStartToCloseTimeout(), "StartToCloseTimeout")
		assert.Equal(t, tc.mergeInto.GetHeartbeatTimeout(), tc.expected.GetHeartbeatTimeout(), "HeartbeatTimeout")
		assert.Equal(t, tc.mergeInto.GetPriority(), tc.expected.GetPriority(), "Priority")
	}
}

func TestApplyActivityOptionsErrors(t *testing.T) {
	var err error
	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_interval"}}))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_attempts"}}))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.backoff_coefficient"}}))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}}))
	require.ErrorContains(t, err, "RetryPolicy is not provided")

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"taskQueue.name"}}))
	require.ErrorContains(t, err, "TaskQueue is not provided")

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"priority.priority_key"}}))
	require.ErrorContains(t, err, "Priority is not provided")

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"priority.fairness_key"}}))
	require.ErrorContains(t, err, "Priority is not provided")

	err = mergeActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"priority.fairness_weight"}}))
	require.ErrorContains(t, err, "Priority is not provided")

}

func TestApplyActivityOptionsReset(t *testing.T) {
	options := activitypb.ActivityOptions_builder{
		TaskQueue:              taskqueuepb.TaskQueue_builder{Name: "task_queue_name"}.Build(),
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		Priority: commonpb.Priority_builder{
			PriorityKey:    42,
			FairnessKey:    "test_key",
			FairnessWeight: 5.0,
		}.Build(),
		RetryPolicy: commonpb.RetryPolicy_builder{
			MaximumInterval:    durationpb.New(time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(time.Second),
		}.Build(),
	}.Build()

	fullMask := &fieldmaskpb.FieldMask{
		Paths: []string{
			"schedule_to_close_timeout",
			"schedule_to_start_timeout",
			"start_to_close_timeout",
			"heartbeat_timeout",
			"priority.priority_key",
			"priority.fairness_key",
			"priority.fairness_weight",
			"retry_policy.backoff_coefficient",
			"retry_policy.initial_interval",
			"retry_policy.maximum_interval",
			"retry_policy.maximum_attempts",
		},
	}

	updateFields := util.ParseFieldMask(fullMask)

	err := mergeActivityOptions(options,
		activitypb.ActivityOptions_builder{
			Priority: commonpb.Priority_builder{
				PriorityKey: 10,
			}.Build(),
			RetryPolicy: commonpb.RetryPolicy_builder{
				MaximumAttempts:    5,
				BackoffCoefficient: 1.0,
			}.Build(),
		}.Build(),
		updateFields)
	assert.NoError(t, err)

	assert.Nil(t, options.GetScheduleToCloseTimeout())
	assert.Nil(t, options.GetScheduleToStartTimeout())
	assert.Nil(t, options.GetStartToCloseTimeout())
	assert.Nil(t, options.GetHeartbeatTimeout())

	assert.Equal(t, int32(10), options.GetPriority().GetPriorityKey())
	assert.Empty(t, options.GetPriority().GetFairnessKey())
	assert.Zero(t, options.GetPriority().GetFairnessWeight())

	assert.Nil(t, options.GetRetryPolicy().GetInitialInterval())
	assert.Nil(t, options.GetRetryPolicy().GetMaximumInterval())
}

type (
	activityOptionsSuite struct {
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

		validator                  *api.CommandAttrValidator
		workflowCache              *wcache.MockCache
		workflowConsistencyChecker api.WorkflowConsistencyChecker

		logger log.Logger
	}
)

func TestActivityOptionsSuite(t *testing.T) {
	s := new(activityOptionsSuite)
	suite.Run(t, s)
}

func (s *activityOptionsSuite) SetupSuite() {
}

func (s *activityOptionsSuite) TearDownSuite() {
}

func (s *activityOptionsSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTaskGenerator = workflow.NewMockTaskGenerator(s.controller)
	s.mockMutableState = historyi.NewMockMutableState(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		persistencespb.ShardInfo_builder{
			ShardId: 0,
			RangeId: 1,
		}.Build(),
		tests.NewDynamicConfig(),
	)

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	s.executionInfo = persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories:                 versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		FirstExecutionRunId:              uuid.NewString(),
		WorkflowExecutionTimerTaskStatus: workflow.TimerTaskStatusCreated,
	}.Build()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(s.executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()

	s.validator = api.NewCommandAttrValidator(
		s.mockShard.GetNamespaceRegistry(),
		s.mockShard.GetConfig(),
		nil,
	)

	s.workflowCache = wcache.NewMockCache(s.controller)
	s.workflowConsistencyChecker = api.NewWorkflowConsistencyChecker(
		s.mockShard,
		s.workflowCache,
	)
}

func (s *activityOptionsSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *activityOptionsSuite) Test_updateActivityOptionsWfNotRunning() {
	request := &historyservice.UpdateActivityOptionsRequest{}

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	_, err := processActivityOptionsRequest(s.validator, s.mockMutableState, request.GetUpdateRequest(), request.GetNamespaceId())
	s.Error(err)
	s.ErrorAs(err, &consts.ErrWorkflowCompleted)
}

func (s *activityOptionsSuite) Test_updateActivityOptionsWfNoActivity() {
	request := historyservice.UpdateActivityOptionsRequest_builder{
		UpdateRequest: workflowservice.UpdateActivityOptionsRequest_builder{
			ActivityOptions: activitypb.ActivityOptions_builder{
				TaskQueue: taskqueuepb.TaskQueue_builder{Name: "task_queue_name"}.Build(),
			}.Build(),
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"TaskQueue.Name",
				},
			},
			Id: proto.String("activity_id"),
		}.Build(),
	}.Build()

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(nil, false)
	_, err := processActivityOptionsRequest(s.validator, s.mockMutableState, request.GetUpdateRequest(), request.GetNamespaceId())
	s.Error(err)
	s.ErrorAs(err, &consts.ErrActivityNotFound)
}

func (s *activityOptionsSuite) Test_updateActivityOptionsAcceptance() {
	fullActivityInfo := persistencespb.ActivityInfo_builder{
		TaskQueue:               "task_queue_name",
		ScheduleToCloseTimeout:  durationpb.New(time.Second),
		ScheduleToStartTimeout:  durationpb.New(time.Second),
		StartToCloseTimeout:     durationpb.New(time.Second),
		HeartbeatTimeout:        durationpb.New(time.Second),
		RetryBackoffCoefficient: 1.0,
		RetryInitialInterval:    durationpb.New(time.Second),
		RetryMaximumInterval:    durationpb.New(time.Second),
		RetryMaximumAttempts:    5,
		ActivityId:              "activity_id",
		ActivityType: commonpb.ActivityType_builder{
			Name: "activity_type",
		}.Build(),
	}.Build()

	updateMask := &fieldmaskpb.FieldMask{
		Paths: []string{
			"task_queue.name",
			"schedule_to_close_timeout",
			"schedule_to_start_timeout",
			"start_to_close_timeout",
			"heartbeat_timeout",
			"retry_policy.backoff_coefficient",
			"retry_policy.initial_interval",
			"retry_policy.maximum_interval",
			"retry_policy.maximum_attempts",
		},
	}

	options := activitypb.ActivityOptions_builder{
		TaskQueue:              taskqueuepb.TaskQueue_builder{Name: "task_queue_name"}.Build(),
		ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
		StartToCloseTimeout:    durationpb.New(2 * time.Second),
		ScheduleToStartTimeout: durationpb.New(2 * time.Second),
		HeartbeatTimeout:       durationpb.New(2 * time.Second),
		RetryPolicy: commonpb.RetryPolicy_builder{
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(2 * time.Second),
		}.Build(),
	}.Build()

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(fullActivityInfo, true)
	s.mockMutableState.EXPECT().RegenerateActivityRetryTask(gomock.Any(), gomock.Any()).Return(nil)
	s.mockMutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any()).Return(nil)

	request := historyservice.UpdateActivityOptionsRequest_builder{
		UpdateRequest: workflowservice.UpdateActivityOptionsRequest_builder{
			ActivityOptions: options,
			UpdateMask:      updateMask,
			Id:              proto.String("activity_id"),
		}.Build(),
	}.Build()

	response, err := processActivityOptionsRequest(
		s.validator, s.mockMutableState, request.GetUpdateRequest(), request.GetNamespaceId())

	s.NoError(err)
	s.NotNil(response)
}

func (s *activityOptionsSuite) Test_updateActivityOptions_RestoreDefaultFail() {
	updateMask := &fieldmaskpb.FieldMask{
		Paths: []string{
			"task_queue.name",
			"schedule_to_close_timeout",
			"schedule_to_start_timeout",
			"start_to_close_timeout",
			"heartbeat_timeout",
			"retry_policy.backoff_coefficient",
			"retry_policy.initial_interval",
			"retry_policy.maximum_interval",
			"retry_policy.maximum_attempts",
		},
	}

	options := activitypb.ActivityOptions_builder{
		TaskQueue:              taskqueuepb.TaskQueue_builder{Name: "task_queue_name"}.Build(),
		ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
		StartToCloseTimeout:    durationpb.New(2 * time.Second),
		ScheduleToStartTimeout: durationpb.New(2 * time.Second),
		HeartbeatTimeout:       durationpb.New(2 * time.Second),
		RetryPolicy: commonpb.RetryPolicy_builder{
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(2 * time.Second),
		}.Build(),
	}.Build()

	request := historyservice.UpdateActivityOptionsRequest_builder{
		UpdateRequest: workflowservice.UpdateActivityOptionsRequest_builder{
			ActivityOptions: options,
			UpdateMask:      updateMask,
			Id:              proto.String("activity_id"),
			RestoreOriginal: true,
		}.Build(),
	}.Build()
	ctx := context.Background()

	// both restore flag and update mask are set
	_, err := Invoke(ctx, request, s.mockShard, s.workflowConsistencyChecker)
	s.Error(err)

	// not pending activity with such type
	request.GetUpdateRequest().ClearActivityOptions()
	request.GetUpdateRequest().ClearUpdateMask()
	request.GetUpdateRequest().SetType("activity_type")
	activityInfos := map[int64]*persistencespb.ActivityInfo{}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	_, err = restoreOriginalOptions(ctx, s.mockMutableState, request.GetUpdateRequest())
	s.Error(err)

	// not pending activity with such id
	request.GetUpdateRequest().ClearActivityOptions()
	request.GetUpdateRequest().ClearUpdateMask()
	request.GetUpdateRequest().SetId("activity_id")
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(nil, false)
	_, err = restoreOriginalOptions(ctx, s.mockMutableState, request.GetUpdateRequest())
	s.Error(err)

	ai := persistencespb.ActivityInfo_builder{
		ActivityId: "activity_id",
		ActivityType: commonpb.ActivityType_builder{
			Name: "activity_type",
		}.Build(),
		TaskQueue:        "task_queue_name",
		ScheduledEventId: 1,
	}.Build()

	// event not found
	err = errors.New("some error")
	s.mockMutableState.EXPECT().GetActivityScheduledEvent(gomock.Any(), gomock.Any()).Return(nil, err)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(ai, true)
	_, err = restoreOriginalOptions(ctx, s.mockMutableState, request.GetUpdateRequest())
	s.Error(err)
}

func (s *activityOptionsSuite) Test_updateActivityOptions_RestoreDefaultSuccess() {
	request := historyservice.UpdateActivityOptionsRequest_builder{
		UpdateRequest: workflowservice.UpdateActivityOptionsRequest_builder{
			Id:              proto.String("activity_id"),
			RestoreOriginal: true,
		}.Build(),
	}.Build()
	ctx := context.Background()

	ai := persistencespb.ActivityInfo_builder{
		ActivityId: "activity_id",
		ActivityType: commonpb.ActivityType_builder{
			Name: "activity_type",
		}.Build(),
		TaskQueue:        "task_queue_name",
		ScheduledEventId: 1,
		StartedEventId:   2,
	}.Build()

	he := historypb.HistoryEvent_builder{
		EventId: -123,
		Version: 123,
		ActivityTaskScheduledEventAttributes: historypb.ActivityTaskScheduledEventAttributes_builder{
			ActivityId: "activity_id",
			ActivityType: commonpb.ActivityType_builder{
				Name: "activity_type",
			}.Build(),
			TaskQueue: taskqueuepb.TaskQueue_builder{
				Name: "task_queue_name",
			}.Build(),
			ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
			StartToCloseTimeout:    durationpb.New(2 * time.Second),
			ScheduleToStartTimeout: durationpb.New(2 * time.Second),
			HeartbeatTimeout:       durationpb.New(2 * time.Second),
			RetryPolicy: commonpb.RetryPolicy_builder{
				MaximumInterval:    durationpb.New(2 * time.Second),
				MaximumAttempts:    5,
				BackoffCoefficient: 1.0,
				InitialInterval:    durationpb.New(2 * time.Second),
			}.Build(),
		}.Build(),
	}.Build()

	// event not found
	s.mockMutableState.EXPECT().GetActivityScheduledEvent(gomock.Any(), gomock.Any()).Return(he, nil)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(ai, true)
	s.mockMutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any()).Return(nil)
	response, err := restoreOriginalOptions(ctx, s.mockMutableState, request.GetUpdateRequest())
	s.NotNil(response)
	s.NoError(err)
}
