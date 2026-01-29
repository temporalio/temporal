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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestApplyActivityOptionsAcceptance(t *testing.T) {
	updateOptions := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		Priority: &commonpb.Priority{
			PriorityKey:    42,
			FairnessKey:    "test_key",
			FairnessWeight: 5.0,
		},
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(time.Second),
		},
	}

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
			mergeFrom: &activitypb.ActivityOptions{
				Priority: &commonpb.Priority{
					PriorityKey:    99,
					FairnessKey:    "newKey",
					FairnessWeight: 7.5,
				},
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval:    durationpb.New(time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
			mergeInto: &activitypb.ActivityOptions{
				Priority: &commonpb.Priority{
					PriorityKey:    10,
					FairnessKey:    "oldKey",
					FairnessWeight: 1.0,
				},
				RetryPolicy: &commonpb.RetryPolicy{},
			},
			expected: &activitypb.ActivityOptions{
				Priority: &commonpb.Priority{
					PriorityKey:    99,
					FairnessKey:    "newKey",
					FairnessWeight: 7.5,
				},
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval:    durationpb.New(time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
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
		assert.Equal(t, tc.mergeInto.RetryPolicy.InitialInterval, tc.expected.RetryPolicy.InitialInterval, "RetryInitialInterval")
		assert.Equal(t, tc.mergeInto.RetryPolicy.MaximumInterval, tc.expected.RetryPolicy.MaximumInterval, "RetryMaximumInterval")
		assert.Equal(t, tc.mergeInto.RetryPolicy.BackoffCoefficient, tc.expected.RetryPolicy.BackoffCoefficient, "RetryBackoffCoefficient")
		assert.Equal(t, tc.mergeInto.RetryPolicy.MaximumAttempts, tc.expected.RetryPolicy.MaximumAttempts, "RetryMaximumAttempts")

		assert.Equal(t, tc.mergeInto.TaskQueue, tc.expected.TaskQueue, "TaskQueue")

		assert.Equal(t, tc.mergeInto.ScheduleToCloseTimeout, tc.expected.ScheduleToCloseTimeout, "ScheduleToCloseTimeout")
		assert.Equal(t, tc.mergeInto.ScheduleToStartTimeout, tc.expected.ScheduleToStartTimeout, "ScheduleToStartTimeout")
		assert.Equal(t, tc.mergeInto.StartToCloseTimeout, tc.expected.StartToCloseTimeout, "StartToCloseTimeout")
		assert.Equal(t, tc.mergeInto.HeartbeatTimeout, tc.expected.HeartbeatTimeout, "HeartbeatTimeout")
		assert.Equal(t, tc.mergeInto.Priority, tc.expected.Priority, "Priority")
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
	options := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		Priority: &commonpb.Priority{
			PriorityKey:    42,
			FairnessKey:    "test_key",
			FairnessWeight: 5.0,
		},
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(time.Second),
		},
	}

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
		&activitypb.ActivityOptions{
			Priority: &commonpb.Priority{
				PriorityKey: 10,
			},
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    5,
				BackoffCoefficient: 1.0,
			},
		},
		updateFields)
	assert.NoError(t, err)

	assert.Nil(t, options.ScheduleToCloseTimeout)
	assert.Nil(t, options.ScheduleToStartTimeout)
	assert.Nil(t, options.StartToCloseTimeout)
	assert.Nil(t, options.HeartbeatTimeout)

	assert.Equal(t, int32(10), options.Priority.PriorityKey)
	assert.Empty(t, options.Priority.FairnessKey)
	assert.Zero(t, options.Priority.FairnessWeight)

	assert.Nil(t, options.RetryPolicy.InitialInterval)
	assert.Nil(t, options.RetryPolicy.MaximumInterval)
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

	s.logger = s.mockShard.GetLogger()
	s.executionInfo = &persistencespb.WorkflowExecutionInfo{
		VersionHistories:                 versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		FirstExecutionRunId:              uuid.NewString(),
		WorkflowExecutionTimerTaskStatus: workflow.TimerTaskStatusCreated,
	}
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
	request := &historyservice.UpdateActivityOptionsRequest{
		UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
			ActivityOptions: &activitypb.ActivityOptions{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "task_queue_name"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"TaskQueue.Name",
				},
			},
			Activity: &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity_id"},
		},
	}

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(nil, false)
	_, err := processActivityOptionsRequest(s.validator, s.mockMutableState, request.GetUpdateRequest(), request.GetNamespaceId())
	s.Error(err)
	s.ErrorAs(err, &consts.ErrActivityNotFound)
}

func (s *activityOptionsSuite) Test_updateActivityOptionsAcceptance() {
	fullActivityInfo := &persistencespb.ActivityInfo{
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
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
	}

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

	options := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
		StartToCloseTimeout:    durationpb.New(2 * time.Second),
		ScheduleToStartTimeout: durationpb.New(2 * time.Second),
		HeartbeatTimeout:       durationpb.New(2 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(2 * time.Second),
		},
	}

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(fullActivityInfo, true)
	s.mockMutableState.EXPECT().RegenerateActivityRetryTask(gomock.Any(), gomock.Any()).Return(nil)
	s.mockMutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any()).Return(nil)

	request := &historyservice.UpdateActivityOptionsRequest{
		UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
			ActivityOptions: options,
			UpdateMask:      updateMask,
			Activity:        &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity_id"},
		},
	}

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

	options := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
		StartToCloseTimeout:    durationpb.New(2 * time.Second),
		ScheduleToStartTimeout: durationpb.New(2 * time.Second),
		HeartbeatTimeout:       durationpb.New(2 * time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			MaximumInterval:    durationpb.New(2 * time.Second),
			MaximumAttempts:    5,
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(2 * time.Second),
		},
	}

	request := &historyservice.UpdateActivityOptionsRequest{
		UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
			ActivityOptions: options,
			UpdateMask:      updateMask,
			Activity:        &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity_id"},
			RestoreOriginal: true,
		},
	}
	ctx := context.Background()

	// both restore flag and update mask are set
	_, err := Invoke(ctx, request, s.mockShard, s.workflowConsistencyChecker)
	s.Error(err)

	// not pending activity with such type
	request.UpdateRequest.ActivityOptions = nil
	request.UpdateRequest.UpdateMask = nil
	request.UpdateRequest.Activity = &workflowservice.UpdateActivityOptionsRequest_Type{Type: "activity_type"}
	activityInfos := map[int64]*persistencespb.ActivityInfo{}
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(activityInfos)
	_, err = restoreOriginalOptions(ctx, s.mockMutableState, request.GetUpdateRequest())
	s.Error(err)

	// not pending activity with such id
	request.UpdateRequest.ActivityOptions = nil
	request.UpdateRequest.UpdateMask = nil
	request.UpdateRequest.Activity = &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity_id"}
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(nil, false)
	_, err = restoreOriginalOptions(ctx, s.mockMutableState, request.GetUpdateRequest())
	s.Error(err)

	ai := &persistencespb.ActivityInfo{
		ActivityId: "activity_id",
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
		TaskQueue:        "task_queue_name",
		ScheduledEventId: 1,
	}

	// event not found
	err = errors.New("some error")
	s.mockMutableState.EXPECT().GetActivityScheduledEvent(gomock.Any(), gomock.Any()).Return(nil, err)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(ai, true)
	_, err = restoreOriginalOptions(ctx, s.mockMutableState, request.GetUpdateRequest())
	s.Error(err)
}

func (s *activityOptionsSuite) Test_updateActivityOptions_RestoreDefaultSuccess() {
	request := &historyservice.UpdateActivityOptionsRequest{
		UpdateRequest: &workflowservice.UpdateActivityOptionsRequest{
			Activity:        &workflowservice.UpdateActivityOptionsRequest_Id{Id: "activity_id"},
			RestoreOriginal: true,
		},
	}
	ctx := context.Background()

	ai := &persistencespb.ActivityInfo{
		ActivityId: "activity_id",
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
		TaskQueue:        "task_queue_name",
		ScheduledEventId: 1,
		StartedEventId:   2,
	}

	he := &historypb.HistoryEvent{
		EventId: -123,
		Version: 123,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId: "activity_id",
				ActivityType: &commonpb.ActivityType{
					Name: "activity_type",
				},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: "task_queue_name",
				},
				ScheduleToCloseTimeout: durationpb.New(2 * time.Second),
				StartToCloseTimeout:    durationpb.New(2 * time.Second),
				ScheduleToStartTimeout: durationpb.New(2 * time.Second),
				HeartbeatTimeout:       durationpb.New(2 * time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval:    durationpb.New(2 * time.Second),
					MaximumAttempts:    5,
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(2 * time.Second),
				},
			},
		},
	}

	// event not found
	s.mockMutableState.EXPECT().GetActivityScheduledEvent(gomock.Any(), gomock.Any()).Return(he, nil)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(ai, true)
	s.mockMutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any()).Return(nil)
	response, err := restoreOriginalOptions(ctx, s.mockMutableState, request.GetUpdateRequest())
	s.NotNil(response)
	s.NoError(err)
}
