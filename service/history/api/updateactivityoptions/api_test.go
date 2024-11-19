// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package updateactivityoptions

import (
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
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
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestApplyActivityOptionsAcceptance(t *testing.T) {

	options := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
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
			name:      "full mix - CamelCase",
			mergeFrom: options,
			mergeInto: &activitypb.ActivityOptions{},
			expected:  options,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"TaskQueue.Name",
					"ScheduleToCloseTimeout",
					"ScheduleToStartTimeout",
					"StartToCloseTimeout",
					"HeartbeatTimeout",
					"RetryPolicy.BackoffCoefficient",
					"RetryPolicy.InitialInterval",
					"RetryPolicy.MaximumInterval",
					"RetryPolicy.MaximumAttempts",
				},
			},
		},
		{
			name:      "full mix - snake_case",
			mergeFrom: options,
			mergeInto: &activitypb.ActivityOptions{},
			expected:  options,
			mask: &fieldmaskpb.FieldMask{
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
			},
		},
		{
			name: "partial",
			mergeFrom: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
				ScheduleToCloseTimeout: durationpb.New(time.Second),
				ScheduleToStartTimeout: durationpb.New(time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval: durationpb.New(time.Second),
					MaximumAttempts: 5,
				},
			},
			mergeInto: &activitypb.ActivityOptions{
				StartToCloseTimeout: durationpb.New(time.Second),
				HeartbeatTimeout:    durationpb.New(time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
			expected: options,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"task_queue.name",
					"schedule_to_close_timeout",
					"schedule_to_start_timeout",
					"retry_policy.maximum_interval",
					"retry_policy.maximum_attempts",
				},
			},
		},
	}
	for _, tc := range testCases {
		updateFields := util.ParseFieldMask(tc.mask)

		t.Run(tc.name, func(t *testing.T) {})
		err := applyActivityOptions(tc.mergeInto, tc.mergeFrom, updateFields)
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

	}
}

func TestApplyActivityOptionsErrors(t *testing.T) {
	var err error
	err = applyActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_interval"}}))
	assert.Error(t, err)

	err = applyActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_attempts"}}))
	assert.Error(t, err)

	err = applyActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.backoff_coefficient"}}))
	assert.Error(t, err)

	err = applyActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}}))
	assert.Error(t, err)

	err = applyActivityOptions(&activitypb.ActivityOptions{}, &activitypb.ActivityOptions{},
		util.ParseFieldMask(&fieldmaskpb.FieldMask{Paths: []string{"taskQueue.name"}}))
	assert.Error(t, err)

}

func TestApplyActivityOptionsReset(t *testing.T) {
	options := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
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
			"retry_policy.backoff_coefficient",
			"retry_policy.initial_interval",
			"retry_policy.maximum_interval",
			"retry_policy.maximum_attempts",
		},
	}

	updateFields := util.ParseFieldMask(fullMask)

	err := applyActivityOptions(options,
		&activitypb.ActivityOptions{
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
		mockMutableState    *workflow.MockMutableState
		mockClusterMetadata *cluster.MockMetadata

		executionInfo *persistencespb.WorkflowExecutionInfo

		validator *api.CommandAttrValidator

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
	s.mockMutableState = workflow.NewMockMutableState(s.controller)

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
		FirstExecutionRunId:              uuid.New(),
		WorkflowExecutionTimerTaskStatus: workflow.TimerTaskStatusCreated,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(s.executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()

	s.validator = api.NewCommandAttrValidator(
		s.mockShard.GetNamespaceRegistry(),
		s.mockShard.GetConfig(),
		nil,
	)
}

func (s *activityOptionsSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *activityOptionsSuite) Test_updateActivityOptionsWfNotRunning() {
	request := &historyservice.UpdateActivityOptionsRequest{}

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)

	_, err := updateActivityOptions(s.validator, s.mockMutableState, request)
	s.Error(err)
	s.ErrorAs(err, &consts.ErrWorkflowCompleted)
}

func (s *activityOptionsSuite) Test_updateActivityOptionsWfNoActivity() {
	request := &historyservice.UpdateActivityOptionsRequest{
		UpdateRequest: &workflowservicepb.UpdateActivityOptionsByIdRequest{
			ActivityOptions: &activitypb.ActivityOptions{
				TaskQueue: &taskqueuepb.TaskQueue{Name: "task_queue_name"},
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"TaskQueue.Name",
				},
			},
		},
	}

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(nil, false)
	_, err := updateActivityOptions(s.validator, s.mockMutableState, request)
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
		UpdateRequest: &workflowservicepb.UpdateActivityOptionsByIdRequest{
			ActivityOptions: options,
			UpdateMask:      updateMask,
		},
	}

	response, err := updateActivityOptions(s.validator, s.mockMutableState, request)

	s.NoError(err)
	s.NotNil(response)
}
