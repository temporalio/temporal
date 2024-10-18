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
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestApplyActivityOptionsAcceptance(t *testing.T) {

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
	}

	allOptions := &activitypb.ActivityOptions{
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
		name     string
		options  *activitypb.ActivityOptions
		ai       *persistencespb.ActivityInfo
		expected *persistencespb.ActivityInfo
		mask     *fieldmaskpb.FieldMask
	}{
		{
			name:     "full mix - CamelCase",
			options:  allOptions,
			ai:       &persistencespb.ActivityInfo{},
			expected: fullActivityInfo,
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
			name:     "full mix - snake_case",
			options:  allOptions,
			ai:       &persistencespb.ActivityInfo{},
			expected: fullActivityInfo,
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
			options: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
				ScheduleToCloseTimeout: durationpb.New(time.Second),
				ScheduleToStartTimeout: durationpb.New(time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval: durationpb.New(time.Second),
					MaximumAttempts: 5,
				},
			},
			ai: &persistencespb.ActivityInfo{
				StartToCloseTimeout:     durationpb.New(time.Second),
				HeartbeatTimeout:        durationpb.New(time.Second),
				RetryBackoffCoefficient: 1.0,
				RetryInitialInterval:    durationpb.New(time.Second),
			},
			expected: fullActivityInfo,
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
		t.Run(tc.name, func(t *testing.T) {})
		err := applyActivityOptions(tc.ai, tc.options, tc.mask)
		assert.NoError(t, err)
		assert.Equal(t, tc.ai.RetryInitialInterval, tc.expected.RetryInitialInterval, "RetryInitialInterval")
		assert.Equal(t, tc.ai.RetryMaximumInterval, tc.expected.RetryMaximumInterval, "RetryMaximumInterval")
		assert.Equal(t, tc.ai.RetryBackoffCoefficient, tc.expected.RetryBackoffCoefficient, "RetryBackoffCoefficient")
		assert.Equal(t, tc.ai.RetryExpirationTime, tc.expected.RetryExpirationTime, "RetryExpirationTime")

		assert.Equal(t, tc.ai.TaskQueue, tc.expected.TaskQueue, "TaskQueue")

		assert.Equal(t, tc.ai.ScheduleToCloseTimeout, tc.expected.ScheduleToCloseTimeout, "ScheduleToCloseTimeout")
		assert.Equal(t, tc.ai.ScheduleToStartTimeout, tc.expected.ScheduleToStartTimeout, "ScheduleToStartTimeout")
		assert.Equal(t, tc.ai.StartToCloseTimeout, tc.expected.StartToCloseTimeout, "StartToCloseTimeout")
		assert.Equal(t, tc.ai.HeartbeatTimeout, tc.expected.HeartbeatTimeout, "HeartbeatTimeout")

	}
}

func TestApplyActivityOptionsErrors(t *testing.T) {
	err := applyActivityOptions(nil, nil, nil)
	assert.Error(t, err)

	err = applyActivityOptions(&persistencespb.ActivityInfo{}, &activitypb.ActivityOptions{},
		&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_interval"}})
	assert.Error(t, err)

	err = applyActivityOptions(&persistencespb.ActivityInfo{}, &activitypb.ActivityOptions{},
		&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.maximum_attempts"}})
	assert.Error(t, err)

	err = applyActivityOptions(&persistencespb.ActivityInfo{}, &activitypb.ActivityOptions{},
		&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.backoff_coefficient"}})
	assert.Error(t, err)

	err = applyActivityOptions(&persistencespb.ActivityInfo{}, &activitypb.ActivityOptions{},
		&fieldmaskpb.FieldMask{Paths: []string{"retry_policy.initial_interval"}})
	assert.Error(t, err)

	err = applyActivityOptions(&persistencespb.ActivityInfo{}, &activitypb.ActivityOptions{},
		&fieldmaskpb.FieldMask{Paths: []string{"taskQueue.name"}})
	assert.Error(t, err)

}

func TestApplyActivityOptionsReset(t *testing.T) {
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

	err := applyActivityOptions(fullActivityInfo,
		&activitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				MaximumAttempts:    5,
				BackoffCoefficient: 1.0,
			},
		},
		fullMask)
	assert.NoError(t, err)

	assert.Nil(t, fullActivityInfo.ScheduleToCloseTimeout)
	assert.Nil(t, fullActivityInfo.ScheduleToStartTimeout)
	assert.Nil(t, fullActivityInfo.StartToCloseTimeout)
	assert.Nil(t, fullActivityInfo.HeartbeatTimeout)

	assert.Nil(t, fullActivityInfo.RetryInitialInterval)
	assert.Nil(t, fullActivityInfo.RetryMaximumInterval)
}

type (
	stateBuilderSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockShard            *shard.ContextTest
		mockEventsCache      *events.MockCache
		mockNamespaceCache   *namespace.MockRegistry
		mockTaskGenerator    *workflow.MockTaskGenerator
		mockMutableState     *workflow.MockMutableState
		mockClusterMetadata  *cluster.MockMetadata
		stateMachineRegistry *hsm.Registry

		executionInfo *persistencespb.WorkflowExecutionInfo

		logger log.Logger
	}
)

func TestStateBuilderSuite(t *testing.T) {
	s := new(stateBuilderSuite)
	suite.Run(t, s)
}

func (s *stateBuilderSuite) SetupSuite() {
}

func (s *stateBuilderSuite) TearDownSuite() {
}

func (s *stateBuilderSuite) SetupTest() {
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

}

func (s *stateBuilderSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *stateBuilderSuite) Test_updateActivityOptionsWfNotRunning() {
	request := &historyservice.UpdateActivityOptionsRequest{}
	response := &historyservice.UpdateActivityOptionsResponse{}

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	_, err := updateActivityOptions(s.mockShard, s.mockMutableState, request, response)
	s.Error(err)
	s.ErrorAs(err, &consts.ErrWorkflowCompleted)
}

func (s *stateBuilderSuite) Test_updateActivityOptionsWfNoActivity() {
	request := &historyservice.UpdateActivityOptionsRequest{}
	response := &historyservice.UpdateActivityOptionsResponse{}

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(nil, false)
	_, err := updateActivityOptions(s.mockShard, s.mockMutableState, request, response)
	s.Error(err)
	s.ErrorAs(err, &consts.ErrActivityTaskNotFound)
}

func (s *stateBuilderSuite) Test_updateActivityOptionsAcceptance() {

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
	}

	updateMask := &fieldmaskpb.FieldMask{
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

	allOptions := &activitypb.ActivityOptions{
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

	request := &historyservice.UpdateActivityOptionsRequest{
		UpdateRequest: &workflowservicepb.UpdateActivityOptionsByIdRequest{
			ActivityOptions: allOptions,
			UpdateMask:      updateMask,
		},
	}
	response := &historyservice.UpdateActivityOptionsResponse{}

	wfAction, err := updateActivityOptions(s.mockShard, s.mockMutableState, request, response)
	s.NoError(err)
	s.Equal(false, wfAction.Noop)
	s.Equal(false, wfAction.CreateWorkflowTask)
}
