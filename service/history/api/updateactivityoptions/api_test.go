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
	"github.com/stretchr/testify/assert"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"testing"
	"time"
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

func TestApplyActivityOptionsAcceptanceErrors(t *testing.T) {
	err := applyActivityOptions(nil, nil, nil)
	t.Error(err)

	err := applyActivityOptions(&persistencespb.ActivityInfo{}, &activitypb.ActivityOptions{}, nil)
	t.Error(err)

	err := applyActivityOptions(&persistencespb.ActivityInfo{}, &activitypb.ActivityOptions{}, nil)
}
