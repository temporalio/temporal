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

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertPathToCamel(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []string
	}{
		{
			input:          "underscore",
			expectedOutput: []string{"underscore"},
		},
		{
			input:          "CamelCase",
			expectedOutput: []string{"camelCase"},
		},
		{
			input:          "UPPERCASE",
			expectedOutput: []string{"uPPERCASE"},
		},
		{
			input:          "Dot.Separated",
			expectedOutput: []string{"dot", "separated"},
		},
		{
			input:          "dash_separated",
			expectedOutput: []string{"dashSeparated"},
		},
		{
			input:          "dash_separated.and_another",
			expectedOutput: []string{"dashSeparated", "andAnother"},
		},
		{
			input:          "Already.CamelCase",
			expectedOutput: []string{"already", "camelCase"},
		},
		{
			input:          "Mix.of.Snake_case.and.CamelCase",
			expectedOutput: []string{"mix", "of", "snakeCase", "and", "camelCase"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			actualOutput := ConvertPathToCamel(tc.input)
			assert.Equal(t, tc.expectedOutput, actualOutput)
		})
	}
}

/*
!!!!!!!!!
protoreflect for some reason is not working well with niL pointers to messages.
Or at least I was not able to make it work.
msg.Get(fd).IsValid() is not returning false for nil values.
I will keep this code around for some time, but I will not use it.
I will remove it or fix it before final commit.


func TestApplyFieldMask(t *testing.T) {

	ao := &activitypb.ActivityOptions{
		TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue"},
		ScheduleToCloseTimeout: durationpb.New(time.Second),
		ScheduleToStartTimeout: durationpb.New(time.Second),
		StartToCloseTimeout:    durationpb.New(time.Second),
		HeartbeatTimeout:       durationpb.New(time.Second),
		RetryPolicy: &commonpb.RetryPolicy{
			BackoffCoefficient: 1.0,
			InitialInterval:    durationpb.New(time.Second),
			MaximumInterval:    durationpb.New(time.Second),
			MaximumAttempts:    5,
		},
	}

	testCases := []struct {
		name     string
		source   *activitypb.ActivityOptions
		target   *activitypb.ActivityOptions
		expected *activitypb.ActivityOptions
		mask     *fieldmaskpb.FieldMask
	}{
		//{
		//	name:   "nil mask",
		//	source: &activitypb.ActivityOptions{},
		//	target: &activitypb.ActivityOptions{},
		//	mask:   nil,
		//},
		//{
		//	name:   "nil source",
		//	source: nil,
		//	target: &activitypb.ActivityOptions{},
		//	mask: &fieldmaskpb.FieldMask{
		//		Paths: []string{"TaskQueue.Name"},
		//	},
		//},
		//{
		//	name:     "nil target",
		//	source:   &activitypb.ActivityOptions{},
		//	target:   nil,
		//	expected: nil,
		//	mask: &fieldmaskpb.FieldMask{
		//		Paths: []string{"TaskQueue.Name"},
		//	},
		//},
		//{
		//	name:     "full mix - CamelCase",
		//	source:   &activitypb.ActivityOptions{},
		//	target:   proto.Clone(ao).(*activitypb.ActivityOptions),
		//	expected: ao,
		//	mask: &fieldmaskpb.FieldMask{
		//		Paths: []string{
		//			"TaskQueue.Name",
		//			"ScheduleToCloseTimeout",
		//			"ScheduleToStartTimeout",
		//			"StartToCloseTimeout",
		//			"HeartbeatTimeout",
		//			"RetryPolicy.BackoffCoefficient",
		//			"RetryPolicy.InitialInterval",
		//			"RetryPolicy.MaximumInterval",
		//			"RetryPolicy.MaximumAttempts",
		//		},
		//	},
		//},
		//{
		//	name:     "full mix - snake_case",
		//	source:   &activitypb.ActivityOptions{},
		//	target:   proto.Clone(ao).(*activitypb.ActivityOptions),
		//	expected: ao,
		//	mask: &fieldmaskpb.FieldMask{
		//		Paths: []string{
		//			"task_queue.name",
		//			"schedule_to_close_timeout",
		//			"schedule_to_start_timeout",
		//			"start_to_close_timeout",
		//			"heartbeat_timeout",
		//			"retry_policy.backoff_coefficient",
		//			"retry_policy.initial_interval",
		//			"retry_policy.maximum_interval",
		//			"retry_policy.maximum_attempts",
		//		},
		//	},
		//},
		{
			name: "partial",
			source: &activitypb.ActivityOptions{
				TaskQueue:              &taskqueuepb.TaskQueue{Name: "task_queue_name"},
				ScheduleToCloseTimeout: durationpb.New(time.Second),
				ScheduleToStartTimeout: durationpb.New(time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					MaximumInterval: durationpb.New(time.Second),
					MaximumAttempts: 5,
				},
			},
			target: &activitypb.ActivityOptions{
				StartToCloseTimeout: durationpb.New(time.Second),
				HeartbeatTimeout:    durationpb.New(time.Second),
				RetryPolicy: &commonpb.RetryPolicy{
					BackoffCoefficient: 1.0,
					InitialInterval:    durationpb.New(time.Second),
				},
			},
			expected: ao,
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{
					"task_queue.name",
					//"schedule_to_close_timeout",
					//"schedule_to_start_timeout",
					//"retry_policy.maximum_interval",
					//"retry_policy.maximum_attempts",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {})
		if tc.target.TaskQueue == nil {
			//tc.target.TaskQueue = &taskqueuepb.TaskQueue{}
			println("tc is nill")
		}
		ApplyFieldMask(tc.source, tc.target, tc.mask)
		assert.True(t, proto.Equal(tc.expected, tc.target))
	}
}
*/
