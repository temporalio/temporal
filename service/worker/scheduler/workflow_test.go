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

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
)

type (
	workflowSuite struct {
		suite.Suite
		testsuite.WorkflowTestSuite
		env *testsuite.TestWorkflowEnvironment
	}
)

func TestWorkflow(t *testing.T) {
	suite.Run(t, new(workflowSuite))
}

func (s *workflowSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *workflowSuite) AfterTest(suiteName, testName string) {
	s.env.AssertExpectations(s.T())
}

func (s *workflowSuite) defaultAction(id string) *schedpb.ScheduleAction {
	return &schedpb.ScheduleAction{
		Action: &schedpb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId:   id,
				WorkflowType: &commonpb.WorkflowType{Name: "mywf"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "mytq"},
				Memo: &commonpb.Memo{
					Fields: map[string]*commonpb.Payload{
						"mymemo": payload.EncodeString("value"),
					},
				},
				SearchAttributes: &commonpb.SearchAttributes{
					IndexedFields: map[string]*commonpb.Payload{
						"myfield": payload.EncodeString("value"),
					},
				},
			},
		},
	}
}

func (s *workflowSuite) run(sched *schedpb.Schedule, iterations int) {
	// test workflows will run until "completion", in our case that means until
	// continue-as-new. we only need a small number of iterations to test, though.
	currentTweakablePolicies.IterationsBeforeContinueAsNew = iterations

	// fixed start time
	s.env.SetStartTime(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC))

	// fill this in so callers don't need to
	sched.Action = s.defaultAction("myid")

	s.env.ExecuteWorkflow(SchedulerWorkflow, &schedspb.StartScheduleArgs{
		Schedule: sched,
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
		},
	})
}

// TestStart and TestOverlap* are written using these low-level mock helpers to
// check that long polls are only done when needed.

func (s *workflowSuite) expectStart(f func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error)) *testsuite.MockCallWrapper {
	return s.env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Once().Return(
		func(_ context.Context, req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
			resp, err := f(req)
			if resp == nil { // fill in defaults so callers can be more concise
				resp = &schedspb.StartWorkflowResponse{
					RunId:         uuid.NewString(),
					RealStartTime: timestamp.TimePtr(time.Now()),
				}
			}
			return resp, err
		})
}

func (s *workflowSuite) expectWatch(f func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error)) *testsuite.MockCallWrapper {
	return s.env.OnActivity(new(activities).WatchWorkflow, mock.Anything, mock.Anything).Once().Return(
		func(_ context.Context, req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
			return f(req)
		})
}

func (s *workflowSuite) expectCancel(f func(req *schedspb.CancelWorkflowRequest) error) *testsuite.MockCallWrapper {
	return s.env.OnActivity(new(activities).CancelWorkflow, mock.Anything, mock.Anything).Once().Return(
		func(_ context.Context, req *schedspb.CancelWorkflowRequest) error {
			return f(req)
		})
}

func (s *workflowSuite) expectTerminate(f func(req *schedspb.TerminateWorkflowRequest) error) *testsuite.MockCallWrapper {
	return s.env.OnActivity(new(activities).TerminateWorkflow, mock.Anything, mock.Anything).Once().Return(
		func(_ context.Context, req *schedspb.TerminateWorkflowRequest) error {
			return f(req)
		})
}

func (s *workflowSuite) TestStart() {
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("mynsid", req.NamespaceId)
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(timestamp.TimeValue(req.StartTime)))
		s.Nil(req.LastCompletionResult)
		s.Nil(req.ContinuedFailure)
		s.Equal("myns", req.Request.Namespace)
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		s.Equal("mywf", req.Request.WorkflowType.Name)
		s.Equal("mytq", req.Request.TaskQueue.Name)
		s.Equal(`"value"`, payload.ToString(req.Request.Memo.Fields["mymemo"]))
		s.Equal(`"value"`, payload.ToString(req.Request.SearchAttributes.IndexedFields["myfield"]))
		s.Equal(`"myschedule"`, payload.ToString(req.Request.SearchAttributes.IndexedFields[searchattribute.TemporalScheduledById]))
		s.Equal(`"2022-06-01T00:15:00Z"`, payload.ToString(req.Request.SearchAttributes.IndexedFields[searchattribute.TemporalScheduledStartTime]))

		return nil, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(55 * time.Minute),
			}},
		},
	}, 2)
	// two iterations to start one workflow: first will sleep, second will start and then sleep again
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestOverlapSkip() {
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	// next one is skipped
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 2, 5, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			ResultFailure: &schedspb.WatchWorkflowResponse_Result{
				Result: payloads.EncodeString("res1"),
			},
		}, nil
	})
	// now it'll run another
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 2, 5, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T02:05:00Z", req.Request.WorkflowId)
		s.Equal(`["res1"]`, payloads.ToString(req.LastCompletionResult))
		return nil, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(55 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}, 4)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestOverlapBufferOne() {
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	// now will start long poll watcher
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 13, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.True(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	}).After(123 * time.Minute) // from 1:10 -> 3:13 (skipped over 1:10, 2:05, 3:00 starts)
	// but will also refresh twice (we could improve this)
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 2, 5, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 0, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	// now will start the buffered one
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 13, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T01:10:00Z", req.Request.WorkflowId)
		// hmm, this is probably wrong
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(timestamp.TimeValue(req.StartTime)))
		return nil, nil
	})
	// check on the buffered one
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 55, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T01:10:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	// then long-poll on it
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 4, 2, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T01:10:00Z", req.Execution.WorkflowId)
		s.True(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	}).After(7 * time.Minute)
	// finally can start next one (note three were skipped)
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 4, 2, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T03:55:00Z", req.Request.WorkflowId)
		return nil, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(55 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	}, 8)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestOverlapBufferAll() {
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	// now will start long poll watcher
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 13, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.True(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	}).After(123 * time.Minute) // from 1:10 -> 3:13 (skipped over 1:10, 2:05, 3:00 starts)
	// but will also refresh twice (we could improve this)
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 2, 5, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 0, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	// now will start the buffered one
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 13, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T01:10:00Z", req.Request.WorkflowId)
		// hmm, this is probably wrong
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(timestamp.TimeValue(req.StartTime)))
		return nil, nil
	})
	// then long-poll on it immediately
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 20, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T01:10:00Z", req.Execution.WorkflowId)
		s.True(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	}).After(7 * time.Minute)
	// next one
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 20, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T02:05:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	// and long poll immediately
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 34, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T02:05:00Z", req.Execution.WorkflowId)
		s.True(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	}).After(14 * time.Minute)
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 3, 34, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T03:00:00Z", req.Request.WorkflowId)
		return nil, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(55 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		},
	}, 8)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestOverlapCancel() {
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	// will cancel and then long poll to wait for it
	s.expectCancel(func(req *schedspb.CancelWorkflowRequest) error {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		return nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 15, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.True(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}, nil
	}).After(15 * time.Second)
	// now it'll run the next one
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 15, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T01:10:00Z", req.Request.WorkflowId)
		return nil, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(55 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		},
	}, 4)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestOverlapTerminate() {
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	// will terminate and then long poll to wait for it (could be improved since
	// we don't have to wait after terminate)
	s.expectTerminate(func(req *schedspb.TerminateWorkflowRequest) error {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		return nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 1, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.True(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}, nil
	}).After(1 * time.Second)
	// now it'll run the next one
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 1, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T01:10:00Z", req.Request.WorkflowId)
		return nil, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(55 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		},
	}, 4)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestOverlapAllowAll() {
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.env.Now()))
		s.Equal("myid-2022-06-01T01:10:00Z", req.Request.WorkflowId)
		return nil, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(55 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}, 3)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

// the following tests are written using these more high-level mock helpers

type workflowRun struct {
	id         string
	start, end time.Time
	result     enumspb.WorkflowExecutionStatus
}

func (s *workflowSuite) setupMocksForWorkflows(runs []workflowRun) {
	for _, runVar := range runs {
		run := runVar
		// one start per workflow (should be in order)
		s.env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Once().Return(
			func(_ context.Context, req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
				s.Equal(run.id, req.Request.WorkflowId)
				s.True(run.start.Equal(s.env.Now()))
				resp := &schedspb.StartWorkflowResponse{
					RunId:         uuid.NewString(),
					RealStartTime: timestamp.TimePtr(time.Now()),
				}
				return resp, nil
			})
		// set up short-poll watchers
		matchShortPoll := mock.MatchedBy(func(req *schedspb.WatchWorkflowRequest) bool {
			return req.Execution.WorkflowId == run.id && !req.LongPoll
		})
		s.env.OnActivity(new(activities).WatchWorkflow, mock.Anything, matchShortPoll).Times(0).Maybe().Return(
			func(_ context.Context, req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
				if s.env.Now().Before(run.end) {
					return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
				}
				return &schedspb.WatchWorkflowResponse{Status: run.result}, nil
			})
		// set up long-poll watchers
		matchLongPoll := mock.MatchedBy(func(req *schedspb.WatchWorkflowRequest) bool {
			return req.Execution.WorkflowId == run.id && req.LongPoll
		})
		s.env.OnActivity(new(activities).WatchWorkflow, mock.Anything, matchLongPoll).Times(0).Maybe().AfterFn(func() time.Duration {
			return run.end.Sub(s.env.Now())
		}).Return(func(_ context.Context, req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
			return &schedspb.WatchWorkflowResponse{Status: run.result}, nil
		})
	}
}

func (s *workflowSuite) describe() *schedspb.DescribeResponse {
	encoded, err := s.env.QueryWorkflow(QueryNameDescribe)
	s.NoError(err)
	var resp schedspb.DescribeResponse
	s.NoError(encoded.Get(&resp))
	return &resp
}

func (s *workflowSuite) TestTriggerImmediate() {
	s.setupMocksForWorkflows([]workflowRun{
		{
			id:     "myid-2022-06-01T00:15:00Z",
			start:  time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 40, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			id:     "myid-2022-06-01T00:30:00Z",
			start:  time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 50, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
	})

	s.env.RegisterDelayedCallback(func() {
		// this gets skipped because a scheduled run is still running
		s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
			TriggerImmediately: &schedpb.TriggerImmediatelyRequest{},
		})
	}, 20*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		// this one runs with overridden overlap policy
		s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
			TriggerImmediately: &schedpb.TriggerImmediatelyRequest{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
		})
	}, 30*time.Minute)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(55 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}, 4)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestBackfill() {
	s.setupMocksForWorkflows([]workflowRun{
		{
			id:     "myid-2022-05-31T19:00:00Z",
			start:  time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 9, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			id:     "myid-2022-05-31T19:17:00Z",
			start:  time.Date(2022, 6, 1, 0, 9, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 13, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			id:     "myid-2022-05-31T19:34:00Z",
			start:  time.Date(2022, 6, 1, 0, 13, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 23, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			id:     "myid-2022-05-31T19:51:00Z",
			start:  time.Date(2022, 6, 1, 0, 23, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 25, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		// this is the scheduled one
		{
			id:     "myid-2022-07-31T19:00:00Z",
			start:  time.Date(2022, 7, 31, 19, 0, 0, 0, time.UTC),
			end:    time.Date(2022, 7, 31, 19, 5, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
	})

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
			BackfillRequest: []*schedpb.BackfillRequest{{
				StartTime:     timestamp.TimePtr(time.Date(2022, 5, 31, 0, 0, 0, 0, time.UTC)),
				EndTime:       timestamp.TimePtr(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC)),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			}},
		})
	}, 5*time.Minute)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Calendar: []*schedpb.CalendarSpec{{
				Minute:     "*/17",
				Hour:       "19",
				DayOfMonth: "31",
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}, 6)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestPause() {
	s.setupMocksForWorkflows([]workflowRun{
		{
			id:     "myid-2022-06-01T00:03:00Z",
			start:  time.Date(2022, 6, 1, 0, 3, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 4, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			id:     "myid-2022-06-01T00:06:00Z",
			start:  time.Date(2022, 6, 1, 0, 6, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 7, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		// paused for a while
		{
			id:     "myid-2022-06-01T00:27:00Z",
			start:  time.Date(2022, 6, 1, 0, 27, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 28, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
	})

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
			Pause: "paused",
		})
	}, 7*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		desc := s.describe()
		s.True(desc.Schedule.State.Paused)
		s.Equal("paused", desc.Schedule.State.Notes)
	}, 12*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
			Unpause: "go ahead",
		})
	}, 26*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		desc := s.describe()
		s.False(desc.Schedule.State.Paused)
		s.Equal("go ahead", desc.Schedule.State.Notes)
	}, 28*time.Minute)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(3 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}, 12)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCompileError() {
	s.env.RegisterDelayedCallback(func() {
		s.Contains(s.describe().Info.InvalidScheduleError, "invalid syntax")
	}, 1*time.Minute)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Calendar: []*schedpb.CalendarSpec{{
				Month: "juneuary",
			}},
		},
	}, 1)
	// doesn't end properly since it sleeps forever
}

func (s *workflowSuite) TestUpdate() {
	s.setupMocksForWorkflows([]workflowRun{
		{
			id:     "myid-2022-06-01T00:03:00Z",
			start:  time.Date(2022, 6, 1, 0, 3, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 7, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		// :06 skipped because still running
		{
			id:     "myid-2022-06-01T00:09:00Z",
			start:  time.Date(2022, 6, 1, 0, 9, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 12, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		// updated to every 5m + allow all
		{
			id:     "newid-2022-06-01T00:10:00Z",
			start:  time.Date(2022, 6, 1, 0, 10, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 16, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			id:     "newid-2022-06-01T00:15:00Z",
			start:  time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 19, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
	})

	s.env.RegisterDelayedCallback(func() {
		desc := s.describe()
		s.env.SignalWorkflow(SignalNameUpdate, &schedspb.FullUpdateRequest{
			ConflictToken: desc.ConflictToken,
			Schedule: &schedpb.Schedule{
				Spec: &schedpb.ScheduleSpec{
					Interval: []*schedpb.IntervalSpec{{
						Interval: timestamp.DurationPtr(5 * time.Minute),
					}},
				},
				Policies: &schedpb.SchedulePolicies{
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				},
				Action: s.defaultAction("newid"),
			},
		})
	}, 9*time.Minute+30*time.Second)
	s.env.RegisterDelayedCallback(func() {
		desc := s.describe()
		s.env.SignalWorkflow(SignalNameUpdate, &schedspb.FullUpdateRequest{
			ConflictToken: desc.ConflictToken + 37, // conflict, should not take effect
			Schedule:      &schedpb.Schedule{},
		})
	}, 12*time.Minute)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(3 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}, 8)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestLimitedActions() {
	s.setupMocksForWorkflows([]workflowRun{
		{
			id:     "myid-2022-06-01T00:03:00Z",
			start:  time.Date(2022, 6, 1, 0, 3, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 4, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			id:     "myid-2022-06-01T00:06:00Z",
			start:  time.Date(2022, 6, 1, 0, 6, 0, 0, time.UTC),
			end:    time.Date(2022, 6, 1, 0, 7, 0, 0, time.UTC),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		// limited to 2
	})

	s.env.RegisterDelayedCallback(func() {
		s.Equal(int64(2), s.describe().Schedule.State.RemainingActions)
	}, 1*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.Equal(int64(1), s.describe().Schedule.State.RemainingActions)
	}, 5*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.Equal(int64(0), s.describe().Schedule.State.RemainingActions)
	}, 7*time.Minute)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(3 * time.Minute),
			}},
		},
		State: &schedpb.ScheduleState{
			LimitedActions:   true,
			RemainingActions: 2,
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}, 6)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

/*

initial patch (trigger immediate)

time range stuff, e.g. what if we sleep for 55 min but only get woken up after 200?

catchup window

completed workflow is not in info anymore

pause on failure

last completion result

continuedfailure

refresh (maybe let this be done by integration test)

failed StartWorkflow

test that weird logic used to set scheduletoclosetimeout in startworkflow?

test stuff across c-a-n
	have a workflow running already and check that we can start a new one


activities:

test long poll watcher more

*/
