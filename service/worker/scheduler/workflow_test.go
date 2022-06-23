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

func (s *workflowSuite) run(sched *schedpb.Schedule, iterations int) {
	// test workflows will run until "completion", in our case that means until
	// continue-as-new. we only need a small number of iterations to test, though.
	currentTweakablePolicies.IterationsBeforeContinueAsNew = iterations

	// fixed start time
	s.env.SetStartTime(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC))

	// fill this in so callers don't need to
	sched.Action = &schedpb.ScheduleAction{
		Action: &schedpb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId:   "myid",
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

/*

initial patch (trigger immediate)

time range stuff, e.g. what if we sleep for 55 min but only get woken up after 200?

early wakeup (by signal). check signal handled and also no other workflows run

schedule compile error

trigger immediately

backfill

pause with message

unpause with message

catchup window

remaining actions

run another while first is still running, with:
	overlap cancel other
	overlap terminate other
	overlap allow

completed workflow is not in info anymore

pause on failure

last completion result

continuedfailure

update config
	rejected update due to conflict token

refresh (maybe let this be done by integration test)

failed StartWorkflow

ensure watcher is started when necessary? or just test result? can't really, need mock results

test that weird logic used to set scheduletoclosetimeout in startworkflow?

test stuff across c-a-n
	have a workflow running already and check that we can start a new one


activities:

test long poll watcher more

*/
