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
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
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
	"go.temporal.io/server/common/util"
)

type (
	workflowSuite struct {
		suite.Suite
		testsuite.WorkflowTestSuite
		env *testsuite.TestWorkflowEnvironment
	}
)

var (
	baseStartTime = time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC)
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

// test helpers

func (s *workflowSuite) now() time.Time {
	return s.env.Now().UTC() // env.Now() returns local time by default, force to UTC
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
	s.env.SetStartTime(baseStartTime)

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

func (s *workflowSuite) describe() *schedspb.DescribeResponse {
	encoded, err := s.env.QueryWorkflow(QueryNameDescribe)
	s.NoError(err)
	var resp schedspb.DescribeResponse
	s.NoError(encoded.Get(&resp))
	return &resp
}

func (s *workflowSuite) runningWorkflows() []string {
	desc := s.describe()
	var out []string
	for _, ex := range desc.Info.RunningWorkflows {
		out = append(out, ex.WorkflowId)
	}
	return out
}

// Low-level mock helpers:

func (s *workflowSuite) expectStart(f func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error)) *testsuite.MockCallWrapper {
	return s.env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Once().Return(
		func(_ context.Context, req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
			resp, err := f(req)
			if resp == nil && err == nil { // fill in defaults so callers can be more concise
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

// High-level mock helpers. This is a small meta-test-framework: it runs a schedule across
// multiple workflow executions with continue-as-new, breaking at a variety of points. To do
// this it has to reinitialize the workflow test framework each time, since the framework only
// supports a single workflow execution. That means we have to reinitialize our activity mocks
// and delayed executions, which means they have to be supplied to runAcrossContinue as data.

type workflowRun struct {
	id         string
	start, end time.Time
	result     enumspb.WorkflowExecutionStatus
}

func (s *workflowSuite) setupMocksForWorkflows(runs []workflowRun, started map[string]time.Time) {
	for _, run := range runs {
		run := run // capture fresh value
		// set up start
		matchStart := mock.MatchedBy(func(req *schedspb.StartWorkflowRequest) bool {
			return req.Request.WorkflowId == run.id
		})
		s.env.OnActivity(new(activities).StartWorkflow, mock.Anything, matchStart).Times(0).Maybe().Return(
			func(_ context.Context, req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
				if _, ok := started[req.Request.WorkflowId]; ok {
					s.Failf("multiple starts for %s", req.Request.WorkflowId)
				}
				started[req.Request.WorkflowId] = s.now()
				return &schedspb.StartWorkflowResponse{
					RunId:         uuid.NewString(),
					RealStartTime: timestamp.TimePtr(time.Now()),
				}, nil
			})
		// set up short-poll watchers
		matchShortPoll := mock.MatchedBy(func(req *schedspb.WatchWorkflowRequest) bool {
			return req.Execution.WorkflowId == run.id && !req.LongPoll
		})
		s.env.OnActivity(new(activities).WatchWorkflow, mock.Anything, matchShortPoll).Times(0).Maybe().Return(
			func(_ context.Context, req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
				if s.now().Before(run.end) {
					return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
				}
				return &schedspb.WatchWorkflowResponse{Status: run.result}, nil
			})
		// set up long-poll watchers
		matchLongPoll := mock.MatchedBy(func(req *schedspb.WatchWorkflowRequest) bool {
			return req.Execution.WorkflowId == run.id && req.LongPoll
		})
		s.env.OnActivity(new(activities).WatchWorkflow, mock.Anything, matchLongPoll).Times(0).Maybe().AfterFn(func() time.Duration {
			return run.end.Sub(s.now())
		}).Return(func(_ context.Context, req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
			return &schedspb.WatchWorkflowResponse{Status: run.result}, nil
		})
	}
}

type delayedCallback struct {
	at time.Time
	f  func()
}

func (s *workflowSuite) setupDelayedCallbacks(start time.Time, cbs []delayedCallback) {
	for _, cb := range cbs {
		if delay := cb.at.Sub(start); delay > 0 {
			s.env.RegisterDelayedCallback(cb.f, delay)
		}
	}
}

func (s *workflowSuite) runAcrossContinue(
	runs []workflowRun,
	cbs []delayedCallback,
	sched *schedpb.Schedule,
	maxIterations int,
) {
	// fill this in so callers don't need to
	sched.Action = s.defaultAction("myid")

	for _, every := range []int{1, 2, 3, 5, 7, 11, 1000} {
		s.T().Logf("running %s with continue-as-new every %d iterations", s.T().Name(), every)

		startTime := baseStartTime
		startArgs := &schedspb.StartScheduleArgs{
			Schedule: sched,
			State: &schedspb.InternalState{
				Namespace:     "myns",
				NamespaceId:   "mynsid",
				ScheduleId:    "myschedule",
				ConflictToken: InitialConflictToken,
			},
		}
		iterations := maxIterations
		gotRuns := make(map[string]time.Time)
		for {
			s.env = s.NewTestWorkflowEnvironment()
			s.env.SetStartTime(startTime)

			s.setupMocksForWorkflows(runs, gotRuns)
			s.setupDelayedCallbacks(startTime, cbs)

			currentTweakablePolicies.IterationsBeforeContinueAsNew = util.Min(iterations, every)

			s.T().Logf("starting workflow for %d iterations out of %d remaining, %d total, start time %s",
				currentTweakablePolicies.IterationsBeforeContinueAsNew, iterations, maxIterations, startTime)
			s.env.ExecuteWorkflow(SchedulerWorkflow, startArgs)
			s.T().Logf("finished workflow, time is now %s", s.now())

			s.True(s.env.IsWorkflowCompleted())
			result := s.env.GetWorkflowError()
			var canErr *workflow.ContinueAsNewError
			s.True(errors.As(result, &canErr))

			s.env.AssertExpectations(s.T())

			iterations -= currentTweakablePolicies.IterationsBeforeContinueAsNew
			if iterations == 0 {
				break
			}

			startTime = s.now()
			startArgs = nil
			s.NoError(payloads.Decode(canErr.Input, &startArgs))
		}
		// check starts that we actually got
		s.Equal(len(runs), len(gotRuns))
		for _, run := range runs {
			s.True(run.start.Equal(gotRuns[run.id]))
		}
	}
}

func (s *workflowSuite) TestStart() {
	// written using low-level mocks so we can test all fields in the start request

	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("mynsid", req.NamespaceId)
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

func (s *workflowSuite) TestInitialPatch() {
	// written using low-level mocks so we can set initial patch

	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:00:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:00:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	})
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})

	currentTweakablePolicies.IterationsBeforeContinueAsNew = 2
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, &schedspb.StartScheduleArgs{
		Schedule: &schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: timestamp.DurationPtr(55 * time.Minute),
				}},
			},
			Action: s.defaultAction("myid"),
		},
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
		},
		InitialPatch: &schedpb.SchedulePatch{
			TriggerImmediately: &schedpb.TriggerImmediatelyRequest{},
		},
	})
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCatchupWindow() {
	// written using low-level mocks so we can set initial state

	// one catchup
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-05-31T23:17:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 17, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-05-31T23:17:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	})
	// one on time
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 17, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:17:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.env.RegisterDelayedCallback(func() {
		s.Equal(int64(5), s.describe().Info.MissedCatchupWindow)
	}, 18*time.Minute)

	currentTweakablePolicies.IterationsBeforeContinueAsNew = 2
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, &schedspb.StartScheduleArgs{
		Schedule: &schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Calendar: []*schedpb.CalendarSpec{{
					Minute: "17",
					Hour:   "*",
				}},
			},
			Action: s.defaultAction("myid"),
			Policies: &schedpb.SchedulePolicies{
				CatchupWindow: timestamp.DurationPtr(1 * time.Hour),
			},
		},
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
			// workflow "woke up" after 6 hours
			LastProcessedTime: timestamp.TimePtr(time.Date(2022, 5, 31, 18, 0, 0, 0, time.UTC)),
		},
	})
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestOverlapSkip() {
	s.runAcrossContinue(
		[]workflowRun{
			{
				id:     "myid-2022-06-01T00:05:00Z",
				start:  time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 12, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			// :10 is skipped
			{
				id:     "myid-2022-06-01T00:15:00Z",
				start:  time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 17, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 0, 11, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(1), s.describe().Info.OverlapSkipped)
					s.Equal([]string{"myid-2022-06-01T00:05:00Z"}, s.runningWorkflows())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 16, 0, 0, time.UTC),
				f: func() {
					s.Equal([]string{"myid-2022-06-01T00:15:00Z"}, s.runningWorkflows())
				},
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: timestamp.DurationPtr(5 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			},
		},
		4,
	)
}

func (s *workflowSuite) TestOverlapBufferOne() {
	s.runAcrossContinue(
		[]workflowRun{
			{
				id:     "myid-2022-06-01T00:05:00Z",
				start:  time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 27, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			// first buffered one:
			{
				id:     "myid-2022-06-01T00:10:00Z",
				start:  time.Date(2022, 6, 1, 0, 27, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 29, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			// skipped over :15, :20
			{
				id:     "myid-2022-06-01T00:30:00Z",
				start:  time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 39, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 0, 6, 0, 0, time.UTC),
				f:  func() { s.Equal([]string{"myid-2022-06-01T00:05:00Z"}, s.runningWorkflows()) },
			},
			{
				at: time.Date(2022, 6, 1, 0, 31, 0, 0, time.UTC),
				f:  func() { s.Equal([]string{"myid-2022-06-01T00:30:00Z"}, s.runningWorkflows()) },
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: timestamp.DurationPtr(5 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			},
		},
		8,
	)
}

func (s *workflowSuite) TestOverlapBufferAll() {
	s.runAcrossContinue(
		[]workflowRun{
			{
				id:     "myid-2022-06-01T00:05:00Z",
				start:  time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 17, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			// first buffered one:
			{
				id:     "myid-2022-06-01T00:10:00Z",
				start:  time.Date(2022, 6, 1, 0, 17, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 19, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			// next buffered one, and also one more gets buffered:
			{
				id:     "myid-2022-06-01T00:15:00Z",
				start:  time.Date(2022, 6, 1, 0, 19, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 22, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			// still buffered:
			{
				id:     "myid-2022-06-01T00:20:00Z",
				start:  time.Date(2022, 6, 1, 0, 22, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 23, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			// finally back on track:
			{
				id:     "myid-2022-06-01T00:25:00Z",
				start:  time.Date(2022, 6, 1, 0, 25, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 27, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 0, 20, 0, 0, time.UTC),
				f:  func() { s.Equal([]string{"myid-2022-06-01T00:15:00Z"}, s.runningWorkflows()) },
			},
			{
				at: time.Date(2022, 6, 1, 0, 22, 30, 0, time.UTC),
				f:  func() { s.Equal([]string{"myid-2022-06-01T00:20:00Z"}, s.runningWorkflows()) },
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: timestamp.DurationPtr(5 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			},
		},
		9,
	)
}

func (s *workflowSuite) TestOverlapCancel() {
	// written using low-level mocks so we can mock CancelWorkflow without adding support in
	// the framework

	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	// will cancel and then long poll to wait for it
	s.expectCancel(func(req *schedspb.CancelWorkflowRequest) error {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		return nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 15, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.True(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	}).After(15 * time.Second)
	// now it'll run the next one
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 15, 0, time.UTC).Equal(s.now()))
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
	// written using low-level mocks so we can mock TerminateWorkflow without adding support in
	// the framework

	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	})
	// will terminate and then long poll to wait for it (could be improved since
	// we don't have to wait after terminate)
	s.expectTerminate(func(req *schedspb.TerminateWorkflowRequest) error {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		return nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 1, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.True(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}, nil
	}).After(1 * time.Second)
	// now it'll run the next one
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 1, 0, time.UTC).Equal(s.now()))
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
	// also contains tests for RunningWorkflows and refresh, since it's convenient to do here
	s.runAcrossContinue(
		[]workflowRun{
			{
				id:     "myid-2022-06-01T00:05:00Z",
				start:  time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 17, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				id:     "myid-2022-06-01T00:10:00Z",
				start:  time.Date(2022, 6, 1, 0, 10, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 22, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				id:     "myid-2022-06-01T00:15:00Z",
				start:  time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 16, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				id:     "myid-2022-06-01T00:20:00Z",
				start:  time.Date(2022, 6, 1, 0, 20, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 22, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 0, 6, 0, 0, time.UTC),
				f:  func() { s.Equal([]string{"myid-2022-06-01T00:05:00Z"}, s.runningWorkflows()) },
			},
			{
				at: time.Date(2022, 6, 1, 0, 11, 0, 0, time.UTC),
				f: func() {
					s.Equal([]string{"myid-2022-06-01T00:05:00Z", "myid-2022-06-01T00:10:00Z"}, s.runningWorkflows())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 15, 30, 0, time.UTC),
				f: func() {
					s.Equal([]string{"myid-2022-06-01T00:05:00Z", "myid-2022-06-01T00:10:00Z", "myid-2022-06-01T00:15:00Z"}, s.runningWorkflows())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 16, 30, 0, time.UTC),
				f: func() {
					// :15 has ended here, but we won't know until we refresh since we don't have a long-poll watcher
					s.Equal([]string{"myid-2022-06-01T00:05:00Z", "myid-2022-06-01T00:10:00Z", "myid-2022-06-01T00:15:00Z"}, s.runningWorkflows())
					// poke it to refresh
					s.env.SignalWorkflow(SignalNameRefresh, nil)
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 16, 31, 0, time.UTC),
				f: func() {
					// now we'll see it end
					s.Equal([]string{"myid-2022-06-01T00:05:00Z", "myid-2022-06-01T00:10:00Z"}, s.runningWorkflows())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 18, 0, 0, time.UTC),
				f: func() {
					// :05 has ended, but we won't see it yet
					s.Equal([]string{"myid-2022-06-01T00:05:00Z", "myid-2022-06-01T00:10:00Z"}, s.runningWorkflows())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 21, 0, 0, time.UTC),
				f: func() {
					// we'll see :05 ended because :20 started and did an implicit refresh
					s.Equal([]string{"myid-2022-06-01T00:10:00Z", "myid-2022-06-01T00:20:00Z"}, s.runningWorkflows())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 23, 0, 0, time.UTC),
				f: func() {
					// we won't see these ended yet
					s.Equal([]string{"myid-2022-06-01T00:10:00Z", "myid-2022-06-01T00:20:00Z"}, s.runningWorkflows())
					// poke it to refresh
					s.env.SignalWorkflow(SignalNameRefresh, nil)
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 23, 1, 0, time.UTC),
				f: func() {
					// now we will
					s.Equal([]string(nil), s.runningWorkflows())
				},
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: timestamp.DurationPtr(5 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
		},
		7,
	)
}

func (s *workflowSuite) TestFailedStart() {
	s.T().Skip("the workflow test framework seems to have bugs around local activities that fail")

	// written using low-level mocks so we can fail a StartWorkflow

	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:05:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:05:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	})
	// failed start, but doesn't do anything else until next scheduled time
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:10:00Z", req.Request.WorkflowId)
		return nil, errors.New("failed to start!")
	})
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	// TODO: buffer two or more starts using backfill, then have the first start fail, and
	// check that the second start is attempted immediately after, without sleeping.

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(5 * time.Minute),
			}},
		},
	}, 4)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestLastCompletionResultAndContinuedFailure() {
	// written using low-level mocks so we can return results/failures and check fields of
	// start workflow requests

	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:05:00Z", req.Request.WorkflowId)
		s.Nil(req.LastCompletionResult)
		s.Nil(req.ContinuedFailure)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:05:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			ResultFailure: &schedspb.WatchWorkflowResponse_Result{
				Result: payloads.EncodeString("res1"),
			},
		}, nil
	})
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:10:00Z", req.Request.WorkflowId)
		s.Equal(`["res1"]`, payloads.ToString(req.LastCompletionResult))
		s.Nil(req.ContinuedFailure)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:10:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			ResultFailure: &schedspb.WatchWorkflowResponse_Failure{
				Failure: &failurepb.Failure{Message: "oops"},
			},
		}, nil
	})
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		s.Equal(`["res1"]`, payloads.ToString(req.LastCompletionResult))
		s.Equal(`oops`, req.ContinuedFailure.Message)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			ResultFailure: &schedspb.WatchWorkflowResponse_Result{
				Result: payloads.EncodeString("works again"),
			},
		}, nil
	})
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:20:00Z", req.Request.WorkflowId)
		s.Equal(`["works again"]`, payloads.ToString(req.LastCompletionResult))
		s.Nil(req.ContinuedFailure)
		return nil, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(5 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}, 5)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestPauseOnFailure() {
	// written using low-level mocks so we can return failures

	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:05:00Z", req.Request.WorkflowId)
		s.Nil(req.LastCompletionResult)
		s.Nil(req.ContinuedFailure)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:05:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			ResultFailure: &schedspb.WatchWorkflowResponse_Failure{
				Failure: &failurepb.Failure{Message: "oops"},
			},
		}, nil
	})
	s.env.RegisterDelayedCallback(func() {
		s.False(s.describe().Schedule.State.Paused)
	}, 9*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		desc := s.describe()
		s.True(desc.Schedule.State.Paused)
		s.Contains(desc.Schedule.State.Notes, "paused due to workflow failure")
		s.Contains(desc.Schedule.State.Notes, "oops")
	}, 11*time.Minute)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: timestamp.DurationPtr(5 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			PauseOnFailure: true,
		},
	}, 6)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCompileError() {
	// written using low-level mocks since it sleeps forever

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

func (s *workflowSuite) TestTriggerImmediate() {
	s.runAcrossContinue(
		[]workflowRun{
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
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 0, 20, 0, 0, time.UTC),
				f: func() {
					// this gets skipped because a scheduled run is still running
					s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
						TriggerImmediately: &schedpb.TriggerImmediatelyRequest{},
					})
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC),
				f: func() {
					// this one runs with overridden overlap policy
					s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
						TriggerImmediately: &schedpb.TriggerImmediatelyRequest{
							OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
						},
					})
				},
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: timestamp.DurationPtr(55 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			},
		},
		4,
	)
}

func (s *workflowSuite) TestBackfill() {
	s.runAcrossContinue(
		[]workflowRun{
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
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC),
				f: func() {
					s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
						BackfillRequest: []*schedpb.BackfillRequest{{
							StartTime:     timestamp.TimePtr(time.Date(2022, 5, 31, 0, 0, 0, 0, time.UTC)),
							EndTime:       timestamp.TimePtr(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC)),
							OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
						}},
					})
				},
			},
		},
		&schedpb.Schedule{
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
		},
		6,
	)
}

func (s *workflowSuite) TestPause() {
	s.runAcrossContinue(
		[]workflowRun{
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
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 0, 7, 7, 0, time.UTC),
				f: func() {
					s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
						Pause: "paused",
					})
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 12, 7, 0, time.UTC),
				f: func() {
					desc := s.describe()
					s.True(desc.Schedule.State.Paused)
					s.Equal("paused", desc.Schedule.State.Notes)
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 26, 7, 0, time.UTC),
				f: func() {
					s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
						Unpause: "go ahead",
					})
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 28, 7, 0, time.UTC),
				f: func() {
					desc := s.describe()
					s.False(desc.Schedule.State.Paused)
					s.Equal("go ahead", desc.Schedule.State.Notes)
				},
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: timestamp.DurationPtr(3 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
		},
		12,
	)
}

func (s *workflowSuite) TestUpdate() {
	s.runAcrossContinue(
		[]workflowRun{
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
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 0, 9, 5, 0, time.UTC),
				f: func() {
					// shouldn't crash
					s.env.SignalWorkflow(SignalNameUpdate, nil)
					s.env.SignalWorkflow(SignalNameUpdate, &schedspb.FullUpdateRequest{})
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 9, 7, 0, time.UTC),
				f: func() {
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
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 12, 7, 0, time.UTC),
				f: func() {
					desc := s.describe()
					s.env.SignalWorkflow(SignalNameUpdate, &schedspb.FullUpdateRequest{
						ConflictToken: desc.ConflictToken + 37, // conflict, should not take effect
						Schedule:      &schedpb.Schedule{},
					})
				},
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: timestamp.DurationPtr(3 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			},
		},
		10,
	)
}

func (s *workflowSuite) TestLimitedActions() {
	s.runAcrossContinue(
		[]workflowRun{
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
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 0, 1, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(2), s.describe().Schedule.State.RemainingActions)
				},
			}, {
				at: time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(1), s.describe().Schedule.State.RemainingActions)
				},
			}, {
				at: time.Date(2022, 6, 1, 0, 7, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(0), s.describe().Schedule.State.RemainingActions)
				},
			},
		},
		&schedpb.Schedule{
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
		},
		6,
	)
}
