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
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	"go.temporal.io/server/common/searchattribute"
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
					RealStartTime: timestamppb.New(s.env.Now()),
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
	id             string
	start, end     time.Time
	startTolerance time.Duration
	result         enumspb.WorkflowExecutionStatus
}

type runAcrossContinueState struct {
	started  map[string]time.Time
	finished bool
}

func (s *workflowSuite) setupMocksForWorkflows(runs []workflowRun, state *runAcrossContinueState) {
	for _, run := range runs {
		run := run // capture fresh value
		// set up start
		matchStart := mock.MatchedBy(func(req *schedspb.StartWorkflowRequest) bool {
			return req.Request.WorkflowId == run.id
		})
		s.env.OnActivity(new(activities).StartWorkflow, mock.Anything, matchStart).Times(0).Maybe().Return(
			func(_ context.Context, req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
				if prev, ok := state.started[req.Request.WorkflowId]; ok {
					s.Failf("multiple starts", "for %s at %s (prev %s)", req.Request.WorkflowId, s.now(), prev)
				}
				state.started[req.Request.WorkflowId] = s.now()
				overhead := time.Duration(100+rand.Intn(100)) * time.Millisecond
				return &schedspb.StartWorkflowResponse{
					RunId:         uuid.NewString(),
					RealStartTime: timestamppb.New(s.now().Add(overhead)),
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
	// catch unexpected starts
	s.env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Times(0).Maybe().Return(
		func(_ context.Context, req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
			s.Failf("unexpected start", "for %s at %s", req.Request.WorkflowId, s.now())
			return nil, nil
		})
}

type delayedCallback struct {
	at         time.Time
	f          func()
	finishTest bool
}

func (s *workflowSuite) setupDelayedCallbacks(start time.Time, cbs []delayedCallback, state *runAcrossContinueState) {
	for _, cb := range cbs {
		if delay := cb.at.Sub(start); delay > 0 {
			if cb.finishTest {
				cb.f = func() {
					s.env.SetCurrentHistoryLength(impossibleHistorySize) // signals workflow loop to exit
					state.finished = true                                // signals test to exit
				}
			}
			s.env.RegisterDelayedCallback(cb.f, delay)
		}
	}
}

func (s *workflowSuite) runAcrossContinue(
	runs []workflowRun,
	cbs []delayedCallback,
	sched *schedpb.Schedule,
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
		currentTweakablePolicies.IterationsBeforeContinueAsNew = every
		state := runAcrossContinueState{
			started: make(map[string]time.Time),
		}
		for {
			s.env = s.NewTestWorkflowEnvironment()
			s.env.SetStartTime(startTime)

			s.setupMocksForWorkflows(runs, &state)
			s.setupDelayedCallbacks(startTime, cbs, &state)

			s.T().Logf("starting workflow with CAN every %d iterations, start time %s",
				currentTweakablePolicies.IterationsBeforeContinueAsNew, startTime)
			s.env.ExecuteWorkflow(SchedulerWorkflow, startArgs)
			s.T().Logf("finished workflow, time is now %s, finished is %v", s.now(), state.finished)

			s.True(s.env.IsWorkflowCompleted())
			result := s.env.GetWorkflowError()
			var canErr *workflow.ContinueAsNewError
			s.Require().True(errors.As(result, &canErr), "result: %v", result)

			s.env.AssertExpectations(s.T())

			if state.finished {
				break
			}

			startTime = s.now()
			startArgs = nil
			s.Require().NoError(payloads.Decode(canErr.Input, &startArgs))
		}
		// check starts that we actually got
		s.Require().Equalf(len(runs), len(state.started), "started %#v", state.started)
		for _, run := range runs {
			actual := state.started[run.id]
			inRange := !actual.Before(run.start.Add(-run.startTolerance)) && !actual.After(run.start.Add(run.startTolerance))
			s.Truef(inRange, "%v != %v for %s", run.start, state.started[run.id], run.id)
		}
	}
}

func (s *workflowSuite) TestStart() {
	// written using low-level mocks so we can test all fields in the start request

	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Nil(req.Request.LastCompletionResult)
		s.Nil(req.Request.ContinuedFailure)
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
				Interval: durationpb.New(55 * time.Minute),
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
					Interval: durationpb.New(55 * time.Minute),
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
				CatchupWindow: durationpb.New(1 * time.Hour),
			},
		},
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
			// workflow "woke up" after 6 hours
			LastProcessedTime: timestamppb.New(time.Date(2022, 5, 31, 18, 0, 0, 0, time.UTC)),
		},
	})
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCatchupWindowWhilePaused() {
	// written using low-level mocks so we can set initial state

	s.env.RegisterDelayedCallback(func() {
		// should not count any "misses" since we were paused
		s.Equal(int64(0), s.describe().Info.MissedCatchupWindow)
		// unpause just to make the test end cleanly
		s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{Unpause: "go ahead"})
	}, 3*time.Minute)
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 17, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:17:00Z", req.Request.WorkflowId)
		return nil, nil
	})

	currentTweakablePolicies.IterationsBeforeContinueAsNew = 3
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
				CatchupWindow: durationpb.New(1 * time.Hour),
			},
			State: &schedpb.ScheduleState{
				Paused: true,
			},
		},
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
			// workflow "woke up" after 6 hours
			LastProcessedTime: timestamppb.New(time.Date(2022, 5, 31, 18, 0, 0, 0, time.UTC)),
		},
	})
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()), s.env.GetWorkflowError())
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
			{
				at:         time.Date(2022, 6, 1, 0, 18, 0, 0, time.UTC),
				finishTest: true,
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: durationpb.New(5 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			},
		},
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
			// skipped over :15, :20, :25
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
				at: time.Date(2022, 6, 1, 0, 11, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(1), s.describe().Info.BufferSize)
					s.Equal(int64(0), s.describe().Info.OverlapSkipped)
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 16, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(1), s.describe().Info.BufferSize)
					s.Equal(int64(1), s.describe().Info.OverlapSkipped)
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 26, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(1), s.describe().Info.BufferSize)
					s.Equal(int64(3), s.describe().Info.OverlapSkipped)
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 31, 0, 0, time.UTC),
				f:  func() { s.Equal([]string{"myid-2022-06-01T00:30:00Z"}, s.runningWorkflows()) },
			},
			{
				at: time.Date(2022, 6, 1, 0, 32, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(0), s.describe().Info.BufferSize)
					s.Equal(int64(3), s.describe().Info.OverlapSkipped)
				},
			},
			{
				at:         time.Date(2022, 6, 1, 0, 34, 59, 0, time.UTC),
				finishTest: true,
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: durationpb.New(5 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			},
		},
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
			{
				at:         time.Date(2022, 6, 1, 0, 29, 30, 0, time.UTC),
				finishTest: true,
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: durationpb.New(5 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			},
		},
	)
}

func (s *workflowSuite) TestBufferLimit() {
	originalMaxBufferSize := currentTweakablePolicies.MaxBufferSize
	currentTweakablePolicies.MaxBufferSize = 2
	defer func() { currentTweakablePolicies.MaxBufferSize = originalMaxBufferSize }()

	s.runAcrossContinue(
		[]workflowRun{
			{
				id:     "myid-2022-06-01T00:05:00Z",
				start:  time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 22, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			// first buffered one:
			{
				id:     "myid-2022-06-01T00:10:00Z",
				start:  time.Date(2022, 6, 1, 0, 22, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 23, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			// next buffered one, and also one more gets buffered:
			{
				id:     "myid-2022-06-01T00:15:00Z",
				start:  time.Date(2022, 6, 1, 0, 23, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 24, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			// run :20 does not fit in the buffer. finally back on track for :25
			{
				id:     "myid-2022-06-01T00:25:00Z",
				start:  time.Date(2022, 6, 1, 0, 25, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 27, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 0, 20, 30, 0, time.UTC),
				f: func() {
					s.Equal([]string{"myid-2022-06-01T00:05:00Z"}, s.runningWorkflows())
					s.Equal(int64(2), s.describe().Info.BufferSize)
					s.Equal(int64(1), s.describe().Info.BufferDropped)
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 23, 30, 0, time.UTC),
				f: func() {
					s.Equal([]string{"myid-2022-06-01T00:15:00Z"}, s.runningWorkflows())
					s.Equal(int64(0), s.describe().Info.BufferSize)
					s.Equal(int64(1), s.describe().Info.BufferDropped)
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 25, 30, 0, time.UTC),
				f: func() {
					s.Equal([]string{"myid-2022-06-01T00:25:00Z"}, s.runningWorkflows())
					s.Equal(int64(0), s.describe().Info.BufferSize)
					s.Equal(int64(1), s.describe().Info.BufferDropped)
				},
			},
			{
				at:         time.Date(2022, 6, 1, 0, 29, 30, 0, time.UTC),
				finishTest: true,
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: durationpb.New(5 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			},
		},
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
				Interval: durationpb.New(55 * time.Minute),
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
				Interval: durationpb.New(55 * time.Minute),
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
				at:         time.Date(2022, 6, 1, 0, 24, 30, 0, time.UTC),
				finishTest: true,
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: durationpb.New(5 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
		},
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
				Interval: durationpb.New(5 * time.Minute),
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
		s.Nil(req.Request.LastCompletionResult)
		s.Nil(req.Request.ContinuedFailure)
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
		s.Equal(`["res1"]`, payloads.ToString(req.Request.LastCompletionResult))
		s.Nil(req.Request.ContinuedFailure)
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
		s.Equal(`["res1"]`, payloads.ToString(req.Request.LastCompletionResult))
		s.Equal(`oops`, req.Request.ContinuedFailure.Message)
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
		s.Equal(`["works again"]`, payloads.ToString(req.Request.LastCompletionResult))
		s.Nil(req.Request.ContinuedFailure)
		return nil, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: durationpb.New(5 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}, 5)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestOnlyStartForAllowAll() {
	// written using low-level mocks so we can check fields of start workflow requests

	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:05:00Z", req.Request.WorkflowId)
		s.Nil(req.Request.LastCompletionResult)
		s.Nil(req.Request.ContinuedFailure)
		return nil, nil
	})
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:10:00Z", req.Request.WorkflowId)
		s.Nil(req.Request.LastCompletionResult)
		s.Nil(req.Request.ContinuedFailure)
		return nil, nil
	})
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		s.Nil(req.Request.LastCompletionResult)
		s.Nil(req.Request.ContinuedFailure)
		return nil, nil
	})

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: durationpb.New(5 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}, 4)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestPauseOnFailure() {
	// written using low-level mocks so we can return failures

	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:05:00Z", req.Request.WorkflowId)
		s.Nil(req.Request.LastCompletionResult)
		s.Nil(req.Request.ContinuedFailure)
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
				Interval: durationpb.New(5 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			PauseOnFailure: true,
		},
	}, 3)
	s.True(s.env.IsWorkflowCompleted())
	// doesn't end properly since it sleeps forever after pausing
}

func (s *workflowSuite) TestCompileError() {
	// written using low-level mocks since it sleeps forever

	s.env.RegisterDelayedCallback(func() {
		s.Contains(s.describe().Info.InvalidScheduleError, "Month is not in range [1-12]")
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
			{
				at:         time.Date(2022, 6, 1, 0, 54, 0, 0, time.UTC),
				finishTest: true,
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: durationpb.New(55 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			},
		},
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
							StartTime:     timestamppb.New(time.Date(2022, 5, 31, 0, 0, 0, 0, time.UTC)),
							EndTime:       timestamppb.New(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC)),
							OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
						}},
					})
				},
			},
			{
				at:         time.Date(2022, 7, 31, 19, 6, 0, 0, time.UTC),
				finishTest: true,
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
	)
}

func (s *workflowSuite) TestBackfillInclusiveStartEnd() {
	s.runAcrossContinue(
		[]workflowRun{
			// if start and end time were not inclusive, this backfill run would not exist
			{
				id:     "myid-2022-05-31T19:17:00Z",
				start:  time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 0, 9, 0, 0, time.UTC),
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
					triggerBackfillTime := time.Date(2022, 5, 31, 19, 17, 0, 0, time.UTC)
					triggerBackfill := &schedpb.BackfillRequest{
						StartTime:     timestamppb.New(triggerBackfillTime),
						EndTime:       timestamppb.New(triggerBackfillTime),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
					}

					ignoreBackfillTime := triggerBackfillTime.Add(500 * time.Millisecond)
					ignoreBackfill := &schedpb.BackfillRequest{
						StartTime:     timestamppb.New(ignoreBackfillTime),
						EndTime:       timestamppb.New(ignoreBackfillTime),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
					}

					s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
						BackfillRequest: []*schedpb.BackfillRequest{triggerBackfill, ignoreBackfill},
					})
				},
			},
			{
				at:         time.Date(2022, 7, 31, 19, 6, 0, 0, time.UTC),
				finishTest: true,
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
	)
}

func (s *workflowSuite) TestHugeBackfillAllowAll() {
	prevTweakables := currentTweakablePolicies
	currentTweakablePolicies.MaxBufferSize = 30 // make smaller for testing
	defer func() { currentTweakablePolicies = prevTweakables }()

	// This has been run for up to 5000, but it takes a very long time. Run only 100 normally.
	const backfillRuns = 100
	const backfills = 4
	// The number that we process per iteration, with a 1s sleep per iteration, makes an
	// effective "rate limit" for processing allow-all backfills. This is different from the
	// explicit rate limit.
	rateLimit := currentTweakablePolicies.BackfillsPerIteration

	base := time.Date(2001, 8, 6, 0, 0, 0, 0, time.UTC)
	runs := make([]workflowRun, backfillRuns)
	for i := range runs {
		t := base.Add(time.Duration(i) * time.Hour)
		actual := baseStartTime.Add(time.Minute).Add(time.Duration(i/rateLimit) * time.Second)
		runs[i] = workflowRun{
			id:             "myid-" + t.Format(time.RFC3339),
			start:          actual,
			startTolerance: 5 * time.Second, // the "rate limit" isn't exact
			end:            actual.Add(time.Minute),
			result:         enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}
	}

	delayedCallbacks := make([]delayedCallback, backfills)
	for i := range delayedCallbacks {
		i := i
		delayedCallbacks[i] = delayedCallback{
			// test environment seems to get confused if the callback falls on the same instant
			// as a workflow timer, so use an odd interval to force it to be different.
			at: baseStartTime.Add(time.Minute).Add(time.Duration(i) * 1113 * time.Millisecond),
			f: func() {
				s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
					BackfillRequest: []*schedpb.BackfillRequest{{
						StartTime:     timestamppb.New(base.Add(time.Duration(i*backfillRuns/backfills) * time.Hour)),
						EndTime:       timestamppb.New(base.Add(time.Duration((i+1)*backfillRuns/backfills-1) * time.Hour)),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					}},
				})
			},
		}
	}

	delayedCallbacks = append(delayedCallbacks, delayedCallback{
		at:         baseStartTime.Add(50 * time.Minute),
		finishTest: true,
	})

	s.runAcrossContinue(
		runs,
		delayedCallbacks,
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
			},
		},
	)
}

func (s *workflowSuite) TestHugeBackfillBuffer() {
	prevTweakables := currentTweakablePolicies
	currentTweakablePolicies.MaxBufferSize = 30 // make smaller for testing
	defer func() { currentTweakablePolicies = prevTweakables }()

	// This has been run for up to 3000, but it takes a very long time. Run only 100 normally.
	const backfillRuns = 100
	const backfills = 4

	base := time.Date(2001, 8, 6, 0, 0, 0, 0, time.UTC)
	runs := make([]workflowRun, backfillRuns)
	duration := 56 * time.Second
	for i := range runs {
		t := base.Add(time.Duration(i) * time.Hour)
		actual := baseStartTime.Add(time.Minute).Add(time.Duration(i) * duration)
		runs[i] = workflowRun{
			id:     "myid-" + t.Format(time.RFC3339),
			start:  actual,
			end:    actual.Add(duration),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}
	}
	testEnd := baseStartTime.Add(time.Minute).Add(time.Duration(backfillRuns) * duration)
	// also add normal runs during the time it takes them all to run
	for t := baseStartTime.Add(time.Hour); t.Before(testEnd); t = t.Add(time.Hour) {
		runs = append(runs, workflowRun{
			id:     "myid-" + t.Format(time.RFC3339),
			start:  t,
			end:    t.Add(duration),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		})
	}

	delayedCallbacks := make([]delayedCallback, backfills)
	for i := range delayedCallbacks {
		i := i
		delayedCallbacks[i] = delayedCallback{
			at: baseStartTime.Add(time.Minute).Add(time.Duration(i) * 1113 * time.Millisecond),
			f: func() {
				s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
					BackfillRequest: []*schedpb.BackfillRequest{{
						StartTime:     timestamppb.New(base.Add(time.Duration(i*backfillRuns/backfills) * time.Hour)),
						EndTime:       timestamppb.New(base.Add(time.Duration((i+1)*backfillRuns/backfills-1) * time.Hour)),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
					}},
				})
			},
		}
	}

	delayedCallbacks = append(delayedCallbacks, delayedCallback{
		at:         testEnd,
		finishTest: true,
	})

	s.runAcrossContinue(
		runs,
		delayedCallbacks,
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{Interval: durationpb.New(time.Hour)}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
		},
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
			{
				at:         time.Date(2022, 6, 1, 0, 28, 8, 0, time.UTC),
				finishTest: true,
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: durationpb.New(3 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
		},
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
									Interval: durationpb.New(5 * time.Minute),
								}},
							},
							Policies: &schedpb.SchedulePolicies{
								OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
							},
							Action: s.defaultAction("newid"),
						},
						SearchAttributes: &commonpb.SearchAttributes{
							IndexedFields: map[string]*commonpb.Payload{
								"myfield": payload.EncodeString("another value"),
							},
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
			{
				at:         time.Date(2022, 6, 1, 0, 19, 30, 0, time.UTC),
				finishTest: true,
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: durationpb.New(3 * time.Minute),
				}},
			},
			Policies: &schedpb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			},
		},
	)
}

func (s *workflowSuite) TestUpdateNotRetroactive() {
	s.runAcrossContinue(
		[]workflowRun{
			{
				id:     "myid-2022-06-01T01:00:00Z",
				start:  time.Date(2022, 6, 1, 1, 0, 0, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 1, 0, 30, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				id:     "newid-2022-06-01T01:07:20Z",
				start:  time.Date(2022, 6, 1, 1, 7, 20, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 1, 7, 30, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				id:     "newid-2022-06-01T01:07:40Z",
				start:  time.Date(2022, 6, 1, 1, 7, 40, 0, time.UTC),
				end:    time.Date(2022, 6, 1, 1, 7, 50, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 1, 7, 10, 0, time.UTC),
				f: func() {
					s.env.SignalWorkflow(SignalNameUpdate, &schedspb.FullUpdateRequest{
						Schedule: &schedpb.Schedule{
							Spec: &schedpb.ScheduleSpec{
								Interval: []*schedpb.IntervalSpec{{
									Interval: durationpb.New(20 * time.Second),
								}},
							},
							Action: s.defaultAction("newid"),
						},
					})
				},
			},
			{
				at:         time.Date(2022, 6, 1, 1, 7, 55, 0, time.UTC),
				finishTest: true,
			},
		},
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: durationpb.New(1 * time.Hour),
				}},
			},
		},
	)
}

// Tests that an update between a nominal time and jittered time for a start, that doesn't
// modify that start, will still start it.
func (s *workflowSuite) TestUpdateBetweenNominalAndJitter() {
	spec := &schedpb.ScheduleSpec{
		Interval: []*schedpb.IntervalSpec{{
			Interval: durationpb.New(1 * time.Hour),
		}},
		Jitter: durationpb.New(1 * time.Hour),
	}
	s.runAcrossContinue(
		[]workflowRun{
			{
				id:     "myid-2022-06-01T01:00:00Z",
				start:  time.Date(2022, 6, 1, 1, 49, 22, 594000000, time.UTC),
				end:    time.Date(2022, 6, 1, 1, 53, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				id:     "myid-2022-06-01T02:00:00Z",
				start:  time.Date(2022, 6, 1, 2, 2, 39, 204000000, time.UTC),
				end:    time.Date(2022, 6, 1, 2, 11, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				id:     "newid-2022-06-01T03:00:00Z",
				start:  time.Date(2022, 6, 1, 3, 37, 29, 538000000, time.UTC),
				end:    time.Date(2022, 6, 1, 3, 41, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				id:     "newid-2022-06-01T04:00:00Z",
				start:  time.Date(2022, 6, 1, 4, 23, 34, 755000000, time.UTC),
				end:    time.Date(2022, 6, 1, 4, 27, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
		[]delayedCallback{
			{
				// update after nominal time 03:00:00 but before jittered time 03:37:29
				at: time.Date(2022, 6, 1, 3, 22, 10, 0, time.UTC),
				f: func() {
					s.env.SignalWorkflow(SignalNameUpdate, &schedspb.FullUpdateRequest{
						Schedule: &schedpb.Schedule{
							Spec:   spec,
							Action: s.defaultAction("newid"),
						},
					})
				},
			},
			{
				at:         time.Date(2022, 6, 1, 5, 0, 0, 0, time.UTC),
				finishTest: true,
			},
		},
		&schedpb.Schedule{
			Spec: spec,
		},
	)
}

func (s *workflowSuite) TestLimitedActions() {
	// written using low-level mocks so we can sleep forever

	// limited to 2
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 3, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:03:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 6, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:03:00Z", req.Execution.WorkflowId)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	})
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 6, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:06:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	// does not watch again at :09, but does at :10
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:06:00Z", req.Execution.WorkflowId)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	})

	s.env.RegisterDelayedCallback(func() {
		s.Equal(int64(2), s.describe().Schedule.State.RemainingActions)
	}, 1*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.Equal(int64(1), s.describe().Schedule.State.RemainingActions)
	}, 5*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.Equal(int64(0), s.describe().Schedule.State.RemainingActions)
		s.Equal(1, len(s.runningWorkflows()))
	}, 7*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		// hasn't updated yet since we slept past :09
		s.Equal(1, len(s.runningWorkflows()))
		s.env.SignalWorkflow(SignalNameRefresh, nil)
	}, 10*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.Equal(0, len(s.runningWorkflows()))
	}, 10*time.Minute+1*time.Second)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: durationpb.New(3 * time.Minute),
			}},
		},
		State: &schedpb.ScheduleState{
			LimitedActions:   true,
			RemainingActions: 2,
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		},
	}, 4)
	s.True(s.env.IsWorkflowCompleted())
	// doesn't end properly since it sleeps forever after pausing
}

func (s *workflowSuite) TestLotsOfIterations() {
	// This is mostly testing getNextTime caching logic.
	const runIterations = 30
	const backfillIterations = 3

	runs := make([]workflowRun, runIterations)
	for i := range runs {
		t := time.Date(2022, 6, 1, i, 27+i%2, 0, 0, time.UTC)
		runs[i] = workflowRun{
			id:     "myid-" + t.Format(time.RFC3339),
			start:  t,
			end:    t.Add(time.Duration(5+i%7) * time.Minute),
			result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}
	}
	testEnd := runs[len(runs)-1].end.Add(time.Second)

	delayedCallbacks := make([]delayedCallback, backfillIterations)

	// schedule some callbacks to spray backfills among scheduled runs
	// each call back adds random number of backfills in [10, 20) range
	for i := range delayedCallbacks {

		maxRuns := rand.Intn(10) + 10
		// a point in time to send the callback request
		offset := i * runIterations / backfillIterations
		callbackTime := time.Date(2022, 6, 1, offset, 2, 0, 0, time.UTC)
		// start time for callback request
		callBackRangeStartTime := time.Date(2022, 5, i, 0, 0, 0, 0, time.UTC)

		// add/process maxRuns schedules
		for j := 0; j < maxRuns; j++ {
			runStartTime := time.Date(2022, 5, i, j, 27+j%2, 0, 0, time.UTC)
			runs = append(runs, workflowRun{
				id:     "myid-" + runStartTime.Format(time.RFC3339),
				start:  callbackTime.Add(time.Duration(j) * time.Minute),
				end:    callbackTime.Add(time.Duration(j+1) * time.Minute),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			})
		}

		delayedCallbacks[i] = delayedCallback{
			at: callbackTime,
			f: func() {
				s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
					BackfillRequest: []*schedpb.BackfillRequest{{
						StartTime:     timestamppb.New(callBackRangeStartTime),
						EndTime:       timestamppb.New(callBackRangeStartTime.Add(time.Duration(maxRuns) * time.Hour)),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
					}},
				})
			},
		}
	}

	delayedCallbacks = append(delayedCallbacks, delayedCallback{
		at:         testEnd,
		finishTest: true,
	})

	s.runAcrossContinue(
		runs,
		delayedCallbacks,
		&schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Calendar: []*schedpb.CalendarSpec{
					{Minute: "27", Hour: "0/2"},
					{Minute: "28", Hour: "1/2"},
				},
			},
		},
	)
}

func (s *workflowSuite) TestExitScheduleWorkflowWhenNoActions() {
	scheduleId := "myschedule"
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedspb.WatchWorkflowRequest) (*schedspb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedspb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	})
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:30:00Z", req.Request.WorkflowId)
		return nil, nil
	})

	currentTweakablePolicies.IterationsBeforeContinueAsNew = 5
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, &schedspb.StartScheduleArgs{
		Schedule: &schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Interval: []*schedpb.IntervalSpec{{
					Interval: durationpb.New(15 * time.Minute),
				}},
			},
			State: &schedpb.ScheduleState{
				LimitedActions:   true,
				RemainingActions: 2,
			},
			Action: s.defaultAction("myid"),
		},
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    scheduleId,
			ConflictToken: InitialConflictToken,
		},
	})
	s.True(s.env.IsWorkflowCompleted())
	s.False(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
	s.True(s.env.Now().Sub(time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC)) == currentTweakablePolicies.RetentionTime)
}

func (s *workflowSuite) TestExitScheduleWorkflowWhenNoNextTime() {
	scheduleId := "myschedule"
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 0, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T01:00:00Z", req.Request.WorkflowId)
		return nil, nil
	})

	currentTweakablePolicies.IterationsBeforeContinueAsNew = 3
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, &schedspb.StartScheduleArgs{
		Schedule: &schedpb.Schedule{
			Spec: &schedpb.ScheduleSpec{
				Calendar: []*schedpb.CalendarSpec{{
					Year:       "2022",
					Month:      "June",
					DayOfMonth: "1",
					Hour:       "1",
					Minute:     "0",
					Second:     "0",
				}},
			},
			Action: s.defaultAction("myid"),
		},
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    scheduleId,
			ConflictToken: InitialConflictToken,
		},
	})
	s.True(s.env.IsWorkflowCompleted())
	s.False(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
	s.True(s.env.Now().Sub(time.Date(2022, 6, 1, 1, 0, 0, 0, time.UTC)) == currentTweakablePolicies.RetentionTime)
}

func (s *workflowSuite) TestExitScheduleWorkflowWhenEmpty() {
	scheduleId := "myschedule"

	currentTweakablePolicies.IterationsBeforeContinueAsNew = 3
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, &schedspb.StartScheduleArgs{
		Schedule: &schedpb.Schedule{
			Action: s.defaultAction("myid"),
		},
		State: &schedspb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    scheduleId,
			ConflictToken: InitialConflictToken,
		},
	})

	s.True(s.env.IsWorkflowCompleted())
	s.False(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
	s.True(s.env.Now().Sub(baseStartTime) == currentTweakablePolicies.RetentionTime)
}

func (s *workflowSuite) TestCANByIterations() {
	// written using low-level mocks so we can control iteration count

	const iters = 30
	// note: one fewer run than iters since the first doesn't start anything
	for i := 1; i < iters; i++ {
		t := baseStartTime.Add(5 * time.Minute * time.Duration(i))
		s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
			s.Equal("myid-"+t.Format(time.RFC3339), req.Request.WorkflowId)
			return nil, nil
		})
	}
	// this one catches and fails if we go over
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Fail("too many starts")
		return nil, nil
	}).Times(0).Maybe()

	// this is ignored because we set iters explicitly
	s.env.RegisterDelayedCallback(func() {
		s.env.SetContinueAsNewSuggested(true)
	}, 5*time.Minute*iters/2-time.Second)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: durationpb.New(5 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}, iters)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCANBySuggested() {
	// written using low-level mocks so we can control iteration count

	const iters = 30
	// note: one fewer run than iters since the first doesn't start anything
	for i := 1; i < iters; i++ {
		t := baseStartTime.Add(5 * time.Minute * time.Duration(i))
		s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
			s.Equal("myid-"+t.Format(time.RFC3339), req.Request.WorkflowId)
			return nil, nil
		})
	}
	// this one catches and fails if we go over
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Fail("too many starts", req.Request.WorkflowId)
		return nil, nil
	}).Times(0).Maybe()

	s.env.RegisterDelayedCallback(func() {
		s.env.SetContinueAsNewSuggested(true)
	}, 5*time.Minute*iters-time.Second)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: durationpb.New(5 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}, 0) // 0 means use suggested
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCANBySuggestedWithSignals() {
	// TODO: remove once default version is CANAfterSignals
	prevTweakables := currentTweakablePolicies
	currentTweakablePolicies.Version = CANAfterSignals
	defer func() { currentTweakablePolicies = prevTweakables }()

	// written using low-level mocks so we can control iteration count

	runs := []time.Duration{
		1 * time.Minute,
		2 * time.Minute,
		3 * time.Minute,
		5 * time.Minute, // suggestCAN will be true for this signal
		8 * time.Minute, // this one won't be reached
	}
	suggestCANAt := 4 * time.Minute
	for _, d := range runs {
		t := baseStartTime.Add(d)
		s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
			s.Equal("myid-"+t.Format(time.RFC3339), req.Request.WorkflowId)
			return nil, nil
		})
		if d > suggestCANAt {
			// the first one after the CAN flag is flipped will run, further ones will not
			break
		}
	}
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Fail("too many starts", req.Request.WorkflowId)
		return nil, nil
	}).Times(0).Maybe()

	s.env.RegisterDelayedCallback(func() {
		s.env.SetContinueAsNewSuggested(true)
	}, suggestCANAt)

	for _, d := range runs {
		s.env.RegisterDelayedCallback(func() {
			s.env.SignalWorkflow(SignalNamePatch, &schedpb.SchedulePatch{
				TriggerImmediately: &schedpb.TriggerImmediatelyRequest{},
			})
		}, d)
	}

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: durationpb.New(100 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		State: &schedpb.ScheduleState{
			Paused: true,
		},
	}, 0) // 0 means use suggested
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCANBySignal() {
	// written using low-level mocks so we can control iteration count

	const iters = 30
	// note: one fewer run than iters since the first doesn't start anything
	for i := 1; i < iters; i++ {
		t := baseStartTime.Add(5 * time.Minute * time.Duration(i))
		s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
			s.Equal("myid-"+t.Format(time.RFC3339), req.Request.WorkflowId)
			return nil, nil
		})
	}
	// this one catches and fails if we go over
	s.expectStart(func(req *schedspb.StartWorkflowRequest) (*schedspb.StartWorkflowResponse, error) {
		s.Fail("too many starts", req.Request.WorkflowId)
		return nil, nil
	}).Times(0).Maybe()

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(SignalNameForceCAN, nil)
	}, 5*time.Minute*iters-time.Second)

	s.run(&schedpb.Schedule{
		Spec: &schedpb.ScheduleSpec{
			Interval: []*schedpb.IntervalSpec{{
				Interval: durationpb.New(5 * time.Minute),
			}},
		},
		Policies: &schedpb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}, 0) // 0 means use suggested
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}
