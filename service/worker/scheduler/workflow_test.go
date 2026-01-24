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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/protoassert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func (s *workflowSuite) defaultAction(id string) *schedulepb.ScheduleAction {
	return schedulepb.ScheduleAction_builder{
		StartWorkflow: workflowpb.NewWorkflowExecutionInfo_builder{
			WorkflowId:   id,
			WorkflowType: commonpb.WorkflowType_builder{Name: "mywf"}.Build(),
			TaskQueue:    taskqueuepb.TaskQueue_builder{Name: "mytq"}.Build(),
			Memo: commonpb.Memo_builder{
				Fields: map[string]*commonpb.Payload{
					"mymemo": payload.EncodeString("value"),
				},
			}.Build(),
			SearchAttributes: commonpb.SearchAttributes_builder{
				IndexedFields: map[string]*commonpb.Payload{
					"myfield": payload.EncodeString("value"),
				},
			}.Build(),
		}.Build(),
	}.Build()
}

func (s *workflowSuite) run(sched *schedulepb.Schedule, iterations int) {
	// test workflows will run until "completion", in our case that means until
	// continue-as-new. we only need a small number of iterations to test, though.
	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = iterations

	// fixed start time
	s.env.SetStartTime(baseStartTime)

	// fill this in so callers don't need to
	if !sched.HasAction() {
		sched.SetAction(s.defaultAction("myid"))
	}

	s.env.ExecuteWorkflow(SchedulerWorkflow, schedulespb.StartScheduleArgs_builder{
		Schedule: sched,
		State: schedulespb.InternalState_builder{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
		}.Build(),
	}.Build())
}

func (s *workflowSuite) describe() *schedulespb.DescribeResponse {
	encoded, err := s.env.QueryWorkflow(QueryNameDescribe)
	s.NoError(err)
	var resp schedulespb.DescribeResponse
	s.NoError(encoded.Get(&resp))
	return &resp
}

func (s *workflowSuite) runningWorkflows() []string {
	desc := s.describe()
	var out []string
	for _, ex := range desc.GetInfo().GetRunningWorkflows() {
		out = append(out, ex.GetWorkflowId())
	}
	return out
}

// Low-level mock helpers:

func (s *workflowSuite) expectStart(f func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error)) *testsuite.MockCallWrapper {
	return s.env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Once().Return(
		func(_ context.Context, req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			resp, err := f(req)
			if resp == nil && err == nil { // fill in defaults so callers can be more concise
				resp = schedulespb.StartWorkflowResponse_builder{
					RunId:         uuid.NewString(),
					RealStartTime: timestamppb.New(s.env.Now()),
				}.Build()
			}

			return resp, err
		})
}

func (s *workflowSuite) expectWatch(f func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error)) *testsuite.MockCallWrapper {
	return s.env.OnActivity(new(activities).WatchWorkflow, mock.Anything, mock.Anything).Once().Return(
		func(_ context.Context, req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
			return f(req)
		})
}

func (s *workflowSuite) expectCancel(f func(req *schedulespb.CancelWorkflowRequest) error) *testsuite.MockCallWrapper {
	return s.env.OnActivity(new(activities).CancelWorkflow, mock.Anything, mock.Anything).Once().Return(
		func(_ context.Context, req *schedulespb.CancelWorkflowRequest) error {
			return f(req)
		})
}

func (s *workflowSuite) expectTerminate(f func(req *schedulespb.TerminateWorkflowRequest) error) *testsuite.MockCallWrapper {
	return s.env.OnActivity(new(activities).TerminateWorkflow, mock.Anything, mock.Anything).Once().Return(
		func(_ context.Context, req *schedulespb.TerminateWorkflowRequest) error {
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
	// capture this to avoid races between end of one test and start of next
	env := s.env

	for _, run := range runs {
		run := run // capture fresh value
		// set up start
		matchStart := mock.MatchedBy(func(req *schedulespb.StartWorkflowRequest) bool {
			return req.GetRequest().GetWorkflowId() == run.id
		})
		s.env.OnActivity(new(activities).StartWorkflow, mock.Anything, matchStart).Times(0).Maybe().Return(
			func(_ context.Context, req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
				if prev, ok := state.started[req.GetRequest().GetWorkflowId()]; ok {
					s.Failf("multiple starts", "for %s at %s (prev %s)", req.GetRequest().GetWorkflowId(), s.now(), prev)
				}
				state.started[req.GetRequest().GetWorkflowId()] = s.now()
				overhead := time.Duration(100+rand.Intn(100)) * time.Millisecond
				return schedulespb.StartWorkflowResponse_builder{
					RunId:         uuid.NewString(),
					RealStartTime: timestamppb.New(s.now().Add(overhead)),
				}.Build(), nil
			})
		// set up short-poll watchers
		matchShortPoll := mock.MatchedBy(func(req *schedulespb.WatchWorkflowRequest) bool {
			return req.GetExecution().GetWorkflowId() == run.id && !req.GetLongPoll()
		})
		s.env.OnActivity(new(activities).WatchWorkflow, mock.Anything, matchShortPoll).Times(0).Maybe().Return(
			func(_ context.Context, req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
				if s.now().Before(run.end) {
					return schedulespb.WatchWorkflowResponse_builder{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}.Build(), nil
				}
				return schedulespb.WatchWorkflowResponse_builder{Status: run.result}.Build(), nil
			})
		// set up long-poll watchers
		matchLongPoll := mock.MatchedBy(func(req *schedulespb.WatchWorkflowRequest) bool {
			return req.GetExecution().GetWorkflowId() == run.id && req.GetLongPoll()
		})
		s.env.OnActivity(new(activities).WatchWorkflow, mock.Anything, matchLongPoll).Times(0).Maybe().AfterFn(func() time.Duration {
			// this can be called after end of workflow, use captured env
			return run.end.Sub(env.Now().UTC())
		}).Return(func(_ context.Context, req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
			return schedulespb.WatchWorkflowResponse_builder{Status: run.result}.Build(), nil
		})
	}
	// catch unexpected starts
	s.env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Times(0).Maybe().Return(
		func(_ context.Context, req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			s.Failf("unexpected start", "for %s at %s", req.GetRequest().GetWorkflowId(), s.now())
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
	sched *schedulepb.Schedule,
) {
	// fill this in so callers don't need to
	sched.SetAction(s.defaultAction("myid"))

	for _, every := range []int{1, 2, 3, 5, 7, 11, 1000} {
		s.T().Logf("running %s with continue-as-new every %d iterations", s.T().Name(), every)

		startTime := baseStartTime
		startArgs := schedulespb.StartScheduleArgs_builder{
			Schedule: sched,
			State: schedulespb.InternalState_builder{
				Namespace:     "myns",
				NamespaceId:   "mynsid",
				ScheduleId:    "myschedule",
				ConflictToken: InitialConflictToken,
			}.Build(),
		}.Build()
		CurrentTweakablePolicies.IterationsBeforeContinueAsNew = every
		state := runAcrossContinueState{
			started: make(map[string]time.Time),
		}
		for {
			s.env = s.NewTestWorkflowEnvironment()
			s.env.SetStartTime(startTime)

			s.setupMocksForWorkflows(runs, &state)
			s.setupDelayedCallbacks(startTime, cbs, &state)

			s.T().Logf("starting workflow with CAN every %d iterations, start time %s",
				CurrentTweakablePolicies.IterationsBeforeContinueAsNew, startTime)
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

	userMetadata := sdkpb.UserMetadata_builder{
		Summary: commonpb.Payload_builder{
			Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
			Data:     []byte(`Test summary Data`),
		}.Build(),
		Details: commonpb.Payload_builder{
			Metadata: map[string][]byte{"test_key": []byte(`test_val`)},
			Data:     []byte(`Test Details Data`),
		}.Build(),
	}.Build()
	action := s.defaultAction("myid")
	action.Action.(*schedulepb.ScheduleAction_StartWorkflow).StartWorkflow.SetUserMetadata(userMetadata)

	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Nil(req.GetRequest().GetLastCompletionResult())
		s.Nil(req.GetRequest().GetContinuedFailure())
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetRequest().GetWorkflowId())
		s.Equal("mywf", req.GetRequest().GetWorkflowType().GetName())
		s.Equal("mytq", req.GetRequest().GetTaskQueue().GetName())
		s.Equal(`"value"`, payload.ToString(req.GetRequest().GetMemo().GetFields()["mymemo"]))
		s.Equal(`"value"`, payload.ToString(req.GetRequest().GetSearchAttributes().GetIndexedFields()["myfield"]))
		s.Equal(`"myschedule"`, payload.ToString(req.GetRequest().GetSearchAttributes().GetIndexedFields()[sadefs.TemporalScheduledById]))
		s.Equal(`"2022-06-01T00:15:00Z"`, payload.ToString(req.GetRequest().GetSearchAttributes().GetIndexedFields()[sadefs.TemporalScheduledStartTime]))
		protoassert.ProtoEqual(s.T(), userMetadata, req.GetRequest().GetUserMetadata())

		return nil, nil
	})

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(55 * time.Minute),
			}.Build()},
		}.Build(),
		Action: action,
	}.Build(), 2)
	// two iterations to start one workflow: first will sleep, second will start and then sleep again
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestInitialPatch() {
	// written using low-level mocks so we can set initial patch

	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:00:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:00:00Z", req.GetExecution().GetWorkflowId())
		s.False(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}.Build(), nil
	})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})

	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 2
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, schedulespb.StartScheduleArgs_builder{
		Schedule: schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(55 * time.Minute),
				}.Build()},
			}.Build(),
			Action: s.defaultAction("myid"),
		}.Build(),
		State: schedulespb.InternalState_builder{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
		}.Build(),
		InitialPatch: schedulepb.SchedulePatch_builder{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		}.Build(),
	}.Build())
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCatchupWindow() {
	// written using low-level mocks so we can set initial state

	// one catchup
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-05-31T23:17:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 17, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-05-31T23:17:00Z", req.GetExecution().GetWorkflowId())
		s.False(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}.Build(), nil
	})
	// one on time
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 17, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:17:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})
	s.env.RegisterDelayedCallback(func() {
		s.Equal(int64(5), s.describe().GetInfo().GetMissedCatchupWindow())
	}, 18*time.Minute)

	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 2
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, schedulespb.StartScheduleArgs_builder{
		Schedule: schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Calendar: []*schedulepb.CalendarSpec{schedulepb.CalendarSpec_builder{
					Minute: "17",
					Hour:   "*",
				}.Build()},
			}.Build(),
			Action: s.defaultAction("myid"),
			Policies: schedulepb.SchedulePolicies_builder{
				CatchupWindow: durationpb.New(1 * time.Hour),
			}.Build(),
		}.Build(),
		State: schedulespb.InternalState_builder{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
			// workflow "woke up" after 6 hours
			LastProcessedTime: timestamppb.New(time.Date(2022, 5, 31, 18, 0, 0, 0, time.UTC)),
		}.Build(),
	}.Build())
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCatchupWindowWhilePaused() {
	// written using low-level mocks so we can set initial state

	s.env.RegisterDelayedCallback(func() {
		// should not count any "misses" since we were paused
		s.Equal(int64(0), s.describe().GetInfo().GetMissedCatchupWindow())
		// unpause just to make the test end cleanly
		s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{Unpause: "go ahead"}.Build())
	}, 3*time.Minute)
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 17, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:17:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})

	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 3
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, schedulespb.StartScheduleArgs_builder{
		Schedule: schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Calendar: []*schedulepb.CalendarSpec{schedulepb.CalendarSpec_builder{
					Minute: "17",
					Hour:   "*",
				}.Build()},
			}.Build(),
			Action: s.defaultAction("myid"),
			Policies: schedulepb.SchedulePolicies_builder{
				CatchupWindow: durationpb.New(1 * time.Hour),
			}.Build(),
			State: schedulepb.ScheduleState_builder{
				Paused: true,
			}.Build(),
		}.Build(),
		State: schedulespb.InternalState_builder{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
			// workflow "woke up" after 6 hours
			LastProcessedTime: timestamppb.New(time.Date(2022, 5, 31, 18, 0, 0, 0, time.UTC)),
		}.Build(),
	}.Build())
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
					s.Equal(int64(1), s.describe().GetInfo().GetOverlapSkipped())
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
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(5 * time.Minute),
				}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			}.Build(),
		}.Build(),
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
					s.Equal(int64(1), s.describe().GetInfo().GetBufferSize())
					s.Equal(int64(0), s.describe().GetInfo().GetOverlapSkipped())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 16, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(1), s.describe().GetInfo().GetBufferSize())
					s.Equal(int64(1), s.describe().GetInfo().GetOverlapSkipped())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 26, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(1), s.describe().GetInfo().GetBufferSize())
					s.Equal(int64(3), s.describe().GetInfo().GetOverlapSkipped())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 31, 0, 0, time.UTC),
				f:  func() { s.Equal([]string{"myid-2022-06-01T00:30:00Z"}, s.runningWorkflows()) },
			},
			{
				at: time.Date(2022, 6, 1, 0, 32, 0, 0, time.UTC),
				f: func() {
					s.Equal(int64(0), s.describe().GetInfo().GetBufferSize())
					s.Equal(int64(3), s.describe().GetInfo().GetOverlapSkipped())
				},
			},
			{
				at:         time.Date(2022, 6, 1, 0, 34, 59, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(5 * time.Minute),
				}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			}.Build(),
		}.Build(),
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
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(5 * time.Minute),
				}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			}.Build(),
		}.Build(),
	)
}

func (s *workflowSuite) TestBufferLimit() {
	originalMaxBufferSize := CurrentTweakablePolicies.MaxBufferSize
	CurrentTweakablePolicies.MaxBufferSize = 2
	defer func() { CurrentTweakablePolicies.MaxBufferSize = originalMaxBufferSize }()

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
					s.Equal(int64(2), s.describe().GetInfo().GetBufferSize())
					s.Equal(int64(1), s.describe().GetInfo().GetBufferDropped())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 23, 30, 0, time.UTC),
				f: func() {
					s.Equal([]string{"myid-2022-06-01T00:15:00Z"}, s.runningWorkflows())
					s.Equal(int64(0), s.describe().GetInfo().GetBufferSize())
					s.Equal(int64(1), s.describe().GetInfo().GetBufferDropped())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 25, 30, 0, time.UTC),
				f: func() {
					s.Equal([]string{"myid-2022-06-01T00:25:00Z"}, s.runningWorkflows())
					s.Equal(int64(0), s.describe().GetInfo().GetBufferSize())
					s.Equal(int64(1), s.describe().GetInfo().GetBufferDropped())
				},
			},
			{
				at:         time.Date(2022, 6, 1, 0, 29, 30, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(5 * time.Minute),
				}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			}.Build(),
		}.Build(),
	)
}

func (s *workflowSuite) TestOverlapCancel() {
	// written using low-level mocks so we can mock CancelWorkflow without adding support in
	// the framework

	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetExecution().GetWorkflowId())
		s.False(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}.Build(), nil
	})
	// will cancel and then long poll to wait for it
	s.expectCancel(func(req *schedulespb.CancelWorkflowRequest) error {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetExecution().GetWorkflowId())
		return nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 15, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetExecution().GetWorkflowId())
		s.True(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}.Build(), nil
	}).After(15 * time.Second)
	// now it'll run the next one
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 15, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T01:10:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(55 * time.Minute),
			}.Build()},
		}.Build(),
		Policies: schedulepb.SchedulePolicies_builder{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		}.Build(),
	}.Build(), 4)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestOverlapTerminate() {
	// written using low-level mocks so we can mock TerminateWorkflow without adding support in
	// the framework

	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetExecution().GetWorkflowId())
		s.False(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}.Build(), nil
	})
	// will terminate and then long poll to wait for it (could be improved since
	// we don't have to wait after terminate)
	s.expectTerminate(func(req *schedulespb.TerminateWorkflowRequest) error {
		s.True(time.Date(2022, 6, 1, 1, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetExecution().GetWorkflowId())
		return nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 1, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetExecution().GetWorkflowId())
		s.True(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}.Build(), nil
	}).After(1 * time.Second)
	// now it'll run the next one
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 10, 1, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T01:10:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(55 * time.Minute),
			}.Build()},
		}.Build(),
		Policies: schedulepb.SchedulePolicies_builder{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		}.Build(),
	}.Build(), 4)
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
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(5 * time.Minute),
				}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			}.Build(),
		}.Build(),
	)
}

func (s *workflowSuite) TestFailedStart() {
	s.T().Skip("the workflow test framework seems to have bugs around local activities that fail")

	// written using low-level mocks so we can fail a StartWorkflow

	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:05:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:05:00Z", req.GetExecution().GetWorkflowId())
		s.False(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}.Build(), nil
	})
	// failed start, but doesn't do anything else until next scheduled time
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:10:00Z", req.GetRequest().GetWorkflowId())
		return nil, errors.New("failed to start!")
	})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})
	// TODO: buffer two or more starts using backfill, then have the first start fail, and
	// check that the second start is attempted immediately after, without sleeping.

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(5 * time.Minute),
			}.Build()},
		}.Build(),
	}.Build(), 4)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestLastCompletionResultAndContinuedFailure() {
	// written using low-level mocks so we can return results/failures and check fields of
	// start workflow requests

	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:05:00Z", req.GetRequest().GetWorkflowId())
		s.Nil(req.GetRequest().GetLastCompletionResult())
		s.Nil(req.GetRequest().GetContinuedFailure())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:05:00Z", req.GetExecution().GetWorkflowId())
		s.False(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			Result: proto.ValueOrDefault(payloads.EncodeString("res1")),
		}.Build(), nil
	})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:10:00Z", req.GetRequest().GetWorkflowId())
		s.Equal(`["res1"]`, payloads.ToString(req.GetRequest().GetLastCompletionResult()))
		s.Nil(req.GetRequest().GetContinuedFailure())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:10:00Z", req.GetExecution().GetWorkflowId())
		s.False(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{
			Status:  enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			Failure: failurepb.Failure_builder{Message: "oops"}.Build(),
		}.Build(), nil
	})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetRequest().GetWorkflowId())
		s.Equal(`["res1"]`, payloads.ToString(req.GetRequest().GetLastCompletionResult()))
		s.Equal(`oops`, req.GetRequest().GetContinuedFailure().GetMessage())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetExecution().GetWorkflowId())
		s.False(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			Result: proto.ValueOrDefault(payloads.EncodeString("works again")),
		}.Build(), nil
	})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:20:00Z", req.GetRequest().GetWorkflowId())
		s.Equal(`["works again"]`, payloads.ToString(req.GetRequest().GetLastCompletionResult()))
		s.Nil(req.GetRequest().GetContinuedFailure())
		return nil, nil
	})

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(5 * time.Minute),
			}.Build()},
		}.Build(),
		Policies: schedulepb.SchedulePolicies_builder{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		}.Build(),
	}.Build(), 5)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestOnlyStartForAllowAll() {
	// written using low-level mocks so we can check fields of start workflow requests

	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:05:00Z", req.GetRequest().GetWorkflowId())
		s.Nil(req.GetRequest().GetLastCompletionResult())
		s.Nil(req.GetRequest().GetContinuedFailure())
		return nil, nil
	})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:10:00Z", req.GetRequest().GetWorkflowId())
		s.Nil(req.GetRequest().GetLastCompletionResult())
		s.Nil(req.GetRequest().GetContinuedFailure())
		return nil, nil
	})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetRequest().GetWorkflowId())
		s.Nil(req.GetRequest().GetLastCompletionResult())
		s.Nil(req.GetRequest().GetContinuedFailure())
		return nil, nil
	})

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(5 * time.Minute),
			}.Build()},
		}.Build(),
		Policies: schedulepb.SchedulePolicies_builder{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}.Build(),
	}.Build(), 4)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestPauseOnFailure() {
	// written using low-level mocks so we can return failures

	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 5, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:05:00Z", req.GetRequest().GetWorkflowId())
		s.Nil(req.GetRequest().GetLastCompletionResult())
		s.Nil(req.GetRequest().GetContinuedFailure())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:05:00Z", req.GetExecution().GetWorkflowId())
		s.False(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{
			Status:  enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			Failure: failurepb.Failure_builder{Message: "oops"}.Build(),
		}.Build(), nil
	})
	s.env.RegisterDelayedCallback(func() {
		s.False(s.describe().GetSchedule().GetState().GetPaused())
	}, 9*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		desc := s.describe()
		s.True(desc.GetSchedule().GetState().GetPaused())
		s.Contains(desc.GetSchedule().GetState().GetNotes(), "paused due to workflow failure")
		s.Contains(desc.GetSchedule().GetState().GetNotes(), "oops")
	}, 11*time.Minute)

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(5 * time.Minute),
			}.Build()},
		}.Build(),
		Policies: schedulepb.SchedulePolicies_builder{
			PauseOnFailure: true,
		}.Build(),
	}.Build(), 3)
	s.True(s.env.IsWorkflowCompleted())
	// doesn't end properly since it sleeps forever after pausing
}

func (s *workflowSuite) TestCompileError() {
	// written using low-level mocks since it sleeps forever

	s.env.RegisterDelayedCallback(func() {
		s.Contains(s.describe().GetInfo().GetInvalidScheduleError(), "Month is not in range [1-12]")
	}, 1*time.Minute)

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Calendar: []*schedulepb.CalendarSpec{schedulepb.CalendarSpec_builder{
				Month: "juneuary",
			}.Build()},
		}.Build(),
	}.Build(), 1)
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
					s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{
						TriggerImmediately: schedulepb.TriggerImmediatelyRequest_builder{
							ScheduledTime: timestamppb.New(time.Date(2022, 6, 1, 0, 20, 0, 0, time.UTC)),
						}.Build(),
					}.Build())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 25, 0, 0, time.UTC),
				f: func() {
					// Validate that the first trigger was skipped
					desc := s.describe()
					s.Equal(int64(1), desc.GetInfo().GetOverlapSkipped(), "First trigger should have been skipped due to overlap policy")
					s.Equal([]string{"myid-2022-06-01T00:15:00Z"}, s.runningWorkflows(), "Only the scheduled workflow should be running")
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC),
				f: func() {
					// this one runs with overridden overlap policy
					s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{
						TriggerImmediately: schedulepb.TriggerImmediatelyRequest_builder{
							ScheduledTime: timestamppb.New(time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC)),
							OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
						}.Build(),
					}.Build())
				},
			},
			{
				at:         time.Date(2022, 6, 1, 0, 54, 0, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(55 * time.Minute),
				}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			}.Build(),
		}.Build(),
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
					s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{
						BackfillRequest: []*schedulepb.BackfillRequest{schedulepb.BackfillRequest_builder{
							StartTime:     timestamppb.New(time.Date(2022, 5, 31, 0, 0, 0, 0, time.UTC)),
							EndTime:       timestamppb.New(time.Date(2022, 6, 1, 0, 0, 0, 0, time.UTC)),
							OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
						}.Build()},
					}.Build())
				},
			},
			{
				at:         time.Date(2022, 7, 31, 19, 6, 0, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Calendar: []*schedulepb.CalendarSpec{schedulepb.CalendarSpec_builder{
					Minute:     "*/17",
					Hour:       "19",
					DayOfMonth: "31",
				}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			}.Build(),
		}.Build(),
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
					triggerBackfill := schedulepb.BackfillRequest_builder{
						StartTime:     timestamppb.New(triggerBackfillTime),
						EndTime:       timestamppb.New(triggerBackfillTime),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
					}.Build()

					ignoreBackfillTime := triggerBackfillTime.Add(500 * time.Millisecond)
					ignoreBackfill := schedulepb.BackfillRequest_builder{
						StartTime:     timestamppb.New(ignoreBackfillTime),
						EndTime:       timestamppb.New(ignoreBackfillTime),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
					}.Build()

					s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{
						BackfillRequest: []*schedulepb.BackfillRequest{triggerBackfill, ignoreBackfill},
					}.Build())
				},
			},
			{
				at:         time.Date(2022, 7, 31, 19, 6, 0, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Calendar: []*schedulepb.CalendarSpec{schedulepb.CalendarSpec_builder{
					Minute:     "*/17",
					Hour:       "19",
					DayOfMonth: "31",
				}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			}.Build(),
		}.Build(),
	)
}

func (s *workflowSuite) TestHugeBackfillAllowAll() {
	prevTweakables := CurrentTweakablePolicies
	CurrentTweakablePolicies.MaxBufferSize = 30 // make smaller for testing
	defer func() { CurrentTweakablePolicies = prevTweakables }()

	// This has been run for up to 5000, but it takes a very long time. Run only 100 normally.
	const backfillRuns = 100
	const backfills = 4
	// The number that we process per iteration, with a 1s sleep per iteration, makes an
	// effective "rate limit" for processing allow-all backfills. This is different from the
	// explicit rate limit.
	rateLimit := CurrentTweakablePolicies.BackfillsPerIteration

	base := time.Date(2001, 8, 6, 0, 0, 0, 0, time.UTC)
	runs := make([]workflowRun, backfillRuns)
	for i := range runs {
		t := base.Add(time.Duration(i) * time.Hour)
		actual := baseStartTime.Add(time.Minute).Add(time.Duration(i/rateLimit) * time.Second)
		runs[i] = workflowRun{
			id:    "myid-" + t.Format(time.RFC3339),
			start: actual,
			// increase this number if this test fails for no reason:
			startTolerance: 8 * time.Second, // the "rate limit" isn't exact
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
				s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{
					BackfillRequest: []*schedulepb.BackfillRequest{schedulepb.BackfillRequest_builder{
						StartTime:     timestamppb.New(base.Add(time.Duration(i*backfillRuns/backfills) * time.Hour)),
						EndTime:       timestamppb.New(base.Add(time.Duration((i+1)*backfillRuns/backfills-1) * time.Hour)),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					}.Build()},
				}.Build())
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
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{Interval: durationpb.New(time.Hour)}.Build()},
			}.Build(),
		}.Build(),
	)
}

func (s *workflowSuite) TestHugeBackfillBuffer() {
	prevTweakables := CurrentTweakablePolicies
	CurrentTweakablePolicies.MaxBufferSize = 30 // make smaller for testing
	defer func() { CurrentTweakablePolicies = prevTweakables }()

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
				s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{
					BackfillRequest: []*schedulepb.BackfillRequest{schedulepb.BackfillRequest_builder{
						StartTime:     timestamppb.New(base.Add(time.Duration(i*backfillRuns/backfills) * time.Hour)),
						EndTime:       timestamppb.New(base.Add(time.Duration((i+1)*backfillRuns/backfills-1) * time.Hour)),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
					}.Build()},
				}.Build())
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
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{Interval: durationpb.New(time.Hour)}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			}.Build(),
		}.Build(),
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
					s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{
						Pause: "paused",
					}.Build())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 12, 7, 0, time.UTC),
				f: func() {
					desc := s.describe()
					s.True(desc.GetSchedule().GetState().GetPaused())
					s.Equal("paused", desc.GetSchedule().GetState().GetNotes())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 26, 7, 0, time.UTC),
				f: func() {
					s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{
						Unpause: "go ahead",
					}.Build())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 28, 7, 0, time.UTC),
				f: func() {
					desc := s.describe()
					s.False(desc.GetSchedule().GetState().GetPaused())
					s.Equal("go ahead", desc.GetSchedule().GetState().GetNotes())
				},
			},
			{
				at:         time.Date(2022, 6, 1, 0, 28, 8, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(3 * time.Minute),
				}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			}.Build(),
		}.Build(),
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
					s.env.SignalWorkflow(SignalNameUpdate, &schedulespb.FullUpdateRequest{})
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 9, 7, 0, time.UTC),
				f: func() {
					desc := s.describe()
					s.env.SignalWorkflow(SignalNameUpdate, schedulespb.FullUpdateRequest_builder{
						ConflictToken: desc.GetConflictToken(),
						Schedule: schedulepb.Schedule_builder{
							Spec: schedulepb.ScheduleSpec_builder{
								Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
									Interval: durationpb.New(5 * time.Minute),
								}.Build()},
							}.Build(),
							Policies: schedulepb.SchedulePolicies_builder{
								OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
							}.Build(),
							Action: s.defaultAction("newid"),
						}.Build(),
						SearchAttributes: commonpb.SearchAttributes_builder{
							IndexedFields: map[string]*commonpb.Payload{
								"myfield": payload.EncodeString("another value"),
							},
						}.Build(),
					}.Build())
				},
			},
			{
				at: time.Date(2022, 6, 1, 0, 12, 7, 0, time.UTC),
				f: func() {
					desc := s.describe()
					s.env.SignalWorkflow(SignalNameUpdate, schedulespb.FullUpdateRequest_builder{
						ConflictToken: desc.GetConflictToken() + 37, // conflict, should not take effect
						Schedule:      &schedulepb.Schedule{},
					}.Build())
				},
			},
			{
				at:         time.Date(2022, 6, 1, 0, 19, 30, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(3 * time.Minute),
				}.Build()},
			}.Build(),
			Policies: schedulepb.SchedulePolicies_builder{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			}.Build(),
		}.Build(),
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
					s.env.SignalWorkflow(SignalNameUpdate, schedulespb.FullUpdateRequest_builder{
						Schedule: schedulepb.Schedule_builder{
							Spec: schedulepb.ScheduleSpec_builder{
								Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
									Interval: durationpb.New(20 * time.Second),
								}.Build()},
							}.Build(),
							Action: s.defaultAction("newid"),
						}.Build(),
					}.Build())
				},
			},
			// After the update above modifies the schedule, we should discard any newly
			// scheduled times that are scheduled prior to the update time.
			{
				at: time.Date(2022, 6, 1, 1, 7, 12, 0, time.UTC),
				f: func() {
					desc := s.describe()
					times := desc.GetInfo().GetFutureActionTimes()
					s.True(times[0].AsTime().After(desc.GetInfo().GetUpdateTime().AsTime()), "getFutureActionTimes returned an action preceding the update time after a schedule change")
				},
			},
			{
				at:         time.Date(2022, 6, 1, 1, 7, 55, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(1 * time.Hour),
				}.Build()},
			}.Build(),
		}.Build(),
	)
}

// Tests that an update between a nominal time and jittered time for a start, that doesn't
// modify that start, will still start it.
func (s *workflowSuite) TestUpdateBetweenNominalAndJitter() {
	spec := schedulepb.ScheduleSpec_builder{
		Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
			Interval: durationpb.New(1 * time.Hour),
		}.Build()},
		Jitter: durationpb.New(1 * time.Hour),
	}.Build()
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
					s.env.SignalWorkflow(SignalNameUpdate, schedulespb.FullUpdateRequest_builder{
						Schedule: schedulepb.Schedule_builder{
							Spec:   spec,
							Action: s.defaultAction("newid"),
						}.Build(),
					}.Build())
				},
			},
			{
				at:         time.Date(2022, 6, 1, 5, 0, 0, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: spec,
		}.Build(),
	)
}

// Tests that a signal between a nominal time and jittered time for a start won't interrupt the start.
func (s *workflowSuite) TestSignalBetweenNominalAndJittered() {
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
				id:     "myid-2022-06-01T03:00:00Z",
				start:  time.Date(2022, 6, 1, 3, 37, 29, 538000000, time.UTC),
				end:    time.Date(2022, 6, 1, 3, 41, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				id:     "myid-2022-06-01T04:00:00Z",
				start:  time.Date(2022, 6, 1, 4, 23, 34, 755000000, time.UTC),
				end:    time.Date(2022, 6, 1, 4, 27, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
		[]delayedCallback{
			{
				// signal between nominal and jittered time
				at: time.Date(2022, 6, 1, 3, 22, 10, 0, time.UTC),
				f: func() {
					s.env.SignalWorkflow(SignalNameRefresh, nil)
				},
			},
			{
				at:         time.Date(2022, 6, 1, 5, 0, 0, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(1 * time.Hour),
				}.Build()},
				Jitter: durationpb.New(1 * time.Hour),
			}.Build(),
		}.Build(),
	)
}

func (s *workflowSuite) TestPauseUnpauseBetweenNominalAndJittered() {
	s.T().Skip("this illustrates an existing bug")

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
				id:     "myid-2022-06-01T03:00:00Z",
				start:  time.Date(2022, 6, 1, 3, 37, 29, 538000000, time.UTC),
				end:    time.Date(2022, 6, 1, 3, 41, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
			{
				id:     "myid-2022-06-01T04:00:00Z",
				start:  time.Date(2022, 6, 1, 4, 23, 34, 755000000, time.UTC),
				end:    time.Date(2022, 6, 1, 4, 27, 0, 0, time.UTC),
				result: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			},
		},
		[]delayedCallback{
			{
				at: time.Date(2022, 6, 1, 3, 20, 0, 0, time.UTC),
				f: func() {
					s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{Pause: "paused"}.Build())
				},
			},
			{
				at: time.Date(2022, 6, 1, 3, 30, 0, 0, time.UTC),
				f: func() {
					s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{Unpause: "go ahead"}.Build())
				},
			},
			{
				at:         time.Date(2022, 6, 1, 5, 0, 0, 0, time.UTC),
				finishTest: true,
			},
		},
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(1 * time.Hour),
				}.Build()},
				Jitter: durationpb.New(1 * time.Hour),
			}.Build(),
		}.Build(),
	)
}

func (s *workflowSuite) TestLimitedActions() {
	// written using low-level mocks so we can sleep forever

	// limited to 2
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 3, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:03:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 6, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:03:00Z", req.GetExecution().GetWorkflowId())
		return schedulespb.WatchWorkflowResponse_builder{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}.Build(), nil
	})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 6, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:06:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})
	// does not watch again at :09, but does at :10
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 10, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:06:00Z", req.GetExecution().GetWorkflowId())
		return schedulespb.WatchWorkflowResponse_builder{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}.Build(), nil
	})

	s.env.RegisterDelayedCallback(func() {
		desc := s.describe()
		s.Equal(int64(2), desc.GetSchedule().GetState().GetRemainingActions())
		s.Equal(2, len(desc.GetInfo().GetFutureActionTimes()))
	}, 1*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		desc := s.describe()
		s.Equal(int64(1), desc.GetSchedule().GetState().GetRemainingActions())
		s.Equal(1, len(desc.GetInfo().GetFutureActionTimes()))
	}, 5*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		desc := s.describe()
		s.Equal(int64(0), desc.GetSchedule().GetState().GetRemainingActions())
		s.Equal(0, len(desc.GetInfo().GetFutureActionTimes()))
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

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(3 * time.Minute),
			}.Build()},
		}.Build(),
		State: schedulepb.ScheduleState_builder{
			LimitedActions:   true,
			RemainingActions: 2,
		}.Build(),
		Policies: schedulepb.SchedulePolicies_builder{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		}.Build(),
	}.Build(), 4)
	s.True(s.env.IsWorkflowCompleted())
	// doesn't end properly since it sleeps forever after pausing
}

func (s *workflowSuite) TestLotsOfIterations() {
	// This is mostly testing GetNextTime caching logic.
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
				s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{
					BackfillRequest: []*schedulepb.BackfillRequest{schedulepb.BackfillRequest_builder{
						StartTime:     timestamppb.New(callBackRangeStartTime),
						EndTime:       timestamppb.New(callBackRangeStartTime.Add(time.Duration(maxRuns) * time.Hour)),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
					}.Build()},
				}.Build())
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
		schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Calendar: []*schedulepb.CalendarSpec{
					schedulepb.CalendarSpec_builder{Minute: "27", Hour: "0/2"}.Build(),
					schedulepb.CalendarSpec_builder{Minute: "28", Hour: "1/2"}.Build(),
				},
			}.Build(),
		}.Build(),
	)
}

func (s *workflowSuite) TestExitScheduleWorkflowWhenNoActions() {
	scheduleId := "myschedule"
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.GetExecution().GetWorkflowId())
		s.False(req.GetLongPoll())
		return schedulespb.WatchWorkflowResponse_builder{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}.Build(), nil
	})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:30:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})

	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 5
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, schedulespb.StartScheduleArgs_builder{
		Schedule: schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
					Interval: durationpb.New(15 * time.Minute),
				}.Build()},
			}.Build(),
			State: schedulepb.ScheduleState_builder{
				LimitedActions:   true,
				RemainingActions: 2,
			}.Build(),
			Action: s.defaultAction("myid"),
		}.Build(),
		State: schedulespb.InternalState_builder{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    scheduleId,
			ConflictToken: InitialConflictToken,
		}.Build(),
	}.Build())
	s.True(s.env.IsWorkflowCompleted())
	s.False(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
	s.True(s.env.Now().Sub(time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC)) == CurrentTweakablePolicies.RetentionTime)
}

func (s *workflowSuite) TestExitScheduleWorkflowWhenNoNextTime() {
	scheduleId := "myschedule"
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 1, 0, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T01:00:00Z", req.GetRequest().GetWorkflowId())
		return nil, nil
	})

	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 3
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, schedulespb.StartScheduleArgs_builder{
		Schedule: schedulepb.Schedule_builder{
			Spec: schedulepb.ScheduleSpec_builder{
				Calendar: []*schedulepb.CalendarSpec{schedulepb.CalendarSpec_builder{
					Year:       "2022",
					Month:      "June",
					DayOfMonth: "1",
					Hour:       "1",
					Minute:     "0",
					Second:     "0",
				}.Build()},
			}.Build(),
			Action: s.defaultAction("myid"),
		}.Build(),
		State: schedulespb.InternalState_builder{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    scheduleId,
			ConflictToken: InitialConflictToken,
		}.Build(),
	}.Build())
	s.True(s.env.IsWorkflowCompleted())
	s.False(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
	s.True(s.env.Now().Sub(time.Date(2022, 6, 1, 1, 0, 0, 0, time.UTC)) == CurrentTweakablePolicies.RetentionTime)
}

func (s *workflowSuite) TestExitScheduleWorkflowWhenEmpty() {
	scheduleId := "myschedule"

	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 3
	s.env.SetStartTime(baseStartTime)
	s.env.ExecuteWorkflow(SchedulerWorkflow, schedulespb.StartScheduleArgs_builder{
		Schedule: schedulepb.Schedule_builder{
			Action: s.defaultAction("myid"),
		}.Build(),
		State: schedulespb.InternalState_builder{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    scheduleId,
			ConflictToken: InitialConflictToken,
		}.Build(),
	}.Build())

	s.True(s.env.IsWorkflowCompleted())
	s.False(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
	s.True(s.env.Now().Sub(baseStartTime) == CurrentTweakablePolicies.RetentionTime)
}

func (s *workflowSuite) TestCANByIterations() {
	// written using low-level mocks so we can control iteration count

	const iters = 30
	// note: one fewer run than iters since the first doesn't start anything
	for i := 1; i < iters; i++ {
		t := baseStartTime.Add(5 * time.Minute * time.Duration(i))
		s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			s.Equal("myid-"+t.Format(time.RFC3339), req.GetRequest().GetWorkflowId())
			return nil, nil
		})
	}
	// this one catches and fails if we go over
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Fail("too many starts")
		return nil, nil
	}).Times(0).Maybe()

	// this is ignored because we set iters explicitly
	s.env.RegisterDelayedCallback(func() {
		s.env.SetContinueAsNewSuggested(true)
	}, 5*time.Minute*iters/2-time.Second)

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(5 * time.Minute),
			}.Build()},
		}.Build(),
		Policies: schedulepb.SchedulePolicies_builder{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}.Build(),
	}.Build(), iters)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCANBySuggested() {
	// written using low-level mocks so we can control iteration count

	const iters = 30
	// note: one fewer run than iters since the first doesn't start anything
	for i := 1; i < iters; i++ {
		t := baseStartTime.Add(5 * time.Minute * time.Duration(i))
		s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			s.Equal("myid-"+t.Format(time.RFC3339), req.GetRequest().GetWorkflowId())
			return nil, nil
		})
	}
	// this one catches and fails if we go over
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Fail("too many starts", req.GetRequest().GetWorkflowId())
		return nil, nil
	}).Times(0).Maybe()

	s.env.RegisterDelayedCallback(func() {
		s.env.SetContinueAsNewSuggested(true)
	}, 5*time.Minute*iters-time.Second)

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(5 * time.Minute),
			}.Build()},
		}.Build(),
		Policies: schedulepb.SchedulePolicies_builder{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}.Build(),
	}.Build(), 0) // 0 means use suggested
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCANBySuggestedWithSignals() {
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
		s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			s.Equal("myid-"+t.Format(time.RFC3339), req.GetRequest().GetWorkflowId())
			return nil, nil
		})
		if d > suggestCANAt {
			// the first one after the CAN flag is flipped will run, further ones will not
			break
		}
	}
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Fail("too many starts", req.GetRequest().GetWorkflowId())
		return nil, nil
	}).Times(0).Maybe()

	s.env.RegisterDelayedCallback(func() {
		s.env.SetContinueAsNewSuggested(true)
	}, suggestCANAt)

	for _, d := range runs {
		s.env.RegisterDelayedCallback(func() {
			s.env.SignalWorkflow(SignalNamePatch, schedulepb.SchedulePatch_builder{
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
			}.Build())
		}, d)
	}

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(100 * time.Minute),
			}.Build()},
		}.Build(),
		Policies: schedulepb.SchedulePolicies_builder{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}.Build(),
		State: schedulepb.ScheduleState_builder{
			Paused: true,
		}.Build(),
	}.Build(), 0) // 0 means use suggested
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

func (s *workflowSuite) TestCANBySignal() {
	// written using low-level mocks so we can control iteration count

	const iters = 30
	// note: one fewer run than iters since the first doesn't start anything
	for i := 1; i < iters; i++ {
		t := baseStartTime.Add(5 * time.Minute * time.Duration(i))
		s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			s.Equal("myid-"+t.Format(time.RFC3339), req.GetRequest().GetWorkflowId())
			return nil, nil
		})
	}
	// this one catches and fails if we go over
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Fail("too many starts", req.GetRequest().GetWorkflowId())
		return nil, nil
	}).Times(0).Maybe()

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(SignalNameForceCAN, nil)
	}, 5*time.Minute*iters-time.Second)

	s.run(schedulepb.Schedule_builder{
		Spec: schedulepb.ScheduleSpec_builder{
			Interval: []*schedulepb.IntervalSpec{schedulepb.IntervalSpec_builder{
				Interval: durationpb.New(5 * time.Minute),
			}.Build()},
		}.Build(),
		Policies: schedulepb.SchedulePolicies_builder{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}.Build(),
	}.Build(), 0) // 0 means use suggested
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}
