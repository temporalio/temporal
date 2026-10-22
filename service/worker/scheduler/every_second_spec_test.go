package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/payloads"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Spec and policies decoded from a production V1 schedule (StartScheduleArgs). The user
// emulates "fixed delay" by firing every second with SKIP; each run ends with a 2m sleep.
const everySecondSpecJSON = `{
  "structuredCalendar": [
    {
      "second": [{"start": 59, "end": 59}],
      "minute": [{"start": 59, "end": 59}],
      "hour": [{"start": 23, "end": 23}],
      "dayOfMonth": [{"start": 31, "end": 31}],
      "month": [{"start": 12, "end": 12}],
      "year": [{"start": 2100, "end": 2100}],
      "dayOfWeek": [{"end": 6, "step": 1}],
      "comment": "For ensuring that the schedule will never be automatically deleted"
    },
    {
      "second": [{"end": 59}],
      "minute": [{"end": 59}],
      "hour": [{"end": 23}],
      "dayOfMonth": [{"start": 1, "end": 31}],
      "month": [{"start": 1, "end": 12}],
      "dayOfWeek": [{"end": 6}]
    }
  ],
  "startTime": "1970-01-01T00:00:00Z",
  "endTime": "2124-11-03T06:25:23.869Z",
  "timezoneName": "Etc/GMT"
}`

const everySecondRunDuration = 2 * time.Minute

func everySecondSchedule(t *testing.T) *schedulepb.Schedule {
	var spec schedulepb.ScheduleSpec
	require.NoError(t, protojson.Unmarshal([]byte(everySecondSpecJSON), &spec))
	return &schedulepb.Schedule{
		Spec: &spec,
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			CatchupWindow: durationpb.New(365 * 24 * time.Hour),
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "myid",
					WorkflowType: &commonpb.WorkflowType{Name: "mywf"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "mytq"},
				},
			},
		},
	}
}

type everySecondCounters struct {
	starts      int
	shortPolls  int
	runEndTimes map[string]time.Time
}

// mockEverySecondActivities simulates runs that each take everySecondRunDuration.
func mockEverySecondActivities(env *testsuite.TestWorkflowEnvironment, c *everySecondCounters) {
	env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Maybe().Return(
		func(_ context.Context, req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			c.starts++
			c.runEndTimes[req.Request.WorkflowId] = env.Now().Add(everySecondRunDuration)
			return &schedulespb.StartWorkflowResponse{
				RunId:         uuid.NewString(),
				RealStartTime: timestamppb.New(env.Now()),
			}, nil
		})
	isLongPoll := func(long bool) any {
		return mock.MatchedBy(func(req *schedulespb.WatchWorkflowRequest) bool { return req.LongPoll == long })
	}
	env.OnActivity(new(activities).WatchWorkflow, mock.Anything, isLongPoll(false)).Maybe().Return(
		func(_ context.Context, req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
			c.shortPolls++
			if end, ok := c.runEndTimes[req.Execution.WorkflowId]; ok && env.Now().Before(end) {
				return &schedulespb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
			}
			return &schedulespb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
		})
	env.OnActivity(new(activities).WatchWorkflow, mock.Anything, isLongPoll(true)).Maybe().AfterFn(func() time.Duration {
		// Only one run is ever in flight under SKIP, so the latest end is the watched run's end.
		var latest time.Time
		for _, end := range c.runEndTimes {
			if end.After(latest) {
				latest = end
			}
		}
		return latest.Sub(env.Now())
	}).Return(&schedulespb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil)
}

func runEverySecondUntilCAN(
	t *testing.T,
	iterations int,
	args *schedulespb.StartScheduleArgs,
	c *everySecondCounters,
) (*schedulespb.StartScheduleArgs, time.Duration) {
	prevIters := CurrentTweakablePolicies.IterationsBeforeContinueAsNew
	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = iterations
	t.Cleanup(func() { CurrentTweakablePolicies.IterationsBeforeContinueAsNew = prevIters })

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(baseStartTime)
	mockEverySecondActivities(env, c)

	env.ExecuteWorkflow(SchedulerWorkflow, args)

	require.True(t, env.IsWorkflowCompleted())
	var canErr *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &canErr)
	var next *schedulespb.StartScheduleArgs
	require.NoError(t, payloads.Decode(canErr.Input, &next))
	return next, env.Now().Sub(baseStartTime)
}

func newEverySecondArgs(t *testing.T) *schedulespb.StartScheduleArgs {
	return &schedulespb.StartScheduleArgs{
		Schedule: everySecondSchedule(t),
		State: &schedulespb.InternalState{
			Namespace:     "myns",
			NamespaceId:   "mynsid",
			ScheduleId:    "myschedule",
			ConflictToken: InitialConflictToken,
		},
	}
}

// The scheduler wakes once per second for the whole lifetime of each run, doing a
// short-poll refresh local activity each time, only to SKIP the start.
func TestEverySecondSpec_SteadyStateWakesEverySecond(t *testing.T) {
	const iterations = 600
	c := &everySecondCounters{runEndTimes: map[string]time.Time{}}

	next, elapsed := runEverySecondUntilCAN(t, iterations, newEverySecondArgs(t), c)

	t.Logf("iterations=%d elapsed=%s starts=%d shortPolls=%d overlapSkipped=%d",
		iterations, elapsed, c.starts, c.shortPolls, next.Info.OverlapSkipped)

	// ~1 workflow task per second of wall time.
	require.LessOrEqual(t, elapsed, time.Duration(iterations)*time.Second+everySecondRunDuration)
	// Each run is ~2m, so useful starts are ~elapsed/2m; everything else is skipped.
	require.InDelta(t, elapsed/everySecondRunDuration, c.starts, 2)
	require.Greater(t, next.Info.OverlapSkipped, int64(iterations*9/10))
	// Every wake buffers a start, which forces a refresh local activity.
	require.Greater(t, c.shortPolls, iterations*9/10)
}

// After the scheduler is stalled (e.g. worker outage), the first iteration walks every
// second of the gap in a single workflow task and overflows the buffer.
func TestEverySecondSpec_CatchupAfterStall(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stall time.Duration
	}{
		{name: "10m", stall: 10 * time.Minute},
		{name: "2h", stall: 2 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &everySecondCounters{runEndTimes: map[string]time.Time{}}
			args := newStalledEverySecondArgs(t, c, tc.stall)

			next, _ := runEverySecondUntilCAN(t, 1, args, c)

			fires := int64(tc.stall / time.Second)
			maxBuf := int64(CurrentTweakablePolicies.MaxBufferSize)
			t.Logf("stall=%s fires=%d bufferDropped=%d overlapSkipped=%d",
				tc.stall, fires, next.Info.BufferDropped, next.Info.OverlapSkipped)

			require.Equal(t, int64(0), int64(c.starts))
			require.Equal(t, max(fires-maxBuf, 0), next.Info.BufferDropped)
			require.Equal(t, min(fires, maxBuf), next.Info.OverlapSkipped)
		})
	}
}

// newStalledEverySecondArgs builds continue-as-new input whose first workflow task runs
// `stall` after LastProcessedTime, with a run still in flight (as in production).
func newStalledEverySecondArgs(t *testing.T, c *everySecondCounters, stall time.Duration) *schedulespb.StartScheduleArgs {
	args := newEverySecondArgs(t)
	args.State.LastProcessedTime = timestamppb.New(baseStartTime.Add(-stall))
	// Something is still running so every caught-up start is buffered then skipped.
	const runningID = "myid-running"
	c.runEndTimes[runningID] = baseStartTime.Add(365 * 24 * time.Hour)
	args.Info = &schedulepb.ScheduleInfo{
		RunningWorkflows: []*commonpb.WorkflowExecution{{WorkflowId: runningID, RunId: "r"}},
	}
	return args
}

// Reproduces the production death spiral: when the first workflow task after
// continue-as-new is picked up long after LastProcessedTime, processTimeRange walks every
// second of the gap in that one task and trips the SDK deadlock detector. A failed task
// persists nothing, so each retry starts from the same LastProcessedTime with a larger gap.
// A pause arriving in the same task can't help: signals/patches are applied after
// processTimeRange.
//
// Production gap was ~102h (~1.5s locally, too close to the 1s detector to be reliable),
// so the deadlock cases use 30d for margin.
func TestEverySecondSpec_LongStallDeadlocks(t *testing.T) {
	const deadlockGap = 30 * 24 * time.Hour
	for _, tc := range []struct {
		name         string
		stall        time.Duration
		mutate       func(*schedulespb.StartScheduleArgs)
		wantDeadlock bool
	}{
		// {
		// 	name:  "short stall completes",
		// 	stall: time.Hour,
		// },
		{
			name:         "long stall deadlocks",
			stall:        deadlockGap,
			wantDeadlock: true,
		},
		// {
		// 	name:  "pause in same task does not help",
		// 	stall: deadlockGap,
		// 	mutate: func(a *schedulespb.StartScheduleArgs) {
		// 		a.InitialPatch = &schedulepb.SchedulePatch{Pause: "rescue"}
		// 	},
		// 	wantDeadlock: true,
		// },
		// {
		// 	// Control: the range walk is the culprit; already-paused schedules skip it.
		// 	name:  "already paused completes",
		// 	stall: deadlockGap,
		// 	mutate: func(a *schedulespb.StartScheduleArgs) {
		// 		a.Schedule.State = &schedulepb.ScheduleState{Paused: true}
		// 	},
		// },
	} {
		t.Run(tc.name, func(t *testing.T) {
			prevIters := CurrentTweakablePolicies.IterationsBeforeContinueAsNew
			CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 1
			t.Cleanup(func() { CurrentTweakablePolicies.IterationsBeforeContinueAsNew = prevIters })

			c := &everySecondCounters{runEndTimes: map[string]time.Time{}}
			args := newStalledEverySecondArgs(t, c, tc.stall)
			if tc.mutate != nil {
				tc.mutate(args)
			}

			var ts testsuite.WorkflowTestSuite
			env := ts.NewTestWorkflowEnvironment()
			env.SetStartTime(baseStartTime)
			mockEverySecondActivities(env, c)
			// Paused schedules sleep without a timer; nudge them to continue-as-new.
			env.RegisterDelayedCallback(func() { env.SignalWorkflow(SignalNameForceCAN, nil) }, time.Minute)

			env.ExecuteWorkflow(SchedulerWorkflow, args)

			require.True(t, env.IsWorkflowCompleted())
			err := env.GetWorkflowError()
			fmt.Println("/>>", err)
			t.Fail()
			if tc.wantDeadlock {
				require.ErrorContains(t, err, "TMPRL1101")
				return
			}
			var canErr *workflow.ContinueAsNewError
			require.ErrorAs(t, err, &canErr)
		})
	}
}
