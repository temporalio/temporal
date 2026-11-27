package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProcessBufferTask_Validate(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	handler := newProcessBufferHandler(env)
	invoker := env.Scheduler.Invoker.Get(env.MutableContext())

	cases := []struct {
		name              string
		lastProcessedTime *timestamppb.Timestamp
		scheduledTime     time.Time
		expectedValid     bool
	}{
		{name: "immediate always valid", lastProcessedTime: timestamppb.New(now), scheduledTime: time.Time{}, expectedValid: true},
		{name: "nil LPT always valid", lastProcessedTime: nil, scheduledTime: now, expectedValid: true},
		{name: "scheduled after LPT is valid", lastProcessedTime: timestamppb.New(now), scheduledTime: now.Add(time.Second), expectedValid: true},
		{name: "scheduled equal to LPT is stale", lastProcessedTime: timestamppb.New(now), scheduledTime: now, expectedValid: false},
		{name: "scheduled before LPT is stale", lastProcessedTime: timestamppb.New(now.Add(time.Second)), scheduledTime: now, expectedValid: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			invoker.LastProcessedTime = c.lastProcessedTime
			valid, err := handler.Validate(env.MutableContext(), invoker, chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: c.scheduledTime}}, &schedulerpb.InvokerProcessBufferTask{})
			require.NoError(t, err)
			require.Equal(t, c.expectedValid, valid)
		})
	}
}

func TestProcessBufferTask_Validate_MigrationPending(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	env.Scheduler.WorkflowMigration = &schedulerpb.WorkflowMigrationState{}

	valid, err := newProcessBufferHandler(env).Validate(ctx, invoker, chasm.TaskInvocation{}, &schedulerpb.InvokerProcessBufferTask{})
	require.NoError(t, err)
	require.False(t, valid)
}

func TestInvoker_AddTasks_DeferredBehindRunningStartEmitsNothing(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	now := env.TimeSource.Now()
	invoker.LastProcessedTime = timestamppb.New(now)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId:   "running",
		WorkflowId:  "wf-running",
		RunId:       "running-run",
		Attempt:     1,
		ActualTime:  timestamppb.New(now),
		StartTime:   timestamppb.New(now),
		HasCallback: true,
	}, {
		RequestId:  "deferred",
		WorkflowId: "wf-deferred",
		Attempt:    -1,
		ActualTime: timestamppb.New(now),
	}}

	env.NodeBackend.TasksByCategory = nil
	invoker.EnqueueBufferedStarts(ctx, nil)
	require.NoError(t, env.CloseTransaction())

	require.False(t, env.HasTaskInCategory(&tasks.ChasmTask{}, tasks.CategoryTransfer, chasm.TaskScheduledTimeImmediate),
		"deferred work behind a running workflow must not emit an immediate workflow-start (execute) task")
	require.False(t, env.HasTask(&tasks.ChasmTaskPure{}, chasm.TaskScheduledTimeImmediate),
		"deferred work behind a running workflow must not emit an immediate ProcessBufferTask")
}

// Regression: addTasks skipped re-arm when processBuffer found no Attempt==0 starts, stranding elapsed retries.
func TestProcessBufferTask_RearmsBackedOffRetry(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	now := env.TimeSource.Now()
	// LPT in the past makes the start eligible only after this run's HWM advance.
	invoker.LastProcessedTime = timestamppb.New(now.Add(-time.Minute))
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		NominalTime:   timestamppb.New(now),
		ActualTime:    timestamppb.New(now),
		DesiredTime:   timestamppb.New(now),
		RequestId:     "retry-ready",
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		Attempt:       2,
		BackoffTime:   timestamppb.New(now),
	}}

	err := newProcessBufferHandler(env).Execute(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	require.False(t, invoker.BufferedStarts[0].BackoffTime.AsTime().After(invoker.LastProcessedTime.AsTime()),
		"setup invariant: start must be eligible after HWM advance")

	require.True(t, env.HasTask(&tasks.ChasmTask{}, chasm.TaskScheduledTimeImmediate),
		"expected an immediate side-effect task to re-arm the backed-off retry")
}

func newProcessBufferHandler(env *testEnv) *scheduler.InvokerProcessBufferTaskHandler {
	return scheduler.NewInvokerProcessBufferTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
	})
}

type processBufferTestCase struct {
	InitialBufferedStarts     []*schedulespb.BufferedStart
	InitialCancelWorkflows    []*commonpb.WorkflowExecution
	InitialTerminateWorkflows []*commonpb.WorkflowExecution
	InitialRunningWorkflows   []*commonpb.WorkflowExecution

	ExpectedBufferedStarts      int
	ExpectedRunningWorkflows    int
	ExpectedTerminateWorkflows  int
	ExpectedCancelWorkflows     int
	ExpectedOverlapSkipped      int64
	ExpectedMissedCatchupWindow int64

	ValidateInvoker func(t *testing.T, invoker *scheduler.Invoker)
}

func runProcessBufferTestCase(t *testing.T, env *testEnv, c *processBufferTestCase) {
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	invoker.BufferedStarts = c.InitialBufferedStarts
	invoker.CancelWorkflows = c.InitialCancelWorkflows
	invoker.TerminateWorkflows = c.InitialTerminateWorkflows

	for _, wf := range c.InitialRunningWorkflows {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
			RequestId:  wf.WorkflowId + "-req",
			WorkflowId: wf.WorkflowId,
			RunId:      wf.RunId,
			Attempt:    1,
		})
	}

	invoker.LastProcessedTime = timestamppb.New(env.TimeSource.Now())

	handler := newProcessBufferHandler(env)
	err := handler.Execute(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	require.Len(t, invoker.GetBufferedStarts(), c.ExpectedBufferedStarts+len(c.InitialRunningWorkflows))

	runningCount := 0
	for _, start := range invoker.GetBufferedStarts() {
		if start.GetRunId() != "" && start.GetCompleted() == nil {
			runningCount++
		}
	}
	require.Equal(t, c.ExpectedRunningWorkflows, runningCount)

	require.Len(t, invoker.TerminateWorkflows, c.ExpectedTerminateWorkflows)
	require.Len(t, invoker.CancelWorkflows, c.ExpectedCancelWorkflows)
	require.Equal(t, c.ExpectedOverlapSkipped, env.Scheduler.Info.OverlapSkipped)
	require.Equal(t, c.ExpectedMissedCatchupWindow, env.Scheduler.Info.MissedCatchupWindow)

	if c.ValidateInvoker != nil {
		c.ValidateInvoker(t, invoker)
	}
}

func TestProcessBufferTask_AllowAll(t *testing.T) {
	env := newTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req3",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:  bufferedStarts,
		ExpectedBufferedStarts: 3,
		ExpectedOverlapSkipped: 0,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			require.Len(t, util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
				return start.Attempt > 0
			}), 3)
		},
	})
}

func TestProcessBufferTask_MissedCatchupWindow(t *testing.T) {
	env := newTestEnv(t)
	now := env.TimeSource.Now()
	startTime := now.Add(-defaultCatchupWindow * 2)
	startTimestamp := timestamppb.New(startTime)
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTimestamp,
			ActualTime:    startTimestamp,
			DesiredTime:   startTimestamp,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:       bufferedStarts,
		ExpectedBufferedStarts:      0,
		ExpectedOverlapSkipped:      0,
		ExpectedMissedCatchupWindow: 1,
	})
}

func TestProcessBufferTask_BufferOne(t *testing.T) {
	env := newTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req3",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: bufferedStarts,
		// No workflows are running, so one start is readied and the next stays buffered.
		ExpectedBufferedStarts: 2,
		ExpectedOverlapSkipped: 1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			// Attempt > 0 marks a start ready for execution.
			require.Len(t, util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
				return start.Attempt > 0
			}), 1)
		},
	})
}

func TestProcessBufferTask_BufferOneKeepsExistingDeferredStart(t *testing.T) {
	env := newTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{
			{
				NominalTime:   startTime,
				ActualTime:    startTime,
				DesiredTime:   startTime,
				RequestId:     "deferred-first",
				WorkflowId:    "deferred-first",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
				Attempt:       -1,
			},
			{
				NominalTime:   startTime,
				ActualTime:    startTime,
				DesiredTime:   startTime,
				RequestId:     "new-later",
				WorkflowId:    "new-later",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			},
		},
		InitialRunningWorkflows:  []*commonpb.WorkflowExecution{{WorkflowId: "running", RunId: "running-run"}},
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedOverlapSkipped:   1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			require.Equal(t, "deferred-first", invoker.GetBufferedStarts()[0].GetRequestId())
			require.Equal(t, int64(-1), invoker.GetBufferedStarts()[0].GetAttempt())
		},
	})
}

func TestProcessBufferTask_BufferOneDropsDeferredStartPastCatchupWindow(t *testing.T) {
	env := newTestEnv(t)
	env.Scheduler.Schedule.Policies.CatchupWindow = durationpb.New(10 * time.Minute)
	startTime := timestamppb.New(env.TimeSource.Now().Add(-15 * time.Minute))
	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			RequestId:     "deferred-expired",
			WorkflowId:    "deferred-expired",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			Attempt:       -1,
		}},
		ExpectedMissedCatchupWindow: 1,
	})
}

func TestProcessBufferTask_Empty(t *testing.T) {
	env := newTestEnv(t)
	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: nil,
	})
}

func TestProcessBufferTask_BackingOff(t *testing.T) {
	env := newTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	backoffTime := startTime.AsTime().Add(30 * time.Minute)
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       2,
			BackoffTime:   timestamppb.New(backoffTime),
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        true,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       3,
			BackoffTime:   timestamppb.New(backoffTime),
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:  bufferedStarts,
		ExpectedBufferedStarts: 2,
	})
}

func TestProcessBufferTask_BackingOffReady(t *testing.T) {
	env := newTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	backoffTime := env.TimeSource.Now().Add(-1 * time.Minute)
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       2,
			BackoffTime:   timestamppb.New(backoffTime),
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:  bufferedStarts,
		ExpectedBufferedStarts: 1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			require.Len(t, util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
				return start.Attempt > 0
			}), 1)
		},
	})
}

func TestProcessBufferTask_NeedsTerminate(t *testing.T) {
	env := newTestEnv(t)

	initialRunningWorkflows := []*commonpb.WorkflowExecution{{
		WorkflowId: "existing-wf",
		RunId:      "existing-run",
	}}

	startTime := timestamppb.New(env.TimeSource.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "new-wf",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:   bufferedStarts,
		InitialRunningWorkflows: initialRunningWorkflows,
		// The Watcher removes the running workflow once termination takes effect.
		ExpectedBufferedStarts:     1,
		ExpectedRunningWorkflows:   1,
		ExpectedTerminateWorkflows: 1,
	})
}

// Regression: useScheduledAction ran before the catchup-window check, consuming RemainingActions for dropped starts.
func TestProcessBufferTask_MissedCatchupPreservesRemainingActions(t *testing.T) {
	env := newTestEnv(t)
	env.Scheduler.Schedule.State.LimitedActions = true
	env.Scheduler.Schedule.State.RemainingActions = 3

	now := env.TimeSource.Now()
	startTime := timestamppb.New(now.Add(-defaultCatchupWindow * 2))
	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "expired",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		}},
		ExpectedBufferedStarts:      0,
		ExpectedMissedCatchupWindow: 1,
	})
	require.Equal(t, int64(3), env.Scheduler.Schedule.State.RemainingActions,
		"RemainingActions must not be consumed by a start that was dropped for missing the catchup window")
}

func TestProcessBufferTask_PausedDropsAutomatedKeepsManual(t *testing.T) {
	env := newTestEnv(t)
	env.Scheduler.Schedule.State.Paused = true

	startTime := timestamppb.New(env.TimeSource.Now())
	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{
			{
				NominalTime:   startTime,
				ActualTime:    startTime,
				DesiredTime:   startTime,
				Manual:        false,
				RequestId:     "auto",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
			{
				NominalTime:   startTime,
				ActualTime:    startTime,
				DesiredTime:   startTime,
				Manual:        true,
				RequestId:     "manual",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
		},
		ExpectedBufferedStarts: 1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			kept := invoker.GetBufferedStarts()[0]
			require.Equal(t, "manual", kept.RequestId)
			require.Equal(t, int64(1), kept.Attempt,
				"manual start must be promoted to Attempt=1 even when schedule is paused")
		},
	})
}

func TestProcessBufferTask_NeedsCancel(t *testing.T) {
	env := newTestEnv(t)

	initialRunningWorkflows := []*commonpb.WorkflowExecution{{
		WorkflowId: "existing-wf",
		RunId:      "existing-run",
	}}

	startTime := timestamppb.New(env.TimeSource.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "new-wf",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		},
	}

	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts:   bufferedStarts,
		InitialRunningWorkflows: initialRunningWorkflows,
		// The Watcher removes the running workflow once cancelation takes effect.
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedCancelWorkflows:  1,
	})
}
