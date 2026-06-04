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
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A buffer of only deferred starts (Attempt=-1) must NOT emit a
// ProcessBufferTask. Deferred starts wait on completion events, not on a
// wall-clock deadline, so emitting an immediate ProcessBufferTask would
// spin-loop on a no-op processBuffer call.
func TestInvoker_AddTasks_AllDeferredEmitsNothing(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	now := env.TimeSource.Now()
	invoker.LastProcessedTime = timestamppb.New(now)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId:  "deferred",
		WorkflowId: "wf-deferred",
		Attempt:    -1,
		ActualTime: timestamppb.New(now),
	}}

	env.NodeBackend.TasksByCategory = nil
	invoker.EnqueueBufferedStarts(ctx, nil)
	require.NoError(t, env.CloseTransaction())

	require.False(t, env.HasTask(&tasks.ChasmTask{}, chasm.TaskScheduledTimeImmediate),
		"all-deferred buffer must not emit any side-effect task")
	require.False(t, env.HasTask(&tasks.ChasmTaskPure{}, chasm.TaskScheduledTimeImmediate),
		"all-deferred buffer must not emit an immediate ProcessBufferTask")
}

// After ProcessBuffer fires for a backed-off start whose BackoffTime has just
// elapsed, the HWM advance must re-arm processing so the retry actually runs.
// Regression for the addTasks gate that previously suppressed re-arm when
// processBuffer found nothing to process (no Attempt==0 starts).
func TestProcessBufferTask_RearmsBackedOffRetry(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	now := env.TimeSource.Now()
	// Set LPT to the past so the start (with BackoffTime at now) becomes
	// eligible only after the HWM advance in this run.
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

	// Precondition for re-arm: the HWM advance in this Execute must have made
	// the previously-backing-off start eligible (BackoffTime <= LPT).
	require.False(t, invoker.BufferedStarts[0].BackoffTime.AsTime().After(invoker.LastProcessedTime.AsTime()),
		"setup invariant: start must be eligible after HWM advance")

	// addTasks must have emitted an immediate side-effect ChasmTask carrying
	// the InvokerExecuteTask payload so the retry actually fires.
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

	// Set up initial state. Note: InitialRunningWorkflows is now represented by
	// BufferedStarts that have RunId set but no Completed field.
	invoker.BufferedStarts = c.InitialBufferedStarts
	invoker.CancelWorkflows = c.InitialCancelWorkflows
	invoker.TerminateWorkflows = c.InitialTerminateWorkflows

	// Add initial running workflows as BufferedStarts with RunId set.
	for _, wf := range c.InitialRunningWorkflows {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
			RequestId:  wf.WorkflowId + "-req",
			WorkflowId: wf.WorkflowId,
			RunId:      wf.RunId,
			Attempt:    1,
		})
	}

	// Set LastProcessedTime to current time to ensure time checks pass.
	invoker.LastProcessedTime = timestamppb.New(env.TimeSource.Now())

	handler := newProcessBufferHandler(env)
	err := handler.Execute(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	// Validate the results.
	// Count BufferedStarts (excluding running ones added from InitialRunningWorkflows).
	require.Len(t, invoker.GetBufferedStarts(), c.ExpectedBufferedStarts+len(c.InitialRunningWorkflows))

	// Count running workflows from BufferedStarts (has RunId but no Completed).
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

	// Callbacks.
	if c.ValidateInvoker != nil {
		c.ValidateInvoker(t, invoker)
	}
}

// ProcessBuffer attempts all buffered starts with ALLOW_ALL policy.
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

// ProcessBuffer processes a start that missed the catchup window.
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

// ProcessBuffer defers a start (from overlap policy) by placing it into NewBuffer.
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
		// Because no workflows are running, we'll immediately kick off one
		// BufferedStart, and then buffer the next. This leaves us with 1 ready start,
		// and 1 still buffered.
		ExpectedBufferedStarts: 2,
		ExpectedOverlapSkipped: 1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			// Only one start should be set for execution (Attempt > 0).
			require.Len(t, util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
				return start.Attempt > 0
			}), 1)
		},
	})
}

// ProcessBuffer is scheduled with an empty buffer.
func TestProcessBufferTask_Empty(t *testing.T) {
	env := newTestEnv(t)
	runProcessBufferTestCase(t, env, &processBufferTestCase{
		InitialBufferedStarts: nil,
	})
}

// ProcessBuffer is scheduled with a buffer of starts all backing off.
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

// ProcessBuffer is scheduled with a start that was backing off, but ready to retry.
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
			// The start should be ready for execution (Attempt > 0).
			require.Len(t, util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
				return start.Attempt > 0
			}), 1)
		},
	})
}

// A buffered start with an overlap policy to terminate other workflows is processed.
func TestProcessBufferTask_NeedsTerminate(t *testing.T) {
	env := newTestEnv(t)

	// Add a running workflow to the Scheduler.
	initialRunningWorkflows := []*commonpb.WorkflowExecution{{
		WorkflowId: "existing-wf",
		RunId:      "existing-run",
	}}

	// Set up the BufferedStart with a policy that will terminate existing workflows.
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
		// Buffer should still contain the buffered start. The existing workflow will still
		// remain in RunningWorkflows as well, since it is the Watcher's job to remove it
		// after termination/cancelation takes effect.
		ExpectedBufferedStarts:     1,
		ExpectedRunningWorkflows:   1,
		ExpectedTerminateWorkflows: 1,
	})
}

// Past-catchup automated starts must drop WITHOUT consuming a LimitedActions
// slot. Regression for the order-of-checks bug where useScheduledAction(true)
// fired before the catchup-window check, decrementing RemainingActions for
// starts that never ran.
//
// Not reachable via the public API today: backfill/TriggerImmediately starts
// are Manual=true (see backfiller_tasks.go) and bypass the catchup-window
// check. The bug only manifests for automated Generator starts that age past
// their catchup window while processing is stalled.
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

// A buffered start with an overlap policy to cancel other workflows is processed.
func TestProcessBufferTask_NeedsCancel(t *testing.T) {
	env := newTestEnv(t)

	// Add a running workflow to the Scheduler.
	initialRunningWorkflows := []*commonpb.WorkflowExecution{{
		WorkflowId: "existing-wf",
		RunId:      "existing-run",
	}}

	// Set up the BufferedStart with a policy that will cancel existing workflows.
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
		// Buffer should still contain the buffered start. The existing workflow will still
		// remain in RunningWorkflows as well, since it is the Watcher's job to remove it
		// after termination/cancelation takes effect.
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedCancelWorkflows:  1,
	})
}
