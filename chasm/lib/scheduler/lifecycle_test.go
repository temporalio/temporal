package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const lifecycleIdleTime = 10 * time.Minute

func lifecycleIdleTask() *schedulerpb.SchedulerIdleTask {
	return &schedulerpb.SchedulerIdleTask{IdleTimeTotal: durationpb.New(lifecycleIdleTime)}
}

func pendingStart(at time.Time) *schedulespb.BufferedStart {
	return &schedulespb.BufferedStart{
		RequestId:  "pending",
		WorkflowId: "pending-wf",
		ActualTime: timestamppb.New(at),
		Attempt:    1,
	}
}

// The idle task is the last line of defence: whatever armed it, it must not
// close a schedule that still owes a start. Instead it re-arms IdleTime from
// now, and closes once the work is gone.
func TestIdleTask_Execute_DefersWhileStartPending(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler
	now := env.TimeSource.Now()

	rec := metricstest.NewCaptureHandler()
	capture := rec.StartCapture()
	defer rec.StopCapture(capture)
	handler := scheduler.NewSchedulerIdleTaskHandler(scheduler.SchedulerIdleTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: rec,
		BaseLogger:     log.NewTestLogger(),
	})

	sched.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{pendingStart(now)}
	require.NoError(t, handler.Execute(ctx, sched, chasm.TaskAttributes{}, lifecycleIdleTask()))
	require.False(t, sched.Closed, "closed with a start still pending")
	require.Equal(t, now.Add(lifecycleIdleTime).UTC(), sched.GetIdleCloseTime().AsTime().UTC(),
		"a deferred idle task must re-arm IdleTime from now, not leave the schedule unarmed")

	recorded := capture.Snapshot()[metrics.ScheduleIdleTask.Name()]
	require.NotEmpty(t, recorded)
	require.Equal(t, "deferred", recorded[0].Tags["outcome"])
	require.Equal(t, "pending_work", recorded[0].Tags["reason"])

	sched.Invoker.Get(ctx).BufferedStarts = nil
	require.NoError(t, handler.Execute(ctx, sched, chasm.TaskAttributes{}, lifecycleIdleTask()))
	require.True(t, sched.Closed)
}

func TestIdleTask_Execute_DefersWhileControlActionPending(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler

	sched.Invoker.Get(ctx).TerminateWorkflows = []*commonpb.WorkflowExecution{{WorkflowId: "wf", RunId: "run"}}
	require.NoError(t, newIdleHandler(lifecycleIdleTime).Execute(ctx, sched, chasm.TaskAttributes{}, lifecycleIdleTask()))
	require.False(t, sched.Closed, "closed with a terminate still owed")
	require.NotNil(t, sched.GetIdleCloseTime())
}

// A spec that has resumed producing actions owns the schedule's progress via
// its GeneratorTask; the idle task stands down without re-arming.
func TestIdleTask_Execute_SpecPendingClearsDeadline(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler
	now := env.TimeSource.Now()

	sched.Generator.Get(ctx).FutureActionTimes = []*timestamppb.Timestamp{timestamppb.New(now.Add(time.Hour))}
	sched.IdleCloseTime = timestamppb.New(now)
	require.NoError(t, newIdleHandler(lifecycleIdleTime).Execute(ctx, sched, chasm.TaskAttributes{}, lifecycleIdleTask()))
	require.False(t, sched.Closed)
	require.Nil(t, sched.GetIdleCloseTime())
}

// Running workflows are where idle close and time skipping deliberately
// disagree: a running final workflow does not keep the schedule open (V1
// RetentionTime parity), but the virtual clock must not skip past it.
func TestLifecycle_RunningWorkflow_ClosesButBlocksSkipping(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	sched := env.Scheduler
	now := env.TimeSource.Now()

	running := pendingStart(now)
	running.RunId = "running"
	running.StartTime = timestamppb.New(now)
	sched.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{running}

	require.False(t, sched.IsExecutionSkippable(ctx), "time skipping must wait for running workflows")

	require.NoError(t, newIdleHandler(lifecycleIdleTime).Execute(ctx, sched, chasm.TaskAttributes{}, lifecycleIdleTask()))
	require.True(t, sched.Closed, "a running workflow alone must not hold the schedule open")
}
