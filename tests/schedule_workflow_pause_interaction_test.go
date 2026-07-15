package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// scheduleStartWait/scheduleStartPoll bound waiting for a schedule to
	// record a recent action for the workflow it just started.
	scheduleStartWait = 30 * time.Second
	scheduleStartPoll = 250 * time.Millisecond

	// workflowStatusWait/workflowStatusPoll bound waiting for a workflow to
	// reach a pause-related status (PAUSED, or RUNNING again after reset).
	workflowStatusWait = 15 * time.Second
	workflowStatusPoll = 200 * time.Millisecond

	// scheduleCompletionWait/scheduleCompletionPoll bound waiting for the
	// scheduler to observe a workflow's completion and record it as a
	// COMPLETED action.
	scheduleCompletionWait = 30 * time.Second
	scheduleCompletionPoll = 500 * time.Millisecond
)

var allOverlapPolicies = []enumspb.ScheduleOverlapPolicy{
	enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
	enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
	enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
	enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
	enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
}

// expectScheduleProgressesWhilePaused returns whether a schedule using the given
// overlap policy continues to take new scheduled actions while the workflow it
// started is paused. The V1 and CHASM schedulers both treat a paused workflow as
// still occupying the overlap slot, so behavior is identical across them:
//
//   - ALLOW_ALL never inspects the running workflow, so it always starts new runs.
//   - TERMINATE_OTHER terminates the paused workflow (a hard close that does not
//     require the workflow to process a workflow task) and then starts the next run.
//   - SKIP / BUFFER_ONE / BUFFER_ALL / CANCEL_OTHER keep the slot occupied by the
//     paused workflow, so no new run is taken. (For CANCEL_OTHER the cancellation
//     additionally never completes while paused, because a paused workflow has no
//     workflow task to process the cancel; it completes once unpaused.)
func expectScheduleProgressesWhilePaused(policy enumspb.ScheduleOverlapPolicy) bool {
	switch policy {
	case enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		return true
	default:
		// SKIP, BUFFER_ONE, BUFFER_ALL, CANCEL_OTHER: the paused workflow keeps
		// the slot, so the schedule does not take new actions.
		return false
	}
}

func TestScheduleV1WorkflowPauseInteraction(t *testing.T) {
	t.Parallel()
	t.Run("Overlap", func(t *testing.T) { runSchedulePauseOverlapMatrix(t, v1ContextFactory) })
	t.Run("UnpauseRecovery", func(t *testing.T) { runSchedulePauseRecoveryMatrix(t, v1ContextFactory) })
	t.Run("ContinueAsNew", func(t *testing.T) { testSchedulePauseContinueAsNew(t, v1ContextFactory) })
	t.Run("Reset", func(t *testing.T) { testSchedulePauseReset(t, v1ContextFactory) })
}

func TestScheduleCHASMWorkflowPauseInteraction(t *testing.T) {
	t.Parallel()
	t.Run("Overlap", func(t *testing.T) { runSchedulePauseOverlapMatrix(t, chasmContextFactory) })
	t.Run("UnpauseRecovery", func(t *testing.T) { runSchedulePauseRecoveryMatrix(t, chasmContextFactory) })
	t.Run("ContinueAsNew", func(t *testing.T) { testSchedulePauseContinueAsNew(t, chasmContextFactory) })
	t.Run("Reset", func(t *testing.T) { testSchedulePauseReset(t, chasmContextFactory) })
}

func runSchedulePauseOverlapMatrix(t *testing.T, newContext contextFactory) {
	for _, policy := range allOverlapPolicies {
		t.Run(policy.String(), func(t *testing.T) {
			testSchedulePauseOverlap(t, newContext, policy)
		})
	}
}

func runSchedulePauseRecoveryMatrix(t *testing.T, newContext contextFactory) {
	for _, policy := range allOverlapPolicies {
		// Recovery is only interesting for policies whose schedule was blocked
		// by the paused workflow. Policies that keep progressing never stall,
		// and may have already closed the paused workflow (e.g. TERMINATE_OTHER
		// terminates it), so there is nothing to recover.
		if expectScheduleProgressesWhilePaused(policy) {
			continue
		}
		t.Run(policy.String(), func(t *testing.T) {
			testSchedulePauseUnpauseRecovery(t, newContext, policy)
		})
	}
}

// pauseInteractionOpts returns the schedule test options plus the dynamic config
// required to enable the workflow pause feature.
func pauseInteractionOpts(t *testing.T) []testcore.TestOption {
	return append(scheduleCommonOpts(t),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowPauseEnabled, true),
	)
}

// scheduledPauseFixture holds the handles produced by setupPausedScheduledWorkflow.
type scheduledPauseFixture struct {
	s              *testcore.TestEnv
	ctx            context.Context
	sid            string
	wid            string
	wt             string
	execution      *commonpb.WorkflowExecution
	actionsAtPause int64
}

// setupPausedScheduledWorkflow creates a 1s-interval schedule with the given
// overlap policy that runs a workflow registered by register, waits for the
// first run to start, pauses it, and waits for it to reach PAUSED. It returns
// the handles needed to drive the rest of a pause-interaction test.
func setupPausedScheduledWorkflow(
	t *testing.T,
	newContext contextFactory,
	policy enumspb.ScheduleOverlapPolicy,
	register func(s *testcore.TestEnv, wt string),
) *scheduledPauseFixture {
	s, _ := newScheduleEnv(t, pauseInteractionOpts(t)...)

	sid := testcore.RandomizeStr("sched-pause-" + policy.String())
	wid := testcore.RandomizeStr("sched-pause-wf-" + policy.String())
	wt := testcore.RandomizeStr("sched-pause-wt-" + policy.String())

	register(s, wt)

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Second)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   wid,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
			Policies: &schedulepb.SchedulePolicies{
				OverlapPolicy: policy,
			},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	require.NoError(t, err)

	// Wait for the schedule to start its first workflow. RecentActions is
	// populated for every overlap policy (including ALLOW_ALL).
	var execution *commonpb.WorkflowExecution
	await.RequireTruef(t, func() bool {
		desc, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		for _, ra := range desc.GetInfo().GetRecentActions() {
			if ex := ra.GetStartWorkflowResult(); ex.GetRunId() != "" {
				execution = ex
				return true
			}
		}
		return false
	}, scheduleStartWait, scheduleStartPoll, "schedule should start its first workflow")

	// Pause that workflow.
	_, err = s.FrontendClient().PauseWorkflowExecution(ctx, &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: execution.GetWorkflowId(),
		RunId:      execution.GetRunId(),
		Identity:   "functional-test",
		Reason:     "schedule-pause-interaction",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Confirm the workflow reaches PAUSED.
	await.RequireTrue(t, func() bool {
		d, dErr := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: execution,
		})
		return dErr == nil && d.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED
	}, workflowStatusWait, workflowStatusPoll)

	actionsAtPause, err := scheduleActionCount(ctx, s, sid)
	require.NoError(t, err)

	return &scheduledPauseFixture{
		s:              s,
		ctx:            ctx,
		sid:            sid,
		wid:            wid,
		wt:             wt,
		execution:      execution,
		actionsAtPause: actionsAtPause,
	}
}

// registerForeverWorkflow registers a workflow that runs (effectively) forever,
// so it stays in the schedule's running set until the schedule closes it.
func registerForeverWorkflow(s *testcore.TestEnv, wt string) {
	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			return workflow.Sleep(ctx, time.Hour)
		},
		workflow.RegisterOptions{Name: wt},
	)
}

// registerSignalCompletableWorkflow registers a workflow that blocks until it
// receives the "complete" signal or its context is cancelled, then closes. This
// lets a test free the schedule's overlap slot on demand (by signalling) or via
// the scheduler's own cancellation (CANCEL_OTHER).
func registerSignalCompletableWorkflow(s *testcore.TestEnv, wt string) {
	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
			return ctx.Err()
		},
		workflow.RegisterOptions{Name: wt},
	)
}

// testSchedulePauseOverlap creates a schedule (1s interval) that starts a single
// long-running workflow, pauses that workflow, and then asserts whether the
// schedule keeps taking scheduled actions, per expectScheduleProgressesWhilePaused.
func testSchedulePauseOverlap(t *testing.T, newContext contextFactory, policy enumspb.ScheduleOverlapPolicy) {
	f := setupPausedScheduledWorkflow(t, newContext, policy, registerForeverWorkflow)

	if expectScheduleProgressesWhilePaused(policy) {
		// The schedule should keep taking new actions despite the paused workflow.
		await.RequireTruef(t, func() bool {
			c, cErr := scheduleActionCount(f.ctx, f.s, f.sid)
			return cErr == nil && c > f.actionsAtPause
		}, 20*time.Second, 500*time.Millisecond,
			"schedule with %s should keep taking actions while its workflow is paused", policy)
	} else {
		// The schedule must not take any new action while the workflow is
		// paused: the paused workflow keeps the overlap slot occupied. Poll
		// over a window during which the 1s schedule would otherwise fire ~8
		// times, confirming ActionCount never advances.
		require.Never(
			t,
			func() bool {
				c, cErr := scheduleActionCount(f.ctx, f.s, f.sid)
				return cErr == nil && c > f.actionsAtPause
			},
			8*time.Second,
			500*time.Millisecond,
			"schedule with %s must not take new actions while its workflow is paused", policy)
	}
}

// testSchedulePauseUnpauseRecovery verifies that a schedule that was blocked by
// a paused workflow resumes taking actions once that workflow is unpaused and
// allowed to close (by signalling it, and/or via the scheduler's own
// cancellation for CANCEL_OTHER).
func testSchedulePauseUnpauseRecovery(t *testing.T, newContext contextFactory, policy enumspb.ScheduleOverlapPolicy) {
	f := setupPausedScheduledWorkflow(t, newContext, policy, registerSignalCompletableWorkflow)

	// Unpause the workflow.
	_, err := f.s.FrontendClient().UnpauseWorkflowExecution(f.ctx, &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  f.s.Namespace().String(),
		WorkflowId: f.execution.GetWorkflowId(),
		RunId:      f.execution.GetRunId(),
		Identity:   "functional-test",
		Reason:     "schedule-pause-interaction-recovery",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Free the overlap slot so the schedule can progress again.
	err = f.s.SdkClient().SignalWorkflow(f.ctx, f.execution.GetWorkflowId(), f.execution.GetRunId(), "complete", nil)
	require.NoError(t, err)

	// Once the slot frees, the schedule should resume taking actions - the
	// schedule is blocked only while the workflow is paused, not permanently.
	await.RequireTruef(t, func() bool {
		c, cErr := scheduleActionCount(f.ctx, f.s, f.sid)
		return cErr == nil && c > f.actionsAtPause
	}, 30*time.Second, 1*time.Second,
		"schedule with %s should resume taking actions after the workflow is unpaused", policy)
}

// setupPausedTriggeredWorkflow creates a schedule with a long interval and
// trigger-immediately (so exactly one run starts), registers the workflow via
// register, waits for that run to start, optionally runs afterStart (e.g. to
// wait for a specific point in history), pauses the run, and waits for it to
// reach PAUSED. It is shared by the pause/continue-as-new and pause/reset
// tests, which both need a single triggered run to pause.
func setupPausedTriggeredWorkflow(
	t *testing.T,
	newContext contextFactory,
	opts []testcore.TestOption,
	idPrefix string,
	register func(s *testcore.TestEnv, wt string),
	afterStart func(s *testcore.TestEnv, firstRun *commonpb.WorkflowExecution),
) *scheduledPauseFixture {
	s, _ := newScheduleEnv(t, opts...)

	sid := testcore.RandomizeStr(idPrefix)
	wid := testcore.RandomizeStr(idPrefix + "-wf")
	wt := testcore.RandomizeStr(idPrefix + "-wt")

	register(s, wt)

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			// Long interval + trigger-immediately so exactly one run starts.
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(24 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   wid,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
			Policies: &schedulepb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			},
		},
		InitialPatch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	require.NoError(t, err)

	// Wait for the first run to start.
	var firstRun *commonpb.WorkflowExecution
	await.RequireTruef(t, func() bool {
		desc, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		for _, ra := range desc.GetInfo().GetRecentActions() {
			if ex := ra.GetStartWorkflowResult(); ex.GetRunId() != "" {
				firstRun = ex
				return true
			}
		}
		return false
	}, scheduleStartWait, scheduleStartPoll, "schedule should start its first workflow")

	if afterStart != nil {
		afterStart(s, firstRun)
	}

	// Pause the first run.
	_, err = s.FrontendClient().PauseWorkflowExecution(ctx, &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: firstRun.GetWorkflowId(),
		RunId:      firstRun.GetRunId(),
		Identity:   "functional-test",
		Reason:     idPrefix,
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)
	await.RequireTrue(t, func() bool {
		d, dErr := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: firstRun,
		})
		return dErr == nil && d.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED
	}, workflowStatusWait, workflowStatusPoll)

	return &scheduledPauseFixture{
		s:         s,
		ctx:       ctx,
		sid:       sid,
		wid:       wid,
		wt:        wt,
		execution: firstRun,
	}
}

// testSchedulePauseContinueAsNew verifies the interaction between pause and
// continue-as-new for a scheduled workflow:
//   - A paused workflow does not process buffered signals, so it cannot
//     continue-as-new until it is unpaused.
//   - After unpause, the workflow continues-as-new and the continued run
//     completes, and the scheduler observes that completion across the
//     continue-as-new boundary.
func testSchedulePauseContinueAsNew(t *testing.T, newContext contextFactory) {
	// The scheduler matches the continued run's completion by the request ID in
	// the completion callback token, which only survives continue-as-new in the
	// envelope token format (gated off by default).
	opts := append(pauseInteractionOpts(t), testcore.WithDynamicConfig(callback.EncodeInternalTokenWithEnvelope, true))

	// First run waits for the "go" signal, then continues-as-new; the continued
	// run completes immediately.
	register := func(s *testcore.TestEnv, wt string) {
		s.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
			if workflow.GetInfo(ctx).ContinuedExecutionRunID == "" {
				workflow.GetSignalChannel(ctx, "go").Receive(ctx, nil)
				return workflow.NewContinueAsNewError(ctx, wt)
			}
			return nil
		}, workflow.RegisterOptions{Name: wt})
	}

	f := setupPausedTriggeredWorkflow(t, newContext, opts, "sched-pause-can", register, nil)

	// Signal "go" while paused. The signal is recorded but not processed, so the
	// workflow must not continue-as-new: it stays on the same run, still PAUSED.
	err := f.s.SdkClient().SignalWorkflow(f.ctx, f.execution.GetWorkflowId(), f.execution.GetRunId(), "go", nil)
	require.NoError(t, err)
	require.Never(t, func() bool {
		d, dErr := f.s.FrontendClient().DescribeWorkflowExecution(f.ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: f.s.Namespace().String(),
			Execution: f.execution,
		})
		return dErr == nil && d.GetWorkflowExecutionInfo().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED
	}, 3*time.Second, 500*time.Millisecond,
		"paused workflow must not continue-as-new while paused")

	// Unpause: the buffered signal is processed, the workflow continues-as-new,
	// and the continued run completes.
	_, err = f.s.FrontendClient().UnpauseWorkflowExecution(f.ctx, &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  f.s.Namespace().String(),
		WorkflowId: f.execution.GetWorkflowId(),
		RunId:      f.execution.GetRunId(),
		Identity:   "functional-test",
		Reason:     "schedule-pause-can-recovery",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// The latest run of the chain (the continued-as-new run) should complete.
	await.RequireTruef(t, func() bool {
		d, dErr := f.s.FrontendClient().DescribeWorkflowExecution(f.ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: f.s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: f.execution.GetWorkflowId()},
		})
		return dErr == nil && d.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, scheduleCompletionWait, scheduleCompletionPoll, "continued-as-new run should complete after unpause")

	// The scheduler should observe the completion across the continue-as-new
	// boundary and record it as a COMPLETED action.
	await.RequireTruef(t, func() bool {
		desc, descErr := f.s.FrontendClient().DescribeSchedule(f.ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  f.s.Namespace().String(),
			ScheduleId: f.sid,
		})
		if descErr != nil {
			return false
		}
		for _, ra := range desc.GetInfo().GetRecentActions() {
			if ra.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				return true
			}
		}
		return false
	}, scheduleCompletionWait, scheduleCompletionPoll, "scheduler should record the continued-as-new workflow as COMPLETED")
}

// testSchedulePauseReset verifies the interaction between pause and resetting a
// scheduled workflow. A workflow is started by a schedule, paused, then reset
// to a point before the pause. The reset run is no longer paused (the pause
// event is not part of the reset history), and the scheduler keeps tracking the
// reset run through to completion.
func testSchedulePauseReset(t *testing.T, newContext contextFactory) {
	register := func(s *testcore.TestEnv, wt string) {
		s.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
			workflow.GetSignalChannel(ctx, "complete").Receive(ctx, nil)
			return nil
		}, workflow.RegisterOptions{Name: wt})
	}

	// Wait until the first workflow task is complete so event 3 is a valid reset point.
	afterStart := func(s *testcore.TestEnv, firstRun *commonpb.WorkflowExecution) {
		s.WaitForHistoryEvents(`
			1 WorkflowExecutionStarted
			2 WorkflowTaskScheduled
			3 WorkflowTaskStarted
			4 WorkflowTaskCompleted`,
			s.GetHistoryFunc(s.Namespace().String(), firstRun),
			10*time.Second,
			250*time.Millisecond,
		)
	}

	f := setupPausedTriggeredWorkflow(t, newContext, pauseInteractionOpts(t), "sched-pause-reset", register, afterStart)

	// Reset the paused workflow to a point before it was paused.
	resetResp, err := f.s.FrontendClient().ResetWorkflowExecution(f.ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 f.s.Namespace().String(),
		WorkflowExecution:         f.execution,
		Reason:                    "schedule-pause-reset",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	require.NoError(t, err)
	resetRun := &commonpb.WorkflowExecution{
		WorkflowId: f.execution.GetWorkflowId(),
		RunId:      resetResp.GetRunId(),
	}

	// The reset run is created from history before the pause event, so it is
	// running, not paused: reset clears the pause.
	await.RequireTruef(t, func() bool {
		d, dErr := f.s.FrontendClient().DescribeWorkflowExecution(f.ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: f.s.Namespace().String(),
			Execution: resetRun,
		})
		return dErr == nil && d.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}, workflowStatusWait, workflowStatusPoll, "reset run should be RUNNING (pause cleared by reset)")

	// The scheduler keeps tracking the reset run: signalling it to completion
	// should be observed and recorded as a COMPLETED action.
	err = f.s.SdkClient().SignalWorkflow(f.ctx, resetRun.GetWorkflowId(), resetRun.GetRunId(), "complete", nil)
	require.NoError(t, err)
	await.RequireTruef(t, func() bool {
		desc, descErr := f.s.FrontendClient().DescribeSchedule(f.ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  f.s.Namespace().String(),
			ScheduleId: f.sid,
		})
		if descErr != nil {
			return false
		}
		for _, ra := range desc.GetInfo().GetRecentActions() {
			if ra.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				return true
			}
		}
		return false
	}, scheduleCompletionWait, scheduleCompletionPoll, "scheduler should record the reset run as COMPLETED")
}

// scheduleActionCount returns the schedule's total ActionCount (number of
// workflows it has started).
func scheduleActionCount(ctx context.Context, s *testcore.TestEnv, sid string) (int64, error) {
	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	if err != nil {
		return 0, err
	}
	return desc.GetInfo().GetActionCount(), nil
}
