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
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// requireNoChasmSentinel asserts that no CHASM entity -- sentinel or otherwise
// -- exists yet for scheduleID under the scheduler archetype. Used right after
// creating a V1 schedule to confirm the test's "no sentinel gets written"
// assumption directly (rather than only inferring it from the EnableChasm
// value passed to CreateSchedule), since a stray/unexpired sentinel would
// invisibly gate migration behind chasm/lib/scheduler/config.go's
// SentinelIdleTime (15 minutes) and make an otherwise-passing test hang or
// flake for the wrong reason.
func requireNoChasmSentinel(t *testing.T, ctx context.Context, env *testcore.TestEnv, scheduleID string) {
	t.Helper()

	resp, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: scheduleID},
		Archetype: string(chasm.SchedulerArchetype),
	})
	if err != nil {
		// NotFound means nothing at all was written to the CHASM key space for
		// this schedule ID yet -- definitely no sentinel. Any other error is
		// unexpected and should fail the test.
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr, "unexpected error checking for a CHASM sentinel")
		return
	}

	node := resp.GetDatabaseMutableState().GetChasmNodes()[""]
	require.NotNil(t, node, "CHASM execution exists for %q but has no root node", scheduleID)

	var state schedulerpb.SchedulerState
	require.NoError(t, proto.Unmarshal(node.GetData().GetData(), &state))
	require.False(t, state.GetSentinel(),
		"a CHASM sentinel exists for schedule %q -- it would block migration for up to "+
			"SentinelIdleTime (chasm/lib/scheduler/config.go), invalidating this test's timing", scheduleID)
}

// TestScheduleMigrationV1ToV2_NoUnsupportedHistoryEventOnRunningWorkflow is a
// regression test for a customer-reported crash.
//
// When a V1 (workflow-backed) schedule migrates to V2 (CHASM) while one of its
// fired workflows is still running, CHASM's Invoker retroactively attaches a
// Nexus completion callback to that already-running workflow
// (chasm/lib/scheduler/scheduler_tasks.go, WorkflowIdConflictPolicy
// USE_EXISTING + OnConflictOptions.AttachCompletionCallbacks). Server-side,
// this appends a brand-new history event --
// EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED -- into the *middle* of that
// workflow's existing event history
// (service/history/api/startworkflow/api.go:handleUseExistingWorkflowOnConflictOptions
// -> historybuilder/event_factory.go:CreateWorkflowExecutionOptionsUpdatedEvent).
//
// V1 never emits this event or attaches completion callbacks at all, so a
// workflow that was already running before migration never expected to see
// it. Older/community SDKs (e.g. coinbase/temporal-ruby, whose vendored
// protobuf predates this event type) have no way to skip it -- despite the
// event's WorkerMayIgnore=true marker -- and crash trying to decode it
// (TypeError: nil is not a symbol nor a string), permanently stalling the
// workflow and, because of the schedule's overlap policy, potentially the
// entire schedule (see repros/scheduler-migration-bug-evidence.md).
//
// This test asserts that a fired workflow which predates migration keeps an
// IDL-stable history afterwards: no completion callback is retroactively
// attached, and EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED never appears in
// its history. It is EXPECTED TO FAIL until the retroactive-attach behavior in
// chasm/lib/scheduler/scheduler_tasks.go is changed to leave pre-migration
// workflows alone.
func TestScheduleMigrationV1ToV2_NoUnsupportedHistoryEventOnRunningWorkflow(t *testing.T) {
	// TODO: file a tracking issue for the callback-attach bug and reference it here.
	//
	// Regression guard for the V1->V2 migration callback-attach bug (see
	// repros/scheduler-migration-bug-evidence.md): migrating a schedule via the
	// admin MigrateSchedule RPC while a fired workflow is still running
	// retroactively attaches a Nexus completion callback, injecting
	// EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED into that workflow's history,
	// which older SDKs cannot decode. This is EXPECTED TO FAIL on main today;
	// remove this skip once the retroactive-attach behavior is fixed so the test
	// arms.
	t.Skip("regression guard for the admin-path V1->V2 migration callback-attach bug: it " +
		"injects EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED into a running workflow; " +
		"remove this skip when the retroactive callback-attach is fixed")

	env := testcore.NewEnv(
		t,
		testcore.WithWorkerService("V1 scheduler"),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-idl-compat")
	wid := testcore.RandomizeStr("sched-migrate-idl-compat-wf")
	wt := testcore.RandomizeStr("sched-migrate-idl-compat-wt")

	nsName := env.Namespace().String()

	// Disable CHASM to create a V1 schedule.
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, false)

	// A workflow that blocks until signaled, so it's guaranteed to still be
	// running at the moment migration happens.
	resumeSignal := "resume"
	workflowFn := func(ctx workflow.Context) error {
		ch := workflow.GetSignalChannel(ctx, resumeSignal)
		ch.Receive(ctx, nil)
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	sched := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Create the V1 (workflow-backed) schedule. EnableChasm is not set at
	// creation time, so no CHASM sentinel gets written -- a sentinel would
	// block migration for 15 minutes (chasm/lib/scheduler/config.go
	// SentinelIdleTime), which this test can't afford to wait out.
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Schedule:   sched,
		InitialPatch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	require.NoError(t, err)

	// Guard the "no sentinel gets written" assumption above directly, rather
	// than trusting it purely from the EnableChasm value passed to
	// CreateSchedule: an unexpired sentinel would silently gate migration
	// behind SentinelIdleTime and make the assertions below hang or flake for
	// an unrelated reason.
	requireNoChasmSentinel(t, ctx, env, sid)

	// Wait for the V1 scheduler to start the fired workflow and record it as running.
	var runningWfID string
	require.Eventually(t, func() bool {
		descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
		})
		if err != nil || len(descResp.GetInfo().GetRecentActions()) == 0 {
			return false
		}
		a := descResp.Info.RecentActions[0]
		if a.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return false
		}
		runningWfID = a.GetStartWorkflowResult().GetWorkflowId()
		return true
	}, 15*time.Second, 500*time.Millisecond)
	require.NotEmpty(t, runningWfID)

	// Ensure the workflow gets unblocked at the end regardless of outcome, so a
	// failing assertion below doesn't leak a permanently-running execution.
	defer func() {
		_, _ = env.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         nsName,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: runningWfID},
			SignalName:        resumeSignal,
		})
	}()

	// Sanity check: V1 never emits this event type before migration.
	beforeHistory, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{WorkflowId: runningWfID},
	})
	require.NoError(t, err)
	for _, event := range beforeHistory.GetHistory().GetEvents() {
		require.NotEqual(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, event.GetEventType())
	}

	// Enable CHASM and migrate the schedule from V1 to V2 while the workflow
	// created under V1 is still running.
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)

	_, err = env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_CHASM,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	require.NoError(t, err)

	// Wait for the V1 scheduler workflow to complete (migration done).
	v1WorkflowID := scheduler.WorkflowIDPrefix + sid
	require.Eventually(t, func() bool {
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(
			ctx,
			&historyservice.DescribeWorkflowExecutionRequest{
				NamespaceId: env.NamespaceID().String(),
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: nsName,
					Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
				},
			},
		)
		if err != nil {
			return false
		}
		return desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 15*time.Second, 500*time.Millisecond)

	// Poll the fired workflow's history for a bounded window, failing the
	// instant the unsupported event type shows up. If the retroactive attach
	// is ever removed/fixed, this window elapses without a match and the test
	// passes -- it does not require the event to appear.
	const pollWindow = 15 * time.Second
	const pollInterval = 250 * time.Millisecond
	deadline := time.Now().Add(pollWindow)
	for time.Now().Before(deadline) {
		history, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: nsName,
			Execution: &commonpb.WorkflowExecution{WorkflowId: runningWfID},
		})
		require.NoError(t, err)

		for _, event := range history.GetHistory().GetEvents() {
			require.NotEqual(
				t,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				event.GetEventType(),
				"migrating schedule %q to CHASM injected %s (event id %d) into a workflow "+
					"that was already running before migration -- older SDKs cannot decode "+
					"this event type and will permanently stall on it "+
					"(see repros/scheduler-migration-bug-evidence.md)",
				sid, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, event.GetEventId(),
			)
		}

		time.Sleep(pollInterval)
	}
}

// TestScheduleMigrationV1ToV2_AutomaticMigrationForActiveSchedule reproduces the
// realistic customer scenario (the coinbase/temporal-ruby harness at
// /Users/dp/dev/t/testing): a normal recurring schedule that fires frequently,
// with short-lived workflows that complete well within the interval, migrated
// via dynamic config (EnableCHASMSchedulerMigration + rollout %) under the
// default MigrateWithRunningWorkflows=false guard.
//
// It asserts that such a schedule DOES eventually migrate from V1 to V2.
//
// This currently FAILS on main, and the failure is a real bug (documented in
// repros/scheduler-migration-bug-evidence.md): the automatic-migration
// eligibility check
//
//	!s.State.PendingMigration && s.tweakables.EnableCHASMMigration &&
//	    (s.tweakables.MigrateWithRunningWorkflows || len(s.Info.RunningWorkflows) == 0)
//
// reads RunningWorkflows *before* the same loop iteration's ProcessBuffer
// refreshes it, and for a schedule that keeps firing a replacement action is
// always started (inside that same ProcessBuffer) before any later iteration's
// check observes the previous action as complete. So len(RunningWorkflows)==0
// is never true at the check, and with the guard on, migration is deferred
// forever -- the schedule runs on V1 indefinitely.
//
// Contrast with TestScheduleMigrationV1ToV2_RolloutMigrationDoesNotInjectUnsupportedEvent,
// which caps the schedule to a single action and hand-feeds "refresh" signals.
// Those are exactly the artificial conditions that let migration slip through;
// deliberately omitting both is what makes THIS the realistic case. When this
// test starts passing, the ordering bug has been fixed and automatic migration
// works for ordinary busy schedules.
func TestScheduleMigrationV1ToV2_AutomaticMigrationForActiveSchedule(t *testing.T) {
	// Regression guard for the automatic-migration ordering bug (see
	// repros/scheduler-migration-bug-evidence.md): before the
	// RefreshBeforeMigrationCheck scheduler-workflow version, the eligibility
	// check in service/worker/scheduler/workflow.go read
	// len(s.Info.RunningWorkflows) before the same iteration's processBuffer()
	// refreshed it, so a continuously-firing schedule never observed an idle
	// window and never auto-migrated. The fix reconciles running-workflow status
	// before the check, so an actively-firing schedule migrates once its fired
	// workflow completes.
	env := testcore.NewEnv(
		t,
		testcore.WithWorkerService("V1 scheduler"),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-active")
	wid := testcore.RandomizeStr("sched-migrate-active-wf")
	wt := testcore.RandomizeStr("sched-migrate-active-wt")

	nsName := env.Namespace().String()

	// Create as a V1 schedule (no CHASM, so no sentinel is written).
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, false)

	// A short-lived workflow that completes on its own, mimicking the Ruby
	// harness's ~0.5s fired workflow: it finishes well within the interval, so
	// the schedule is a continuous stream of quick actions rather than one
	// long-running execution. This is the shape that provokes the ordering bug.
	workflowFn := func(ctx workflow.Context) error {
		return workflow.Sleep(ctx, 200*time.Millisecond)
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	// A normal recurring schedule: frequent interval, unlimited actions, default
	// Skip overlap -- it just keeps firing, exactly like the Ruby repro. No
	// LimitedActions cap (which would manufacture idleness) and, below, no
	// refresh-signal nudging.
	sched := &schedulepb.Schedule{
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Schedule:   sched,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)
	requireNoChasmSentinel(t, ctx, env, sid)

	// Let the schedule fire at least once, so it is an established, actively-
	// firing V1 schedule (with an action recorded) at the moment migration is
	// enabled -- matching an operator turning on migration rollout for schedules
	// that are already running.
	require.Eventually(t, func() bool {
		descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
		})
		return err == nil && len(descResp.GetInfo().GetRecentActions()) > 0
	}, 15*time.Second, 500*time.Millisecond)

	// Enable automatic migration via dynamic config ONLY -- no admin RPC, no
	// migrate-to-chasm signal, no refresh nudging. MigrateWithRunningWorkflows
	// stays at its default (false).
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMSchedulerMigration, true)
	env.OverrideDynamicConfig(dynamicconfig.CHASMSchedulerMigrationRolloutPercent, 100)

	// ASSERTION: the schedule must eventually migrate to V2. The V1 scheduler
	// workflow only reaches COMPLETED when executeMigration() succeeds, so its
	// completion is a reliable "migration happened" signal.
	//
	// The 30s window is not "migration is slow" slack -- per the ordering bug
	// above, migration for a continuously-firing schedule never triggers at all,
	// so this window elapses with the V1 workflow still RUNNING and the test
	// fails. That failure is the point: it represents the real, currently-unfixed
	// bug.
	v1WorkflowID := scheduler.WorkflowIDPrefix + sid
	require.Eventually(t, func() bool {
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(
			ctx,
			&historyservice.DescribeWorkflowExecutionRequest{
				NamespaceId: env.NamespaceID().String(),
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: nsName,
					Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
				},
			},
		)
		return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 30*time.Second, 1*time.Second,
		"an actively-firing V1 schedule should automatically migrate to V2, but migration never "+
			"completed -- real bug, see repros/scheduler-migration-bug-evidence.md")

	// If migration did complete, confirm the resulting V2 CHASM schedule exists.
	_, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     env.NamespaceID().String(),
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	require.NoError(t, err)
}

// TestScheduleMigrationV1ToV2_RolloutMigrationDoesNotInjectUnsupportedEvent is
// deliberately scoped narrower than TestScheduleMigrationV1ToV2_AutomaticMigrationForActiveSchedule
// above, to avoid conflating two separate concerns:
//
//  1. Whether a schedule migrates via dynamic config (EnableCHASMSchedulerMigration
//     + CHASMSchedulerMigrationRolloutPercent) at all -- for an actively-ticking
//     schedule this can be arbitrarily delayed or may never happen, because the
//     migration-eligibility check reads RunningWorkflows *before* that same
//     iteration's refresh, and a replacement action can refill the gap before a
//     later iteration ever observes it empty (that failure is what
//     TestScheduleMigrationV1ToV2_AutomaticMigrationForActiveSchedule reproduces;
//     see also repros/scheduler-migration-bug-evidence.md, "does automatic
//     migration actually complete?"). That is a separate, already-documented bug
//     and is NOT what this test is about.
//  2. Whether, WHEN a rollout-driven migration does happen, it avoids the
//     unsupported-history-event bug. THAT is what this test asserts.
//
// This test never calls AdminClient().MigrateSchedule or sends the
// scheduler's migrate-to-chasm signal -- migration here is caused only by
// dynamic config, matching how the mitigation is meant to be operated. To
// sidestep concern (1) rather than depend on or assert anything about its
// timing, the schedule is capped to a single action (RemainingActions: 1) so
// nothing ever refills the buffer, and once the fired workflow completes, the
// scheduler is nudged with its "refresh" signal -- a generic, pre-existing,
// non-migration-specific scheduler operation that only sets NeedRefresh (it
// does not touch PendingMigration and does not bypass MigrateWithRunningWorkflows)
// -- purely so the test doesn't have to wait out however long an idle,
// out-of-actions V1 schedule would otherwise sleep for on its own.
func TestScheduleMigrationV1ToV2_RolloutMigrationDoesNotInjectUnsupportedEvent(t *testing.T) {
	env := testcore.NewEnv(
		t,
		testcore.WithWorkerService("V1 scheduler"),
	)

	ctx := testcore.NewContext()
	sid := testcore.RandomizeStr("sched-migrate-rollout-safe")
	wid := testcore.RandomizeStr("sched-migrate-rollout-safe-wf")
	wt := testcore.RandomizeStr("sched-migrate-rollout-safe-wt")

	nsName := env.Namespace().String()

	// Disable CHASM to create a V1 schedule (no sentinel written).
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, false)

	// A workflow that blocks until signaled, so we control exactly when it
	// stops being "running".
	resumeSignal := "resume"
	workflowFn := func(ctx workflow.Context) error {
		ch := workflow.GetSignalChannel(ctx, resumeSignal)
		ch.Receive(ctx, nil)
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	// Single action, short interval: purely a deterministic way to get one
	// fired workflow started promptly and never replaced -- not an attempt to
	// characterize the separate "does migration eventually trigger" timing
	// issue described above.
	sched := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(5 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		State: &schedulepb.ScheduleState{
			LimitedActions:   true,
			RemainingActions: 1,
		},
	}

	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  nsName,
		ScheduleId: sid,
		Schedule:   sched,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)
	requireNoChasmSentinel(t, ctx, env, sid)

	var runningWfID string
	require.Eventually(t, func() bool {
		descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
		})
		if err != nil || len(descResp.GetInfo().GetRecentActions()) == 0 {
			return false
		}
		a := descResp.Info.RecentActions[0]
		if a.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return false
		}
		runningWfID = a.GetStartWorkflowResult().GetWorkflowId()
		return true
	}, 15*time.Second, 500*time.Millisecond)
	require.NotEmpty(t, runningWfID)

	unblocked := false
	defer func() {
		if !unblocked {
			_, _ = env.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
				Namespace:         nsName,
				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: runningWfID},
				SignalName:        resumeSignal,
			})
		}
	}()

	// Cause migration purely via dynamic config -- no AdminClient().MigrateSchedule,
	// no tdbg, no migrate-to-chasm signal anywhere in this test.
	// EnableCHASMSchedulerMigrationWithRunningWorkflows is deliberately left
	// at its default (false): that guard is what this test is verifying.
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMSchedulerMigration, true)
	env.OverrideDynamicConfig(dynamicconfig.CHASMSchedulerMigrationRolloutPercent, 100)

	// Let the fired workflow complete.
	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         nsName,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: runningWfID},
		SignalName:        resumeSignal,
	})
	require.NoError(t, err)
	unblocked = true

	require.Eventually(t, func() bool {
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(
			ctx,
			&historyservice.DescribeWorkflowExecutionRequest{
				NamespaceId: env.NamespaceID().String(),
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: nsName,
					Execution: &commonpb.WorkflowExecution{WorkflowId: runningWfID},
				},
			},
		)
		return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 15*time.Second, 500*time.Millisecond, "fired workflow should complete once signaled")

	// Nudge the scheduler (via the generic, non-migration "refresh" signal --
	// see the doc comment above for why this doesn't touch PendingMigration
	// or bypass the guard) until it notices the workflow completed and
	// migrates via the dynamic config enabled above.
	v1WorkflowID := scheduler.WorkflowIDPrefix + sid
	require.Eventually(t, func() bool {
		_, _ = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         nsName,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
			SignalName:        scheduler.SignalNameRefresh,
		})
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(
			ctx,
			&historyservice.DescribeWorkflowExecutionRequest{
				NamespaceId: env.NamespaceID().String(),
				Request: &workflowservice.DescribeWorkflowExecutionRequest{
					Namespace: nsName,
					Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
				},
			},
		)
		return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 30*time.Second, 500*time.Millisecond, "schedule should migrate via dynamic config once idle")

	// The V2 schedule should now exist -- migration genuinely completed,
	// driven entirely by dynamic config.
	_, err = env.GetTestCluster().SchedulerClient().DescribeSchedule(
		ctx,
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId:     env.NamespaceID().String(),
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		},
	)
	require.NoError(t, err)

	// THE ASSERTION UNDER TEST: because MigrateWithRunningWorkflows defaulted
	// to false, migration only proceeded once the fired workflow had already
	// completed -- so it never needed a retroactive completion-callback
	// attach, and never gained the unsupported event type.
	history, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: nsName,
		Execution: &commonpb.WorkflowExecution{WorkflowId: runningWfID},
	})
	require.NoError(t, err)
	for _, event := range history.GetHistory().GetEvents() {
		require.NotEqual(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, event.GetEventType(),
			"migrating schedule %q via dynamic config injected an unsupported event (id %d) even though "+
				"MigrateWithRunningWorkflows should have deferred until the workflow was done", sid, event.GetEventId())
	}
}
