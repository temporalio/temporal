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
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
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
func requireNoChasmSentinel(ctx context.Context, t *testing.T, env *testcore.TestEnv, scheduleID string) {
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

// createV1Schedule creates a V1 (workflow-backed) schedule with CHASM disabled,
// then asserts no CHASM sentinel was written. initialPatch may be nil.
func createV1Schedule(
	ctx context.Context,
	t *testing.T,
	env *testcore.TestEnv,
	scheduleID string,
	sched *schedulepb.Schedule,
	initialPatch *schedulepb.SchedulePatch,
) {
	t.Helper()

	// EnableChasm is unset at creation time, so no CHASM sentinel gets written --
	// a sentinel would block migration for 15 minutes (chasm/lib/scheduler/config.go
	// SentinelIdleTime), which these tests can't afford to wait out.
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, false)

	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:    env.Namespace().String(),
		ScheduleId:   scheduleID,
		Schedule:     sched,
		InitialPatch: initialPatch,
		Identity:     "test",
		RequestId:    uuid.NewString(),
	})
	require.NoError(t, err)
	requireNoChasmSentinel(ctx, t, env, scheduleID)
}

// awaitRunningAction waits until the schedule has fired an action whose workflow
// is still RUNNING, and returns that workflow's ID.
func awaitRunningAction(ctx context.Context, t *testing.T, env *testcore.TestEnv, scheduleID string) string {
	t.Helper()

	var runningWfID string
	await.RequireTrue(t, func() bool {
		descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: scheduleID,
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
	return runningWfID
}

// awaitAnyAction waits until the schedule has fired at least one action
// (regardless of the fired workflow's status) and returns that workflow's ID.
func awaitAnyAction(ctx context.Context, t *testing.T, env *testcore.TestEnv, scheduleID string) string {
	t.Helper()

	var wfID string
	await.RequireTrue(t, func() bool {
		descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: scheduleID,
		})
		if err != nil || len(descResp.GetInfo().GetRecentActions()) == 0 {
			return false
		}
		wfID = descResp.Info.RecentActions[0].GetStartWorkflowResult().GetWorkflowId()
		return wfID != ""
	}, 15*time.Second, 500*time.Millisecond)
	require.NotEmpty(t, wfID)
	return wfID
}

// awaitV1SchedulerCompleted waits until the V1 scheduler workflow reaches
// COMPLETED. The V1 scheduler workflow only completes when executeMigration()
// succeeds, so its completion is a reliable "migration happened" signal.
func awaitV1SchedulerCompleted(ctx context.Context, t *testing.T, env *testcore.TestEnv, scheduleID string) {
	t.Helper()

	v1WorkflowID := scheduler.WorkflowIDPrefix + scheduleID
	await.RequireTruef(t, func() bool {
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
			NamespaceId: env.NamespaceID().String(),
			Request: &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
			},
		})
		return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 30*time.Second, 1*time.Second, "V1 scheduler workflow should complete once migration succeeds")
}

// requireNoOptionsUpdatedEvent asserts the workflow's history does not contain
// EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED. V1 never emits this event, so
// older/community SDKs (whose vendored protobuf predates it) cannot decode it
// and crash -- permanently stalling the workflow (see
// repros/scheduler-migration-bug-evidence.md).
func requireNoOptionsUpdatedEvent(ctx context.Context, t *testing.T, env *testcore.TestEnv, workflowID string) {
	t.Helper()

	history, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	require.NoError(t, err)
	for _, event := range history.GetHistory().GetEvents() {
		require.NotEqual(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, event.GetEventType(),
			"workflow %q gained %s (event id %d) during migration -- older SDKs cannot decode this "+
				"event type and will permanently stall on it (see repros/scheduler-migration-bug-evidence.md)",
			workflowID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, event.GetEventId())
	}
}

// requireV2ScheduleExists asserts a V2 (CHASM) schedule exists for scheduleID.
func requireV2ScheduleExists(ctx context.Context, t *testing.T, env *testcore.TestEnv, scheduleID string) {
	t.Helper()

	_, err := env.GetTestCluster().SchedulerClient().DescribeSchedule(ctx, &schedulerpb.DescribeScheduleRequest{
		NamespaceId:     env.NamespaceID().String(),
		FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: scheduleID},
	})
	require.NoError(t, err)
}

// TestScheduleMigrationV1ToV2_AdminMigratePreservesRunningWorkflowHistory is a
// regression guard for a customer-reported crash on the admin (tdbg/CLI)
// MigrateSchedule path.
//
// When a V1 schedule is migrated to V2 via the admin MigrateSchedule RPC while
// one of its fired workflows is still running, CHASM's Invoker retroactively
// attaches a Nexus completion callback to that already-running workflow
// (chasm/lib/scheduler/scheduler_tasks.go, WorkflowIdConflictPolicy USE_EXISTING
// + OnConflictOptions.AttachCompletionCallbacks). Server-side this appends a
// brand-new EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED event into the middle
// of that workflow's existing history. V1 never emits this event, so a workflow
// that was already running before migration never expected to see it, and older
// SDKs (e.g. coinbase/temporal-ruby) crash decoding it -- permanently stalling
// the workflow and, via the schedule's overlap policy, potentially the whole
// schedule (see repros/scheduler-migration-bug-evidence.md).
//
// Unlike the dynamic-config rollout path -- which defers migration until no
// workflow is running (see TestScheduleMigrationV1ToV2_RolloutMigration) -- the
// admin MigrateSchedule RPC migrates immediately regardless of running
// workflows, so this bug is still live on that path. Skipped until the
// retroactive callback-attach is fixed.
func TestScheduleMigrationV1ToV2_AdminMigratePreservesRunningWorkflowHistory(t *testing.T) {
	// TODO: file a tracking issue for the admin-path callback-attach bug and reference it here.
	t.Skip("admin MigrateSchedule path still injects EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED " +
		"into a running workflow's history; remove this skip when the retroactive callback-attach is fixed")

	env := testcore.NewEnv(t, testcore.WithWorkerService("V1 scheduler"))
	ctx := testcore.NewContext()

	sid := testcore.RandomizeStr("sched-admin-migrate")
	wid := testcore.RandomizeStr("sched-admin-migrate-wf")
	wt := testcore.RandomizeStr("sched-admin-migrate-wt")

	// A workflow that blocks until signaled, so it is guaranteed to still be
	// running at the moment migration happens.
	resumeSignal := "resume"
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		workflow.GetSignalChannel(ctx, resumeSignal).Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: wt})

	sched := &schedulepb.Schedule{
		Spec:   &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Hour)}}},
		Action: startWorkflowAction(env, wid, wt),
	}
	createV1Schedule(ctx, t, env, sid, sched, &schedulepb.SchedulePatch{
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
	})

	runningWfID := awaitRunningAction(ctx, t, env, sid)

	// Ensure the workflow gets unblocked at the end regardless of outcome, so a
	// failing assertion doesn't leak a permanently-running execution.
	defer func() {
		_, _ = env.FrontendClient().SignalWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: runningWfID},
			SignalName:        resumeSignal,
		})
	}()

	// Sanity: V1 never emits this event type before migration.
	requireNoOptionsUpdatedEvent(ctx, t, env, runningWfID)

	// Migrate via the admin RPC while the fired workflow is still running.
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	_, err := env.AdminClient().MigrateSchedule(ctx, &adminservice.MigrateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Target:     adminservice.MigrateScheduleRequest_SCHEDULER_TARGET_CHASM,
		Identity:   "test",
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	require.NoError(t, err)

	// Migration is fully applied once the V1 scheduler workflow completes, so its
	// side effects (including any retroactive callback attach) are already
	// reflected in the fired workflow's history by this point.
	awaitV1SchedulerCompleted(ctx, t, env, sid)

	requireNoOptionsUpdatedEvent(ctx, t, env, runningWfID)
}

// TestScheduleMigrationV1ToV2_RolloutMigration verifies the dynamic-config
// rollout migration path for a normal, actively-firing schedule, covering two
// properties at once:
//
//  1. It migrates. A continuously-firing schedule must eventually move from V1
//     to V2 once migration is enabled via dynamic config. Regression guard for
//     the eligibility-check ordering fix (RefreshBeforeMigrationCheck): the check
//     previously read len(RunningWorkflows) before the same run-loop iteration's
//     processBuffer() reconciled it, so a busy schedule -- which always started a
//     replacement action before any later iteration observed the previous one
//     complete -- never saw an idle window and never auto-migrated.
//  2. It stays IDL-safe. Because EnableCHASMSchedulerMigrationWithRunningWorkflows
//     defaults to false, migration only proceeds once no fired workflow is
//     running, so it never retroactively attaches a Nexus completion callback and
//     never injects EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED (which older
//     SDKs cannot decode -- see repros/scheduler-migration-bug-evidence.md).
//
// Migration here is driven purely by dynamic config -- no admin MigrateSchedule
// RPC and no migrate-to-chasm signal. Contrast the admin path, which migrates
// even with a workflow still running and is still broken (see
// TestScheduleMigrationV1ToV2_AdminMigratePreservesRunningWorkflowHistory).
func TestScheduleMigrationV1ToV2_RolloutMigration(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithWorkerService("V1 scheduler"))
	ctx := testcore.NewContext()

	sid := testcore.RandomizeStr("sched-rollout-migrate")
	wid := testcore.RandomizeStr("sched-rollout-migrate-wf")
	wt := testcore.RandomizeStr("sched-rollout-migrate-wt")

	// A short-lived workflow that completes well within the interval, so the
	// schedule is a continuous stream of quick actions -- the shape that both
	// exercises the ordering fix and leaves brief idle windows for the
	// (default-off) MigrateWithRunningWorkflows guard to migrate in.
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		return workflow.Sleep(ctx, 200*time.Millisecond)
	}, workflow.RegisterOptions{Name: wt})

	sched := &schedulepb.Schedule{
		Spec:   &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Second)}}},
		Action: startWorkflowAction(env, wid, wt),
	}
	createV1Schedule(ctx, t, env, sid, sched, nil)

	// Establish it as an actively-firing V1 schedule (at least one action) before
	// enabling migration, matching an operator turning on rollout for schedules
	// that are already running.
	firedWfID := awaitAnyAction(ctx, t, env, sid)

	// Enable automatic migration via dynamic config only. MigrateWithRunningWorkflows
	// stays at its default (false).
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMSchedulerMigration, true)
	env.OverrideDynamicConfig(dynamicconfig.CHASMSchedulerMigrationRolloutPercent, 100)

	// (1) Migration completes, and the resulting V2 schedule exists.
	awaitV1SchedulerCompleted(ctx, t, env, sid)
	requireV2ScheduleExists(ctx, t, env, sid)

	// (2) The fired workflow never gained the unsupported event: migration waited
	// (guard default false) until it was no longer running, so no callback was
	// retroactively attached.
	requireNoOptionsUpdatedEvent(ctx, t, env, firedWfID)
}
