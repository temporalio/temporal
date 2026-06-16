package schedulertest_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	historyservice "go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/chasm/lib/scheduler/schedulertest"
	"go.temporal.io/server/common/sdk"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

// TestMigrationRoundTrip_V2toV1toV2 exercises a full V2 -> V1 -> V2 migration
// round trip and asserts state is preserved end to end.
//
// Both hops run real code:
//   - V2 -> V1: a real V2 scheduler accrues state, then the real
//     SchedulerMigrateToWorkflowTask side-effect task fires, calling
//     CHASMToLegacyStartScheduleArgs and serializing StartScheduleArgs exactly as
//     it would for the production V1 workflow start. The driver captures that
//     start request from the history client.
//   - V1 -> V2: the captured StartScheduleArgs is fed through
//     migration.LegacyToCreateFromMigrationStateRequest — the same function the V1
//     workflow's MigrateScheduleToChasm activity calls — and re-imported into a
//     fresh V2 engine via the real CreateSchedulerFromMigration path, then run
//     forward with invariant checks.
//
// The V1 workflow's own runtime ticking between hops is NOT executed here: doing
// so requires the V1 SDK testsuite plus unexported fixtures in package scheduler,
// which would create an import cycle with this package. That runtime behavior is
// covered independently by service/worker/scheduler/workflow_test.go. This test
// covers the state-preservation contract across both real conversion functions
// and both real V2 engine paths.
func TestMigrationRoundTrip_V2toV1toV2(t *testing.T) {
	// --- Phase A: a V2 scheduler runs and accrues state. ---
	d1 := schedulertest.NewDriver(t)
	d1.RunToQuiescence(12) // advance ~12 interval ticks; HWM moves, actions run

	var (
		tokenBeforeMigration int64
		hwmBeforeMigration   time.Time
	)
	d1.ReadScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) {
		tokenBeforeMigration = s.ConflictToken
		hwmBeforeMigration = s.Generator.Get(ctx).LastProcessedTime.AsTime()
	})
	require.False(t, hwmBeforeMigration.IsZero(), "HWM should have advanced")

	// --- Phase B: V2 -> V1 export. Capture the V1 workflow start request. ---
	var captured *historyservice.StartWorkflowExecutionRequest
	d1.History.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *historyservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*historyservice.StartWorkflowExecutionResponse, error) {
			captured = req
			return &historyservice.StartWorkflowExecutionResponse{RunId: "v1-run-id"}, nil
		}).Times(1)

	require.NoError(t, d1.MigrateToWorkflow())
	d1.RunToQuiescence(5) // fire the migrate side-effect task
	require.NotNil(t, captured, "migrate task should have started a V1 workflow")

	// Decode the StartScheduleArgs exactly as the V1 workflow would receive them.
	var args schedulespb.StartScheduleArgs
	require.NoError(t, sdk.PreferProtoDataConverter.FromPayloads(captured.GetStartRequest().GetInput(), &args))

	// The exported V1 state must preserve the conflict token and high-water mark.
	require.Equal(t, tokenBeforeMigration, args.GetState().GetConflictToken(),
		"conflict token must survive V2->V1 export")
	require.Equal(t, hwmBeforeMigration, args.GetState().GetLastProcessedTime().AsTime(),
		"high-water mark must survive V2->V1 export")
	// Migration restores the pre-migration paused state (default schedule: not paused).
	require.False(t, args.GetSchedule().GetState().GetPaused(),
		"exported schedule should reflect pre-migration (unpaused) state")

	// The V2 scheduler should now be closed (handed off to V1).
	d1.ReadScheduler(func(s *scheduler.Scheduler, _ chasm.Context) {
		require.True(t, s.Closed, "V2 scheduler should be closed after migration")
	})

	// --- Phase C: V1 -> V2 re-import via the same conversion the V1 activity uses. ---
	migrationTime := d1.Now()
	req := migration.LegacyToCreateFromMigrationStateRequest(
		args.GetSchedule(),
		args.GetInfo(),
		args.GetState(),
		nil, // no custom search attributes
		nil, // no memo
		migrationTime,
	)

	// Sanity: the conversion preserved the token and HWM into the migration state.
	require.Equal(t, tokenBeforeMigration, req.GetState().GetSchedulerState().GetConflictToken())
	require.Equal(t, hwmBeforeMigration, req.GetState().GetGeneratorState().GetLastProcessedTime().AsTime())

	// Re-import into a fresh V2 engine and run it forward with invariants enforced.
	d2 := schedulertest.NewDriverFromMigration(t, req)
	d2.AfterStep = schedulertest.CheckInvariantsHook(t)
	d2.RunToQuiescence(12)

	// The round-tripped V2 scheduler preserved the conflict token, is alive, and
	// resumed making progress (HWM at or beyond where it was before migration).
	d2.ReadScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) {
		require.Equal(t, tokenBeforeMigration, s.ConflictToken,
			"conflict token must survive the full V2->V1->V2 round trip")
		require.False(t, s.Closed, "re-imported scheduler should be running")
		require.False(t, s.Generator.Get(ctx).LastProcessedTime.AsTime().Before(hwmBeforeMigration),
			"re-imported HWM must not regress below the pre-migration value")
	})
}
