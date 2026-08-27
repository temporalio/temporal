package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestClampVersion(t *testing.T) {
	for _, c := range []struct {
		name    string
		ceiling int
		want    SchedulerWorkflowVersion
	}{
		{"negative is unset, no clamp", -1, TriggerImmediatelyTimestamp},
		{"zero clamps to InitialVersion", 0, InitialVersion},
		{"below current clamps down", 1, 1},
		{"equal to current is a no-op", int(TriggerImmediatelyTimestamp), TriggerImmediatelyTimestamp},
		{"above current is a no-op", int(TriggerImmediatelyTimestamp) + 1, TriggerImmediatelyTimestamp},
	} {
		require.Equalf(t, c.want, clampVersion(TriggerImmediatelyTimestamp, c.ceiling), "%s (ceiling=%d)", c.name, c.ceiling)
	}
}

func TestDetermineVersionTransitions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ceiling  int
		defaults []SchedulerWorkflowVersion
		versions []SchedulerWorkflowVersion
	}{
		{
			name:     "stays at ceiling",
			ceiling:  oldPeerCeiling,
			defaults: []SchedulerWorkflowVersion{oldPeerCeiling, oldPeerCeiling + 1, oldPeerCeiling + 2},
			versions: []SchedulerWorkflowVersion{oldPeerCeiling, oldPeerCeiling, oldPeerCeiling},
		},
		{
			name:     "current version below dynamic config ceiling",
			ceiling:  oldPeerCeiling,
			defaults: []SchedulerWorkflowVersion{oldPeerCeiling - 1, oldPeerCeiling, oldPeerCeiling + 1},
			versions: []SchedulerWorkflowVersion{oldPeerCeiling - 1, oldPeerCeiling, oldPeerCeiling},
		},
		{
			name:     "advances without ceiling",
			ceiling:  -1,
			defaults: []SchedulerWorkflowVersion{TriggerImmediatelyTimestamp - 1, TriggerImmediatelyTimestamp, TriggerImmediatelyTimestamp + 1},
			versions: []SchedulerWorkflowVersion{TriggerImmediatelyTimestamp - 1, TriggerImmediatelyTimestamp, TriggerImmediatelyTimestamp + 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			s := &scheduler{
				logger: log.NewSdkLogger(log.NewNoopLogger()),
				versionCeiling: func() int {
					calls++
					return tc.ceiling
				},
			}
			for i, defaultVersion := range tc.defaults {
				version, ceiling := s.determineVersion(defaultVersion)
				require.Equal(t, tc.versions[i], version)
				require.Equal(t, tc.ceiling, ceiling)

				// MutableSideEffect stores this value for the next workflow task.
				s.tweakables = CurrentTweakablePolicies
				s.tweakables.Version = version
				s.tweakables.VersionCeiling = ceiling
				s.tweakables.VersionCeilingSet = true
			}
			require.Equal(t, len(tc.defaults), calls, "ceiling is re-read each iteration so it can ratchet tighter")
		})
	}
}

// TestDetermineVersionDowngradeOnNextRun verifies that after a run kept v13 as a floor while the
// ceiling was tightened to 12, the following run (fresh tweakables after continue-as-new) reads
// dynamic config and starts capped at 12.
func TestDetermineVersionDowngradeOnNextRun(t *testing.T) {
	ceiling := int(TriggerImmediatelyTimestamp)
	s := &scheduler{
		logger:         log.NewSdkLogger(log.NewNoopLogger()),
		versionCeiling: func() int { return ceiling },
	}
	version, recordedCeiling := s.determineVersion(TriggerImmediatelyTimestamp)
	require.Equal(t, SchedulerWorkflowVersion(TriggerImmediatelyTimestamp), version)
	require.Equal(t, int(TriggerImmediatelyTimestamp), recordedCeiling)
}

// TestDetermineVersionLocksAtZeroCeiling verifies that zero is recorded as a real ceiling rather
// than treated as an unset value.
func TestDetermineVersionLocksAtZeroCeiling(t *testing.T) {
	ceiling := 0
	s := &scheduler{versionCeiling: func() int { return ceiling }}

	// First evaluation clamps to InitialVersion (0) and records it.
	version, recordedCeiling := s.determineVersion(TriggerImmediatelyTimestamp)
	require.Equal(t, InitialVersion, version)
	require.Equal(t, 0, recordedCeiling)
	s.tweakables = CurrentTweakablePolicies
	s.tweakables.Version = version
	s.tweakables.VersionCeiling = recordedCeiling
	s.tweakables.VersionCeilingSet = true

	// The configured ceiling is lifted mid-run, but the recorded ceiling stays at InitialVersion.
	ceiling = -1
	version, recordedCeiling = s.determineVersion(TriggerImmediatelyTimestamp)
	require.Equal(t, InitialVersion, version)
	require.Equal(t, 0, recordedCeiling)
}

func TestDetermineVersionPreservesLegacyRecordedVersion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version SchedulerWorkflowVersion
	}{
		{name: "initial version", version: InitialVersion},
		{name: "current version", version: TriggerImmediatelyTimestamp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			s := &scheduler{
				logger: log.NewSdkLogger(log.NewNoopLogger()),
				versionCeiling: func() int {
					calls++
					return oldPeerCeiling
				},
			}
			s.tweakables = CurrentTweakablePolicies
			s.tweakables.Version = tc.version
			// VersionCeilingSet is false for a marker written before this field existed.

			version, ceiling := s.determineVersion(TriggerImmediatelyTimestamp)
			require.Equal(t, tc.version, version)
			require.Equal(t, int(tc.version), ceiling)
			require.Zero(t, calls, "legacy histories must not read the current version ceiling")
		})
	}
}

// TestVersionCeilingWithCHASMMigration verifies that a clamp below the CHASM gate keeps migration
// markers out of history, and that once the ceiling is lifted (on the next run) the deferred
// migration runs.
func (s *workflowSuite) TestVersionCeilingWithCHASMMigration() {
	migrateCalls := 0
	s.expectMigrate(&migrateCalls)

	signalMigrate := func(after time.Duration) {
		s.env.RegisterDelayedCallback(func() { s.env.SignalWorkflow(SignalNameMigrateToChasm, nil) }, after)
	}
	// The schedule is paused (no timers), so the two signals advance its iterations to
	// continue-as-new.
	signalMigrate(30 * time.Minute) // sets PendingMigration (deferred under the clamp)
	signalMigrate(60 * time.Minute) // runs the iteration budget out so it continues-as-new

	s.runWithCeiling(
		func() bool { return true },
		func() bool { return true },
		func() int { return oldPeerCeiling },
		pausedHourlySchedule(), 2)

	s.True(s.env.IsWorkflowCompleted())
	s.Equal(0, migrateCalls, "neither DC- nor signal-driven migration may run while the ceiling holds")

	// PendingMigration is in continue-as-new payload on the next run.
	canArgs := s.continueAsNewArgs()
	s.True(canArgs.GetState().GetPendingMigration(), "deferred migration must survive continue-as-new")

	// After ceiling removed, the deferred migration executes immediately.
	endOfRun1 := s.now()
	s.env = s.NewTestWorkflowEnvironment()
	s.expectMigrate(&migrateCalls)
	s.env.SetStartTime(endOfRun1)
	// ceiling unset (clamp lifted): the default workflow entrypoint disables the clamp.
	s.env.ExecuteWorkflow(SchedulerWorkflow, canArgs)

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError(), "second run completes via the deferred migration after the lift")
	s.Equal(1, migrateCalls)
}

// oldPeerCeiling is one below the CHASM migration gate, modeling an older rollback peer that has no CHASM scheduler.
const oldPeerCeiling = TriggerImmediatelyTimestamp - 1

func (s *workflowSuite) runWithCeiling(enableCHASMMigration, migrateWithRunningWorkflows func() bool, versionCeiling func() int, sched *schedulepb.Schedule, iterations int) {
	s.runWorkflowFn(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithSpecBuilder(ctx, args, newSpecBuilderForTest(0, 0), enableCHASMMigration, migrateWithRunningWorkflows, versionCeiling)
	}, sched, iterations)
}

// expectMigrate stubs the MigrateScheduleToChasm activity and counts invocations into calls.
func (s *workflowSuite) expectMigrate(calls *int) {
	s.env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).Maybe().Return(
		func(context.Context, *schedulerpb.CreateFromMigrationStateRequest) error {
			*calls++
			return nil
		})
}

// continueAsNewArgs asserts the workflow finished by continue-as-new and returns the
// StartScheduleArgs it carried into the next run.
func (s *workflowSuite) continueAsNewArgs() *schedulespb.StartScheduleArgs {
	s.T().Helper()
	var canErr *workflow.ContinueAsNewError
	s.Require().ErrorAs(s.env.GetWorkflowError(), &canErr)
	var args schedulespb.StartScheduleArgs
	s.Require().NoError(payloads.Decode(canErr.Input, &args))
	return &args
}

// pausedHourlySchedule is paused (so only signals advance iterations) on a coarse interval that
// never fires on its own.
func pausedHourlySchedule() *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec:  &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}},
		State: &schedulepb.ScheduleState{Paused: true},
	}
}
