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

func TestDetermineVersionTransitions(t *testing.T) {
	for _, tc := range []struct {
		name              string
		defaultVersion    SchedulerWorkflowVersion
		recordedVersion   SchedulerWorkflowVersion
		recordedCeiling   int
		configuredCeiling int
		override          int
		wantVersion       SchedulerWorkflowVersion
		wantCeiling       int
	}{
		{
			name:              "no ceiling or override uses the default",
			defaultVersion:    TriggerImmediatelyTimestamp,
			recordedCeiling:   -1,
			configuredCeiling: -1,
			override:          -1,
			wantVersion:       TriggerImmediatelyTimestamp,
			wantCeiling:       -1,
		},
		{
			name:              "zero is a ceiling",
			defaultVersion:    TriggerImmediatelyTimestamp,
			recordedCeiling:   -1,
			configuredCeiling: 0,
			override:          -1,
			wantVersion:       InitialVersion,
			wantCeiling:       0,
		},
		{
			name:              "configured ceiling caps the default",
			defaultVersion:    oldPeerCeiling + 1,
			recordedCeiling:   -1,
			configuredCeiling: oldPeerCeiling,
			override:          -1,
			wantVersion:       oldPeerCeiling,
			wantCeiling:       oldPeerCeiling,
		},
		{
			name:              "default can advance up to the configured ceiling",
			defaultVersion:    oldPeerCeiling,
			recordedVersion:   oldPeerCeiling - 1,
			recordedCeiling:   -1,
			configuredCeiling: oldPeerCeiling,
			override:          -1,
			wantVersion:       oldPeerCeiling,
			wantCeiling:       oldPeerCeiling,
		},
		{
			name:              "recorded version is retained below a newly lowered ceiling",
			defaultVersion:    oldPeerCeiling,
			recordedVersion:   oldPeerCeiling + 1,
			recordedCeiling:   -1,
			configuredCeiling: oldPeerCeiling,
			override:          -1,
			wantVersion:       oldPeerCeiling + 1,
			wantCeiling:       oldPeerCeiling,
		},
		{
			name:              "recorded ceiling cannot increase",
			defaultVersion:    MigrationHandoffFixes,
			recordedVersion:   oldPeerCeiling,
			recordedCeiling:   oldPeerCeiling,
			configuredCeiling: int(MigrationHandoffFixes),
			override:          int(MigrationHandoffFixes),
			wantVersion:       oldPeerCeiling,
			wantCeiling:       oldPeerCeiling,
		},
		{
			name:              "unset config cannot increase the recorded ceiling",
			defaultVersion:    MigrationHandoffFixes,
			recordedVersion:   oldPeerCeiling,
			recordedCeiling:   oldPeerCeiling,
			configuredCeiling: -1,
			override:          int(MigrationHandoffFixes),
			wantVersion:       oldPeerCeiling,
			wantCeiling:       oldPeerCeiling,
		},
		{
			name:              "valid override advances the version",
			defaultVersion:    TriggerImmediatelyTimestamp,
			recordedCeiling:   -1,
			configuredCeiling: -1,
			override:          int(LatestSchedulerWorkflowVersion),
			wantVersion:       LatestSchedulerWorkflowVersion,
			wantCeiling:       -1,
		},
		{
			name:              "ceiling caps an override",
			defaultVersion:    TriggerImmediatelyTimestamp,
			recordedCeiling:   -1,
			configuredCeiling: oldPeerCeiling,
			override:          int(LatestSchedulerWorkflowVersion),
			wantVersion:       oldPeerCeiling,
			wantCeiling:       oldPeerCeiling,
		},
		{
			name:              "override below the default is ignored",
			defaultVersion:    TriggerImmediatelyTimestamp,
			recordedCeiling:   -1,
			configuredCeiling: -1,
			override:          oldPeerCeiling,
			wantVersion:       TriggerImmediatelyTimestamp,
			wantCeiling:       -1,
		},
		{
			name:              "override above the latest supported version is ignored",
			defaultVersion:    TriggerImmediatelyTimestamp,
			recordedCeiling:   -1,
			configuredCeiling: -1,
			override:          int(LatestSchedulerWorkflowVersion) + 1,
			wantVersion:       TriggerImmediatelyTimestamp,
			wantCeiling:       -1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			version, ceiling := determineVersionTransition(
				tc.defaultVersion,
				tc.recordedVersion,
				tc.recordedCeiling,
				tc.configuredCeiling,
				tc.override,
			)
			require.Equal(t, tc.wantVersion, version)
			require.Equal(t, tc.wantCeiling, ceiling)
		})
	}
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
