package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/payloads"
	"google.golang.org/protobuf/types/known/durationpb"
)

type warningLogger struct {
	warnings []string
}

var _ log.Logger = (*warningLogger)(nil)

func (*warningLogger) Debug(string, ...any) {}
func (*warningLogger) Info(string, ...any)  {}
func (l *warningLogger) Warn(msg string, _ ...any) {
	l.warnings = append(l.warnings, msg)
}
func (*warningLogger) Error(string, ...any) {}

func TestShouldWarnForVersionCeiling(t *testing.T) {
	unsupportedCeiling := int(TriggerImmediatelyTimestamp) + 1
	for _, tc := range []struct {
		name       string
		tweakables TweakablePolicies
		ceiling    int
		want       bool
	}{
		{
			name:    "first unsupported observation",
			ceiling: unsupportedCeiling,
			want:    true,
		},
		{
			name: "restored unsupported observation",
			tweakables: TweakablePolicies{
				VersionCeiling:    unsupportedCeiling,
				VersionCeilingSet: true,
			},
			ceiling: unsupportedCeiling,
			want:    false,
		},
		{
			name: "changed unsupported observation",
			tweakables: TweakablePolicies{
				VersionCeiling:    unsupportedCeiling,
				VersionCeilingSet: true,
			},
			ceiling: unsupportedCeiling + 1,
			want:    true,
		},
		{
			name: "legacy marker with recorded ceiling",
			tweakables: TweakablePolicies{
				VersionCeiling: unsupportedCeiling,
			},
			ceiling: unsupportedCeiling,
			want:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, shouldWarnForVersionCeiling(tc.tweakables, TriggerImmediatelyTimestamp, tc.ceiling))
		})
	}
}

func TestDetermineVersionTransition(t *testing.T) {
	for defaultVersion := InitialVersion; defaultVersion <= LatestSchedulerWorkflowVersion; defaultVersion++ {
		for recordedVersion := InitialVersion; recordedVersion <= LatestSchedulerWorkflowVersion; recordedVersion++ {
			for ceiling := -1; ceiling <= int(LatestSchedulerWorkflowVersion)+1; ceiling++ {
				for override := -1; override <= int(LatestSchedulerWorkflowVersion)+1; override++ {
					wantVersion := defaultVersion
					if override >= int(wantVersion) && override <= int(LatestSchedulerWorkflowVersion) {
						wantVersion = SchedulerWorkflowVersion(override)
					}
					if ceiling >= 0 && ceiling < int(wantVersion) {
						wantVersion = SchedulerWorkflowVersion(ceiling)
					}
					if recordedVersion > wantVersion {
						wantVersion = recordedVersion
					}

					version, capturedCeiling := determineVersionTransition(defaultVersion, recordedVersion, ceiling, override)
					require.Equalf(t, wantVersion, version, "default=%d recorded=%d ceiling=%d override=%d", defaultVersion, recordedVersion, ceiling, override)
					require.Equalf(t, ceiling, capturedCeiling, "default=%d recorded=%d ceiling=%d override=%d", defaultVersion, recordedVersion, ceiling, override)
				}
			}
		}
	}
}

func TestDetermineVersionDiagnostics(t *testing.T) {
	t.Run("reports invalid override warnings", func(t *testing.T) {
		logger := &warningLogger{}
		s := &scheduler{
			logger:          logger,
			versionCeiling:  func() int { return -1 },
			versionOverride: func() int { return int(LatestSchedulerWorkflowVersion) + 1 },
		}

		s.determineVersion(TriggerImmediatelyTimestamp)
		s.determineVersion(TriggerImmediatelyTimestamp)

		require.Equal(t, []string{
			"worker.schedulerV1VersionOverride is outside the supported range; ignored",
			"worker.schedulerV1VersionOverride is outside the supported range; ignored",
		}, logger.warnings)
	})

	t.Run("does not report a ceiling that caps an override as ineffective", func(t *testing.T) {
		logger := &warningLogger{}
		s := &scheduler{
			logger:          logger,
			versionCeiling:  func() int { return int(MigrationHandoffFixes) },
			versionOverride: func() int { return int(LatestSchedulerWorkflowVersion) },
		}

		version, _ := s.determineVersion(TriggerImmediatelyTimestamp)

		require.Equal(t, SchedulerWorkflowVersion(MigrationHandoffFixes), version)
		require.Empty(t, logger.warnings)
	})
}

// TestVersionCeilingDefersCHASMMigration verifies that a clamp below the CHASM gate keeps
// migration markers out of history. The pending migration is retained for a later fresh run.
func (s *workflowSuite) TestVersionCeilingDefersCHASMMigration() {
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

func (s *workflowSuite) TestVersionCeilingLiftAdvancesWithinRun() {
	migrateCalls := 0
	s.expectMigrate(&migrateCalls)

	ceiling := int(oldPeerCeiling)
	s.env.RegisterDelayedCallback(func() {
		ceiling = -1
		s.env.SignalWorkflow(SignalNameMigrateToChasm, nil)
	}, 30*time.Minute)

	s.runWithCeiling(
		func() bool { return true },
		func() bool { return true },
		func() int { return ceiling },
		pausedHourlySchedule(), 0)

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
	s.Equal(1, migrateCalls)
}

func (s *workflowSuite) TestVersionOverrideAdvancesWithinRunAfterCeilingLift() {
	migrateCalls := 0
	s.expectMigrate(&migrateCalls)

	ceiling := int(oldPeerCeiling)
	s.env.RegisterDelayedCallback(func() {
		ceiling = -1
		s.env.SignalWorkflow(SignalNameMigrateToChasm, nil)
	}, 30*time.Minute)

	s.runWorkflowFn(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithSpecBuilder(ctx, args, newSpecBuilderForTest(0, 0), schedulerDynamicConfig{
			enableCHASMMigration:        func() bool { return true },
			migrateWithRunningWorkflows: func() bool { return true },
			versionCeiling:              func() int { return ceiling },
			versionOverride:             func() int { return int(LatestSchedulerWorkflowVersion) },
		})
	}, pausedHourlySchedule(), 0)

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError())
	s.Equal(1, migrateCalls)
}

// oldPeerCeiling is one below the CHASM migration gate, modeling an older rollback peer that has no CHASM scheduler.
const oldPeerCeiling = TriggerImmediatelyTimestamp - 1

func (s *workflowSuite) runWithCeiling(enableCHASMMigration, migrateWithRunningWorkflows func() bool, versionCeiling func() int, sched *schedulepb.Schedule, iterations int) {
	s.runWorkflowFn(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithSpecBuilder(ctx, args, newSpecBuilderForTest(0, 0), schedulerDynamicConfig{
			enableCHASMMigration:        enableCHASMMigration,
			migrateWithRunningWorkflows: migrateWithRunningWorkflows,
			versionCeiling:              versionCeiling,
		})
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
