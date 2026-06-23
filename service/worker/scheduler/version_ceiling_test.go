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
	"go.temporal.io/server/common/payloads"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestClampVersion(t *testing.T) {
	for _, c := range []struct {
		name    string
		ceiling int
		want    SchedulerWorkflowVersion
	}{
		{"negative is unset, no clamp", -1, HighestVersion},
		{"zero clamps to InitialVersion", 0, InitialVersion},
		{"below current clamps down", 1, 1},
		{"equal to current is a no-op", int(HighestVersion), HighestVersion},
		{"above current is a no-op", int(HighestVersion) + 1, HighestVersion},
	} {
		require.Equalf(t, c.want, clampVersion(HighestVersion, c.ceiling), "%s (ceiling=%d)", c.name, c.ceiling)
	}
}

// TestVersionCeilingDefersMigration verifies a clamp below the CHASM gate keeps migration markers out of history
func (s *workflowSuite) TestVersionCeilingDefersMigration() {
	migrateCalls := 0
	s.expectMigrate(&migrateCalls)

	signalMigrate := func(after time.Duration) {
		s.env.RegisterDelayedCallback(func() { s.env.SignalWorkflow(SignalNameMigrateToChasm, nil) }, after)
	}
	// The schedule is paused (no timers), so the two signals advance its iterations to
	// continue-as-new.
	signalMigrate(30 * time.Minute) // sets PendingMigration (deferred under the clamp)
	signalMigrate(60 * time.Minute) // runs the iteration budget out so it continues-as-new

	s.runWithDeps(schedulerDeps{
		enableCHASMMigration:        func() bool { return true },
		migrateWithRunningWorkflows: func() bool { return true },
		versionCeiling:              func() int { return oldPeerCeiling },
	}, pausedHourlySchedule(), 2)

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
	s.env.ExecuteWorkflow(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithDeps(ctx, args, schedulerDeps{}) // ceiling unset: clamp lifted
	}, canArgs)

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError(), "second run completes via the deferred migration after the lift")
	s.Equal(1, migrateCalls)
}

// oldPeerCeiling is the version just below the CHASM migration gate: an older rollback peer with no CHASM scheduler
const oldPeerCeiling = chasmMigrationMinVersion - 1

func (s *workflowSuite) runWithDeps(deps schedulerDeps, sched *schedulepb.Schedule, iterations int) {
	s.runWorkflowFn(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithDeps(ctx, args, deps)
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
