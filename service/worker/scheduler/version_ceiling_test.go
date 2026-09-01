package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

func TestDetermineVersionTransitions(t *testing.T) {
	for _, tc := range []struct {
		name            string
		defaultVersion  SchedulerWorkflowVersion
		recordedVersion SchedulerWorkflowVersion
		ceiling         int
		override        int
		wantVersion     SchedulerWorkflowVersion
		wantCeiling     int
	}{
		{
			name:           "no ceiling or override uses the default",
			defaultVersion: TriggerImmediatelyTimestamp,
			ceiling:        -1,
			override:       -1,
			wantVersion:    TriggerImmediatelyTimestamp,
			wantCeiling:    -1,
		},
		{
			name:           "zero is a ceiling",
			defaultVersion: TriggerImmediatelyTimestamp,
			ceiling:        0,
			override:       -1,
			wantVersion:    InitialVersion,
			wantCeiling:    0,
		},
		{
			name:           "ceiling caps the default",
			defaultVersion: oldPeerCeiling + 1,
			ceiling:        oldPeerCeiling,
			override:       -1,
			wantVersion:    oldPeerCeiling,
			wantCeiling:    oldPeerCeiling,
		},
		{
			name:            "recorded version is retained below a lower ceiling",
			defaultVersion:  oldPeerCeiling,
			recordedVersion: oldPeerCeiling + 1,
			ceiling:         oldPeerCeiling,
			override:        -1,
			wantVersion:     oldPeerCeiling + 1,
			wantCeiling:     oldPeerCeiling,
		},
		{
			name:            "raising the ceiling advances on the next iteration",
			defaultVersion:  MigrationHandoffFixes,
			recordedVersion: oldPeerCeiling,
			ceiling:         int(MigrationHandoffFixes),
			override:        -1,
			wantVersion:     MigrationHandoffFixes,
			wantCeiling:     int(MigrationHandoffFixes),
		},
		{
			name:            "removing the ceiling advances on the next iteration",
			defaultVersion:  MigrationHandoffFixes,
			recordedVersion: oldPeerCeiling,
			ceiling:         -1,
			override:        -1,
			wantVersion:     MigrationHandoffFixes,
			wantCeiling:     -1,
		},
		{
			name:           "valid override advances the version",
			defaultVersion: TriggerImmediatelyTimestamp,
			ceiling:        -1,
			override:       int(LatestSchedulerWorkflowVersion),
			wantVersion:    LatestSchedulerWorkflowVersion,
			wantCeiling:    -1,
		},
		{
			name:           "ceiling caps an override",
			defaultVersion: TriggerImmediatelyTimestamp,
			ceiling:        oldPeerCeiling,
			override:       int(LatestSchedulerWorkflowVersion),
			wantVersion:    oldPeerCeiling,
			wantCeiling:    oldPeerCeiling,
		},
		{
			name:           "override below the default is ignored",
			defaultVersion: TriggerImmediatelyTimestamp,
			ceiling:        -1,
			override:       oldPeerCeiling,
			wantVersion:    TriggerImmediatelyTimestamp,
			wantCeiling:    -1,
		},
		{
			name:           "override above the latest supported version is ignored",
			defaultVersion: TriggerImmediatelyTimestamp,
			ceiling:        -1,
			override:       int(LatestSchedulerWorkflowVersion) + 1,
			wantVersion:    TriggerImmediatelyTimestamp,
			wantCeiling:    -1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			version, ceiling := determineVersionTransition(tc.defaultVersion, tc.recordedVersion, tc.ceiling, tc.override)
			require.Equal(t, tc.wantVersion, version)
			require.Equal(t, tc.wantCeiling, ceiling)
		})
	}
}

func TestDetermineVersionTransitionCeilingTruthTable(t *testing.T) {
	var table struct {
		TestCases []struct {
			DefaultVersion  SchedulerWorkflowVersion `json:"defaultVersion"`
			RecordedVersion SchedulerWorkflowVersion `json:"recordedVersion"`
			CeilingCases    []struct {
				Name        string                   `json:"name"`
				Ceiling     int                      `json:"ceiling"`
				WantVersion SchedulerWorkflowVersion `json:"wantVersion"`
				WantCeiling int                      `json:"wantCeiling"`
			} `json:"ceilingCases"`
		} `json:"testCases"`
	}

	data, err := os.ReadFile(filepath.Join("testdata", "version_ceiling_truth_table.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &table))
	require.NotEmpty(t, table.TestCases)

	for _, versionCase := range table.TestCases {
		for _, ceilingCase := range versionCase.CeilingCases {
			t.Run(fmt.Sprintf("default_%d/recorded_%d/%s", versionCase.DefaultVersion, versionCase.RecordedVersion, ceilingCase.Name), func(t *testing.T) {
				version, capturedCeiling := determineVersionTransition(versionCase.DefaultVersion, versionCase.RecordedVersion, ceilingCase.Ceiling, -1)
				require.Equal(t, ceilingCase.WantVersion, version)
				require.Equal(t, ceilingCase.WantCeiling, capturedCeiling)
			})
		}
	}
}

func TestDetermineVersionTransitionOverrideTruthTable(t *testing.T) {
	var table struct {
		TestCases []struct {
			DefaultVersion  SchedulerWorkflowVersion `json:"defaultVersion"`
			RecordedVersion SchedulerWorkflowVersion `json:"recordedVersion"`
			CeilingCases    []struct {
				Ceiling       int `json:"ceiling"`
				OverrideCases []struct {
					Override    int                      `json:"override"`
					WantVersion SchedulerWorkflowVersion `json:"wantVersion"`
					WantCeiling int                      `json:"wantCeiling"`
				} `json:"overrideCases"`
			} `json:"ceilingCases"`
		} `json:"testCases"`
	}

	data, err := os.ReadFile(filepath.Join("testdata", "version_override_truth_table.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &table))
	require.NotEmpty(t, table.TestCases)

	for _, versionCase := range table.TestCases {
		for _, ceilingCase := range versionCase.CeilingCases {
			for _, overrideCase := range ceilingCase.OverrideCases {
				t.Run(fmt.Sprintf("default_%d/recorded_%d/ceiling_%d/override_%d", versionCase.DefaultVersion, versionCase.RecordedVersion, ceilingCase.Ceiling, overrideCase.Override), func(t *testing.T) {
					version, capturedCeiling := determineVersionTransition(versionCase.DefaultVersion, versionCase.RecordedVersion, ceilingCase.Ceiling, overrideCase.Override)
					require.Equal(t, overrideCase.WantVersion, version)
					require.Equal(t, overrideCase.WantCeiling, capturedCeiling)
				})
			}
		}
	}
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
