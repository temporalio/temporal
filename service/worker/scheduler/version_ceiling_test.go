package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/payloads"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Tests for the worker.schedulerVersionCeiling dynamic config, which clamps the recorded
// TweakablePolicies.Version (e.g. during a cross-version multi-cluster migration with a
// rollback window to an older binary). Per-version behavioral coverage lives in the golden
// snapshot corpus (see versionguard); these tests cover the clamp mechanics themselves:
// the arithmetic, the marker wire shape, dynamic config transitions mid-run, exit paths,
// and the interplay with CHASM migration.

// tweakablePoliciesV1_23_1 mirrors tweakablePolicies as shipped in v1.23.1. Ref:
// https://github.com/temporalio/temporal/blob/v1.23.1/service/worker/scheduler/workflow.go#L147
type tweakablePoliciesV1_23_1 struct {
	DefaultCatchupWindow              time.Duration
	MinCatchupWindow                  time.Duration
	RetentionTime                     time.Duration
	CanceledTerminatedCountAsFailures bool
	AlwaysAppendTimestamp             bool
	FutureActionCount                 int
	RecentActionCount                 int
	FutureActionCountForList          int
	RecentActionCountForList          int
	IterationsBeforeContinueAsNew     int
	SleepWhilePaused                  bool
	MaxBufferSize                     int
	BackfillsPerIteration             int
	AllowZeroSleep                    bool
	ReuseTimer                        bool
	NextTimeCacheV2Size               int
	Version                           SchedulerWorkflowVersion
}

func TestClampVersion(t *testing.T) {
	v := SchedulerWorkflowVersion(12)
	for _, c := range []struct {
		ceiling int
		want    SchedulerWorkflowVersion
	}{
		{-1, 12}, {0, 12}, {1, 1}, {7, 7}, {12, 12}, {13, 12}, {99, 12},
	} {
		require.Equal(t, c.want, clampVersion(v, c.ceiling), "ceiling=%d", c.ceiling)
	}
}

// TestTweakablePoliciesJSONCompatibleWithV1_23Layout pins the wire shape of the "tweakables"
// MutableSideEffect marker against the struct layout shipped in v1.23.1.
func TestTweakablePoliciesJSONCompatibleWithV1_23Layout(t *testing.T) {
	p := CurrentTweakablePolicies
	p.Version = clampVersion(p.Version, 7) // the v1.23.1 ceiling

	dc := converter.GetDefaultDataConverter()
	pl, err := dc.ToPayload(p)
	require.NoError(t, err)

	var old tweakablePoliciesV1_23_1
	require.NoError(t, dc.FromPayload(pl, &old))
	require.Equal(t, tweakablePoliciesV1_23_1{
		DefaultCatchupWindow:              p.DefaultCatchupWindow,
		MinCatchupWindow:                  p.MinCatchupWindow,
		RetentionTime:                     p.RetentionTime,
		CanceledTerminatedCountAsFailures: p.CanceledTerminatedCountAsFailures,
		AlwaysAppendTimestamp:             p.AlwaysAppendTimestamp,
		FutureActionCount:                 p.FutureActionCount,
		RecentActionCount:                 p.RecentActionCount,
		FutureActionCountForList:          p.FutureActionCountForList,
		RecentActionCountForList:          p.RecentActionCountForList,
		IterationsBeforeContinueAsNew:     p.IterationsBeforeContinueAsNew,
		SleepWhilePaused:                  p.SleepWhilePaused,
		MaxBufferSize:                     p.MaxBufferSize,
		BackfillsPerIteration:             p.BackfillsPerIteration,
		AllowZeroSleep:                    p.AllowZeroSleep,
		ReuseTimer:                        p.ReuseTimer,
		NextTimeCacheV2Size:               p.NextTimeCacheV2Size,
		Version:                           p.Version,
	}, old)
}

// TestTweakablePoliciesJSONZeroFillAfterFailback pins the reverse decode direction: after
// failover plus rollback an older peer records 17-field tweakables markers; when the
// namespace later fails back, this binary replays them and the newer fields decode as zero
// values, which must be safe.
func TestTweakablePoliciesJSONZeroFillAfterFailback(t *testing.T) {
	// Non-default values for every v1.23.1 field so each one provably round-trips.
	old := tweakablePoliciesV1_23_1{
		DefaultCatchupWindow:              100 * time.Hour,
		MinCatchupWindow:                  5 * time.Second,
		RetentionTime:                     10 * 24 * time.Hour,
		CanceledTerminatedCountAsFailures: true,
		AlwaysAppendTimestamp:             false,
		FutureActionCount:                 15,
		RecentActionCount:                 12,
		FutureActionCountForList:          8,
		RecentActionCountForList:          6,
		IterationsBeforeContinueAsNew:     100,
		SleepWhilePaused:                  false,
		MaxBufferSize:                     500,
		BackfillsPerIteration:             5,
		AllowZeroSleep:                    false,
		ReuseTimer:                        false,
		NextTimeCacheV2Size:               20,
		Version:                           CANAfterSignals, // 7, v1.23.1's maximum
	}

	dc := converter.GetDefaultDataConverter()
	pl, err := dc.ToPayload(old)
	require.NoError(t, err)

	var current TweakablePolicies
	require.NoError(t, dc.FromPayload(pl, &current))

	// Newer fields zero-fill. Safe today: SpecFieldLengthLimit is read only behind the
	// LimitMemoSpecSize(11) gate, unreachable at recorded versions <= 7; the CHASM bools
	// zero to disabled.
	require.Zero(t, current.SpecFieldLengthLimit)
	require.False(t, current.EnableCHASMMigration)
	require.False(t, current.MigrateWithRunningWorkflows)

	// Shared fields round-trip exactly.
	require.Equal(t, TweakablePolicies{
		DefaultCatchupWindow:              old.DefaultCatchupWindow,
		MinCatchupWindow:                  old.MinCatchupWindow,
		RetentionTime:                     old.RetentionTime,
		CanceledTerminatedCountAsFailures: old.CanceledTerminatedCountAsFailures,
		AlwaysAppendTimestamp:             old.AlwaysAppendTimestamp,
		FutureActionCount:                 old.FutureActionCount,
		RecentActionCount:                 old.RecentActionCount,
		FutureActionCountForList:          old.FutureActionCountForList,
		RecentActionCountForList:          old.RecentActionCountForList,
		IterationsBeforeContinueAsNew:     old.IterationsBeforeContinueAsNew,
		SleepWhilePaused:                  old.SleepWhilePaused,
		MaxBufferSize:                     old.MaxBufferSize,
		BackfillsPerIteration:             old.BackfillsPerIteration,
		AllowZeroSleep:                    old.AllowZeroSleep,
		ReuseTimer:                        old.ReuseTimer,
		NextTimeCacheV2Size:               old.NextTimeCacheV2Size,
		Version:                           old.Version,
	}, current)
}

// runWithCeiling is run() with a worker.schedulerVersionCeiling closure threaded through,
// mirroring how fx.go passes dynamic config into the workflow. It asserts the workflow
// finishes by continue-as-new.
func (s *workflowSuite) runWithCeiling(sched *schedulepb.Schedule, iterations int, ceiling func() int) {
	s.runWorkflowFn(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithDeps(ctx, args, schedulerDeps{versionCeiling: ceiling})
	}, sched, iterations)
	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
}

// expectWatchRunningMaybe allows any number of watches on tracked workflows and reports
// them running. Needed because pre-DontTrackOverlapping logic tracks ALLOW_ALL runs in
// RunningWorkflows and the refresh path short-polls them (the very behavior v3 removed).
// Times(0) is load-bearing: it resets expectWatch's Once() back to unlimited, and Maybe()
// makes the expectation optional (the suite's established chaining idiom).
func (s *workflowSuite) expectWatchRunningMaybe() {
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		return &schedulespb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, nil
	}).Times(0).Maybe()
}

func allowAllSchedule(interval time.Duration) *schedulepb.Schedule {
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(interval)}},
		},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}
}

func (s *workflowSuite) TestVersionCeilingNoopWhenZeroNegativeOrAboveCurrent() {
	for _, ceiling := range []int{0, -5, int(CurrentTweakablePolicies.Version), 99} {
		s.T().Logf("ceiling %d", ceiling)
		s.env = s.NewTestWorkflowEnvironment()
		s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			s.Equal("myid-2022-06-01T00:05:00Z", req.Request.WorkflowId)
			return nil, nil
		})
		s.env.RegisterDelayedCallback(func() {
			s.Empty(s.runningWorkflows(), "current version must not track ALLOW_ALL runs")
		}, 7*time.Minute)

		s.runWithCeiling(allowAllSchedule(5*time.Minute), 2, func() int { return ceiling })
		s.env.AssertExpectations(s.T())
	}
}

func (s *workflowSuite) TestVersionCeilingAppearsMidRun() {
	// The ceiling closure is re-evaluated inside the "tweakables" MutableSideEffect every
	// iteration: when the dynamic config appears mid-run, a new marker is recorded and
	// subsequent iterations run the clamped decision procedure (the next-time cache
	// self-invalidates on the version change).
	s.runCeilingChangeMidRun(0, 2,
		nil,                                   // fires before the clamp must not be tracked
		[]string{"myid-2022-06-01T00:15:00Z"}, // fire after the clamp runs the pre-DontTrackOverlapping procedure
	)
}

func (s *workflowSuite) TestVersionCeilingLiftedMidRun() {
	// Lifting the ceiling mid-run is the same transition as a normal release bumping
	// CurrentTweakablePolicies.Version: a new marker is recorded and later iterations run
	// the current decision procedure (the 00:15 fire after the lift is not tracked).
	tracked := []string{"myid-2022-06-01T00:05:00Z", "myid-2022-06-01T00:10:00Z"}
	s.runCeilingChangeMidRun(2, 0, tracked, tracked)
}

// runCeilingChangeMidRun drives a 5-minute ALLOW_ALL schedule for fires at 00:05/00:10/00:15,
// flips the ceiling from before to after at 00:12, and asserts Info.RunningWorkflows at
// 00:13 and 00:17. Pass nil (not an empty slice) when no runs should be tracked.
func (s *workflowSuite) runCeilingChangeMidRun(before, after int, trackedAt13, trackedAt17 []string) {
	for _, id := range []string{
		"myid-2022-06-01T00:05:00Z",
		"myid-2022-06-01T00:10:00Z",
		"myid-2022-06-01T00:15:00Z",
	} {
		s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			s.Equal(id, req.Request.WorkflowId)
			return nil, nil
		})
	}
	s.expectWatchRunningMaybe()
	ceiling := before
	s.env.RegisterDelayedCallback(func() { ceiling = after }, 12*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.Equal(trackedAt13, s.runningWorkflows())
	}, 13*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.Equal(trackedAt17, s.runningWorkflows())
	}, 17*time.Minute)

	s.runWithCeiling(allowAllSchedule(5*time.Minute), 4, func() int { return ceiling })
}

func (s *workflowSuite) TestVersionCeilingRetentionExitClamped() {
	// At ceiling 7 the retention exit must fire at the same instant as the current
	// version: getLastEvent/getRetentionExpiration are unchanged since v1.23.1, and
	// UseLastAction(8) only gates LastProcessedTime, not retention inputs. The
	// ActionResultIncludesStatus(10) gate leaves the recorded statuses UNSPECIFIED.
	s.runLimitedActionsScheduleWithCeiling(7,
		enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
		enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
	)
}

func (s *workflowSuite) TestVersionCeilingRetentionExitUnclamped() {
	// Control: at the current version the first action's status is updated to COMPLETED by
	// the watcher result and the second stays RUNNING; same retention exit instant.
	s.runLimitedActionsScheduleWithCeiling(0,
		enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	)
}

// runLimitedActionsScheduleWithCeiling drives the TestExitScheduleWorkflowWhenNoActions
// scenario (15-minute interval, two limited actions at 00:15 and 00:30, first one observed
// COMPLETED at 00:30) under a version ceiling, asserting the recorded RecentActions
// statuses at 00:35 and the retention-time completion exit.
func (s *workflowSuite) runLimitedActionsScheduleWithCeiling(ceiling int, wantFirst, wantSecond enumspb.WorkflowExecutionStatus) {
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 15, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.expectWatch(func(req *schedulespb.WatchWorkflowRequest) (*schedulespb.WatchWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:15:00Z", req.Execution.WorkflowId)
		s.False(req.LongPoll)
		return &schedulespb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil
	})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.True(time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC).Equal(s.now()))
		s.Equal("myid-2022-06-01T00:30:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.env.RegisterDelayedCallback(func() {
		recent := s.describe().Info.RecentActions
		s.Require().Len(recent, 2)
		s.Equal(wantFirst, recent[0].StartWorkflowStatus)
		s.Equal(wantSecond, recent[1].StartWorkflowStatus)
	}, 35*time.Minute)

	s.runWorkflowFn(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithDeps(ctx, args, schedulerDeps{versionCeiling: func() int { return ceiling }})
	}, &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(15 * time.Minute)}},
		},
		State: &schedulepb.ScheduleState{
			LimitedActions:   true,
			RemainingActions: 2,
		},
	}, 5)
	s.True(s.env.IsWorkflowCompleted())
	s.False(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
	s.Equal(s.env.Now().Sub(time.Date(2022, 6, 1, 0, 30, 0, 0, time.UTC)), CurrentTweakablePolicies.RetentionTime)
}

func (s *workflowSuite) TestVersionCeilingBlocksMigrationDynamicConfig() {
	// With a clamp active, the tweakables getter zeroes EnableCHASMMigration, so DC-driven
	// CHASM migration cannot start; the schedule keeps firing and CANs normally.
	migrateCalls := 0
	s.env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).Maybe().Return(
		func(context.Context, *schedulerpb.CreateFromMigrationStateRequest) error {
			migrateCalls++
			return nil
		})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T01:00:00Z", req.Request.WorkflowId)
		return nil, nil
	})

	s.runWorkflowFn(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithDeps(ctx, args, schedulerDeps{
			enableCHASMMigration:        func() bool { return true },
			migrateWithRunningWorkflows: func() bool { return true },
			versionCeiling:              func() int { return 7 },
		})
	}, &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Hour)}},
		},
	}, 2)

	s.True(s.env.IsWorkflowCompleted())
	s.True(workflow.IsContinueAsNewError(s.env.GetWorkflowError()))
	s.Equal(0, migrateCalls, "DC-driven migration must not run while the ceiling is active")
}

func (s *workflowSuite) TestVersionCeilingDefersMigrationOnLiveSchedule() {
	// Like TestVersionCeilingDefersMigrationSignalUntilLifted, but the schedule is live:
	// migration stays deferred while the schedule keeps firing, and the run CANs normally.
	migrateCalls := 0
	s.env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).Maybe().Return(
		func(context.Context, *schedulerpb.CreateFromMigrationStateRequest) error {
			migrateCalls++
			return nil
		})
	s.expectStart(func(req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
		s.Equal("myid-2022-06-01T01:00:00Z", req.Request.WorkflowId)
		return nil, nil
	})
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(SignalNameMigrateToChasm, nil)
	}, 30*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.Equal(0, migrateCalls, "migration must stay deferred while the schedule fires")
	}, 90*time.Minute)

	s.runWithCeiling(&schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Hour)}},
		},
	}, 3, func() int { return 7 })
	s.Equal(0, migrateCalls)
}

func (s *workflowSuite) TestVersionCeilingDefersMigrationSignalUntilLifted() {
	// The migrate-to-chasm admin signal bypasses dynamic config; under a clamp the
	// execution gate defers it (PendingMigration stays set), and migration proceeds
	// automatically once the ceiling is lifted.
	ceiling := 7
	migrateCalls := 0
	s.env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).Maybe().Return(
		func(context.Context, *schedulerpb.CreateFromMigrationStateRequest) error {
			migrateCalls++
			return nil
		})
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(SignalNameMigrateToChasm, nil)
	}, 30*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.Equal(0, migrateCalls, "migration must be deferred while the ceiling is active")
		ceiling = 0
		// Wake the paused workflow so the next iteration picks up the lifted ceiling.
		s.env.SignalWorkflow(SignalNameMigrateToChasm, nil)
	}, 90*time.Minute)

	s.runWorkflowFn(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithDeps(ctx, args, schedulerDeps{
			migrateWithRunningWorkflows: func() bool { return true },
			versionCeiling:              func() int { return ceiling },
		})
	}, &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Hour)}},
		},
		State: &schedulepb.ScheduleState{Paused: true},
	}, 10)

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError(), "workflow should complete after the ceiling lift triggers the deferred migration")
	s.Equal(1, migrateCalls)
}

func (s *workflowSuite) TestVersionCeilingDefersMigrationAcrossCAN() {
	// A migration deferred by the ceiling must survive continue-as-new (PendingMigration
	// rides the CAN payload) and execute in a later run once the ceiling is lifted.
	// Cross-version note: PendingMigration is a v1.31.0+ proto field; an old binary
	// replaying the CAN payload ignores it, which is safe because the ceiling kept the
	// migration markers out of the history it replays.
	migrateCalls := 0
	s.env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).Maybe().Return(
		func(context.Context, *schedulerpb.CreateFromMigrationStateRequest) error {
			migrateCalls++
			return nil
		})
	// First signal sets PendingMigration (deferred under the clamp); second wakes the
	// paused workflow again so the iteration budget runs out and it CANs.
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(SignalNameMigrateToChasm, nil)
	}, 30*time.Minute)
	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(SignalNameMigrateToChasm, nil)
	}, 60*time.Minute)

	pausedSchedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Hour)}},
		},
		State: &schedulepb.ScheduleState{Paused: true},
	}
	s.runWorkflowFn(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithDeps(ctx, args, schedulerDeps{versionCeiling: func() int { return 7 }})
	}, pausedSchedule, 2)

	s.True(s.env.IsWorkflowCompleted())
	s.Equal(0, migrateCalls, "migration must not run in the clamped first run")
	var canErr *workflow.ContinueAsNewError
	s.Require().ErrorAs(s.env.GetWorkflowError(), &canErr)
	var canArgs schedulespb.StartScheduleArgs
	s.Require().NoError(payloads.Decode(canErr.Input, &canArgs))
	s.Require().NotNil(canArgs.State)
	s.True(canArgs.State.PendingMigration, "deferred migration must survive CAN")
	endOfRun1 := s.env.Now()

	// Second run: fresh env, ceiling lifted; the deferred migration executes immediately.
	s.env = s.NewTestWorkflowEnvironment()
	s.env.OnActivity(new(activities).MigrateScheduleToChasm, mock.Anything, mock.Anything).Return(
		func(context.Context, *schedulerpb.CreateFromMigrationStateRequest) error {
			migrateCalls++
			return nil
		})
	s.env.SetStartTime(endOfRun1)
	s.env.ExecuteWorkflow(func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithDeps(ctx, args, schedulerDeps{versionCeiling: func() int { return 0 }})
	}, &canArgs)

	s.True(s.env.IsWorkflowCompleted())
	s.Require().NoError(s.env.GetWorkflowError(), "second run completes via migration after the lift")
	s.Equal(1, migrateCalls)
}
