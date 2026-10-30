package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestGeneratorJitterFixKillswitch(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		enabled        bool
		pendingActions int
	}{
		{name: "enabled", enabled: true, pendingActions: 1},
		{name: "disabled", enabled: false, pendingActions: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			createdAt := time.Date(2026, 10, 26, 21, 59, 59, 0, time.UTC)
			fire := time.Date(2026, 10, 26, 22, 8, 0, 459000000, time.UTC)
			followingFire := time.Date(2026, 10, 26, 22, 22, 43, 38000000, time.UTC)
			ts := clock.NewEventTimeSource()
			ts.Update(createdAt)
			tweakables := scheduler.DefaultTweakables
			require.True(t, tweakables.EnableGeneratorJitterFix, "the fix must be enabled by default")
			config := defaultConfig()
			config.Tweakables = func(ns string) scheduler.Tweakables {
				require.Equal(t, namespace, ns)
				return tweakables
			}
			processor := scheduler.NewSpecProcessor(config, metrics.NoopMetricsHandler,
				testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly), newLegacySpecBuilder(0, 0))
			schedule := defaultSchedule()
			schedule.Spec.Interval[0].Interval = durationpb.New(15 * time.Minute)
			schedule.Spec.Jitter = durationpb.New(10 * time.Minute)
			env := newSchedulerTestEngine(t, schedule, withEngineTimeSource(ts), withEngineSpecProcessor(processor))

			// Change the setting after creation to exercise existing schedules.
			tweakables.EnableGeneratorJitterFix = tc.enabled
			if !tc.enabled {
				require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
					// Manual backfill progress is unaffected by the generator switch.
					result, err := processor.ProcessTimeRange(s, createdAt, createdAt.Add(4*time.Minute),
						schedule.Policies.OverlapPolicy, s.WorkflowID(), "backfill", true, nil)
					require.NoError(t, err)
					require.Equal(t, createdAt, result.LastActionTime)
					require.Equal(t, fire, result.NextWakeupTime)
					result, err = processor.ProcessTimeRange(s, result.LastActionTime, fire,
						schedule.Policies.OverlapPolicy, s.WorkflowID(), "backfill", true, nil)
					require.NoError(t, err)
					require.Len(t, result.BufferedStarts, 1)
					return nil
				}))
			}
			for _, now := range []time.Time{
				time.Date(2026, 10, 26, 22, 2, 0, 0, time.UTC),
				time.Date(2026, 10, 26, 22, 4, 0, 0, time.UTC),
			} {
				ts.Update(now)
				require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
					s.Generator.Get(ctx).Generate(ctx)
					return nil
				}))
				require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
					cursor := createdAt
					if !tc.enabled {
						cursor = now
					}
					generator := s.Generator.Get(ctx)
					require.Equal(t, cursor, generator.LastProcessedTime.AsTime())
					require.Equal(t, now, generator.LastProcessedTaskTime.AsTime())
					return nil
				}))
			}

			// CHASM validates and executes the original timer. Disabling the fix
			// reproduces the old skip, while task completion still invalidates it.
			ts.Update(fire)
			for range 2 {
				_, err := env.engine.FirePureTasks(env.rootRef, fire)
				require.NoError(t, err)
				require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
					require.Len(t, s.Invoker.Get(ctx).BufferedStarts, tc.pendingActions)
					require.Equal(t, fire, s.Generator.Get(ctx).LastProcessedTaskTime.AsTime())
					return nil
				}))
			}

			// Re-enable without rewriting state. The next eligible slot still
			// fires once; a slot skipped while disabled cannot be recovered.
			tweakables.EnableGeneratorJitterFix = true
			ts.Update(followingFire)
			for range 2 {
				_, err := env.engine.FirePureTasks(env.rootRef, followingFire)
				require.NoError(t, err)
				require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
					require.Len(t, s.Invoker.Get(ctx).BufferedStarts, tc.pendingActions+1)
					return nil
				}))
			}
		})
	}
}
