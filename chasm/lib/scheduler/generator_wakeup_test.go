package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestGeneratorNextWakeupTimeStaysAtPendingJitteredFire(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 10, 26, 21, 59, 59, 0, time.UTC)
	fire := time.Date(2026, 10, 26, 22, 8, 0, 459000000, time.UTC)
	followingFire := time.Date(2026, 10, 26, 22, 22, 43, 38000000, time.UTC)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(createdAt)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	realProcessor := scheduler.NewSpecProcessor(defaultConfig(), metrics.NoopMetricsHandler, logger, newLegacySpecBuilder(0, 0))

	// Record the real processor's output, rather than inferring it from a
	// previously queued timer that could hide an incorrect new wakeup time.
	type output struct {
		nextWakeupTime time.Time
		actions        int
	}
	var outputs []output
	processor := scheduler.NewMockSpecProcessor(gomock.NewController(t))
	processor.EXPECT().NextTime(gomock.Any(), gomock.Any()).DoAndReturn(realProcessor.NextTime).AnyTimes()
	processor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).DoAndReturn(func(
		s *scheduler.Scheduler, start, end time.Time, policy enumspb.ScheduleOverlapPolicy,
		workflowID, backfillID string, manual bool, limit *int,
	) (*scheduler.ProcessedTimeRange, error) {
		result, err := realProcessor.ProcessTimeRange(s, start, end, policy, workflowID, backfillID, manual, limit)
		if err == nil {
			outputs = append(outputs, output{result.NextWakeupTime, len(result.BufferedStarts)})
		}
		return result, err
	}).AnyTimes()
	schedule := defaultSchedule()
	schedule.Spec.Interval[0].Interval = durationpb.New(15 * time.Minute)
	schedule.Spec.Jitter = durationpb.New(10 * time.Minute)
	generatorPath, err := chasm.DefaultPathEncoder.Encode(nil, []string{"Generator"})
	require.NoError(t, err)
	var timerDeadlines []time.Time
	observeTimers := func(config *engineTestConfig) {
		config.engineOpts = append(config.engineOpts, chasmtest.WithInvariantCheck(func(_ *testing.T, node *chasm.Node, _ chasm.RootComponent) {
			timerDeadlines = nil
			generator := node.Snapshot(nil).Nodes[generatorPath]
			for _, task := range generator.GetMetadata().GetComponentAttributes().GetPureTasks() {
				timerDeadlines = append(timerDeadlines, task.GetScheduledTime().AsTime())
			}
		}))
	}
	env := newSchedulerTestEngine(t, schedule, withEngineTimeSource(timeSource), withEngineSpecProcessor(processor), observeTimers)
	requireTimerDeadline := func(expected time.Time) {
		require.NotEmpty(t, timerDeadlines)
		for _, deadline := range timerDeadlines {
			require.Equal(t, expected, deadline, "every persisted generator timer must use the expected wakeup time")
		}
	}

	// These fire times are fixed for namespace "ns-id" and schedule "sched-id".
	// No expected timestamp is calculated by the implementation under test.
	expected := []output{
		{fire, 0},          // Creation: the 22:00 nominal slot is still in the future.
		{fire, 0},          // 22:02: inside its jitter delay.
		{fire, 0},          // 22:04: another empty wakeup must return the same deadline.
		{fire, 0},          // 22:06: still waiting for the same slot.
		{followingFire, 1}, // At the fire time, generate it and advance to the next slot.
	}
	require.Equal(t, expected[:1], outputs)
	requireTimerDeadline(fire)
	for i, now := range []time.Time{
		time.Date(2026, 10, 26, 22, 2, 0, 0, time.UTC),
		time.Date(2026, 10, 26, 22, 4, 0, 0, time.UTC),
		time.Date(2026, 10, 26, 22, 6, 0, 0, time.UTC),
	} {
		timeSource.Update(now)
		require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
			s.Generator.Get(ctx).Generate(ctx)
			return nil
		}))
		require.Equal(t, expected[:i+2], outputs, "empty wakeup at %s", now)
		requireTimerDeadline(fire)
	}

	// Deliver the timer through CHASM's validation and transaction machinery.
	// A second delivery must neither regenerate the action nor recalculate a wakeup.
	timeSource.Update(fire)
	for range 2 {
		_, err := env.engine.FirePureTasks(env.rootRef, fire)
		require.NoError(t, err)
		require.Equal(t, expected, outputs)
		requireTimerDeadline(followingFire)
	}
}
