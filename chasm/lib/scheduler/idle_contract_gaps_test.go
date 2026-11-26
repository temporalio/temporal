//go:build scheduler_contract

package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
)

// These executable specifications assert stronger guarantees than the current
// hotfix provides. Run with -tags test_dep,scheduler_contract to expose gaps;
// they deliberately fail until the corresponding behavior is implemented.
func TestIdleContractPendingStartsSurviveDeadline(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"invoker delayed", "generator delayed"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			fire := pendingCreateTime.Add(idleWeek + time.Hour).Truncate(time.Second)
			e := newIdleGuaranteesEnv(t, idleGuaranteesSchedule(fire))
			if scenario == "invoker delayed" {
				e.fireAt(t, fire.Add(10*time.Millisecond))
			}
			// Default catchup allows this action for a year. Neither delayed
			// generation nor a stalled invoker should silently abandon it.
			e.fireAt(t, fire.Add(idleWeek+time.Hour))
			e.fireAt(t, e.timeSource.Now())
		})
	}
}

func TestIdleContractStartRPCOutlivesDeadline(t *testing.T) {
	t.Parallel()
	testIdleStartRPCInFlight(t, idleWeek+time.Hour)
}

func TestIdleContractUpgradePreservesIdleTimer(t *testing.T) {
	t.Parallel()
	// Six days lets the old binary queue its last start without immediately
	// expiring its original seven-day timer. No replacement timer exists yet.
	fire := pendingCreateTime.Add(6 * 24 * time.Hour).Truncate(time.Second)
	e := newIdleGuaranteesEnv(t, idleGuaranteesSchedule(fire))
	e.tweakables.EnablePendingStartIdleFix = false
	e.fireAt(t, fire)
	oldDeadline := pendingCreateTime.Add(idleWeek)
	requireSchedulerState(t, e.schedulerTestEngine, func(s *scheduler.Scheduler, ctx chasm.Context) {
		require.Equal(t, oldDeadline, s.IdleCloseTime.AsTime())
		require.Empty(t, s.Invoker.Get(ctx).GetBufferedStarts()[0].GetRunId())
	})

	// Validation after upgrade must not remove the sole close path. The
	// observer catches the missing timer even while an execute task remains.
	e.tweakables.EnablePendingStartIdleFix = true
	e.fireAt(t, oldDeadline)
}

func TestIdleContractFutureBackfillDoesNotExtendRetention(t *testing.T) {
	t.Parallel()
	// The handler records wall-clock start time, so choose a calendar label
	// guaranteed to be later even as this test is run in future years.
	testIdleBackfillRetention(t, time.Date(2090, 1, 1, 0, 0, 0, 0, time.UTC).Sub(pendingCreateTime))
}
