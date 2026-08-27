package queues

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.uber.org/mock/gomock"
)

// A zero per pass release cap must mean unlimited, matching the unset default. Read literally
// it would compare 0 >= 0 before the first release, so every gated class would break, release
// nothing, and wake again forever without draining.
func TestReschedule_ZeroMaxThrottledReleasesMeansUnlimited(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 100
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state, 0)

	key := apsKey("ns-1")
	for i := 0; i < 10; i++ {
		e := newThrottledExecutable(ctrl, key, true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		r.Add(e, now)
	}

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).AnyTimes()

	r.reschedule()

	require.Zero(t, r.Len(), "a zero cap must not stall gated releases")
}

// One pass must release everything the budget allows, not one task per pass. If a change made
// releases one-per-pass, throughput would silently become a function of the poll interval
// rather than of the admitted rate, and the poll rate would have to rise to compensate.
func TestReschedule_PassReleasesFullBudgetNotOnePerPass(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	// burst = rate * window, so an initial rate of 5 over a one second window is 5 tokens.
	overrides.initialRate = 5
	overrides.minRate = 5
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state, 1000)

	key := apsKey("ns-1")
	for i := 0; i < 20; i++ {
		e := newThrottledExecutable(ctrl, key, true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		r.Add(e, now)
	}

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).AnyTimes()

	r.reschedule()

	released := 20 - r.Len()
	require.Equal(t, 5, released, "a single pass must drain the whole burst, not one task")
}

// The controller is host level, so N hosts run N independent controllers against one shared
// namespace budget. This is the closest unit level analogue of the multi host behaviour and
// guards the two failure modes seen on the cluster: the aggregate ratcheting down to the floor,
// and the aggregate running away above the shared ceiling.
func TestThrottleState_IndependentInstancesConvergeOnSharedBudget(t *testing.T) {
	const (
		hosts        = 8
		sharedBudget = 200.0
		windows      = 120
		warmup       = 20
	)

	states := make([]*ThrottleState, hosts)
	clocks := make([]*clock.EventTimeSource, hosts)
	for i := range states {
		o := defaultThrottleOverrides()
		o.initialRate = sharedBudget / hosts
		o.maxRate = sharedBudget
		states[i], clocks[i] = newTestThrottleState(o)
	}

	key := apsKey("ns-1")
	now := clocks[0].Now()

	// Offered load is deliberately uneven. Real hosts own different shard counts, and identical
	// stimulus would make any lockstep an artifact of the test rather than a property of AIMD.
	offered := [hosts]int{60, 50, 45, 40, 35, 30, 25, 20}

	aggregate := func() float64 {
		var total float64
		for _, s := range states {
			total += s.AdmittedRate(key)
		}
		return total
	}

	// Seeded on the first post warmup window: before the first Admit the key does not exist
	// yet and AdmittedRate reports 0, which would pin the minimum at zero for the whole run.
	minAggregate := math.Inf(1)
	var lastAggregate float64

	for w := 0; w < windows; w++ {
		now = now.Add(testThrottleWindow)
		for _, c := range clocks {
			c.Update(now)
		}

		admitted := [hosts]int{}
		total := 0
		for i, s := range states {
			for j := 0; j < offered[i]; j++ {
				if s.Admit(key) {
					admitted[i]++
					total++
				}
			}
		}

		// The shared limiter rejects everything past its ceiling. Charge the overflow back in
		// proportion to what each host actually sent, which is what the real limiter does.
		if float64(total) > sharedBudget {
			over := float64(total) - sharedBudget
			for i, s := range states {
				if admitted[i] == 0 {
					continue
				}
				// One report per rejected release. Reporting a single throttle for a whole
				// window would understate loss for a large host and overstate it for a small
				// one, which is exactly the size dependence the loss ratio removes.
				rejected := int(math.Round(over * float64(admitted[i]) / float64(total)))
				for j := 0; j < rejected; j++ {
					s.ReportThrottled(key, true)
				}
			}
		}

		lastAggregate = aggregate()
		if w >= warmup && lastAggregate < minAggregate {
			minAggregate = lastAggregate
		}
	}

	t.Logf("aggregate admitted rate: final=%.1f min-after-warmup=%.1f budget=%.1f",
		lastAggregate, minAggregate, sharedBudget)

	// Generous bounds. The point is to catch a collapse to the floor or a runaway above the
	// ceiling, not to pin AIMD's steady state oscillation to a narrow band.
	require.Greater(t, lastAggregate, sharedBudget*0.4,
		"aggregate collapsed far below the shared budget")
	require.Less(t, lastAggregate, sharedBudget*2.0,
		"aggregate ran away above the shared budget")
	require.Greater(t, minAggregate, sharedBudget*0.10,
		"aggregate suffered a sustained collective collapse")

	for i, s := range states {
		decreases, increases := s.Counters(key)
		require.Positive(t, decreases, "host %d never decreased; it is not seeing back pressure", i)
		require.Positive(t, increases, "host %d never increased; it is stuck at the floor", i)
		require.Greater(t, s.AdmittedRate(key), defaultThrottleOverrides().minRate,
			"host %d ratcheted down to MinRate", i)
	}
}

// A key that is denied must not be denied more than once per pass. The rescheduler breaks the
// class on the first denial, so the denial counters measure refused polls rather than tasks
// held back, and a change that made them per task would make the metric mean something else.
func TestReschedule_DeniedClassIsProbedOncePerPass(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1
	overrides.minRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, gate := newTestRescheduler(t, ctrl, timeSource, state, 1000)

	key := apsKey("ns-1")
	// Drain the initial burst so the class is over budget for the whole test.
	for state.Admit(key) { //nolint:revive // draining, body intentionally empty
	}

	for i := 0; i < 50; i++ {
		e := newThrottledExecutable(ctrl, key, true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		r.Add(e, now)
	}

	scheduler.EXPECT().TrySubmit(gomock.Any()).Times(0)

	gate.updates = nil
	r.reschedule()

	require.Equal(t, 50, r.Len(), "a denied pass must release nothing")
	require.Len(t, gate.updates, 1, "a denied pass must set exactly one wake")
	require.Equal(t, now.Add(testThrottleWindow/10), gate.updates[0],
		"a denied class must retry inside the control window, not at the task's own backoff")
}

// budgetRetryInterval is derived from the control window, so shrinking the window shrinks the
// poll interval proportionally. The floor is the only thing standing between a small window and
// a thousand wakes per second per gated shard.
func TestReschedule_BudgetRetryIntervalTracksWindow(t *testing.T) {
	for _, tc := range []struct {
		window time.Duration
		want   time.Duration
	}{
		{window: 10 * time.Second, want: time.Second},
		{window: time.Second, want: 100 * time.Millisecond},
		{window: 200 * time.Millisecond, want: 20 * time.Millisecond},
	} {
		o := defaultThrottleOverrides()
		state, _ := newTestThrottleStateWithWindow(o, tc.window)
		r := &reschedulerImpl{throttleState: state}
		require.Equal(t, tc.want, r.budgetRetryInterval(),
			"window %s should give a retry interval of %s", tc.window, tc.want)
	}
}

// The admitted flag must be set before the task reaches the scheduler. TrySubmit hands the
// executable to a worker that can reach HandleErr immediately, and a rejection the gate is not
// recorded as having issued is discarded by the control law - silently losing the only
// feedback it acts on.
func TestReschedule_AdmittedIsMarkedBeforeSubmit(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 10
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state, 1000)

	key := apsKey("ns-1")
	e := newThrottledExecutable(ctrl, key, true)
	e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	r.Add(e, now)

	// Observe the flag from inside TrySubmit, which is where a worker would first see it.
	var admittedAtSubmit bool
	scheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(Executable) bool {
		admittedAtSubmit = e.admitted
		return true
	})

	r.reschedule()

	require.True(t, admittedAtSubmit,
		"the executable must already be marked admitted when it reaches the scheduler")
}

// A submit that fails means the gate issued no dispatch, so the mark must be taken back or a
// later rejection this gate never caused would be counted against the control law.
func TestReschedule_FailedSubmitUnmarksAdmitted(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 10
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state, 1000)

	key := apsKey("ns-1")
	e := newThrottledExecutable(ctrl, key, true)
	e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	r.Add(e, now)

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(false)

	r.reschedule()

	require.False(t, e.admitted, "a failed submit must not leave the task marked admitted")
	require.Equal(t, 1, r.Len(), "the task stays parked")
}
