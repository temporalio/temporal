package queues

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
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
	var tail []float64

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
		if w >= warmup {
			if lastAggregate < minAggregate {
				minAggregate = lastAggregate
			}
			tail = append(tail, lastAggregate)
		}
	}

	t.Logf("aggregate admitted rate: final=%.1f min-after-warmup=%.1f budget=%.1f",
		lastAggregate, minAggregate, sharedBudget)

	// Generous bounds. The point is to catch a collapse to the floor or a runaway above the
	// ceiling, not to pin AIMD's steady state oscillation to a narrow band.
	// Mean over the settled tail rather than the final sample: a multiplicative law oscillates,
	// so a single reading measures where in the sawtooth the loop stopped, not where it settled.
	var sum float64
	for _, v := range tail {
		sum += v
	}
	mean := sum / float64(len(tail))
	t.Logf("settled aggregate: mean=%.1f final=%.1f budget=%.1f", mean, lastAggregate, sharedBudget)

	require.Greater(t, mean, sharedBudget*0.4,
		"aggregate collapsed far below the shared budget")
	require.Less(t, mean, sharedBudget*2.0,
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
	// One wake for fifty parked tasks, placed on the budget's schedule: at 1/s the next token
	// is a second away, which is also the window cap. The task's own backoff is far longer.
	require.Equal(t, now.Add(time.Second), gate.updates[0],
		"a denied class must retry on the budget's schedule, not at the task's own backoff")
}

// budgetRetryInterval lets the gate's estimate push the wait out but never pull it in. A class
// at a low rate waits the whole second its next token needs instead of re-asking ten times to
// learn nothing; a class at a high rate keeps the window fraction.
//
// The fraction must stay a floor. The bucket is shared by every shard's rescheduler on the host,
// so a per-shard estimate is computed as though this shard were the only consumer. Honouring a
// shorter estimate would make each shard poll at the whole class's refill rate - at 200/s a 5ms
// wake per shard, twenty times the cost, releasing the same tasks.
func TestReschedule_BudgetRetryIntervalOnlyEverWaitsLonger(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window time.Duration
		eta    time.Duration
		want   time.Duration
	}{
		{name: "no estimate falls back to a tenth of the window",
			window: 10 * time.Second, eta: 0, want: time.Second},
		{name: "no estimate, one second window",
			window: time.Second, eta: 0, want: 100 * time.Millisecond},
		{name: "no estimate, small window",
			window: 200 * time.Millisecond, eta: 0, want: 20 * time.Millisecond},

		{name: "an estimate longer than the fraction is honoured",
			window: time.Second, eta: 400 * time.Millisecond, want: 400 * time.Millisecond},

		// The regression this floor exists to prevent: a fast class must not poll faster.
		{name: "an estimate shorter than the fraction is floored to the fraction",
			window: time.Second, eta: 40 * time.Millisecond, want: 100 * time.Millisecond},
		{name: "a high rate class whose token is microseconds away still waits the fraction",
			window: time.Second, eta: 50 * time.Microsecond, want: 100 * time.Millisecond},

		// The rate moves at window close, so a longer wait could sleep through an increase.
		{name: "an estimate past the window is capped at the window",
			window: time.Second, eta: 30 * time.Second, want: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := defaultThrottleOverrides()
			state, _ := newTestThrottleStateWithWindow(o, tc.window)
			r := &reschedulerImpl{throttleState: state}
			require.Equal(t, tc.want, r.budgetRetryInterval(tc.eta))
		})
	}
}

// The estimate has to come from the gate, not be guessed by the caller: a denied class waits
// exactly as long as its own bucket needs, which is what turns a wasted wake into a productive
// one. At 4/s a denied class is one quarter of a second from its next token, so it must not be
// told to come back at the window fraction of 100ms and find nothing three times over.
func TestThrottleState_DeniedAdmitReportsWhenTheNextTokenArrives(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 4
	state, _ := newTestThrottleStateWithWindow(o, time.Second)

	key := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")

	// Burst is rate*window = 4 tokens. Drain them, then the next admit must be denied.
	for i := 0; i < 4; i++ {
		allowed, _, retryAfter := state.admit(key)
		require.True(t, allowed, "token %d of the burst should be admitted", i+1)
		require.Zero(t, retryAfter, "an admitted release reports no wait")
	}

	allowed, metered, retryAfter := state.admit(key)
	require.False(t, allowed, "the burst is spent, so this release must be denied")
	require.False(t, metered)
	require.Equal(t, 250*time.Millisecond, retryAfter,
		"at 4/s the next whole token is a quarter second away")
}

// A rate that cannot refill has no answer to give, and inventing one would park the class for a
// wait nothing will satisfy. Reporting zero hands the choice back to the caller's fallback.
func TestThrottleState_NonPositiveRateReportsNoEstimate(t *testing.T) {
	e := &throttleEntry{rate: 0, tokens: 0}
	require.Zero(t, e.tokenETALocked(), "a zero rate never refills, so there is no ETA to give")

	e = &throttleEntry{rate: -1, tokens: 0}
	require.Zero(t, e.tokenETALocked(), "a negative rate never refills either")

	// A rate so small the wait overflows a Duration must not wrap into a short one.
	e = &throttleEntry{rate: math.SmallestNonzeroFloat64, tokens: 0}
	require.Zero(t, e.tokenETALocked(), "an unrepresentable wait reports no estimate")
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

// A release the scheduler refused never reached the enforcer, so it cannot have been rejected.
// Leaving it in the denominator makes a saturated scheduler read as a run of clean windows: the
// class would climb to MaxRate having dispatched nothing, then dump a full burst the moment the
// scheduler drained, which is the storm the controller exists to prevent.
func TestThrottleState_ReturnedReleaseLeavesTheLossDenominator(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 100
	state, timeSource := newTestThrottleState(overrides)
	key := apsKey("ns-1")

	// Twenty admits the scheduler refused, then one real dispatch that was rejected.
	for i := 0; i < 20; i++ {
		require.True(t, state.Admit(key))
		state.Return(key)
	}
	require.True(t, state.Admit(key))
	state.ReportThrottled(key, true)
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, 85.0, state.AdmittedRate(key), 1e-9,
		"one dispatch, one rejection is total loss; the refused admits must not dilute it")
}

// Past the key cap the gate admits without tracking anything, so the release is not metered and
// a rejection from it is not evidence. Counting it would let a class the gate is not governing
// drive that class's rate down the moment the cap frees up.
func TestThrottleState_FailOpenAdmitIsNotMetered(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.maxKeys = 1
	state, _ := newTestThrottleState(overrides)

	tracked, overflow := apsKey("ns-tracked"), apsKey("ns-overflow")
	require.True(t, state.Admit(tracked))

	allowed, metered, _ := state.admit(overflow)
	require.True(t, allowed, "past the cap the real limiter stays the enforcement point")
	require.False(t, metered, "an untracked release must not be reported as metered")
}

// KeyTTL drives the idle reset, so a non positive value would make every access look idle and
// refill the bucket to its burst on every call - a silent fail open for tracked keys.
func TestThrottleState_NonPositiveKeyTTLStillEnforces(t *testing.T) {
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1
	overrides.minRate = 1
	overrides.keyTTL = 0
	state, timeSource := newTestThrottleState(overrides)
	key := apsKey("ns-1")

	require.True(t, state.Admit(key), "the burst allows the first release")
	for i := 0; i < 5; i++ {
		timeSource.Update(timeSource.Now().Add(time.Millisecond))
		require.False(t, state.Admit(key), "an invalid TTL must not refill the bucket")
	}
}

// The idle reset is the only thing that restores a stale rate: the sweep runs solely when a new
// key is inserted, and a host whose key set has gone stable never inserts one. Without it a
// class driven to the floor by an incident crawls back at the increase ratio instead of
// restarting fresh, which is minutes of the host's retry path pinned near one release a second.
func TestThrottleState_IdleKeyRestartsAtInitialRate(t *testing.T) {
	o := defaultThrottleOverrides()
	o.keyTTL = time.Minute
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	for i := 0; i < 40; i++ {
		state.ReportThrottled(key, true)
		closeWindow(state, timeSource, key)
	}
	require.InEpsilon(t, o.minRate, state.AdmittedRate(key), 1e-9, "driven to the floor")

	timeSource.Update(timeSource.Now().Add(2 * o.keyTTL))
	require.True(t, state.Admit(key), "the first touch after the retention period")

	require.InEpsilon(t, o.initialRate, state.AdmittedRate(key), 1e-9,
		"a class idle past its TTL must restart at InitialRate, not crawl up from the floor")
}

// A rate decided at a window close governs the time after that close, not the window that just
// ended. Refilling after the decision credits the elapsed second at the new rate, handing the
// class tokens it never earned and bringing every increase forward by a whole window.
func TestThrottleState_RefillCreditsTheWindowAtTheRateThatGovernedIt(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 100
	o.maxRate = 10000
	state, timeSource := newTestThrottleState(o)
	key := testKey()

	// Drain the whole burst inside the first window, with no rejections.
	drained := 0
	for state.Admit(key) {
		drained++
	}
	require.Equal(t, 100, drained, "burst is rate x window")

	// Close that window. It was clean, so the rate rises to 110 - but the second that just
	// elapsed ran at 100, so only 100 tokens were earned by it.
	timeSource.Update(timeSource.Now().Add(testThrottleWindow))

	refilled := 0
	for state.Admit(key) {
		refilled++
	}

	require.InEpsilon(t, 110.0, state.AdmittedRate(key), 1e-9, "the clean window earned an increase")
	require.Equal(t, 100, refilled,
		"the elapsed window must be credited at the rate that governed it, not at the new one")
}

// A NaN rate makes tokens NaN, and every comparison against NaN is false - including the
// tokens < 1 that decides admission. One bad dynamic config push would open the gate
// permanently while the controller still reported itself enabled.
func TestThrottleState_NaNInitialRateDoesNotOpenTheGate(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = math.NaN()
	o.minRate = 2
	state, _ := newTestThrottleState(o)

	key := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")

	// MinRate is 2 and the window is a second, so the burst is 2. A gate that has fallen open
	// would admit indefinitely instead of stopping there.
	admitted := 0
	for i := 0; i < 50; i++ {
		if allowed, _, _ := state.admit(key); allowed {
			admitted++
		}
	}
	require.Equal(t, 2, admitted,
		"a NaN initial rate must fall back to the floor, not disable the gate")
	require.False(t, math.IsNaN(state.AdmittedRate(key)), "the learned rate must not be NaN")
}

// The wall clock can step backwards under NTP. Liveness must only ever move forward: stamping
// an older time here makes the recovery step read as a full TTL of idleness, which resets a
// continuously busy class back to InitialRate and hands it a fresh burst.
func TestThrottleState_BackwardClockDoesNotResetABusyClass(t *testing.T) {
	o := defaultThrottleOverrides()
	o.initialRate = 5
	o.keyTTL = 5 * time.Minute
	state, ts := newTestThrottleState(o)

	key := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
	start := ts.Now()

	state.admit(key)
	// Drive the rate down so a reset to InitialRate would be visible.
	for i := 0; i < 6; i++ {
		ts.Update(start.Add(time.Duration(i+1) * time.Second))
		state.ReportThrottled(key, true)
		state.admit(key)
	}
	driven := state.AdmittedRate(key)
	require.Less(t, driven, float64(5), "the class should have been driven below InitialRate")

	// Step back further than the TTL, then recover past where we were. Neither may look like an
	// idle period. Without the guard the recovery reads as 10m of inactivity against a 5m TTL
	// and resets the class to InitialRate.
	ts.Update(start.Add(-10 * time.Minute))
	state.admit(key)
	ts.Update(start.Add(10 * time.Second))
	state.admit(key)

	after := state.AdmittedRate(key)
	require.Less(t, after, float64(5),
		"a clock step must not read as idleness and reset the rate to InitialRate")
	// The class keeps what it learned. It may still have taken an ordinary clean-window
	// increase on the way through, which is the control law working, not a reset.
	require.Greater(t, after, driven,
		"the surviving rate should be the learned one, carried forward")
	require.Less(t, after, driven*1.25,
		"only ordinary increases may apply; a jump beyond that is a reset in disguise")
}

// At or below zero the cap is already met by an empty map, so every class fails open and the
// controller gates nothing while still reporting itself enabled.
func TestThrottleState_NonPositiveMaxKeysStillEnforces(t *testing.T) {
	for _, maxKeys := range []int{0, -1} {
		o := defaultThrottleOverrides()
		o.maxKeys = maxKeys
		o.initialRate = 3
		state, _ := newTestThrottleState(o)

		key := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
		admitted := 0
		for i := 0; i < 50; i++ {
			if allowed, _, _ := state.admit(key); allowed {
				admitted++
			}
		}
		require.Equal(t, 3, admitted,
			"maxKeys %d must fall back to a usable cap, not disable the controller", maxKeys)
	}
}

// Past the cap the untracked population is every namespace the host has seen. Tagging those
// emissions by namespace moves the unbounded cardinality the cap exists to prevent out of the
// map and into the metrics pipeline.
func TestThrottleKey_CappedTagsOmitTheNamespace(t *testing.T) {
	key := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")

	require.Contains(t, throttleTagKeys(key.metricsTags()), "namespace_id",
		"a tracked key is bounded by the cap and keeps its namespace")
	require.NotContains(t, throttleTagKeys(key.cappedTags()), "namespace_id",
		"an untracked key must not carry per namespace cardinality")
	require.Contains(t, throttleTagKeys(key.cappedTags()), "resource_exhausted_cause",
		"the cause is still needed to tell the fail open paths apart")
}

func throttleTagKeys(tags []metrics.Tag) []string {
	keys := make([]string, 0, len(tags))
	for _, tag := range tags {
		keys = append(keys, tag.Key)
	}
	return keys
}
