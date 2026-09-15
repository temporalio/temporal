package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
	"go.uber.org/mock/gomock"
)

type (
	// throttledExecutable is a MockExecutable that also reports a throttle controller key,
	// which is what routes it into a gated rescheduler class.
	throttledExecutable struct {
		*MockExecutable
		key      ThrottleKey
		known    bool
		admitted bool
	}

	// recordingGate counts Update calls so a test can assert that one reschedule pass wakes
	// the loop exactly once.
	recordingGate struct {
		fireCh  chan struct{}
		updates []time.Time
	}
)

func (e *throttledExecutable) ThrottleKey() (ThrottleKey, bool) {
	return e.key, e.known
}

func (e *throttledExecutable) SetThrottleAdmitted(key ThrottleKey) {
	e.admitted = key != ThrottleKey{}
}

func (g *recordingGate) FireCh() <-chan struct{}    { return g.fireCh }
func (g *recordingGate) FireAfter(_ time.Time) bool { return false }
func (g *recordingGate) Close()                     {}
func (g *recordingGate) Update(next time.Time) bool {
	g.updates = append(g.updates, next)
	return true
}

func newThrottledExecutable(ctrl *gomock.Controller, key ThrottleKey, known bool) *throttledExecutable {
	mock := NewMockExecutable(ctrl)
	mock.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
	mock.EXPECT().SetScheduledTime(gomock.Any()).AnyTimes()
	return &throttledExecutable{MockExecutable: mock, key: key, known: known}
}

func newTestRescheduler(
	t testing.TB,
	ctrl *gomock.Controller,
	timeSource clock.TimeSource,
	state *ThrottleState,
	maxThrottledReleasesPerPass int,
) (*reschedulerImpl, *MockScheduler, *recordingGate) {
	t.Helper()

	scheduler := NewMockScheduler(ctrl)
	scheduler.EXPECT().ChannelWeightFn().Return(nil).AnyTimes()
	scheduler.EXPECT().TaskChannelKeyFn().Return(
		func(e Executable) TaskChannelKey { return TaskChannelKey{NamespaceID: e.GetNamespaceID()} },
	).AnyTimes()

	r := NewRescheduler(
		scheduler,
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		state,
		dynamicconfig.GetIntPropertyFn(maxThrottledReleasesPerPass),
	)
	gate := &recordingGate{fireCh: make(chan struct{}, 1)}
	r.timerGate = gate
	return r, scheduler, gate
}

func apsKey(namespaceID string) ThrottleKey {
	return NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, namespaceID)
}

// A class waiting on a throttle budget must not hold up a class that failed for an unrelated
// reason and is ready now.
func TestReschedule_ThrottledClassDoesNotBlockHealthyClass(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state, 1000)

	key := apsKey("ns-throttled")
	// Drain the class's single burst token so the next pass has no budget for it.
	require.True(t, admitOK(state, key))

	throttled := newThrottledExecutable(ctrl, key, true)
	throttled.EXPECT().GetNamespaceID().Return("ns-throttled").AnyTimes()
	healthy := newThrottledExecutable(ctrl, ThrottleKey{}, false)
	healthy.EXPECT().GetNamespaceID().Return("ns-healthy").AnyTimes()

	r.Add(throttled, now)
	r.Add(healthy, now)

	submitted := make([]Executable, 0, 2)
	scheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(e Executable) bool {
		submitted = append(submitted, e)
		return true
	}).AnyTimes()

	r.reschedule()

	require.Len(t, submitted, 1)
	require.Same(t, Executable(healthy), submitted[0])
	require.Equal(t, 1, r.Len())
}

// The release budget is a ceiling, not a quota: per task backoff still governs eligibility, and
// the rescheduler must never reach past a class head that is not due yet.
func TestReschedule_BudgetIsCeilingNotQuota(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1000
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, gate := newTestRescheduler(t, ctrl, timeSource, state, 1000)

	key := apsKey("ns-1")
	notDue := newThrottledExecutable(ctrl, key, true)
	notDue.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	r.Add(notDue, now.Add(time.Minute))

	scheduler.EXPECT().TrySubmit(gomock.Any()).Times(0)

	gate.updates = nil
	r.reschedule()

	require.Equal(t, 1, r.Len(), "budget must not pull forward a task that is not due")
	require.Len(t, gate.updates, 1)
	require.Equal(t, now.Add(time.Minute), gate.updates[0])

	// The due check deliberately precedes admission. Reversing them would burn a token every
	// pass on a task that is never released, and at a 100ms poll the class would be denied by
	// the time its head finally came due.
	require.Zero(t, state.Len(),
		"a not-due head must not reach the gate at all, so the class is not even created")
}

// One pass must wake the loop once, at the earliest of every reason it has to wake.
func TestReschedule_SingleTimerGateUpdatePerPass(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, stateClock := newTestThrottleState(defaultThrottleOverrides())

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, gate := newTestRescheduler(t, ctrl, timeSource, state, 1000)
	scheduler.EXPECT().TrySubmit(gomock.Any()).Times(0)

	for i, delay := range []time.Duration{5 * time.Minute, time.Minute, 3 * time.Minute} {
		e := newThrottledExecutable(ctrl, ThrottleKey{}, false)
		namespaceID := string(rune('a' + i))
		e.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		r.Add(e, now.Add(delay))
	}

	gate.updates = nil
	r.reschedule()

	require.Len(t, gate.updates, 1)
	require.Equal(t, now.Add(time.Minute), gate.updates[0], "wake at the running minimum")
}

// With a global cap on throttled releases, the pass cursor must rotate so that a class late in
// Go's map order is not permanently starved.
func TestReschedule_RoundRobinAcrossThrottledClasses(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1000
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state, 1)

	namespaces := []string{"ns-a", "ns-b", "ns-c"}
	for _, namespaceID := range namespaces {
		for i := 0; i < 3; i++ {
			e := newThrottledExecutable(ctrl, apsKey(namespaceID), true)
			e.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
			r.Add(e, now)
		}
	}

	released := make(map[string]int)
	scheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(e Executable) bool {
		released[e.GetNamespaceID()]++
		return true
	}).AnyTimes()

	for i := 0; i < len(namespaces); i++ {
		r.reschedule()
	}

	require.Len(t, released, len(namespaces), "every class must get a turn")
	for _, namespaceID := range namespaces {
		require.Equal(t, 1, released[namespaceID])
	}
}

// A class that is over its admitted rate stops draining, and comes back inside the control
// window rather than at the head's own much longer backoff.
func TestReschedule_BudgetDeniedWakesInsideControlWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, gate := newTestRescheduler(t, ctrl, timeSource, state, 1000)

	key := apsKey("ns-1")
	require.True(t, admitOK(state, key))

	for i := 0; i < 5; i++ {
		e := newThrottledExecutable(ctrl, key, true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		r.Add(e, now)
	}

	scheduler.EXPECT().TrySubmit(gomock.Any()).Times(0)

	gate.updates = nil
	r.reschedule()

	require.Equal(t, 5, r.Len())
	require.Len(t, gate.updates, 1)
	// At 1/s the next whole token is a second out, which is also the window cap. The point is
	// that the class comes back on the budget's schedule and not on the task's own backoff,
	// which is far longer.
	require.Equal(t, now.Add(time.Second), gate.updates[0])
}

// A disabled controller must leave the rescheduler behaving exactly as it did before.
func TestReschedule_DisabledControllerReleasesEverythingDue(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.enabled = false
	overrides.initialRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state, 1)

	for i := 0; i < 10; i++ {
		e := newThrottledExecutable(ctrl, apsKey("ns-1"), true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		r.Add(e, now)
	}

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(10)

	r.reschedule()

	require.Zero(t, r.Len())
}

// The same cause reported at two different scopes is two different budgets. They must not share
// a class, or whichever arrived last would decide how the other drains.
func TestReschedule_DifferentCausesAreDifferentClasses(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state, 1000)

	persistence := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "ns-1")
	aps := NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, "ns-1")
	require.NotEqual(t, persistence, aps)

	// Exhaust only the namespace scoped budget.
	require.True(t, admitOK(state, persistence))

	blocked := newThrottledExecutable(ctrl, persistence, true)
	blocked.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	free := newThrottledExecutable(ctrl, aps, true)
	free.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()

	r.Add(blocked, now)
	r.Add(free, now)

	submitted := make([]Executable, 0, 2)
	scheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(e Executable) bool {
		submitted = append(submitted, e)
		return true
	}).AnyTimes()

	r.reschedule()

	require.Len(t, submitted, 1)
	require.Same(t, Executable(free), submitted[0])
}

// A release the gate paid for but the scheduler refused must not cost the class a token.
func TestReschedule_FailedSubmitReturnsTheToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state, 1000)

	key := apsKey("ns-1")
	e := newThrottledExecutable(ctrl, key, true)
	e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	r.Add(e, now)

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(false).Times(1)
	r.reschedule()
	require.Equal(t, 1, r.Len())

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(1)
	r.reschedule()
	require.Zero(t, r.Len(), "the token survived the refused submit")
	require.True(t, e.admitted, "a gated release must tell the task the controller metered it")
}

// weightsByPriority is the ChannelWeightFn a mock scheduler reports when a test needs the
// rescheduler to order gated classes by priority.
func weightsByPriority(weights map[ctasks.Priority]int) ChannelWeightFn {
	return func(key TaskChannelKey) int { return weights[key.Priority] }
}

// One namespace's budget is one bucket shared by every priority in it, and a class drains until
// the gate refuses it. So whichever class is offered the bucket first takes what is in it, and
// offering them in insertion order lets preemptable work spend the budget ahead of high priority
// work. That matters here and not below, because the gate sits upstream of the IWRR scheduler:
// a task the gate never releases is never submitted for IWRR to deprioritise.
func TestReschedule_HighPriorityGetsTheBudgetFirst(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	// burst = rate x window, so exactly one release is available this pass.
	overrides.initialRate = 1
	overrides.minRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	mock := NewMockScheduler(ctrl)
	mock.EXPECT().TaskChannelKeyFn().Return(
		func(e Executable) TaskChannelKey {
			return TaskChannelKey{NamespaceID: e.GetNamespaceID(), Priority: e.GetPriority()}
		},
	).AnyTimes()
	mock.EXPECT().ChannelWeightFn().Return(weightsByPriority(map[ctasks.Priority]int{
		ctasks.PriorityHigh:        10,
		ctasks.PriorityPreemptable: 1,
	})).AnyTimes()

	r := NewRescheduler(mock, timeSource, log.NewTestLogger(), metrics.NoopMetricsHandler,
		state, dynamicconfig.GetIntPropertyFn(1000))
	r.timerGate = &recordingGate{fireCh: make(chan struct{}, 1)}

	key := apsKey("ns-1")
	// Added first, so insertion order would offer it the bucket first.
	preemptable := newThrottledExecutable(ctrl, key, true)
	preemptable.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	preemptable.EXPECT().GetPriority().Return(ctasks.PriorityPreemptable).AnyTimes()
	high := newThrottledExecutable(ctrl, key, true)
	high.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	high.EXPECT().GetPriority().Return(ctasks.PriorityHigh).AnyTimes()

	r.Add(preemptable, now)
	r.Add(high, now)

	var submitted []Executable
	mock.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(e Executable) bool {
		submitted = append(submitted, e)
		return true
	}).AnyTimes()

	r.reschedule()

	require.Len(t, submitted, 1, "the burst allows exactly one release")
	require.Same(t, Executable(high), submitted[0],
		"the single token must go to high priority, not to whichever class was added first")
}

// A scheduler that cannot report weights must leave the order exactly as it was, so nothing
// changes for a caller that does not implement ChannelWeightProvider.
func TestReschedule_NoWeightsKeepsInsertionOrder(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1
	overrides.minRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	// newTestRescheduler uses a plain MockScheduler, which provides no weights.
	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state, 1000)
	require.Nil(t, r.channelWeightFn, "a plain scheduler must not supply weights")

	key := apsKey("ns-1")
	first := newThrottledExecutable(ctrl, key, true)
	first.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	second := newThrottledExecutable(ctrl, key, true)
	second.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()

	r.Add(first, now)
	r.Add(second, now)

	var submitted []Executable
	scheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(e Executable) bool {
		submitted = append(submitted, e)
		return true
	}).AnyTimes()

	r.reschedule()

	require.Len(t, submitted, 1)
	require.Same(t, Executable(first), submitted[0], "without weights, insertion order stands")
}

// Low priority is deliberately not released while a higher priority has demand, and this pins
// that so it is not "fixed" later as a starvation bug.
//
// Both governed enforcers cascade priority: a request consumes its own priority's tokens and
// every lower priority's too, so while high priority saturates the budget a preemptable dispatch
// is refused no matter what the rescheduler does. Releasing it would spend a token on an attempt
// that cannot succeed - and because this class's key carries no priority, the rejection is
// charged here and drags the admitted rate down for the high priority work sharing it.
func TestReschedule_LowPriorityIsNotReleasedAheadOfDemand(t *testing.T) {
	ctrl := gomock.NewController(t)
	o := defaultThrottleOverrides()
	// A rate far below demand, so every pass is a contest for the same two tokens.
	o.initialRate, o.minRate, o.maxRate = 2, 2, 2
	state, stateClock := newTestThrottleState(o)

	now := stateClock.Now()
	ts := clock.NewEventTimeSource()
	ts.Update(now)

	mock := NewMockScheduler(ctrl)
	mock.EXPECT().TaskChannelKeyFn().Return(
		func(e Executable) TaskChannelKey {
			return TaskChannelKey{NamespaceID: e.GetNamespaceID(), Priority: e.GetPriority()}
		},
	).AnyTimes()
	mock.EXPECT().ChannelWeightFn().Return(weightsByPriority(map[ctasks.Priority]int{
		ctasks.PriorityHigh: 10, ctasks.PriorityPreemptable: 1,
	})).AnyTimes()

	r := NewRescheduler(mock, ts, log.NewTestLogger(), metrics.NoopMetricsHandler,
		state, dynamicconfig.GetIntPropertyFn(1000))
	r.timerGate = &recordingGate{fireCh: make(chan struct{}, 1)}

	key := apsKey("ns-1")
	for _, tc := range []struct {
		priority ctasks.Priority
		count    int
	}{{ctasks.PriorityHigh, 400}, {ctasks.PriorityPreemptable, 100}} {
		for i := 0; i < tc.count; i++ {
			e := newThrottledExecutable(ctrl, key, true)
			e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
			e.EXPECT().GetPriority().Return(tc.priority).AnyTimes()
			r.Add(e, now)
		}
	}

	released := map[ctasks.Priority]int{}
	mock.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(e Executable) bool {
		released[e.GetPriority()]++
		return true
	}).AnyTimes()

	for i := 0; i < 100; i++ {
		now = now.Add(testThrottleWindow)
		ts.Update(now)
		stateClock.Update(now)
		r.reschedule()
	}

	require.Positive(t, released[ctasks.PriorityHigh], "high priority must drain")
	require.Zero(t, released[ctasks.PriorityPreemptable],
		"a preemptable release the enforcer would refuse must not be spent, or its rejection "+
			"is charged to the class high priority shares")
}
