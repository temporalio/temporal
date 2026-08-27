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
	require.True(t, state.Admit(key))

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
	require.True(t, state.Admit(key))

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
	require.Equal(t, now.Add(testThrottleWindow/10), gate.updates[0])
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
	require.True(t, state.Admit(persistence))

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
