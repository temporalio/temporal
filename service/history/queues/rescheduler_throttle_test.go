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
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.uber.org/mock/gomock"
)

type (
	throttledExecutable struct {
		*MockExecutable
		key      ThrottleKey
		known    bool
		admitted bool
	}

	recordingGate struct {
		fireCh  chan struct{}
		updates []time.Time
	}
)

func (e *throttledExecutable) SetThrottleAdmitted(admitted bool) {
	e.admitted = admitted
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
	)
	gate := &recordingGate{fireCh: make(chan struct{}, 1)}
	r.timerGate = gate
	return r, scheduler, gate
}

// addThrottled parks a task the way the executable does: with the class it last failed under.
func addThrottled(r *reschedulerImpl, e *throttledExecutable, at time.Time) {
	key := ThrottleKey{}
	if e.known {
		key = e.key
	}
	r.Add(e, at, key)
}

func apsKey(namespaceName namespace.Name) ThrottleKey {
	return NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, namespaceName)
}

func TestReschedule_ThrottledClassDoesNotBlockHealthyClass(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state)

	key := apsKey("ns-throttled")
	require.True(t, admitOK(state, key))

	throttled := newThrottledExecutable(ctrl, key, true)
	throttled.EXPECT().GetNamespaceID().Return("ns-throttled").AnyTimes()
	healthy := newThrottledExecutable(ctrl, ThrottleKey{}, false)
	healthy.EXPECT().GetNamespaceID().Return("ns-healthy").AnyTimes()

	addThrottled(r, throttled, now)
	addThrottled(r, healthy, now)

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

func TestReschedule_BudgetIsCeilingNotQuota(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1000
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, gate := newTestRescheduler(t, ctrl, timeSource, state)

	key := apsKey("ns-1")
	notDue := newThrottledExecutable(ctrl, key, true)
	notDue.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	addThrottled(r, notDue, now.Add(time.Minute))

	scheduler.EXPECT().TrySubmit(gomock.Any()).Times(0)

	gate.updates = nil
	r.reschedule()

	require.Equal(t, 1, r.Len(), "budget must not pull forward a task that is not due")
	require.Len(t, gate.updates, 1)
	require.Equal(t, now.Add(time.Minute), gate.updates[0])

	require.Zero(t, throttleLen(state),
		"a not-due head must not reach the gate at all, so the class is not even created")
}

func TestReschedule_EveryPendingClassArmsAWake(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, stateClock := newTestThrottleState(defaultThrottleOverrides())

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, gate := newTestRescheduler(t, ctrl, timeSource, state)
	scheduler.EXPECT().TrySubmit(gomock.Any()).Times(0)

	for i, delay := range []time.Duration{5 * time.Minute, time.Minute, 3 * time.Minute} {
		e := newThrottledExecutable(ctrl, ThrottleKey{}, false)
		namespaceID := string(rune('a' + i))
		e.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
		addThrottled(r, e, now.Add(delay))
	}

	gate.updates = nil
	r.reschedule()

	require.Len(t, gate.updates, 3, "each class that is not due yet arms its own wake")
	for _, delay := range []time.Duration{time.Minute, 3 * time.Minute, 5 * time.Minute} {
		require.Contains(t, gate.updates, now.Add(delay),
			"a class must not be left parked with no wake; the gate keeps the earliest")
	}
}

// A blocked class looks again a fixed number of times per window, whatever its rate.
func TestReschedule_BudgetRetryIntervalIsAFractionOfTheWindow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window time.Duration
		want   time.Duration
	}{
		{name: "default window", window: time.Second, want: 100 * time.Millisecond},
		{name: "long window", window: 10 * time.Second, want: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			overrides := defaultThrottleOverrides()
			overrides.initialRate = 1
			state, stateClock := newTestThrottleStateWithWindow(overrides, tc.window)

			now := stateClock.Now()
			timeSource := clock.NewEventTimeSource()
			timeSource.Update(now)
			r, scheduler, gate := newTestRescheduler(t, ctrl, timeSource, state)

			key := apsKey("ns-1")
			// Burst is one window's worth of credit, so drain it all to deny the next Admit.
			for admitOK(state, key) {
			}

			e := newThrottledExecutable(ctrl, key, true)
			e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
			addThrottled(r, e, now)

			scheduler.EXPECT().TrySubmit(gomock.Any()).Times(0)
			gate.updates = nil
			r.reschedule()

			require.Len(t, gate.updates, 1)
			require.Equal(t, now.Add(tc.want), gate.updates[0])
		})
	}
}

func TestReschedule_DisablingControllerDrainsExistingGatedQueues(t *testing.T) {
	ctrl := gomock.NewController(t)
	enabled := true
	o := defaultThrottleOverrides()
	o.initialRate = 1
	state, stateClock := newTestThrottleState(o)
	overrideThrottleSetting(state, func(c *dynamicconfig.TaskThrottleControllerSettings) { c.Enabled = enabled })

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state)
	key := apsKey("ns-1")
	require.True(t, admitOK(state, key))

	for range 3 {
		e := newThrottledExecutable(ctrl, key, true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		addThrottled(r, e, now)
	}

	enabled = false
	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(3)
	r.reschedule()
	require.Zero(t, r.Len())
}

func TestReschedule_PermitIsVisibleBeforeSubmit(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, stateClock := newTestThrottleState(defaultThrottleOverrides())
	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state)

	key := apsKey("ns-1")
	e := newThrottledExecutable(ctrl, key, true)
	e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	addThrottled(r, e, now)

	scheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(Executable) bool {
		require.True(t, e.admitted)
		return true
	})
	r.reschedule()
	require.Zero(t, r.Len())
}

func TestReschedule_HighPriorityGetsBudgetFirst(t *testing.T) {
	ctrl := gomock.NewController(t)
	o := defaultThrottleOverrides()
	o.initialRate = 1
	state, stateClock := newTestThrottleState(o)
	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	scheduler := NewMockScheduler(ctrl)
	scheduler.EXPECT().TaskChannelKeyFn().Return(func(e Executable) TaskChannelKey {
		return TaskChannelKey{NamespaceID: e.GetNamespaceID(), Priority: e.GetPriority()}
	}).AnyTimes()
	r := NewRescheduler(scheduler, timeSource, log.NewTestLogger(), metrics.NoopMetricsHandler, state)
	r.timerGate = &recordingGate{fireCh: make(chan struct{}, 1)}

	key := apsKey("ns-1")
	preemptable := newThrottledExecutable(ctrl, key, true)
	preemptable.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	preemptable.EXPECT().GetPriority().Return(ctasks.PriorityPreemptable).AnyTimes()
	high := newThrottledExecutable(ctrl, key, true)
	high.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	high.EXPECT().GetPriority().Return(ctasks.PriorityHigh).AnyTimes()
	addThrottled(r, preemptable, now)
	addThrottled(r, high, now)

	var submitted []Executable
	scheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(e Executable) bool {
		submitted = append(submitted, e)
		return true
	}).AnyTimes()
	r.reschedule()

	require.Len(t, submitted, 1)
	require.Same(t, Executable(high), submitted[0])
}

func TestReschedule_FailedSubmitRefundsTheToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state)

	key := apsKey("ns-1")
	e := newThrottledExecutable(ctrl, key, true)
	e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	addThrottled(r, e, now)

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(false).Times(1)
	r.reschedule()
	require.Equal(t, 1, r.Len())
	require.False(t, e.admitted)

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(1)
	r.reschedule()
	require.Zero(t, r.Len())
	require.True(t, e.admitted, "a gated release must tell the task the controller metered it")

	releases, rejections := throttleCounters(state, key)
	require.Equal(t, int64(1), releases, "only the submit that happened may be counted")
	require.Zero(t, rejections)
}

// Priority order is a property of the rescheduler, not of the throttle flag.
func TestReschedule_PriorityOrderHoldsWithTheControllerOff(t *testing.T) {
	ctrl := gomock.NewController(t)
	o := defaultThrottleOverrides()
	o.enabled = false
	state, stateClock := newTestThrottleState(o)
	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	scheduler := NewMockScheduler(ctrl)
	scheduler.EXPECT().TaskChannelKeyFn().Return(func(e Executable) TaskChannelKey {
		return TaskChannelKey{NamespaceID: e.GetNamespaceID(), Priority: e.GetPriority()}
	}).AnyTimes()
	r := NewRescheduler(scheduler, timeSource, log.NewTestLogger(), metrics.NoopMetricsHandler, state)
	r.timerGate = &recordingGate{fireCh: make(chan struct{}, 1)}

	// Parked under a budget, but the controller is off, so nothing is gated.
	key := apsKey("ns-1")
	for _, p := range []ctasks.Priority{
		ctasks.PriorityPreemptable, ctasks.PriorityLow, ctasks.PriorityHigh,
	} {
		e := newThrottledExecutable(ctrl, key, true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		e.EXPECT().GetPriority().Return(p).AnyTimes()
		addThrottled(r, e, now)
	}

	var submitted []ctasks.Priority
	scheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(e Executable) bool {
		submitted = append(submitted, e.GetPriority())
		return true
	}).AnyTimes()
	r.reschedule()

	require.Equal(t,
		[]ctasks.Priority{ctasks.PriorityHigh, ctasks.PriorityLow, ctasks.PriorityPreemptable},
		submitted,
		"a disabled controller must not cost the rescheduler its priority order")
	require.Zero(t, throttleLen(state), "a disabled controller must track nothing")
}

// A queue wired without a controller still parks tasks under a class key, so the gate must
// tolerate a nil state rather than dereference it.
func TestReschedule_NilControllerStillDispatchesClassedWork(t *testing.T) {
	ctrl := gomock.NewController(t)
	timeSource := clock.NewEventTimeSource()
	now := time.Unix(0, 0)
	timeSource.Update(now)

	scheduler := NewMockScheduler(ctrl)
	scheduler.EXPECT().TaskChannelKeyFn().Return(
		func(e Executable) TaskChannelKey { return TaskChannelKey{NamespaceID: e.GetNamespaceID()} },
	).AnyTimes()
	r := NewRescheduler(scheduler, timeSource, log.NewTestLogger(), metrics.NoopMetricsHandler, nil)
	r.timerGate = &recordingGate{fireCh: make(chan struct{}, 1)}

	e := newThrottledExecutable(ctrl, apsKey("ns-1"), true)
	e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	addThrottled(r, e, now)
	r.Lock()
	require.Len(t, r.pqMap, 1)
	for key := range r.pqMap {
		require.NotEqual(t, ThrottleKey{}, key.Throttle, "the class key must carry the cause")
	}
	r.Unlock()

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(1)
	require.NotPanics(t, r.reschedule)
	require.Zero(t, r.Len())
	require.False(t, e.admitted, "a task the controller never metered must not be marked")
}

// A priority PriorityOrder does not name must still drain. Without the trailing band its
// tasks sit in the queue forever, which is how TaskChannelKeyFn returning a zero key hangs.
func TestReschedule_UnnamedPriorityStillDrains(t *testing.T) {
	ctrl := gomock.NewController(t)
	state, stateClock := newTestThrottleState(defaultThrottleOverrides())
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(stateClock.Now())
	now := timeSource.Now()

	unnamed := ctasks.Priority(-1)
	require.NotContains(t, ctasks.PriorityName, unnamed)

	scheduler := NewMockScheduler(ctrl)
	scheduler.EXPECT().TaskChannelKeyFn().Return(func(e Executable) TaskChannelKey {
		return TaskChannelKey{NamespaceID: e.GetNamespaceID(), Priority: unnamed}
	}).AnyTimes()
	r := NewRescheduler(scheduler, timeSource, log.NewTestLogger(), metrics.NoopMetricsHandler, state)
	r.timerGate = &recordingGate{fireCh: make(chan struct{}, 1)}

	e := newThrottledExecutable(ctrl, apsKey("ns-1"), true)
	e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	addThrottled(r, e, now)

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(1)
	r.reschedule()

	require.Zero(t, r.Len(), "a priority outside PriorityOrder must still be drained")
}
