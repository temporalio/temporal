package queues

import (
	"sync/atomic"
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

func (e *throttledExecutable) ThrottleKey() ThrottleKey {
	if !e.known {
		return ThrottleKey{}
	}
	return e.key
}

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
	)
	gate := &recordingGate{fireCh: make(chan struct{}, 1)}
	r.timerGate = gate
	return r, scheduler, gate
}

func apsKey(namespaceID string) ThrottleKey {
	return NewThrottleKey(enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, namespaceID)
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
	r.Add(notDue, now.Add(time.Minute))

	scheduler.EXPECT().TrySubmit(gomock.Any()).Times(0)

	gate.updates = nil
	r.reschedule()

	require.Equal(t, 1, r.Len(), "budget must not pull forward a task that is not due")
	require.Len(t, gate.updates, 1)
	require.Equal(t, now.Add(time.Minute), gate.updates[0])

	require.Zero(t, throttleLen(state),
		"a not-due head must not reach the gate at all, so the class is not even created")
}

func TestReschedule_SingleTimerGateUpdatePerPass(t *testing.T) {
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
		r.Add(e, now.Add(delay))
	}

	gate.updates = nil
	r.reschedule()

	require.Len(t, gate.updates, 1)
	require.Equal(t, now.Add(time.Minute), gate.updates[0], "wake at the running minimum")
}

func TestReschedule_BudgetDeniedWakesInsideControlWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.initialRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, gate := newTestRescheduler(t, ctrl, timeSource, state)

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
	require.Equal(t, now.Add(time.Second), gate.updates[0])
}

func TestReschedule_BudgetRetryIntervalOnlyWaitsLonger(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window time.Duration
		eta    time.Duration
		want   time.Duration
	}{
		{name: "fallback", window: time.Second, want: 100 * time.Millisecond},
		{name: "longer estimate", window: time.Second, eta: 400 * time.Millisecond, want: 400 * time.Millisecond},
		{name: "short estimate", window: time.Second, eta: 40 * time.Millisecond, want: 100 * time.Millisecond},
		{name: "window cap", window: time.Second, eta: 30 * time.Second, want: time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, _ := newTestThrottleStateWithWindow(defaultThrottleOverrides(), tc.window)
			r := reschedulerImpl{throttleState: state}
			require.Equal(t, tc.want, r.budgetRetryInterval(tc.eta))
		})
	}
}

func TestReschedule_DisabledControllerReleasesEverythingDue(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	overrides.enabled = false
	overrides.initialRate = 1
	state, stateClock := newTestThrottleState(overrides)

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state)

	for i := 0; i < 10; i++ {
		e := newThrottledExecutable(ctrl, apsKey("ns-1"), true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		r.Add(e, now)
	}

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).Times(10)

	r.reschedule()

	require.Zero(t, r.Len())
}

func TestReschedule_DisablingControllerDrainsExistingGatedQueues(t *testing.T) {
	ctrl := gomock.NewController(t)
	enabled := true
	o := defaultThrottleOverrides()
	o.initialRate = 1
	state, stateClock := newTestThrottleState(o)
	state.options.Enabled = func() bool { return enabled }

	now := stateClock.Now()
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state)
	key := apsKey("ns-1")
	require.True(t, admitOK(state, key))

	for range 3 {
		e := newThrottledExecutable(ctrl, key, true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		r.Add(e, now)
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
	r.Add(e, now)

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
	scheduler.EXPECT().ChannelWeightFn().Return(func(key TaskChannelKey) int {
		if key.Priority == ctasks.PriorityHigh {
			return 10
		}
		return 1
	})
	r := NewRescheduler(scheduler, timeSource, log.NewTestLogger(), metrics.NoopMetricsHandler, state)
	r.timerGate = &recordingGate{fireCh: make(chan struct{}, 1)}

	key := apsKey("ns-1")
	preemptable := newThrottledExecutable(ctrl, key, true)
	preemptable.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	preemptable.EXPECT().GetPriority().Return(ctasks.PriorityPreemptable).AnyTimes()
	high := newThrottledExecutable(ctrl, key, true)
	high.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	high.EXPECT().GetPriority().Return(ctasks.PriorityHigh).AnyTimes()
	r.Add(preemptable, now)
	r.Add(high, now)

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
	r.Add(e, now)

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

// An operator turns this on during an incident, with the rescheduler already full. Those
// tasks were parked before the controller was gating, so their class carries no key — and if
// the release path trusted the class key alone it would drain the whole backlog in one pass,
// unpaced. That is the largest wave in the system and the one the design exists to remove.
func TestReschedule_EnablingTheControllerPacesWorkAlreadyParked(t *testing.T) {
	ctrl := gomock.NewController(t)
	var enabled atomic.Bool

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	state := NewThrottleState(
		ThrottleStateOptions{
			Enabled:       enabled.Load,
			Beta:          dynamicconfig.GetFloatPropertyFn(0.85),
			IncreaseRatio: dynamicconfig.GetFloatPropertyFn(0.10),
			LossThreshold: dynamicconfig.GetFloatPropertyFn(0.05),
			Window:        dynamicconfig.GetDurationPropertyFn(testThrottleWindow),
			MaxKeys:       dynamicconfig.GetIntPropertyFn(1024),
			MinRate:       dynamicconfig.GetFloatPropertyFn(1),
			MaxRate:       dynamicconfig.GetFloatPropertyFn(10000),
			InitialRate:   dynamicconfig.GetFloatPropertyFn(1),
			KeyTTL:        dynamicconfig.GetDurationPropertyFn(5 * time.Minute),
		},
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state)
	key := apsKey("ns-1")
	now := timeSource.Now()

	// Parked while the controller was off: the class is created without a throttle key.
	for i := 0; i < 50; i++ {
		e := newThrottledExecutable(ctrl, key, true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		r.Add(e, now)
	}
	require.Equal(t, 50, r.Len())

	enabled.Store(true)
	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).AnyTimes()
	r.reschedule()

	require.Equal(t, 49, r.Len(),
		"a rate of 1/s must release one task, not the whole backlog")
	require.Equal(t, 1, throttleLen(state),
		"the class must be tracked, not bypassed")
}

// A class driven to the floor climbs back multiplicatively and the idle reset cannot rescue
// it, because a class with a backlog is never idle. Raising the floor is the lever that
// exists for that, so it has to take effect on a class already sitting there.
func TestThrottleState_RaisingTheFloorLiftsAClassAlreadyAtIt(t *testing.T) {
	floor := 1.0
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	state := NewThrottleState(
		ThrottleStateOptions{
			Enabled:       dynamicconfig.GetBoolPropertyFn(true),
			Beta:          dynamicconfig.GetFloatPropertyFn(0.85),
			IncreaseRatio: dynamicconfig.GetFloatPropertyFn(0.10),
			LossThreshold: dynamicconfig.GetFloatPropertyFn(0.05),
			Window:        dynamicconfig.GetDurationPropertyFn(testThrottleWindow),
			MaxKeys:       dynamicconfig.GetIntPropertyFn(1024),
			MinRate:       func() float64 { return floor },
			MaxRate:       dynamicconfig.GetFloatPropertyFn(10000),
			InitialRate:   dynamicconfig.GetFloatPropertyFn(100),
			KeyTTL:        dynamicconfig.GetDurationPropertyFn(5 * time.Minute),
		},
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)
	key := testKey()

	for i := 0; i < 60; i++ {
		reportThrottle(state, key, true)
		closeWindow(state, timeSource, key)
	}
	require.InEpsilon(t, 1.0, throttleRate(state, key), 1e-9, "the class must be at the floor")

	floor = 50
	reportThrottle(state, key, true)
	closeWindow(state, timeSource, key)

	require.InEpsilon(t, 50.0, throttleRate(state, key), 1e-9,
		"raising the floor must lift a class already pinned to it")
}

// The cursor rotates so that classes of equal priority take turns leading the pass. Without
// it whichever class the sort left first would be offered the scheduler's capacity every
// time, and a class behind it would only ever get what the first one did not take.
func TestReschedule_CursorRotatesBetweenEqualClasses(t *testing.T) {
	ctrl := gomock.NewController(t)
	overrides := defaultThrottleOverrides()
	state, stateClock := newTestThrottleState(overrides)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(stateClock.Now())

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state)
	now := timeSource.Now()

	for _, ns := range []string{"ns-1", "ns-2", "ns-3"} {
		e := newThrottledExecutable(ctrl, apsKey(ns), true)
		e.EXPECT().GetNamespaceID().Return(ns).AnyTimes()
		r.Add(e, now)
	}
	require.Len(t, r.keyOrder, 3)

	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(true).AnyTimes()
	leaders := make(map[reschedulerKey]bool)
	for i := 0; i < 3; i++ {
		r.Lock()
		leaders[r.visitOrderLocked()[0].key] = true
		r.Unlock()
		r.reschedule()
	}
	require.Len(t, leaders, 3, "every class must get a turn at the head of the pass")
}

// A task the controller does not govern must not wait on another task's budget. Deciding
// gating per task inside one shared queue meant a governed task at the head, denied by its
// bucket, stopped the pass and stranded every ungoverned task behind it.
func TestReschedule_UngovernedTasksDoNotWaitOnAnotherClassBudget(t *testing.T) {
	ctrl := gomock.NewController(t)
	var enabled atomic.Bool

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Unix(0, 0))
	state := NewThrottleState(
		ThrottleStateOptions{
			Enabled:       enabled.Load,
			Beta:          dynamicconfig.GetFloatPropertyFn(0.85),
			IncreaseRatio: dynamicconfig.GetFloatPropertyFn(0.10),
			LossThreshold: dynamicconfig.GetFloatPropertyFn(0.05),
			Window:        dynamicconfig.GetDurationPropertyFn(testThrottleWindow),
			MaxKeys:       dynamicconfig.GetIntPropertyFn(1024),
			MinRate:       dynamicconfig.GetFloatPropertyFn(1),
			MaxRate:       dynamicconfig.GetFloatPropertyFn(10000),
			InitialRate:   dynamicconfig.GetFloatPropertyFn(100),
			KeyTTL:        dynamicconfig.GetDurationPropertyFn(5 * time.Minute),
		},
		timeSource,
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
	)

	r, scheduler, _ := newTestRescheduler(t, ctrl, timeSource, state)
	now := timeSource.Now()

	key := apsKey("ns-1")
	governed := newThrottledExecutable(ctrl, key, true)
	governed.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	r.Add(governed, now)

	ungoverned := newThrottledExecutable(ctrl, ThrottleKey{}, false)
	ungoverned.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
	r.Add(ungoverned, now)

	// Both were parked before the controller was gating, which is when the classes are formed.
	enabled.Store(true)

	// The governed class has nothing left to give, so its task cannot be released this pass.
	for admitOK(state, key) { //nolint:revive // draining, body intentionally empty
	}

	submitted := make([]Executable, 0, 2)
	scheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(e Executable) bool {
		submitted = append(submitted, e)
		return true
	}).AnyTimes()
	r.reschedule()

	require.Contains(t, submitted, Executable(ungoverned),
		"an ungoverned task is not blocked by a budget it is not waiting on")
	require.NotContains(t, submitted, Executable(governed),
		"and the governed one is still held by its own budget")
}
