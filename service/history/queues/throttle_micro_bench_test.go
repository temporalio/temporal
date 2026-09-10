package queues

import (
	"testing"
	"time"

	"go.temporal.io/server/common/clock"
	"go.uber.org/mock/gomock"
)

// noopGate discards timer updates. recordingGate accumulates every Update in a slice, which
// would dominate the allocation counts of a benchmark that runs millions of passes.
type noopGate struct {
	fireCh chan struct{}
}

func (g *noopGate) FireCh() <-chan struct{}    { return g.fireCh }
func (g *noopGate) FireAfter(_ time.Time) bool { return false }
func (g *noopGate) Close()                     {}
func (g *noopGate) Update(_ time.Time) bool    { return true }

// benchThrottleState returns a state whose single key either always admits or always denies,
// with a frozen clock so no refill happens mid benchmark and every iteration takes the same
// path. alwaysAdmit sizes the burst so tokens cannot run out over b.N iterations.
func benchThrottleState(b *testing.B, alwaysAdmit bool) (*ThrottleState, ThrottleKey) {
	b.Helper()

	o := defaultThrottleOverrides()
	if alwaysAdmit {
		o.initialRate = 1e9
		o.maxRate = 1e9
	}
	state, _ := newTestThrottleState(o)
	key := apsKey("ns-1")

	// Materialise the key so the benchmark measures the steady state lookup, not creation.
	admitOK(state, key)
	if !alwaysAdmit {
		// A new entry starts with a full burst. Drain it so every measured call is denied,
		// which is the path a gated class actually takes at steady state.
		for admitOK(state, key) { //nolint:revive // draining, body intentionally empty
		}
	}
	return state, key
}

// BenchmarkThrottleState_AdmitAllowed measures the admit path when the class has budget.
func BenchmarkThrottleState_AdmitAllowed(b *testing.B) {
	state, key := benchThrottleState(b, true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !admitOK(state, key) {
			b.Fatal("expected admit to succeed")
		}
	}
}

// BenchmarkThrottleState_AdmitDenied measures the path taken on the overwhelming majority of
// calls at steady state. In the 400K cluster run 99.7% of rescheduler passes ended in a denial,
// so this is the cost that actually shows up as controller CPU.
func BenchmarkThrottleState_AdmitDenied(b *testing.B) {
	state, key := benchThrottleState(b, false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if admitOK(state, key) {
			b.Fatal("expected admit to be denied")
		}
	}
}

// BenchmarkThrottleState_AdmitDeniedParallel measures the same denial path under contention.
// ThrottleState is host level while reschedulers are per shard, so hundreds of shards hit one
// key's mutex concurrently. This is the benchmark to watch when changing the locking or moving
// to a batched lease.
func BenchmarkThrottleState_AdmitDeniedParallel(b *testing.B) {
	state, key := benchThrottleState(b, false)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			admitOK(state, key)
		}
	})
}

// benchRescheduler builds a rescheduler holding backlog due tasks in one gated class.
func benchRescheduler(
	b *testing.B,
	ctrl *gomock.Controller,
	state *ThrottleState,
	key ThrottleKey,
	backlog int,
	submitSucceeds bool,
	maxPerPass int,
) *reschedulerImpl {
	b.Helper()

	timeSource := clock.NewEventTimeSource()
	now := time.Unix(0, 0)
	timeSource.Update(now)

	r, scheduler, _ := newTestRescheduler(b, ctrl, timeSource, state, maxPerPass)
	r.timerGate = &noopGate{fireCh: make(chan struct{}, 1)}
	scheduler.EXPECT().TrySubmit(gomock.Any()).Return(submitSucceeds).AnyTimes()

	for i := 0; i < backlog; i++ {
		e := newThrottledExecutable(ctrl, key, true)
		e.EXPECT().GetNamespaceID().Return("ns-1").AnyTimes()
		r.Add(e, now)
	}
	return r
}

// BenchmarkReschedule_DeniedPass measures one complete wasted pass: wake, take the rescheduler
// lock, walk the class order, get denied, set the next wake, return having released nothing.
// This is the unit of work the unjittered Window/10 poll repeats, so it is the number to beat
// when batching the lease or adding jitter.
func BenchmarkReschedule_DeniedPass(b *testing.B) {
	ctrl := gomock.NewController(b)
	state, key := benchThrottleState(b, false)
	r := benchRescheduler(b, ctrl, state, key, 1000, false, 1000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.reschedule()
	}
	b.StopTimer()
	if r.Len() != 1000 {
		b.Fatalf("expected a denied pass to release nothing, released %d", 1000-r.Len())
	}
}

// BenchmarkReschedule_ReleasingPass is the contrast case: a pass that has budget and drains
// the class. Comparing per op cost against the denied pass shows how much of the poll cost is
// fixed overhead rather than useful work. b.N tasks are staged before the timer starts and
// released by a single pass, so the reported cost is per released task.
func BenchmarkReschedule_ReleasingPass(b *testing.B) {
	ctrl := gomock.NewController(b)
	state, key := benchThrottleState(b, true)
	r := benchRescheduler(b, ctrl, state, key, b.N, true, 0)

	b.ReportAllocs()
	b.ResetTimer()
	r.reschedule()
	b.StopTimer()

	if r.Len() != 0 {
		b.Fatalf("expected the pass to drain the class, %d left", r.Len())
	}
}
