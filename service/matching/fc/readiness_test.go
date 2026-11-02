package fc

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testcontext"
)

type readinessSuite struct {
	parallelsuite.Suite[*readinessSuite]
}

func TestReadiness(t *testing.T) {
	parallelsuite.Run(t, &readinessSuite{})
}

func (s *readinessSuite) newReadiness() *Readiness {
	r := NewReadiness()
	s.T().Cleanup(r.Stop)
	return r
}

type testLimiter struct {
	rs        *readinessShard
	wakeCount int
	onSync    func(*readinessSyncUpdate)
}

func (l *testLimiter) syncLocked(update *readinessSyncUpdate) {
	if l.onSync != nil {
		l.onSync(update)
	}
	update.wake(l.wakeCount)
}

type testCallback struct {
	onReady func()
}

func (cb *testCallback) OnReady() {
	if cb.onReady != nil {
		cb.onReady()
	}
}

func (s *readinessSuite) newLimiter(r *Readiness, ns namespace.ID) *testLimiter {
	return &testLimiter{rs: r.getShard(ns)}
}

func (s *readinessSuite) newCallback(notifications chan<- string, name string) *testCallback {
	return &testCallback{onReady: func() { notifications <- name }}
}

func (s *readinessSuite) register(limiter *testLimiter, cb ReadinessCallback, pri wakePriority) {
	limiter.rs.lock.Lock()
	defer limiter.rs.lock.Unlock()
	limiter.rs.addEdgeLocked(limiter, cb, pri)
}

func (s *readinessSuite) unregister(limiter *testLimiter, cb ReadinessCallback) {
	limiter.rs.lock.Lock()
	defer limiter.rs.lock.Unlock()
	limiter.rs.removeEdgeLocked(limiter, cb)
}

func (s *readinessSuite) wake(limiter *testLimiter, count int) {
	s.T().Helper()
	s.NoError(limiter.rs.update(limiter, notifySync, func() error {
		limiter.wakeCount = count
		return nil
	}))
}

func (s *readinessSuite) requireNotifications(notifications <-chan string, expected ...string) {
	s.T().Helper()
	// Synchronous updates must deliver all selected notifications before returning.
	s.Len(notifications, len(expected))
	for _, name := range expected {
		s.Equal(name, s.Rcv(notifications))
	}
}

func (s *readinessSuite) TestRepeatedRegistrationNotifiesOnce() {
	r := s.newReadiness()
	limiter := s.newLimiter(r, "namespace")
	notifications := make(chan string, 8)
	cb := s.newCallback(notifications, "callback")
	s.register(limiter, cb, 10)
	s.register(limiter, cb, 10)

	s.wake(limiter, 10)
	s.requireNotifications(notifications, "callback")
	s.wake(limiter, 10)
	s.requireNotifications(notifications)
}

func (s *readinessSuite) TestRegistrationUpdatesPriority() {
	r := s.newReadiness()
	limiter := s.newLimiter(r, "namespace")
	notifications := make(chan string, 8)
	first, second := s.newCallback(notifications, "first"), s.newCallback(notifications, "second")
	s.register(limiter, first, 20)
	s.register(limiter, second, 10)
	s.register(limiter, first, 5)

	s.wake(limiter, 1)
	s.requireNotifications(notifications, "first")
	s.wake(limiter, 10)
	s.requireNotifications(notifications, "second")
	s.wake(limiter, 10)
	s.requireNotifications(notifications)
}

func (s *readinessSuite) TestEqualPriorityCallbacks() {
	r := s.newReadiness()
	limiter := s.newLimiter(r, "namespace")
	notifications := make(chan string, 8)
	s.register(limiter, s.newCallback(notifications, "first"), 10)
	s.register(limiter, s.newCallback(notifications, "second"), 10)

	s.wake(limiter, 1)
	s.Len(notifications, 1)
	first := s.Rcv(notifications)
	s.wake(limiter, 1)
	s.Len(notifications, 1)
	second := s.Rcv(notifications)
	s.ElementsMatch([]string{"first", "second"}, []string{first, second})
	s.wake(limiter, 10)
	s.requireNotifications(notifications)
}

func (s *readinessSuite) TestNilCallback() {
	r := s.newReadiness()
	limiter := s.newLimiter(r, "namespace")
	notifications := make(chan string, 8)
	s.register(limiter, nil, 5)
	s.register(limiter, s.newCallback(notifications, "callback"), 10)

	s.wake(limiter, 10)
	s.requireNotifications(notifications, "callback")
}

func (s *readinessSuite) TestRemoveRegistrationPreservesOtherWaiters() {
	r := s.newReadiness()
	first, second := s.newLimiter(r, "namespace"), s.newLimiter(r, "namespace")
	notifications := make(chan string, 8)
	cb, other := s.newCallback(notifications, "callback"), s.newCallback(notifications, "other")
	s.register(first, cb, 10)
	s.register(second, cb, 20)
	s.register(first, other, 30)

	s.unregister(first, &testCallback{})
	s.unregister(s.newLimiter(r, "namespace"), cb)
	r.CancelAllCallbacks("namespace", &testCallback{})
	s.unregister(first, cb)
	s.unregister(first, cb)

	s.wake(first, 10)
	s.requireNotifications(notifications, "other")
	s.wake(second, 10)
	s.requireNotifications(notifications, "callback")
	s.wake(first, 10)
	s.wake(second, 10)
	s.requireNotifications(notifications)
}

func (s *readinessSuite) TestCancelAllCallbacks() {
	r := s.newReadiness()
	notifications := make(chan string, 8)
	canceled, other := s.newCallback(notifications, "canceled"), s.newCallback(notifications, "other")
	limiters := []*testLimiter{s.newLimiter(r, "namespace"), s.newLimiter(r, "namespace"), s.newLimiter(r, "namespace")}
	for _, limiter := range limiters {
		s.register(limiter, canceled, 10)
		s.register(limiter, other, 20)
	}

	r.CancelAllCallbacks("namespace", canceled)
	r.CancelAllCallbacks("namespace", canceled)

	for _, limiter := range limiters {
		s.wake(limiter, 10)
		s.requireNotifications(notifications, "other")
	}
}

func (s *readinessSuite) TestCancellationCanWakeOtherCallbacks() {
	r := s.newReadiness()
	notifications := make(chan string, 8)
	canceled, other := s.newCallback(notifications, "canceled"), s.newCallback(notifications, "other")
	for range 3 {
		limiter := s.newLimiter(r, "namespace")
		s.register(limiter, canceled, 10)
		s.register(limiter, other, 20)
		limiter.wakeCount = 1
	}

	r.CancelAllCallbacks("namespace", canceled)

	for range 3 {
		s.Equal("other", s.Rcv(notifications))
	}
}

func (s *readinessSuite) TestNamespaceScoping() {
	r := s.newReadiness()
	firstNamespace := namespace.ID("11111111-1111-1111-1111-111111111111")
	secondNamespace := namespace.ID("22222222-2222-2222-2222-222222222222")
	first, second := s.newLimiter(r, firstNamespace), s.newLimiter(r, secondNamespace)
	notifications := make(chan string, 8)
	firstCallback, secondCallback := s.newCallback(notifications, "first"), s.newCallback(notifications, "second")
	s.register(first, firstCallback, 10)
	s.register(second, secondCallback, 20)

	r.CancelAllCallbacks(firstNamespace, firstCallback)

	s.wake(first, 10)
	s.requireNotifications(notifications)
	s.wake(second, 10)
	s.requireNotifications(notifications, "second")
}

func (s *readinessSuite) TestWakePrefix() {
	r := s.newReadiness()
	limiter := s.newLimiter(r, "namespace")
	notifications := make(chan string, 16)
	best := s.newCallback(notifications, "best")
	s.register(limiter, s.newCallback(notifications, "last"), 40)
	s.register(limiter, s.newCallback(notifications, "second"), 20)
	s.register(limiter, s.newCallback(notifications, "third"), 30)
	s.register(limiter, best, 10)

	// No wake leaves every registration intact.
	s.wake(limiter, 0)
	s.requireNotifications(notifications)

	// Priority wins over registration order.
	s.wake(limiter, 1)
	s.requireNotifications(notifications, "best")
	s.wake(limiter, 2)
	s.requireNotifications(notifications, "second", "third")

	// Excess wake capacity is clipped to the remaining waiter count.
	s.wake(limiter, 10)
	s.requireNotifications(notifications, "last")
	s.wake(limiter, 10)
	s.requireNotifications(notifications)

	// Re-registration makes the callback eligible for another notification.
	s.register(limiter, best, 5)
	s.wake(limiter, 1)
	s.requireNotifications(notifications, "best")
}

func (s *readinessSuite) TestUpdateErrorStillSynchronizes() {
	r := s.newReadiness()
	limiter := s.newLimiter(r, "namespace")
	notifications := make(chan string, 8)
	s.register(limiter, s.newCallback(notifications, "callback"), 10)
	updated, synchronizedAfterUpdate := false, false
	limiter.onSync = func(*readinessSyncUpdate) { synchronizedAfterUpdate = updated }
	updateErr := errors.New("update failed")

	err := limiter.rs.update(limiter, notifySync, func() error {
		updated = true
		limiter.wakeCount = 1
		return updateErr
	})

	s.ErrorIs(err, updateErr)
	s.True(synchronizedAfterUpdate)
	s.requireNotifications(notifications, "callback")
}

func (s *readinessSuite) TestSyncUsesCurrentWaiters() {
	r := s.newReadiness()
	first, second := s.newLimiter(r, "namespace"), s.newLimiter(r, "namespace")
	firstCallback, secondCallback := &testCallback{}, &testCallback{}
	s.register(first, firstCallback, 10)
	s.register(second, secondCallback, 20)
	var observed [][]fwdMapEntry
	observe := func(update *readinessSyncUpdate) { observed = append(observed, slices.Clone(update.waiters)) }
	first.onSync, second.onSync = observe, observe

	s.wake(first, 1)
	s.wake(second, 0)
	s.wake(first, 1)

	s.Equal([][]fwdMapEntry{{{cb: firstCallback, pri: 10}}, {{cb: secondCallback, pri: 20}}, nil}, observed)
}

func (s *readinessSuite) TestAsynchronousNotifications() {
	ctx := testcontext.For(s.T(), testcontext.WithTimeout(5*time.Second))
	r := s.newReadiness()
	limiter := s.newLimiter(r, "namespace")
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	s.T().Cleanup(unblock)
	cb := &testCallback{onReady: func() {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
		close(finished)
	}}
	s.register(limiter, cb, 10)
	returned := make(chan error, 1)

	go func() {
		returned <- limiter.rs.update(limiter, notifyAsync, func() error {
			limiter.wakeCount = 1
			return nil
		})
	}()
	s.Rcv(entered)
	s.NoError(s.Rcv(returned))
	select {
	case <-finished:
		s.T().Fatal("update should return while the callback is still blocked")
	default:
	}
	unblock()
	s.Rcv(finished)
}

func (s *readinessSuite) TestCallbacksCanReenterReadiness_Sync() {
	s.testCallbacksCanReenterReadiness(notifySync)
}

func (s *readinessSuite) TestCallbacksCanReenterReadiness_Async() {
	s.testCallbacksCanReenterReadiness(notifyAsync)
}

func (s *readinessSuite) testCallbacksCanReenterReadiness(mode notifyMode) {
	testcontext.For(s.T(), testcontext.WithTimeout(5*time.Second))
	// A lock-order regression can deadlock these goroutines, so stop only after they return.
	r := NewReadiness()
	limiter := s.newLimiter(r, "namespace")
	notifications := make(chan string, 8)
	replacement := s.newCallback(notifications, "replacement")
	reentered := make(chan struct{})
	cb := &testCallback{onReady: func() {
		s.register(limiter, replacement, 20)
		r.CancelAllCallbacks("namespace", replacement)
		close(reentered)
	}}
	s.register(limiter, cb, 10)
	returned := make(chan error, 1)

	go func() {
		returned <- limiter.rs.update(limiter, mode, func() error {
			limiter.wakeCount = 1
			return nil
		})
	}()
	s.Rcv(reentered)
	s.NoError(s.Rcv(returned))
	s.wake(limiter, 10)
	s.requireNotifications(notifications)
	r.Stop()
}

func (s *readinessSuite) TestRegistrationCleanup() {
	r := s.newReadiness()
	first, second := s.newLimiter(r, "namespace"), s.newLimiter(r, "namespace")
	cb, other := &testCallback{}, &testCallback{}
	s.register(first, cb, 10)
	s.register(second, cb, 20)
	s.register(first, other, 30)

	s.unregister(first, cb)
	r.CancelAllCallbacks("namespace", cb)

	// Stale indexes can retain callbacks even when notification behavior is correct.
	s.Equal(map[limiterState][]fwdMapEntry{first: {{cb: other, pri: 30}}}, first.rs.fwd)
	s.Equal(map[ReadinessCallback][]revMapEntry{other: {{limiter: first, pri: 30}}}, first.rs.rev)
	s.wake(first, 10)
	s.Empty(first.rs.fwd)
	s.Empty(first.rs.rev)
}

func (s *readinessSuite) TestStopClearsRegistrations() {
	r := s.newReadiness()
	for _, ns := range []namespace.ID{"11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"} {
		s.register(s.newLimiter(r, ns), &testCallback{}, 10)
	}

	r.Stop()
	r.Stop()

	for i := range r.shards {
		s.Empty(r.shards[i].fwd)
		s.Empty(r.shards[i].rev)
	}
}
