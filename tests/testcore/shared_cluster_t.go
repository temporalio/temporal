package testcore

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/testing/testlogger"
)

// sharedClusterT is the testlogger.CleanupCapableT that backs a shared
// FunctionalTestBase's logger. It tracks the cluster's active tests and
// routes T-shaped calls to them, so one logger can serve many tests across
// the cluster's lifetime.
type sharedClusterT struct {
	name string

	mu          sync.Mutex
	activeTests []testlogger.CleanupCapableT
	cleanups    []func()

	failed atomic.Bool
}

var _ testlogger.CleanupCapableT = (*sharedClusterT)(nil)

func (s *sharedClusterT) addTest(t testlogger.CleanupCapableT) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeTests = append(s.activeTests, t)
}

// removeTest drops t from the active set and reports whether the set is now empty.
func (s *sharedClusterT) removeTest(t testlogger.CleanupCapableT) (wasLast bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, x := range s.activeTests {
		if x == t {
			s.activeTests = append(s.activeTests[:i], s.activeTests[i+1:]...)
			break
		}
	}
	return len(s.activeTests) == 0
}

// currentT returns the sole registered test, or nil if zero or many.
func (s *sharedClusterT) currentT() testlogger.CleanupCapableT {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.activeTests) == 1 {
		return s.activeTests[0]
	}
	return nil
}

// activeTestsSnapshot copies activeTests so callers can iterate without
// holding the lock while forwarding.
func (s *sharedClusterT) activeTestsSnapshot() []testlogger.CleanupCapableT {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := make([]testlogger.CleanupCapableT, len(s.activeTests))
	copy(snap, s.activeTests)
	return snap
}

func (s *sharedClusterT) Logf(format string, args ...any) {
	if t := s.currentT(); t != nil {
		t.Logf(format, args...)
		return
	}
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func (s *sharedClusterT) Log(args ...any) {
	if t := s.currentT(); t != nil {
		t.Log(args...)
		return
	}
	fmt.Fprintln(os.Stderr, args...)
}

func (s *sharedClusterT) Errorf(format string, args ...any) {
	for _, t := range s.activeTestsSnapshot() {
		t.Errorf(format, args...)
	}
}

func (s *sharedClusterT) Fail() {
	for _, t := range s.activeTestsSnapshot() {
		t.Fail()
	}
}

// FailNow does not forward to the underlying tests, since we do not know what test
// we are targetting or what goroutine we are calling FailNow from.
func (s *sharedClusterT) FailNow() {
	s.failed.Store(true)
}

// Fatalf does not forward to the underlying tests, since we do not know what test
// we are targetting or what goroutine we are calling Fatalf from.
func (s *sharedClusterT) Fatalf(format string, args ...any) {
	s.failed.Store(true)
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
}

// Fatal does not forward to the underlying tests, since we do not know what test
// we are targetting or what goroutine we are calling Fatal from.
func (s *sharedClusterT) Fatal(args ...any) {
	s.failed.Store(true)
	fmt.Fprintln(os.Stderr, append([]any{"FATAL:"}, args...)...)
}

func (s *sharedClusterT) Failed() bool {
	return s.failed.Load()
}

func (s *sharedClusterT) Helper() {}

func (s *sharedClusterT) Name() string {
	return s.name
}

func (s *sharedClusterT) Cleanup(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanups = append(s.cleanups, fn)
}

// doCleanups runs queued cleanup callbacks in LIFO order (matches *testing.T.Cleanup).
func (s *sharedClusterT) doCleanups() {
	s.mu.Lock()
	cs := s.cleanups
	s.cleanups = nil
	s.mu.Unlock()
	for i := len(cs) - 1; i >= 0; i-- {
		cs[i]()
	}
}
