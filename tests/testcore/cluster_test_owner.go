package testcore

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/testing/testlogger"
)

// clusterTestOwner gives a cluster logger a stable test-shaped owner while the
// cluster boots before its test is attached.
type clusterTestOwner struct {
	name string

	mu          sync.Mutex
	test        testlogger.CleanupCapableT
	cleanups    []func()
	pendingLogs []string
	bootFailure error

	failed  atomic.Bool
	booting atomic.Bool
}

var _ testlogger.CleanupCapableT = (*clusterTestOwner)(nil)

type clusterBootAbort struct{}

func newDetachedClusterTestOwner(name string) *clusterTestOwner {
	owner := &clusterTestOwner{name: name}
	owner.booting.Store(true)
	return owner
}

func (s *clusterTestOwner) addTest(t testlogger.CleanupCapableT) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.test = t
	for _, line := range s.pendingLogs {
		t.Log(line)
	}
	s.pendingLogs = nil
}

func (s *clusterTestOwner) removeTest(t testlogger.CleanupCapableT) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.test == t {
		s.test = nil
	}
	return s.test == nil
}

func (s *clusterTestOwner) Logf(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.test != nil {
		s.test.Logf(format, args...)
		return
	}
	if s.booting.Load() {
		s.pendingLogs = append(s.pendingLogs, fmt.Sprintf(format, args...))
		return
	}
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func (s *clusterTestOwner) Log(args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.test != nil {
		s.test.Log(args...)
		return
	}
	if s.booting.Load() {
		s.pendingLogs = append(s.pendingLogs, strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
		return
	}
	fmt.Fprintln(os.Stderr, args...)
}

func (s *clusterTestOwner) Errorf(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.test != nil {
		s.test.Errorf(format, args...)
		return
	}
	s.failed.Store(true)
	message := fmt.Sprintf(format, args...)
	if s.bootFailure == nil {
		s.bootFailure = fmt.Errorf("cluster boot failed: %s", message)
	}
	s.pendingLogs = append(s.pendingLogs, message)
}

func (s *clusterTestOwner) Fail() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.test != nil {
		s.test.Fail()
		return
	}
	s.failed.Store(true)
	if s.bootFailure == nil {
		s.bootFailure = errors.New("cluster boot failed")
	}
}

// FailNow does not forward to the underlying test because it may be called from
// a goroutine other than the test goroutine.
func (s *clusterTestOwner) FailNow() {
	s.Fail()
	if s.booting.Load() {
		panic(clusterBootAbort{})
	}
}

// Fatalf does not forward to the underlying test because it may be called from
// a goroutine other than the test goroutine.
func (s *clusterTestOwner) Fatalf(format string, args ...any) {
	s.recordFatal(fmt.Sprintf(format, args...))
}

// Fatal does not forward to the underlying test because it may be called from
// a goroutine other than the test goroutine.
func (s *clusterTestOwner) Fatal(args ...any) {
	s.recordFatal(strings.TrimSuffix(fmt.Sprintln(args...), "\n"))
}

func (s *clusterTestOwner) recordFatal(message string) {
	s.mu.Lock()
	s.failed.Store(true)
	if s.bootFailure == nil {
		s.bootFailure = fmt.Errorf("cluster boot failed: %s", message)
	}
	s.pendingLogs = append(s.pendingLogs, "FATAL: "+message)
	s.mu.Unlock()
	fmt.Fprintln(os.Stderr, "FATAL: "+message)
	if s.booting.Load() {
		panic(clusterBootAbort{})
	}
}

func (s *clusterTestOwner) Failed() bool {
	return s.failed.Load()
}

func (s *clusterTestOwner) bootError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bootFailure
}

func (s *clusterTestOwner) finishBoot() {
	s.booting.Store(false)
}

func (s *clusterTestOwner) Helper() {}

func (s *clusterTestOwner) Name() string {
	return s.name
}

func (s *clusterTestOwner) Cleanup(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanups = append(s.cleanups, fn)
}

// doCleanups runs queued cleanup callbacks in LIFO order (matches *testing.T.Cleanup).
func (s *clusterTestOwner) doCleanups() {
	s.mu.Lock()
	cleanups := s.cleanups
	s.cleanups = nil
	s.mu.Unlock()
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}
