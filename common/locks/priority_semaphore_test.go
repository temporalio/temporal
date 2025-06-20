package locks

import (
	"context"
	"regexp"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	prioritySemaphoreSuite struct {
		*require.Assertions
		suite.Suite
	}
)

func BenchmarkPrioritySemaphore_High(b *testing.B) {
	b.ReportAllocs()

	semaphore := NewPrioritySemaphore(1)
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		_ = semaphore.Acquire(ctx, PriorityHigh, 1)
		semaphore.Release(1)
	}
}

func BenchmarkPrioritySemaphore_Low(b *testing.B) {
	b.ReportAllocs()

	semaphore := NewPrioritySemaphore(1)
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		_ = semaphore.Acquire(ctx, PriorityLow, 1)
		semaphore.Release(1)
	}
}

func TestPrioritySemaphoreSuite(t *testing.T) {
	s := new(prioritySemaphoreSuite)
	suite.Run(t, s)
}

func (s *prioritySemaphoreSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *prioritySemaphoreSuite) TestTryAcquire() {
	semaphore := NewPrioritySemaphore(2)
	s.True(semaphore.TryAcquire(1))
	s.True(semaphore.TryAcquire(1))
	s.False(semaphore.TryAcquire(1))
	s.False(semaphore.TryAcquire(1))
	semaphore.Release(2)
	s.True(semaphore.TryAcquire(1))
}

func (s *prioritySemaphoreSuite) TestAcquire_High_Success() {
	semaphore := NewPrioritySemaphore(1)
	ctx := context.Background()
	err := semaphore.Acquire(ctx, PriorityHigh, 1)
	s.NoError(err)
	s.False(semaphore.TryAcquire(1))
	semaphore.Release(1)
	s.True(semaphore.TryAcquire(1))
}

func (s *prioritySemaphoreSuite) TestAcquire_Low_Success() {
	semaphore := NewPrioritySemaphore(1)
	ctx := context.Background()
	err := semaphore.Acquire(ctx, PriorityLow, 1)
	s.NoError(err)
	s.False(semaphore.TryAcquire(1))
	semaphore.Release(1)
	s.True(semaphore.TryAcquire(1))
}

func (s *prioritySemaphoreSuite) TestTryAcquire_HighAfterWaiting() {
	semaphore := NewPrioritySemaphore(1)
	cLock := make(chan struct{})
	go func() {
		// Acquire the function to make the next call blocking.
		s.True(semaphore.TryAcquire(1))
		// Let the other thread start which will block on this semaphore.
		cLock <- struct{}{}
		// Wait for other thread to block on this semaphore.
		s.waitUntilBlockedInSemaphore(1)
		// Release the semaphore so that blocking thread can resume.
		semaphore.Release(1)
	}()
	<-cLock
	s.NoError(semaphore.Acquire(context.Background(), PriorityHigh, 1))
}

func (s *prioritySemaphoreSuite) TestTryAcquire_LowAfterWaiting() {
	semaphore := NewPrioritySemaphore(1)
	cLock := make(chan struct{})
	go func() {
		// Acquire the function to make the next call blocking.
		s.True(semaphore.TryAcquire(1))
		// Let the other thread start which will block on this semaphore.
		cLock <- struct{}{}
		// Wait for other thread to block on this semaphore.
		s.waitUntilBlockedInSemaphore(1)
		// Release the semaphore so that blocking thread can resume.
		semaphore.Release(1)
	}()
	<-cLock
	s.NoError(semaphore.Acquire(context.Background(), PriorityLow, 1))
}

func (s *prioritySemaphoreSuite) TestTryAcquire_HighAllowedBeforeLow() {
	semaphore := NewPrioritySemaphore(1)
	wg := sync.WaitGroup{}
	s.True(semaphore.TryAcquire(1))
	wg.Add(1)
	go func() {
		s.waitUntilBlockedInSemaphore(2)
		semaphore.Release(1)
		wg.Done()
	}()
	wg.Add(1)
	lowAcquired := false
	go func() {
		s.NoError(semaphore.Acquire(context.Background(), PriorityLow, 1))
		lowAcquired = true
		wg.Done()
	}()
	s.NoError(semaphore.Acquire(context.Background(), PriorityHigh, 1))
	// Checking if LowPriority goroutine is still waiting.
	s.False(lowAcquired)
	semaphore.Release(1)
	wg.Wait()
	// Checking if LowPriority goroutine acquired semaphore.
	s.True(lowAcquired)
}

func (s *prioritySemaphoreSuite) Test_AllThreadsAreWokenUp() {
	semaphore := NewPrioritySemaphore(10)
	ctx := context.Background()
	s.NoError(semaphore.Acquire(ctx, PriorityHigh, 6))
	s.NoError(semaphore.Acquire(ctx, PriorityHigh, 4))

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 5; i++ {
		go func() {
			s.NoError(semaphore.Acquire(ctx, PriorityHigh, 1))
			wg.Done()
		}()
	}
	for i := 5; i < 10; i++ {
		go func() {
			s.NoError(semaphore.Acquire(ctx, PriorityLow, 1))
			wg.Done()
		}()
	}

	// Waiting for all above goroutines to block on semaphore.
	s.waitUntilBlockedInSemaphore(10)

	semaphore.Release(6)
	semaphore.Release(4)

	wg.Wait()
}

func (s *prioritySemaphoreSuite) Test_ContextCanceledBeforeAcquire() {
	semaphore := NewPrioritySemaphore(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.ErrorIs(semaphore.Acquire(ctx, PriorityHigh, 1), ctx.Err())
}

func (s *prioritySemaphoreSuite) Test_InvalidPriority() {
	semaphore := NewPrioritySemaphore(1)
	s.Panics(func() {
		_ = semaphore.Acquire(context.Background(), Priority(10), 1)
	})
}

func (s *prioritySemaphoreSuite) Test_TimedOutWaitingForLock() {
	semaphore := NewPrioritySemaphore(1)
	s.NoError(semaphore.Acquire(context.Background(), PriorityHigh, 1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.ErrorIs(semaphore.Acquire(ctx, PriorityHigh, 1), ctx.Err())
}

func (s *prioritySemaphoreSuite) Test_AcquireMoreThanAvailable() {
	semaphore := NewPrioritySemaphore(1)
	s.ErrorIs(semaphore.Acquire(context.Background(), PriorityHigh, 2), ErrRequestTooLarge)
}

// Checks if n number of threads are blocked in semaphore.
func (s *prioritySemaphoreSuite) waitUntilBlockedInSemaphore(n int) {
	pattern := `\[select\]\:\n\S*\(\*PrioritySemaphoreImpl\)\.Acquire`
	re := regexp.MustCompile(pattern)
	s.Eventually(
		func() bool {
			buf := make([]byte, 100000)
			size := runtime.Stack(buf, true)
			threads := len(re.FindAllIndex(buf[:size], -1))
			if threads == n {
				return true
			}
			return false
		},
		10*time.Second,
		100*time.Millisecond,
	)
}
