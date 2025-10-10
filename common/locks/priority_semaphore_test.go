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

func (s *prioritySemaphoreSuite) TestTryAcquire() {
	semaphore := NewPrioritySemaphore(2)
	require.True(s.T(), semaphore.TryAcquire(1))
	require.True(s.T(), semaphore.TryAcquire(1))
	require.False(s.T(), semaphore.TryAcquire(1))
	require.False(s.T(), semaphore.TryAcquire(1))
	semaphore.Release(2)
	require.True(s.T(), semaphore.TryAcquire(1))
}

func (s *prioritySemaphoreSuite) TestAcquire_High_Success() {
	semaphore := NewPrioritySemaphore(1)
	ctx := context.Background()
	err := semaphore.Acquire(ctx, PriorityHigh, 1)
	require.NoError(s.T(), err)
	require.False(s.T(), semaphore.TryAcquire(1))
	semaphore.Release(1)
	require.True(s.T(), semaphore.TryAcquire(1))
}

func (s *prioritySemaphoreSuite) TestAcquire_Low_Success() {
	semaphore := NewPrioritySemaphore(1)
	ctx := context.Background()
	err := semaphore.Acquire(ctx, PriorityLow, 1)
	require.NoError(s.T(), err)
	require.False(s.T(), semaphore.TryAcquire(1))
	semaphore.Release(1)
	require.True(s.T(), semaphore.TryAcquire(1))
}

func (s *prioritySemaphoreSuite) TestTryAcquire_HighAfterWaiting() {
	semaphore := NewPrioritySemaphore(1)
	cLock := make(chan struct{})
	go func() {
		// Acquire the function to make the next call blocking.
		require.True(s.T(), semaphore.TryAcquire(1))
		// Let the other thread start which will block on this semaphore.
		cLock <- struct{}{}
		// Wait for other thread to block on this semaphore.
		s.waitUntilBlockedInSemaphore(1)
		// Release the semaphore so that blocking thread can resume.
		semaphore.Release(1)
	}()
	<-cLock
	require.NoError(s.T(), semaphore.Acquire(context.Background(), PriorityHigh, 1))
}

func (s *prioritySemaphoreSuite) TestTryAcquire_LowAfterWaiting() {
	semaphore := NewPrioritySemaphore(1)
	cLock := make(chan struct{})
	go func() {
		// Acquire the function to make the next call blocking.
		require.True(s.T(), semaphore.TryAcquire(1))
		// Let the other thread start which will block on this semaphore.
		cLock <- struct{}{}
		// Wait for other thread to block on this semaphore.
		s.waitUntilBlockedInSemaphore(1)
		// Release the semaphore so that blocking thread can resume.
		semaphore.Release(1)
	}()
	<-cLock
	require.NoError(s.T(), semaphore.Acquire(context.Background(), PriorityLow, 1))
}

func (s *prioritySemaphoreSuite) TestTryAcquire_HighAllowedBeforeLow() {
	semaphore := NewPrioritySemaphore(1)
	wg := sync.WaitGroup{}
	require.True(s.T(), semaphore.TryAcquire(1))
	wg.Add(1)
	go func() {
		s.waitUntilBlockedInSemaphore(2)
		semaphore.Release(1)
		wg.Done()
	}()
	wg.Add(1)
	lowAcquired := false
	go func() {
		require.NoError(s.T(), semaphore.Acquire(context.Background(), PriorityLow, 1))
		lowAcquired = true
		wg.Done()
	}()
	require.NoError(s.T(), semaphore.Acquire(context.Background(), PriorityHigh, 1))
	// Checking if LowPriority goroutine is still waiting.
	require.False(s.T(), lowAcquired)
	semaphore.Release(1)
	wg.Wait()
	// Checking if LowPriority goroutine acquired semaphore.
	require.True(s.T(), lowAcquired)
}

func (s *prioritySemaphoreSuite) Test_AllThreadsAreWokenUp() {
	semaphore := NewPrioritySemaphore(10)
	ctx := context.Background()
	require.NoError(s.T(), semaphore.Acquire(ctx, PriorityHigh, 6))
	require.NoError(s.T(), semaphore.Acquire(ctx, PriorityHigh, 4))

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 5; i++ {
		go func() {
			require.NoError(s.T(), semaphore.Acquire(ctx, PriorityHigh, 1))
			wg.Done()
		}()
	}
	for i := 5; i < 10; i++ {
		go func() {
			require.NoError(s.T(), semaphore.Acquire(ctx, PriorityLow, 1))
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
	require.ErrorIs(s.T(), semaphore.Acquire(ctx, PriorityHigh, 1), ctx.Err())
}

func (s *prioritySemaphoreSuite) Test_InvalidPriority() {
	semaphore := NewPrioritySemaphore(1)
	require.Panics(s.T(), func() {
		_ = semaphore.Acquire(context.Background(), Priority(10), 1)
	})
}

func (s *prioritySemaphoreSuite) Test_TimedOutWaitingForLock() {
	semaphore := NewPrioritySemaphore(1)
	require.NoError(s.T(), semaphore.Acquire(context.Background(), PriorityHigh, 1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(s.T(), semaphore.Acquire(ctx, PriorityHigh, 1), ctx.Err())
}

func (s *prioritySemaphoreSuite) Test_AcquireMoreThanAvailable() {
	semaphore := NewPrioritySemaphore(1)
	require.ErrorIs(s.T(), semaphore.Acquire(context.Background(), PriorityHigh, 2), ErrRequestTooLarge)
}

// Checks if n number of threads are blocked in semaphore.
func (s *prioritySemaphoreSuite) waitUntilBlockedInSemaphore(n int) {
	pattern := `\[select\]\:\n\S*\(\*PrioritySemaphoreImpl\)\.Acquire`
	re := regexp.MustCompile(pattern)
	require.Eventually(s.T(),
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
