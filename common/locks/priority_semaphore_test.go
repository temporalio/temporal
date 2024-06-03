// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package locks

import (
	"context"
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
	s.T().Parallel()
	semaphore := NewPrioritySemaphore(2)
	s.True(semaphore.TryAcquire(1))
	s.True(semaphore.TryAcquire(1))
	s.False(semaphore.TryAcquire(1))
	s.False(semaphore.TryAcquire(1))
	semaphore.Release(2)
	s.True(semaphore.TryAcquire(1))
}

func (s *prioritySemaphoreSuite) TestAcquire_High_Success() {
	s.T().Parallel()
	semaphore := NewPrioritySemaphore(1)
	ctx := context.Background()
	err := semaphore.Acquire(ctx, PriorityHigh, 1)
	s.NoError(err)
	s.False(semaphore.TryAcquire(1))
	semaphore.Release(1)
	s.True(semaphore.TryAcquire(1))
}

func (s *prioritySemaphoreSuite) TestAcquire_Low_Success() {
	s.T().Parallel()
	semaphore := NewPrioritySemaphore(1)
	ctx := context.Background()
	err := semaphore.Acquire(ctx, PriorityLow, 1)
	s.NoError(err)
	s.False(semaphore.TryAcquire(1))
	semaphore.Release(1)
	s.True(semaphore.TryAcquire(1))
}

func (s *prioritySemaphoreSuite) TestTryAcquire_HighAfterWaiting() {
	s.T().Parallel()
	semaphore := NewPrioritySemaphore(1)
	cLock := make(chan struct{})
	go func() {
		// Acquire the function to make the next call blocking.
		s.True(semaphore.TryAcquire(1))
		// Let the other thread start which will block on this semaphore.
		cLock <- struct{}{}
		// Wait for other thread to block on this semaphore.
		time.Sleep(time.Second) // nolint
		// Release the semaphore so that blocking thread can resume.
		semaphore.Release(1)
	}()
	<-cLock
	s.NoError(semaphore.Acquire(context.Background(), PriorityHigh, 1))
}

func (s *prioritySemaphoreSuite) TestTryAcquire_LowAfterWaiting() {
	s.T().Parallel()
	semaphore := NewPrioritySemaphore(1)
	cLock := make(chan struct{})
	go func() {
		// Acquire the function to make the next call blocking.
		s.True(semaphore.TryAcquire(1))
		// Let the other thread start which will block on this semaphore.
		cLock <- struct{}{}
		// Wait for other thread to block on this semaphore.
		time.Sleep(time.Second) // nolint
		// Release the semaphore so that blocking thread can resume.
		semaphore.Release(1)
	}()
	<-cLock
	s.NoError(semaphore.Acquire(context.Background(), PriorityLow, 1))
}

func (s *prioritySemaphoreSuite) TestTryAcquire_HighAllowedBeforeLow() {
	s.T().Parallel()
	semaphore := NewPrioritySemaphore(1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s.True(semaphore.TryAcquire(1))
		time.Sleep(3 * time.Second) // nolint
		semaphore.Release(1)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		time.Sleep(1 * time.Second) // nolint
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		s.Error(semaphore.Acquire(ctx, PriorityLow, 1))
		wg.Done()
	}()
	time.Sleep(2 * time.Second) // nolint
	s.NoError(semaphore.Acquire(context.Background(), PriorityHigh, 1))
	semaphore.Release(1)
	wg.Wait()
}

func (s *prioritySemaphoreSuite) Test_AllThreadsAreWokenUp() {
	s.T().Parallel()
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
	time.Sleep(time.Second) // nolint

	semaphore.Release(6)
	semaphore.Release(4)

	wg.Wait()
}

func (s *prioritySemaphoreSuite) Test_ContextCanceledBeforeAcquire() {
	s.T().Parallel()
	semaphore := NewPrioritySemaphore(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.ErrorIs(semaphore.Acquire(ctx, PriorityHigh, 1), ctx.Err())
}

func (s *prioritySemaphoreSuite) Test_InvalidPriority() {
	s.T().Parallel()
	semaphore := NewPrioritySemaphore(1)
	s.Panics(func() {
		_ = semaphore.Acquire(context.Background(), Priority(10), 1)
	})
}

func (s *prioritySemaphoreSuite) Test_TimedOutWaitingForLock() {
	s.T().Parallel()
	semaphore := NewPrioritySemaphore(1)
	s.NoError(semaphore.Acquire(context.Background(), PriorityHigh, 1))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.ErrorIs(semaphore.Acquire(ctx, PriorityHigh, 1), ctx.Err())
}

func (s *prioritySemaphoreSuite) Test_AcquireMoreThanAvailable() {
	s.T().Parallel()
	semaphore := NewPrioritySemaphore(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.ErrorIs(semaphore.Acquire(ctx, PriorityHigh, 2), ctx.Err())
}
