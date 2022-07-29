// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	priorityMutexSuite struct {
		*require.Assertions
		suite.Suite

		lock *PriorityMutexImpl
	}
)

func BenchmarkPriorityMutex_High(b *testing.B) {
	b.ReportAllocs()

	lock := NewPriorityMutex()
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		_ = lock.LockHigh(ctx)
		lock.UnlockHigh()
	}
}

func BenchmarkPriorityMutex_Low(b *testing.B) {
	b.ReportAllocs()

	lock := NewPriorityMutex()
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		_ = lock.LockLow(ctx)
		lock.UnlockLow()
	}
}

func TestPriorityMutexSuite(t *testing.T) {
	s := new(priorityMutexSuite)
	suite.Run(t, s)
}

func (s *priorityMutexSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *priorityMutexSuite) TearDownSuite() {

}

func (s *priorityMutexSuite) SetupTest() {
	s.lock = NewPriorityMutex()
}

func (s *priorityMutexSuite) TearDownTest() {

}

func (s *priorityMutexSuite) TestTryLock_High() {
	s.True(s.lock.TryLockHigh())
	s.False(s.lock.TryLockHigh())
	s.False(s.lock.TryLockLow())
	s.lock.UnlockHigh()
	s.False(s.lock.IsLocked())
}

func (s *priorityMutexSuite) TestLock_High_Success() {
	ctx := context.Background()
	err := s.lock.LockHigh(ctx)
	s.NoError(err)
	s.True(s.lock.IsLocked())
	s.lock.UnlockHigh()
	s.False(s.lock.IsLocked())
}

func (s *priorityMutexSuite) TestLock_High_Fail() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	err := s.lock.LockHigh(ctx)
	s.Error(err)
	s.False(s.lock.IsLocked())
}

func (s *priorityMutexSuite) TestTryLock_Low() {
	s.True(s.lock.TryLockLow())
	s.False(s.lock.TryLockHigh())
	s.False(s.lock.TryLockLow())
	s.lock.UnlockLow()
	s.False(s.lock.IsLocked())
}

func (s *priorityMutexSuite) TestLock_Low_Success() {
	ctx := context.Background()
	err := s.lock.LockLow(ctx)
	s.NoError(err)
	s.True(s.lock.IsLocked())
	s.lock.UnlockLow()
	s.False(s.lock.IsLocked())
}

func (s *priorityMutexSuite) TestLock_Low_Fail() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	err := s.lock.LockLow(ctx)
	s.Error(err)
	s.False(s.lock.IsLocked())
}

func (s *priorityMutexSuite) Test_LockHigh_UnlockLow() {
	ctx := context.Background()

	err := s.lock.LockHigh(ctx)
	s.NoError(err)
	s.Panics(s.lock.UnlockLow)
	s.lock.UnlockHigh()
}

func (s *priorityMutexSuite) Test_LockLow_UnlockHigh() {
	ctx := context.Background()

	err := s.lock.LockLow(ctx)
	s.NoError(err)
	s.Panics(s.lock.UnlockHigh)
	s.lock.UnlockLow()
}

func (s *priorityMutexSuite) TestLock_Mixed() {
	concurrency := 1024
	ctx := context.Background()
	err := s.lock.LockHigh(ctx)
	s.NoError(err)

	startWaitGroup := sync.WaitGroup{}
	endWaitGroup := sync.WaitGroup{}
	startWaitGroup.Add(concurrency * 2)
	endWaitGroup.Add(concurrency * 2)

	resultChan := make(chan int, concurrency*2)

	lowFn := func() {
		startWaitGroup.Done()
		err := s.lock.LockLow(ctx)
		s.NoError(err)
		resultChan <- 0

		s.lock.UnlockLow()
		endWaitGroup.Done()
	}
	highFn := func() {
		startWaitGroup.Done()
		err := s.lock.LockHigh(ctx)
		s.NoError(err)
		resultChan <- 1

		s.lock.UnlockHigh()
		endWaitGroup.Done()
	}
	for i := 0; i < concurrency; i++ {
		go lowFn()
		go highFn()
	}

	startWaitGroup.Wait()
	s.lock.UnlockHigh()
	endWaitGroup.Wait()
	close(resultChan)

	results := make([]int, 0, 2*concurrency)
	for result := range resultChan {
		results = append(results, result)
	}
	s.Equal(2*concurrency, len(results))

	zeros := float64(0)
	totalZeros := float64(concurrency)
	possibility := float64(0)
	for i := 2*concurrency - 1; i > -1; i-- {
		switch results[i] {
		case 0:
			zeros += 1
		case 1:
			possibility += zeros / totalZeros
		default:
			panic("unexpected number, can only be 0 or 1")
		}
	}

	overallPossibility := possibility / float64(concurrency)
	fmt.Printf("overall possibility: %.2f\n", overallPossibility)
	s.True(overallPossibility >= 0.5)
}
