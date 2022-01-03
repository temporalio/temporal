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
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

type (
	idMutexSuite struct {
		suite.Suite
	}
)

func TestIDMutexSuite(t *testing.T) {
	s := new(idMutexSuite)
	suite.Run(t, s)
}

func (s *idMutexSuite) TestLockOnDiffIDs() {
	idMutex := NewIDMutex()

	identifier1 := "some random identifier 1"
	identifier2 := "some random identifier 2"

	unlock1 := idMutex.LockID(identifier1)
	unlock2 := idMutex.LockID(identifier2)
	unlock2()
	unlock1()

	unlock1 = idMutex.LockID(identifier1)
	unlock2 = idMutex.LockID(identifier2)
	unlock1()
	unlock2()
}

func (s *idMutexSuite) TestLockOnSameID() {
	idMutex := NewIDMutex()
	count := 0
	identifier := "some random identifier"

	waitGroupBegin := &sync.WaitGroup{}
	waitGroupBegin.Add(1)

	waitGroupEnd := &sync.WaitGroup{}
	waitGroupEnd.Add(1)

	unlock := idMutex.LockID(identifier)
	go func() {
		waitGroupBegin.Done()
		unlock := idMutex.LockID(identifier)
		s.Equal(1, count)
		unlock()
		waitGroupEnd.Done()
	}()
	waitGroupBegin.Wait()
	count = 1
	unlock()
	waitGroupEnd.Wait()
}

func (s *idMutexSuite) TestConcurrentAccess() {
	idMutex := NewIDMutex()
	counter := 0
	identifier := "some random identifier"
	iteration := 2000

	waitGroupBegin := &sync.WaitGroup{}
	waitGroupBegin.Add(1)

	waitGroupEnd := &sync.WaitGroup{}
	waitGroupEnd.Add(iteration)

	fn := func() {
		waitGroupBegin.Wait()

		unlock := idMutex.LockID(identifier)
		counter++
		unlock()

		waitGroupEnd.Done()
	}

	for i := 0; i < iteration; i++ {
		go fn()
	}
	waitGroupBegin.Done()
	waitGroupEnd.Wait()

	s.Equal(iteration, counter)
}
