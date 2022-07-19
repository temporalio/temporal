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

	"github.com/dgryski/go-farm"
	"github.com/stretchr/testify/suite"
)

type (
	idMutexSuite struct {
		suite.Suite

		numShard uint32
		idMutex  IDMutex
	}

	testIdentifier struct {
		A string
		B string
		C string
	}
)

func BenchmarkGolangMutex(b *testing.B) {
	lock := &sync.Mutex{}
	for i := 0; i < b.N; i++ {
		lock.Lock()
		func() {}()
		lock.Unlock()
	}
}

func BenchmarkIDMutex_String(b *testing.B) {
	identifier := "random string"
	idLock := NewIDMutex(32, func(key interface{}) uint32 {
		id, ok := key.(string)
		if !ok {
			return 0
		}
		return farm.Fingerprint32([]byte(id))
	})

	for i := 0; i < b.N; i++ {
		idLock.LockID(identifier)
		idLock.UnlockID(identifier)
	}
}

func BenchmarkIDMutex_Struct(b *testing.B) {
	identifier := testIdentifier{
		A: "some random A",
		B: "some random B",
		C: "some random C",
	}
	idLock := NewIDMutex(32, func(key interface{}) uint32 {
		id, ok := key.(testIdentifier)
		if !ok {
			return 0
		}
		return farm.Fingerprint32([]byte(id.A))
	})

	for i := 0; i < b.N; i++ {
		idLock.LockID(identifier)
		idLock.UnlockID(identifier)
	}
}

func BenchmarkIDMutex_StringConcurrent(b *testing.B) {
	identifier := "random string"
	idLock := NewIDMutex(32, func(key interface{}) uint32 {
		id, ok := key.(string)
		if !ok {
			return 0
		}
		return farm.Fingerprint32([]byte(id))
	})

	for i := 0; i < b.N; i++ {
		counter := 0
		iteration := 2000

		waitGroupBegin := &sync.WaitGroup{}
		waitGroupBegin.Add(1)

		waitGroupEnd := &sync.WaitGroup{}
		waitGroupEnd.Add(iteration)

		fn := func() {
			waitGroupBegin.Wait()

			idLock.LockID(identifier)
			counter++
			idLock.UnlockID(identifier)

			waitGroupEnd.Done()
		}

		for i := 0; i < iteration; i++ {
			go fn()
		}
		waitGroupBegin.Done()
		waitGroupEnd.Wait()
	}
}

func TestIDMutexSuite(t *testing.T) {
	s := new(idMutexSuite)
	suite.Run(t, s)
}

func (s *idMutexSuite) SetupSuite() {
}

func (s *idMutexSuite) TearDownSuite() {

}

func (s *idMutexSuite) SetupTest() {
	s.numShard = 32
	s.idMutex = NewIDMutex(s.numShard, func(key interface{}) uint32 {
		id, ok := key.(string)
		if !ok {
			return 0
		}
		return farm.Fingerprint32([]byte(id))
	})
}

func (s *idMutexSuite) TearDownTest() {

}

func (s *idMutexSuite) TestLockOnDiffIDs() {
	identifier1 := "some random identifier 1"
	identifier2 := "some random identifier 2"

	s.idMutex.LockID(identifier1)
	s.idMutex.LockID(identifier2)
	s.idMutex.UnlockID(identifier2)
	s.idMutex.UnlockID(identifier1)

	s.idMutex.LockID(identifier1)
	s.idMutex.LockID(identifier2)
	s.idMutex.UnlockID(identifier1)
	s.idMutex.UnlockID(identifier2)
}

func (s *idMutexSuite) TestLockOnSameID() {
	count := 0
	identifier := "some random identifier"

	waitGroupBegin := &sync.WaitGroup{}
	waitGroupBegin.Add(1)

	waitGroupEnd := &sync.WaitGroup{}
	waitGroupEnd.Add(1)

	s.idMutex.LockID(identifier)
	go func() {
		waitGroupBegin.Done()
		s.idMutex.LockID(identifier)
		s.Equal(1, count)
		s.idMutex.UnlockID(identifier)
		waitGroupEnd.Done()
	}()
	waitGroupBegin.Wait()
	count = 1
	s.idMutex.UnlockID(identifier)
	waitGroupEnd.Wait()
}

func (s *idMutexSuite) TestConcurrentAccess() {
	counter := 0
	identifier := "some random identifier"
	iteration := 2000

	waitGroupBegin := &sync.WaitGroup{}
	waitGroupBegin.Add(1)

	waitGroupEnd := &sync.WaitGroup{}
	waitGroupEnd.Add(iteration)

	fn := func() {
		waitGroupBegin.Wait()

		s.idMutex.LockID(identifier)
		counter++
		s.idMutex.UnlockID(identifier)

		waitGroupEnd.Done()
	}

	for i := 0; i < iteration; i++ {
		go fn()
	}
	waitGroupBegin.Done()
	waitGroupEnd.Wait()

	s.Equal(iteration, counter)
	impl := s.idMutex.(*idMutexImpl)
	for i := uint32(0); i < s.numShard; i++ {
		s.Equal(0, len(impl.shards[i].mutexInfos))
	}
}
