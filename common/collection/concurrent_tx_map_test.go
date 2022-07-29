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

package collection

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	ConcurrentTxMapSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
	boolType bool
	intType  int
)

func TestConcurrentTxMapSuite(t *testing.T) {
	suite.Run(t, new(ConcurrentTxMapSuite))
}

func (s *ConcurrentTxMapSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *ConcurrentTxMapSuite) TestLen() {
	testMap := NewShardedConcurrentTxMap(1, UUIDHashCode)

	key1 := "0001"
	testMap.Put(key1, boolType(true))
	s.Equal(1, testMap.Len(), "Wrong concurrent map size")

	testMap.Put(key1, boolType(false))
	s.Equal(1, testMap.Len(), "Wrong concurrent map size")

	key2 := "0002"
	testMap.Put(key2, boolType(false))
	s.Equal(2, testMap.Len(), "Wrong concurrent map size")

	testMap.PutIfNotExist(key2, boolType(false))
	s.Equal(2, testMap.Len(), "Wrong concurrent map size")

	testMap.Remove(key2)
	s.Equal(1, testMap.Len(), "Wrong concurrent map size")

	testMap.Remove(key2)
	s.Equal(1, testMap.Len(), "Wrong concurrent map size")
}

func (s *ConcurrentTxMapSuite) TestGetAndDo() {
	testMap := NewShardedConcurrentTxMap(1, UUIDHashCode)
	key := uuid.New()
	var value intType
	fnApplied := false

	interf, ok, err := testMap.GetAndDo(key, func(key interface{}, value interface{}) error {
		fnApplied = true
		return nil
	})
	s.Nil(interf, "GetAndDo should return nil when key not found")
	s.Nil(err, "GetAndDo should return nil when function not applied")
	s.False(ok, "GetAndDo should return false when key not found")
	s.False(fnApplied, "GetAndDo should not apply function when key not exixts")

	value = intType(1)
	testMap.Put(key, &value)
	interf, ok, err = testMap.GetAndDo(key, func(key interface{}, value interface{}) error {
		fnApplied = true
		intValue := value.(*intType)
		*intValue++
		return errors.New("some err")
	})

	value1 := interf.(*intType)
	s.Equal(*(value1), intType(2))
	s.NotNil(err, "GetAndDo should return non nil when function applied")
	s.True(ok, "GetAndDo should return true when key found")
	s.True(fnApplied, "GetAndDo should apply function when key exixts")
}

func (s *ConcurrentTxMapSuite) TestPutOrDo() {
	testMap := NewShardedConcurrentTxMap(1, UUIDHashCode)
	key := uuid.New()
	var value intType
	fnApplied := false

	value = intType(1)
	interf, ok, err := testMap.PutOrDo(key, &value, func(key interface{}, value interface{}) error {
		fnApplied = true
		return errors.New("some err")
	})
	valueRetuern := interf.(*intType)
	s.Equal(value, *valueRetuern)
	s.Nil(err, "PutOrDo should return nil when function not applied")
	s.False(ok, "PutOrDo should return false when function not applied")
	s.False(fnApplied, "PutOrDo should not apply function when key not exixts")

	anotherValue := intType(111)
	interf, ok, err = testMap.PutOrDo(key, &anotherValue, func(key interface{}, value interface{}) error {
		fnApplied = true
		intValue := value.(*intType)
		*intValue++
		return errors.New("some err")
	})
	valueRetuern = interf.(*intType)
	s.Equal(value, *valueRetuern)
	s.NotNil(err, "PutOrDo should return non nil when function applied")
	s.True(ok, "PutOrDo should return true when function applied")
	s.True(fnApplied, "PutOrDo should apply function when key exixts")
}

func (s *ConcurrentTxMapSuite) TestRemoveIf() {
	testMap := NewShardedConcurrentTxMap(1, UUIDHashCode)
	key := uuid.New()
	value := intType(1)
	testMap.Put(key, &value)

	removed := testMap.RemoveIf(key, func(key interface{}, value interface{}) bool {
		intValue := value.(*intType)
		return *intValue == intType(2)
	})
	s.Equal(1, testMap.Len(), "TestRemoveIf should only entry if condition is met")
	s.False(removed, "TestRemoveIf should return false if key is not deleted")

	removed = testMap.RemoveIf(key, func(key interface{}, value interface{}) bool {
		intValue := value.(*intType)
		return *intValue == intType(1)
	})
	s.Equal(0, testMap.Len(), "TestRemoveIf should only entry if condition is met")
	s.True(removed, "TestRemoveIf should return true if key is deleted")
}

func (s *ConcurrentTxMapSuite) TestGetAfterPut() {

	countMap := make(map[string]int)
	testMap := NewShardedConcurrentTxMap(1, UUIDHashCode)

	for i := 0; i < 1024; i++ {
		key := uuid.New()
		countMap[key] = 0
		testMap.Put(key, boolType(true))
	}

	for k := range countMap {
		v, ok := testMap.Get(k)
		boolValue := v.(boolType)
		s.True(ok, "Get after put failed")
		s.True(bool(boolValue), "Wrong value returned from map")
	}

	s.Equal(len(countMap), testMap.Len(), "Size() returned wrong value")

	it := testMap.Iter()
	for entry := range it.Entries() {
		countMap[entry.Key.(string)]++
	}
	it.Close()

	for _, v := range countMap {
		s.Equal(1, v, "Iterator test failed")
	}

	for k := range countMap {
		testMap.Remove(k)
	}

	s.Equal(0, testMap.Len(), "Map returned non-zero size after deleting all entries")
}

func (s *ConcurrentTxMapSuite) TestPutIfNotExist() {
	testMap := NewShardedConcurrentTxMap(1, UUIDHashCode)
	key := uuid.New()
	ok := testMap.PutIfNotExist(key, boolType(true))
	s.True(ok, "PutIfNotExist failed to insert item")
	ok = testMap.PutIfNotExist(key, boolType(true))
	s.False(ok, "PutIfNotExist invariant failed")
}

func (s *ConcurrentTxMapSuite) TestMapConcurrency() {
	nKeys := 1024
	keys := make([]string, nKeys)
	for i := 0; i < nKeys; i++ {
		keys[i] = uuid.New()
	}

	var total int32
	var startWG sync.WaitGroup
	var doneWG sync.WaitGroup
	testMap := NewShardedConcurrentTxMap(1024, UUIDHashCode)

	startWG.Add(1)

	for i := 0; i < 10; i++ {

		doneWG.Add(1)

		go func() {
			startWG.Wait()
			for n := 0; n < nKeys; n++ {
				val := intType(rand.Int())
				if testMap.PutIfNotExist(keys[n], val) {
					atomic.AddInt32(&total, int32(val))
					_, ok := testMap.Get(keys[n])
					s.True(ok, "Concurrency Get test failed")
				}
			}
			doneWG.Done()
		}()
	}

	startWG.Done()
	doneWG.Wait()

	s.Equal(nKeys, testMap.Len(), "Wrong concurrent map size")

	var gotTotal int32
	for i := 0; i < nKeys; i++ {
		v, ok := testMap.Get(keys[i])
		s.True(ok, "Get failed to find previously inserted key")
		intVal := v.(intType)
		gotTotal += int32(intVal)
	}

	s.Equal(total, gotTotal, "Concurrent put test failed, wrong sum of values inserted")
}
