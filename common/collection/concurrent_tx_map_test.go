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
		suite.Suite
	}
	boolType bool
	intType  int
)

func TestConcurrentTxMapSuite(t *testing.T) {
	suite.Run(t, new(ConcurrentTxMapSuite))
}

func (s *ConcurrentTxMapSuite) TestLen() {
	testMap := NewShardedConcurrentTxMap(1, UUIDHashCode)

	key1 := "0001"
	testMap.Put(key1, boolType(true))
	require.Equal(s.T(), 1, testMap.Len(), "Wrong concurrent map size")

	testMap.Put(key1, boolType(false))
	require.Equal(s.T(), 1, testMap.Len(), "Wrong concurrent map size")

	key2 := "0002"
	testMap.Put(key2, boolType(false))
	require.Equal(s.T(), 2, testMap.Len(), "Wrong concurrent map size")

	testMap.PutIfNotExist(key2, boolType(false))
	require.Equal(s.T(), 2, testMap.Len(), "Wrong concurrent map size")

	testMap.Remove(key2)
	require.Equal(s.T(), 1, testMap.Len(), "Wrong concurrent map size")

	testMap.Remove(key2)
	require.Equal(s.T(), 1, testMap.Len(), "Wrong concurrent map size")
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
	require.Nil(s.T(), interf, "GetAndDo should return nil when key not found")
	require.Nil(s.T(), err, "GetAndDo should return nil when function not applied")
	require.False(s.T(), ok, "GetAndDo should return false when key not found")
	require.False(s.T(), fnApplied, "GetAndDo should not apply function when key not exixts")

	value = intType(1)
	testMap.Put(key, &value)
	interf, ok, err = testMap.GetAndDo(key, func(key interface{}, value interface{}) error {
		fnApplied = true
		intValue := value.(*intType)
		*intValue++
		return errors.New("some err")
	})

	value1 := interf.(*intType)
	require.Equal(s.T(), *(value1), intType(2))
	require.NotNil(s.T(), err, "GetAndDo should return non nil when function applied")
	require.True(s.T(), ok, "GetAndDo should return true when key found")
	require.True(s.T(), fnApplied, "GetAndDo should apply function when key exixts")
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
	require.Equal(s.T(), value, *valueRetuern)
	require.Nil(s.T(), err, "PutOrDo should return nil when function not applied")
	require.False(s.T(), ok, "PutOrDo should return false when function not applied")
	require.False(s.T(), fnApplied, "PutOrDo should not apply function when key not exixts")

	anotherValue := intType(111)
	interf, ok, err = testMap.PutOrDo(key, &anotherValue, func(key interface{}, value interface{}) error {
		fnApplied = true
		intValue := value.(*intType)
		*intValue++
		return errors.New("some err")
	})
	valueRetuern = interf.(*intType)
	require.Equal(s.T(), value, *valueRetuern)
	require.NotNil(s.T(), err, "PutOrDo should return non nil when function applied")
	require.True(s.T(), ok, "PutOrDo should return true when function applied")
	require.True(s.T(), fnApplied, "PutOrDo should apply function when key exixts")
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
	require.Equal(s.T(), 1, testMap.Len(), "TestRemoveIf should only entry if condition is met")
	require.False(s.T(), removed, "TestRemoveIf should return false if key is not deleted")

	removed = testMap.RemoveIf(key, func(key interface{}, value interface{}) bool {
		intValue := value.(*intType)
		return *intValue == intType(1)
	})
	require.Equal(s.T(), 0, testMap.Len(), "TestRemoveIf should only entry if condition is met")
	require.True(s.T(), removed, "TestRemoveIf should return true if key is deleted")
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
		require.True(s.T(), ok, "Get after put failed")
		require.True(s.T(), bool(boolValue), "Wrong value returned from map")
	}

	require.Equal(s.T(), len(countMap), testMap.Len(), "Size() returned wrong value")

	it := testMap.Iter()
	for entry := range it.Entries() {
		countMap[entry.Key.(string)]++
	}
	it.Close()

	for _, v := range countMap {
		require.Equal(s.T(), 1, v, "Iterator test failed")
	}

	for k := range countMap {
		testMap.Remove(k)
	}

	require.Equal(s.T(), 0, testMap.Len(), "Map returned non-zero size after deleting all entries")
}

func (s *ConcurrentTxMapSuite) TestPutIfNotExist() {
	testMap := NewShardedConcurrentTxMap(1, UUIDHashCode)
	key := uuid.New()
	ok := testMap.PutIfNotExist(key, boolType(true))
	require.True(s.T(), ok, "PutIfNotExist failed to insert item")
	ok = testMap.PutIfNotExist(key, boolType(true))
	require.False(s.T(), ok, "PutIfNotExist invariant failed")
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
					require.True(s.T(), ok, "Concurrency Get test failed")
				}
			}
			doneWG.Done()
		}()
	}

	startWG.Done()
	doneWG.Wait()

	require.Equal(s.T(), nKeys, testMap.Len(), "Wrong concurrent map size")

	var gotTotal int32
	for i := 0; i < nKeys; i++ {
		v, ok := testMap.Get(keys[i])
		require.True(s.T(), ok, "Get failed to find previously inserted key")
		intVal := v.(intType)
		gotTotal += int32(intVal)
	}

	require.Equal(s.T(), total, gotTotal, "Concurrent put test failed, wrong sum of values inserted")
}
