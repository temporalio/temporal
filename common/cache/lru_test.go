// Copyright (c) 2017 Uber Technologies, Inc.
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

package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type keyType struct {
	dummyString string
	dummyInt    int
}

func TestLRU(t *testing.T) {
	cache := New(&Options{MaxCount: 5})

	cache.Put("A", "Foo")
	assert.Equal(t, "Foo", cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Equal(t, 1, cache.Size())

	cache.Put("B", "Bar")
	cache.Put("C", "Cid")
	cache.Put("D", "Delt")
	assert.Equal(t, 4, cache.Size())

	assert.Equal(t, "Bar", cache.Get("B"))
	assert.Equal(t, "Cid", cache.Get("C"))
	assert.Equal(t, "Delt", cache.Get("D"))

	cache.Put("A", "Foo2")
	assert.Equal(t, "Foo2", cache.Get("A"))

	cache.Put("E", "Epsi")
	assert.Equal(t, "Epsi", cache.Get("E"))
	assert.Equal(t, "Foo2", cache.Get("A"))
	assert.Nil(t, cache.Get("B")) // Oldest, should be evicted

	// Access C, D is now LRU
	cache.Get("C")
	cache.Put("F", "Felp")
	assert.Nil(t, cache.Get("D"))
	assert.Equal(t, 4, cache.Size())

	cache.Delete("A")
	assert.Nil(t, cache.Get("A"))
}

func TestGenerics(t *testing.T) {
	key := keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}
	value := "some random value"

	cache := New(&Options{MaxCount: 5})
	cache.Put(key, value)

	assert.Equal(t, value, cache.Get(key))
	assert.Equal(t, value, cache.Get(keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}))
	assert.Nil(t, cache.Get(keyType{
		dummyString: "some other random key",
		dummyInt:    56,
	}))
}

func TestLRUWithTTL(t *testing.T) {
	cache := New(&Options{
		MaxCount: 5,
		TTL:      time.Millisecond * 100,
	})
	cache.Put("A", "foo")
	assert.Equal(t, "foo", cache.Get("A"))
	time.Sleep(time.Millisecond * 300)
	assert.Nil(t, cache.Get("A"))
	assert.Equal(t, 0, cache.Size())
}

func TestLRUCacheConcurrentAccess(t *testing.T) {
	cache := New(&Options{MaxCount: 5})
	values := map[string]string{
		"A": "foo",
		"B": "bar",
		"C": "zed",
		"D": "dank",
		"E": "ezpz",
	}

	for k, v := range values {
		cache.Put(k, v)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)

		// concurrent get and put
		go func() {
			defer wg.Done()

			<-start

			for j := 0; j < 1000; j++ {
				cache.Get("A")
				cache.Put("A", "fooo")
			}
		}()

		// concurrent iteration
		go func() {
			defer wg.Done()

			<-start

			for j := 0; j < 50; j++ {
				var result []Entry
				it := cache.Iterator()
				for it.HasNext() {
					entry := it.Next()
					result = append(result, entry) //nolint:staticcheck
				}
				it.Close()
			}
		}()
	}

	close(start)
	wg.Wait()
}

func TestRemoveFunc(t *testing.T) {
	ch := make(chan bool)
	cache := New(&Options{
		MaxCount: 5,
		RemovedFunc: func(i interface{}) {
			_, ok := i.(*testing.T)
			assert.True(t, ok)
			ch <- true
		},
	})

	cache.Put("testing", t)
	cache.Delete("testing")
	assert.Nil(t, cache.Get("testing"))

	timeout := time.NewTimer(time.Millisecond * 300)
	select {
	case b := <-ch:
		assert.True(t, b)
	case <-timeout.C:
		t.Error("RemovedFunc did not send true on channel ch")
	}
}

func TestRemovedFuncWithTTL(t *testing.T) {
	ch := make(chan bool)
	cache := New(&Options{
		MaxCount: 5,
		TTL:      time.Millisecond * 50,
		RemovedFunc: func(i interface{}) {
			_, ok := i.(*testing.T)
			assert.True(t, ok)
			ch <- true
		},
	})

	cache.Put("A", t)
	assert.Equal(t, t, cache.Get("A"))
	time.Sleep(time.Millisecond * 100)
	assert.Nil(t, cache.Get("A"))

	timeout := time.NewTimer(time.Millisecond * 300)
	select {
	case b := <-ch:
		assert.True(t, b)
	case <-timeout.C:
		t.Error("RemovedFunc did not send true on channel ch")
	}
}

func TestRemovedFuncWithTTL_Pin(t *testing.T) {
	ch := make(chan bool)
	cache := New(&Options{
		MaxCount: 5,
		TTL:      time.Millisecond * 50,
		Pin:      true,
		RemovedFunc: func(i interface{}) {
			_, ok := i.(*testing.T)
			assert.True(t, ok)
			ch <- true
		},
	})

	_, err := cache.PutIfNotExist("A", t)
	assert.NoError(t, err)
	assert.Equal(t, t, cache.Get("A"))
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, t, cache.Get("A"))
	// release 3 time since put if not exist also increase the counter
	cache.Release("A")
	cache.Release("A")
	cache.Release("A")
	assert.Nil(t, cache.Get("A"))

	timeout := time.NewTimer(time.Millisecond * 300)
	select {
	case b := <-ch:
		assert.True(t, b)
	case <-timeout.C:
		t.Error("RemovedFunc did not send true on channel ch")
	}
}

func TestIterator(t *testing.T) {
	expected := map[string]string{
		"A": "Alpha",
		"B": "Beta",
		"G": "Gamma",
		"D": "Delta",
	}

	cache := New(&Options{MaxCount: 5})

	for k, v := range expected {
		cache.Put(k, v)
	}

	actual := map[string]string{}

	it := cache.Iterator()
	for it.HasNext() {
		entry := it.Next()
		actual[entry.Key().(string)] = entry.Value().(string)
	}
	it.Close()
	assert.Equal(t, expected, actual)

	it = cache.Iterator()
	for i := 0; i < len(expected); i++ {
		entry := it.Next()
		actual[entry.Key().(string)] = entry.Value().(string)
	}
	it.Close()
	assert.Equal(t, expected, actual)
}

func TestLRU_SizeBased_SizeExceeded(t *testing.T) {
	valueSize := 5
	cache := New(&Options{
		MaxCount: 5,
		GetCacheItemSizeFunc: func(interface{}) uint64 {
			return uint64(valueSize)
		},
		MaxSize: 15,
	})

	cache.Put("A", "Foo")
	assert.Equal(t, "Foo", cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Equal(t, 1, cache.Size())

	cache.Put("B", "Bar")
	cache.Put("C", "Cid")
	cache.Put("D", "Delt")
	assert.Nil(t, cache.Get("A"))
	assert.Equal(t, 3, cache.Size())

	assert.Equal(t, "Bar", cache.Get("B"))
	assert.Equal(t, "Cid", cache.Get("C"))
	assert.Equal(t, "Delt", cache.Get("D"))

	cache.Put("A", "Foo2")
	assert.Equal(t, "Foo2", cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Equal(t, 3, cache.Size())

	valueSize = 15 // put large value to evict the rest in a loop
	cache.Put("E", "Epsi")
	assert.Nil(t, cache.Get("C"))
	assert.Equal(t, "Epsi", cache.Get("E"))
	assert.Nil(t, cache.Get("A"))
	assert.Equal(t, 1, cache.Size())

	valueSize = 25 // put large value greater than maxSize to evict everything
	cache.Put("M", "Mepsi")
	assert.Nil(t, cache.Get("M"))
	assert.Equal(t, 0, cache.Size())
}

func TestLRU_SizeBased_CountExceeded(t *testing.T) {
	cache := New(&Options{
		MaxCount: 5,
		GetCacheItemSizeFunc: func(interface{}) uint64 {
			return 5
		},
		MaxSize: 0,
	})

	cache.Put("A", "Foo")
	assert.Equal(t, "Foo", cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Equal(t, 1, cache.Size())

	cache.Put("B", "Bar")
	cache.Put("C", "Cid")
	cache.Put("D", "Delt")
	assert.Equal(t, 4, cache.Size())

	assert.Equal(t, "Bar", cache.Get("B"))
	assert.Equal(t, "Cid", cache.Get("C"))
	assert.Equal(t, "Delt", cache.Get("D"))

	cache.Put("A", "Foo2")
	assert.Equal(t, "Foo2", cache.Get("A"))
	assert.Equal(t, 4, cache.Size())

	cache.Put("E", "Epsi")
	assert.Nil(t, cache.Get("B"))
	assert.Equal(t, "Epsi", cache.Get("E"))
	assert.Equal(t, "Foo2", cache.Get("A"))
	assert.Equal(t, 4, cache.Size())
}

func TestPanicMaxCountAndSizeNotProvided(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The LRU was initialized without panic")
		}
	}()

	New(&Options{
		TTL: time.Millisecond * 100,
		GetCacheItemSizeFunc: func(interface{}) uint64 {
			return 5
		},
	})
}

func TestPanicMaxCountAndSizeFuncNotProvided(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The LRU was initialized without panic")
		}
	}()

	New(&Options{
		TTL:     time.Millisecond * 100,
		MaxSize: 25,
	})
}

func TestPanicOptionsIsNil(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The LRU was initialized without panic")
		}
	}()

	New(nil)
}
