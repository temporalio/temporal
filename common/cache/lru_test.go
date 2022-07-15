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
	cache := NewLRU(4)

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

	cache.Delete("A")
	assert.Nil(t, cache.Get("A"))
}

func TestGenerics(t *testing.T) {
	key := keyType{
		dummyString: "some random key",
		dummyInt:    59,
	}
	value := "some random value"

	cache := NewLRU(5)
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
	cache := New(5, &Options{
		TTL: time.Millisecond * 100,
	})
	cache.Put("A", "foo")
	assert.Equal(t, "foo", cache.Get("A"))
	time.Sleep(time.Millisecond * 300)
	assert.Nil(t, cache.Get("A"))
	assert.Equal(t, 0, cache.Size())
}

func TestLRUCacheConcurrentAccess(t *testing.T) {
	cache := NewLRU(5)
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
				it := cache.Iterator()
				for it.HasNext() {
					_ = it.Next()
				}
				it.Close()
			}
		}()
	}

	close(start)
	wg.Wait()
}

func TestTTL(t *testing.T) {
	cache := New(5, &Options{
		TTL: time.Millisecond * 50,
	})

	cache.Put("A", t)
	assert.Equal(t, t, cache.Get("A"))
	time.Sleep(time.Millisecond * 100)
	assert.Nil(t, cache.Get("A"))
}

func TestTTLWithPin(t *testing.T) {
	cache := New(5, &Options{
		TTL: time.Millisecond * 50,
		Pin: true,
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
}

func TestMaxSizeWithPin_MidItem(t *testing.T) {
	cache := New(2, &Options{
		TTL: time.Millisecond * 50,
		Pin: true,
	})

	_, err := cache.PutIfNotExist("A", t)
	assert.NoError(t, err)

	_, err = cache.PutIfNotExist("B", t)
	assert.NoError(t, err)

	_, err = cache.PutIfNotExist("C", t)
	assert.Error(t, err)

	assert.Equal(t, t, cache.Get("A"))
	cache.Release("A") // get will also increase the ref count
	assert.Equal(t, t, cache.Get("B"))
	cache.Release("B") // get will also increase the ref count

	cache.Release("B") // B's ref count is 0
	_, err = cache.PutIfNotExist("C", t)
	assert.NoError(t, err)
	assert.Equal(t, t, cache.Get("C"))
	cache.Release("C") // get will also increase the ref count

	cache.Release("A") // A's ref count is 0
	cache.Release("C") // C's ref count is 0

	time.Sleep(time.Millisecond * 100)
	assert.Nil(t, cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Nil(t, cache.Get("C"))
}

func TestMaxSizeWithPin_LastItem(t *testing.T) {
	cache := New(2, &Options{
		TTL: time.Millisecond * 50,
		Pin: true,
	})

	_, err := cache.PutIfNotExist("A", t)
	assert.NoError(t, err)

	_, err = cache.PutIfNotExist("B", t)
	assert.NoError(t, err)

	_, err = cache.PutIfNotExist("C", t)
	assert.Error(t, err)

	assert.Equal(t, t, cache.Get("A"))
	cache.Release("A") // get will also increase the ref count
	assert.Equal(t, t, cache.Get("B"))
	cache.Release("B") // get will also increase the ref count

	cache.Release("A") // A's ref count is 0
	_, err = cache.PutIfNotExist("C", t)
	assert.NoError(t, err)
	assert.Equal(t, t, cache.Get("C"))
	cache.Release("C") // get will also increase the ref count

	cache.Release("B") // B's ref count is 0
	cache.Release("C") // C's ref count is 0

	time.Sleep(time.Millisecond * 100)
	assert.Nil(t, cache.Get("A"))
	assert.Nil(t, cache.Get("B"))
	assert.Nil(t, cache.Get("C"))
}

func TestIterator(t *testing.T) {
	expected := map[string]string{
		"A": "Alpha",
		"B": "Beta",
		"G": "Gamma",
		"D": "Delta",
	}

	cache := NewLRU(5)

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

func TestZeroSizeCache(t *testing.T) {
	cache := NewLRU(0)
	_, err := cache.PutIfNotExist("A", t)
	assert.NoError(t, err)
	assert.Equal(t, nil, cache.Get("A"))
	assert.Equal(t, 0, cache.Size())
	it := cache.Iterator()
	assert.False(t, it.HasNext())
	it.Close()
	cache.Release("A")
	cache.Delete("A")
	v, err := cache.PutIfNotExist("A", t)
	assert.Equal(t, v, t)
	assert.Nil(t, err)
	assert.Equal(t, 0, cache.Size())
}
