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

package collection_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.temporal.io/server/common/collection"
)

// This isn't exhaustive but serves as a basic stress test to ensure our implementation is collection
func TestMap_MultiThreaded(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	wg.Add(5)
	go func() {
		defer wg.Done()
		<-barrier
		for i := 0; i < 1000; i++ {
			m.Set(i, i)
		}
	}()
	go func() {
		<-barrier
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			m.Get(i)
		}
	}()
	go func() {
		<-barrier
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			m.GetOrSet(i, i)
		}
	}()
	go func() {
		<-barrier
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			m.Pop(i)
		}
	}()
	go func() {
		<-barrier
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			m.PopAll()
		}
	}()
	close(barrier)
	wg.Wait()
}

func TestMap_Get(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	m.Set(1, 1)
	v, ok := m.Get(1)
	if !ok {
		t.Error("Expected true, got false")
	}
	if v != 1 {
		t.Errorf("Expected 1, got %v", v)
	}
}

func TestMap_GetOrSet(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	m.Set(1, 1)
	v, ok := m.GetOrSet(1, 2)
	assert.True(t, ok, "expected exist key")
	assert.Equal(t, 1, v, "expected the existing value")

	v, ok = m.GetOrSet(2, 2)
	assert.False(t, ok, "expected non exist key")
	assert.Equal(t, 2, v, "expected the new set value")

	v, ok = m.Get(2)
	assert.True(t, ok, "expected exist key")
	assert.Equal(t, 2, v, "expected the existing value")
}

func TestMap_Delete(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	m.Set(1, 1)
	m.Set(2, 1)
	m.Delete(1)
	_, ok := m.Get(1)
	if ok {
		t.Error("Expected false, got true")
	}
	v, ok := m.Get(2)
	if !ok {
		t.Error("Expected true, got false")
	}
	if v != 1 {
		t.Errorf("Expected 1, got %v", v)
	}
}

func TestMap_Pop_ReturnsFalseWhenKeyDoesNotExist(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	_, ok := m.Pop(1)
	if ok {
		t.Error("Expected false, got true")
	}
}

func TestMap_Pop_ReturnsTrueWhenKeyExists(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	m.Set(1, 1)
	v, ok := m.Pop(1)
	if !ok {
		t.Error("Expected true, got false")
	}
	if v != 1 {
		t.Errorf("Expected 1, got %v", v)
	}
}

func TestMap_PopAll(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	values := m.PopAll()
	assert.Equal(t, 0, len(values))

	m.Set(1, 1)
	m.Set(2, 2)
	m.Set(3, 3)
	m.Set(4, 4)
	m.Pop(4)

	values = m.PopAll()
	assert.Equal(t, 3, len(values))
	sum := 0
	for _, v := range values {
		sum += v
	}
	assert.Equal(t, 6, sum)
}
