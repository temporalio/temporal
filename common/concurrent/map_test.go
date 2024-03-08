package concurrent_test

import (
	"sync"
	"testing"

	"go.temporal.io/server/common/concurrent"
)

// This isn't exhaustive but serves as a basic stress test to ensure our implementation is concurrent
func TestMap_MultiThreaded(t *testing.T) {
	m := concurrent.NewMap[int, int]()
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	wg.Add(3)
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
			m.Pop(i)
		}
	}()
	close(barrier)
	wg.Wait()
}

func TestMap_Get(t *testing.T) {
	m := concurrent.NewMap[int, int]()
	m.Set(1, 1)
	v, ok := m.Get(1)
	if !ok {
		t.Error("Expected true, got false")
	}
	if v != 1 {
		t.Errorf("Expected 1, got %v", v)
	}
}

func TestMap_Delete(t *testing.T) {
	m := concurrent.NewMap[int, int]()
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
	m := concurrent.NewMap[int, int]()
	_, ok := m.Pop(1)
	if ok {
		t.Error("Expected false, got true")
	}
}

func TestMap_Pop_ReturnsTrueWhenKeyExists(t *testing.T) {
	m := concurrent.NewMap[int, int]()
	m.Set(1, 1)
	v, ok := m.Pop(1)
	if !ok {
		t.Error("Expected true, got false")
	}
	if v != 1 {
		t.Errorf("Expected 1, got %v", v)
	}
}
