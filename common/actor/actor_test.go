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

package actor

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActorCast(t *testing.T) {
	a := New("test")
	var calledWith int64
	done := make(chan struct{})
	a.RegisterCast(0, func(req interface{}) {
		atomic.StoreInt64(&calledWith, int64(req.(int)))
		close(done)
	})
	a.Start()
	defer a.Stop()

	assert.EqualValues(t, 0, atomic.LoadInt64(&calledWith))
	a.Cast(0, 42)
	<-done
	assert.EqualValues(t, 42, atomic.LoadInt64(&calledWith))
}

// Note: Disabling the test as it is time sensitive and we are not using actor code.
func testActorScheduleCast(t *testing.T) {
	a := New("test")
	var calledWith int64
	done := make(chan struct{})
	a.RegisterCast(0, func(req interface{}) {
		atomic.StoreInt64(&calledWith, int64(req.(int)))
		close(done)
	})
	a.Start()
	defer a.Stop()

	assert.EqualValues(t, 0, atomic.LoadInt64(&calledWith))
	before := time.Now()
	a.ScheduleCast(0, 42, 10*time.Millisecond)
	<-done
	assert.True(t, time.Now().Sub(before) >= 9*time.Millisecond) // 9 as timer has 1 ms resolution
	assert.EqualValues(t, 42, atomic.LoadInt64(&calledWith))
}

func TestActorCall(t *testing.T) {
	a := New("test")
	a.RegisterCall(0, func(req interface{}) (interface{}, error) {
		return req.(int) + 1, nil
	})
	a.Start()
	defer a.Stop()

	res := a.Call(0, 41)
	assert.Equal(t, 42, res)

	res, err := a.CallErr(0, 41)
	assert.Equal(t, 42, res)
	assert.NoError(t, err)
}

func TestActorCallError(t *testing.T) {
	a := New("test")
	a.RegisterCall(0, func(req interface{}) (interface{}, error) {
		return nil, errors.New("test error")
	})
	a.Start()
	defer a.Stop()

	res, err := a.CallErr(0, nil)
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Equal(t, "test error", err.Error())

	assert.Panics(t, func() { a.Call(0, nil) })
}

func TestActorAsyncCall(t *testing.T) {
	a := New("test")
	reply := make(chan struct{})
	replied := make(chan interface{})
	a.RegisterAsyncCall(0, func(req interface{}, resp *Response) {
		<-reply
		resp.Reply(req.(int)+1, nil)
	})
	a.Start()
	defer a.Stop()

	go func() {
		replied <- a.Call(0, 41)
	}()

	select {
	case <-replied:
		require.Fail(t, "got reply too soon")
	case <-time.After(100 * time.Millisecond):
	}

	close(reply)

	select {
	case res := <-replied:
		assert.Equal(t, 42, res)
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "didn't get reply in time")
	}
}

func TestMultipleCallsAndCasts(t *testing.T) {
	// calls and casts do not share ID space
	const (
		callStr Action = iota
		callInt
		callBytes
	)
	const (
		castBytes Action = iota
		castStr
		castInt
	)

	invocations := map[interface{}]bool{}

	a := New("test")
	a.RegisterCast(castStr, func(req interface{}) { invocations[req.(string)] = true })
	a.RegisterCast(castInt, func(req interface{}) { invocations[req.(int)] = true })
	a.RegisterCast(castBytes, func(req interface{}) { invocations[req.([3]byte)] = true })
	a.RegisterCall(callStr, func(req interface{}) (interface{}, error) {
		val := req.(string)
		invocations[req] = true
		return val, nil
	})
	a.RegisterCall(callInt, func(req interface{}) (interface{}, error) {
		val := req.(int)
		invocations[req] = true
		return val, nil
	})
	a.RegisterCall(callBytes, func(req interface{}) (interface{}, error) {
		val := req.([2]byte)
		invocations[req] = true
		return val, nil
	})

	a.Start()

	a.Cast(castInt, 777)
	a.Cast(castStr, "str-cast")
	a.Cast(castBytes, [3]byte{7, 7, 7})
	assert.Equal(t, "str-call", a.Call(callStr, "str-call"))
	assert.Equal(t, 42, a.Call(callInt, 42))
	assert.Equal(t, [2]byte{4, 2}, a.Call(callBytes, [2]byte{4, 2}))

	<-a.Stop()

	assert.Equal(t, map[interface{}]bool{
		"str-call":       true,
		"str-cast":       true,
		42:               true,
		777:              true,
		[3]byte{7, 7, 7}: true,
		[2]byte{4, 2}:    true,
	}, invocations)
}

func TestNoRegistrationAfterStart(t *testing.T) {
	a := New("test")
	a.RegisterCast(0, func(interface{}) {})
	a.Start()

	assert.Panics(t, func() {
		a.RegisterCall(0, func(interface{}) (interface{}, error) { return nil, nil })
	})
	assert.Panics(t, func() {
		a.RegisterCast(0, func(interface{}) {})
	})
}

func TestStopping(t *testing.T) {
	stopCalled := false

	a := New("test")
	a.RegisterCast(0, func(interface{}) {})
	a.RegisterStop(func() { stopCalled = true })
	a.Start()

	assert.False(t, stopCalled)
	select {
	case <-a.Done():
		assert.FailNow(t, "done channel was closed")
	default:
	}

	select {
	case <-a.Stop():
	case <-time.After(100 * time.Millisecond):
		assert.FailNow(t, "server stopping timed out")
	}

	assert.True(t, stopCalled)

	select {
	case <-a.Done():
	default:
		assert.FailNow(t, "done channel was still not closed")
	}

	assert.NotPanics(t, func() { a.Cast(0, nil) }, "cast failed after stop")
}

func TestActionAfterStopped(t *testing.T) {
	a := New("test")
	var calledWith int64
	done := make(chan struct{})
	a.RegisterCast(0, func(req interface{}) {
		atomic.StoreInt64(&calledWith, int64(req.(int)))
		close(done)
	})
	a.Start()

	assert.EqualValues(t, 0, atomic.LoadInt64(&calledWith))
	a.Cast(0, 42)
	<-done
	assert.EqualValues(t, 42, atomic.LoadInt64(&calledWith))

	<-a.Stop()

	done = make(chan struct{})
	a.Cast(0, 777)
	select {
	case <-done:
		assert.Fail(t, "cast was called")
	case <-time.After(50 * time.Millisecond):
	}
	// assert calledWith was not modified
	assert.EqualValues(t, 42, atomic.LoadInt64(&calledWith))
}

func TestSequentialExecution(t *testing.T) {
	workers := 10
	reqsPerWorker := 100000
	cnt := 0

	a := New("test")
	a.RegisterCall(0, func(interface{}) (interface{}, error) {
		cnt++
		return nil, nil
	})
	a.Start()

	start := make(chan struct{})

	var readyWG, doneWG sync.WaitGroup
	readyWG.Add(workers)
	doneWG.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			readyWG.Done()
			<-start
			for j := 0; j < reqsPerWorker; j++ {
				a.Call(0, nil)
			}
			doneWG.Done()
		}()
	}

	readyWG.Wait()
	close(start)
	doneWG.Wait()

	// if execution happens in multiple goroutines, chances are high that we'll miss some counts
	// due to races
	assert.Equal(t, cnt, workers*reqsPerWorker)
}
