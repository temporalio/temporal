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

package lib

import (
	"sync"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIM "go.temporal.io/server/tools/gomad/runtime"
	"go.temporal.io/server/tools/gomad/util/verify"
)

type Mutex struct {
	locked bool
	lockCh chan struct{}
}

func (m *Mutex) Lock() {
	if m.lockCh == nil {
		// no lock yet; initialize new channel to represent lock
		m.lockCh = make(chan struct{}, 1)
	}

	SIMLANG.ChanSend(m.lockCh, struct{}{})
	SIM.Dbg("🔐", "locked", SIM.ChanTag(SIM.ChanId(m.lockCh)))
	m.locked = true
}

func (m *Mutex) Unlock() {
	verify.T(m.lockCh != nil, "unlock of (never) locked mutex")
	verify.T(m.locked, "unlock of unlocked mutex")

	SIM.Dbg("🔓", "unlocked", SIM.ChanTag(SIM.ChanId(m.lockCh)))
	m.locked = false // needs to come before the rcv!
	SIMLANG.ChanRcv(m.lockCh)
}

func (m *Mutex) TryLock() bool {
	if m.locked {
		return false
	}
	m.Lock()
	return true
}

type RWMutex struct {
	readLock  Mutex
	readers   WaitGroup
	writeLock Mutex
	writers   WaitGroup
}

func (m *RWMutex) RLock() {
	// make sure all writers are done first
	m.writers.Wait()

	// if first reader, grab lock
	if m.readers.Counter() == 0 {
		m.readLock.Lock()
	}

	m.readers.Add(1)
}

func (m *RWMutex) TryRLock() bool {
	if m.writers.Counter() > 0 {
		return false
	}
	m.RLock()
	return true
}

func (m *RWMutex) RUnlock() {
	if m.readers.Counter() == 1 {
		m.readLock.Unlock()
	}
	m.readers.Done()
}

func (m *RWMutex) RLocker() sync.Locker {
	return &readerMutex{m: m}
}

func (m *RWMutex) Lock() {
	// make sure all readers and writers are done first
	m.readers.Wait()
	m.writers.Wait()

	m.writers.Add(1)
	m.writeLock.Lock()
}

func (m *RWMutex) TryLock() bool {
	if m.readers.Counter() > 0 || m.writers.Counter() > 0 {
		return false
	}
	m.Lock()
	return true
}

func (m *RWMutex) Unlock() {
	m.writeLock.Unlock()
	m.writers.Done()
}

type readerMutex struct {
	m *RWMutex
}

func (r *readerMutex) Lock() {
	r.m.RLock()
}

func (r *readerMutex) Unlock() {
	r.m.RUnlock()
}

type Cond struct {
	L  sync.Locker
	ch chan struct{}
}

func NewCond(l sync.Locker) *Cond {
	return &Cond{l, make(chan struct{}, 1)}
}

// Wait atomically unlocks c.L and suspends execution of the calling goroutine.
// After later resuming execution, Wait locks c.L before returning.
func (c *Cond) Wait() {
	ch := c.ch

	c.L.Unlock()
	SIMLANG.ChanRcv(ch) // wait until either Signal or Broadcast is called
	c.L.Lock()
}

// Signal wakes one goroutine waiting on c, if there is any.
func (c *Cond) Signal() {
	// NOTE: default case exists for when no goroutine is actually waiting
	SIMLANG.Select(1, SIMLANG.SndChan(c.ch), func() any { return struct{}{} }, nil)
}

// Broadcast wakes all goroutines waiting on c.
func (c *Cond) Broadcast() {
	SIMLANG.ChanClose(c.ch)
	c.ch = make(chan struct{})
}

type Once struct {
	done bool
}

func (o *Once) Do(f func()) {
	if !o.done {
		f()
		o.done = true
	}
}

func OnceFunc(f func()) func() {
	var once Once
	return func() {
		once.Do(f)
	}
}

func OnceValue[T any](f func() T) func() T {
	var (
		once   Once
		result T
	)
	return func() T {
		once.Do(func() {
			result = f()
		})
		return result
	}
}

func OnceValues[T1, T2 any](f func() (T1, T2)) func() (T1, T2) {
	var (
		once Once
		r1   T1
		r2   T2
	)
	return func() (T1, T2) {
		once.Do(func() {
			r1, r2 = f()
		})
		return r1, r2
	}
}
