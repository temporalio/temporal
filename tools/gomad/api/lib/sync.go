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

// RWMutex is a reader/writer mutex for the cooperative simulator.
//
// mode encodes lock state: 0 = unlocked, N > 0 = N active readers, -1 = write-locked.
// A Cond on the embedded Mutex serializes all state changes and wakes waiters on
// transitions that may unblock them (readers when mode returns to 0 from write-locked,
// writers when mode reaches 0 from any reader/writer state).
//
// pendingWriters prevents writer starvation: RLock blocks while any writer is waiting,
// ensuring that once a writer starts waiting it will eventually acquire the lock.
type RWMutex struct {
	mu             Mutex
	cond           *Cond
	mode           int // 0=unlocked, N>0=N readers, -1=write-locked
	pendingWriters int // writers waiting for the lock
}

func (m *RWMutex) ensureInit() {
	if m.cond == nil {
		m.cond = NewCond(&m.mu)
	}
}

func (m *RWMutex) RLock() {
	m.ensureInit()
	m.mu.Lock()
	for m.mode < 0 || m.pendingWriters > 0 {
		m.cond.Wait()
	}
	m.mode++
	m.mu.Unlock()
}

func (m *RWMutex) TryRLock() bool {
	m.ensureInit()
	m.mu.Lock()
	if m.mode < 0 {
		m.mu.Unlock()
		return false
	}
	m.mode++
	m.mu.Unlock()
	return true
}

func (m *RWMutex) RUnlock() {
	m.mu.Lock()
	m.mode--
	if m.mode == 0 {
		m.cond.Broadcast()
	}
	m.mu.Unlock()
}

func (m *RWMutex) RLocker() sync.Locker {
	return &readerMutex{m: m}
}

func (m *RWMutex) Lock() {
	m.ensureInit()
	m.mu.Lock()
	m.pendingWriters++
	for m.mode != 0 {
		m.cond.Wait()
	}
	m.pendingWriters--
	m.mode = -1
	m.mu.Unlock()
}

func (m *RWMutex) TryLock() bool {
	m.ensureInit()
	m.mu.Lock()
	if m.mode != 0 {
		m.mu.Unlock()
		return false
	}
	m.mode = -1
	m.mu.Unlock()
	return true
}

func (m *RWMutex) Unlock() {
	m.mu.Lock()
	verify.T(m.mode == -1, "unlock of (never) locked mutex")
	m.mode = 0
	m.cond.Broadcast()
	m.mu.Unlock()
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

// Once is a SIMLIB-aware equivalent of sync.Once. It guarantees f() runs
// exactly once even if f() yields to the cooperative scheduler mid-execution.
//
// Concurrent callers (SIMLIB goroutines or native goroutines interacting with
// the simulation) that arrive while f() is in progress block cooperatively via
// a SIMLIB channel until f() completes, then return without calling f() again.
type Once struct {
	mu      sync.Mutex
	running bool
	done    bool
	waiters chan struct{} // closed by the first caller after f() completes
}

func (o *Once) Do(f func()) {
	o.mu.Lock()
	if o.done {
		o.mu.Unlock()
		return
	}
	if o.running {
		// Another goroutine is running f(); wait cooperatively for it to finish.
		waiters := o.waiters
		o.mu.Unlock()
		SIMLANG.ChanRcv((<-chan struct{})(waiters))
		return
	}
	o.running = true
	o.waiters = make(chan struct{})
	o.mu.Unlock()

	f()

	o.mu.Lock()
	o.done = true
	waiters := o.waiters
	o.mu.Unlock()

	// Wake all goroutines blocked in ChanRcv(waiters).
	SIMLANG.ChanClose(waiters)
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
