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

//go:build fixture

package fixtures

import (
	"sync"

	SIMAPI "gomad.local/go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "gomad.local/go.temporal.io/server/tools/gomad/api/lib"
)

type sync_struct struct {
	m    SIMLIB.Mutex
	cond map[*SIMLIB.Cond]int
}

func sync_fixture() {
	SIMAPI.FuncStart()
	var wg1, wg2 SIMLIB.WaitGroup
	wg1.Add(1)
	wg2 = SIMLIB.WaitGroup{}
	wg2.Done()
	wg3 := SIMLIB.WaitGroup{}
	wg3.Wait()

	var m1 SIMLIB.Mutex
	defer m1.Unlock()
	m1.Lock()
	m2 := SIMLIB.Mutex{}
	m2.TryLock()

	var rwm SIMLIB.RWMutex
	defer rwm.Unlock()
	defer rwm.RUnlock()
	rwm.Lock()
	rwm.RLock()
	rwm = SIMLIB.RWMutex{}
	rwm.TryLock()
	rwm.TryRLock()
	rwm.RLocker()

	var once SIMLIB.Once
	var onceValue SIMLIB.OnceValue[error]
	var onceValues1 SIMLIB.OnceValues[int, error]
	var onceValues2 SIMLIB.OnceValues[int, error]
	onceValues3 := SIMLIB.OnceValues(fn2)

	var l sync.Locker
	l = &rwm
	var c1 *SIMLIB.Cond
	c1 = SIMLIB.NewCond(l)
	c1.Broadcast()
	c1.Wait()
	c1.Signal()
	_ = c1.L

	_ = sync_struct{m: SIMLIB.Mutex{}}
}

func fn2(float32, error) {
	SIMAPI.FuncStart()
	return 0, nil
}

type _ = sync.Cond
type _ = sync.Mutex

var _ = sync.NewCond

type _ = sync.Once

var _ = sync.OnceValue[any]
var _ = sync.OnceValues[int, error]
var _ = sync.OnceValues[any, any]
var _ = sync.OnceValues[int, error]

type _ = sync.RWMutex
type _ = sync.WaitGroup
