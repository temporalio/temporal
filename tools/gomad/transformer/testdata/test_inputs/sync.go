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

import "sync"

type sync_struct struct {
	m    sync.Mutex
	cond map[*sync.Cond]int
}

func sync_fixture() {
	var wg1, wg2 sync.WaitGroup
	wg1.Add(1)
	wg2 = sync.WaitGroup{}
	wg2.Done()
	wg3 := sync.WaitGroup{}
	wg3.Wait()

	var m1 sync.Mutex
	defer m1.Unlock()
	m1.Lock()
	m2 := sync.Mutex{}
	m2.TryLock()

	var rwm sync.RWMutex
	defer rwm.Unlock()
	defer rwm.RUnlock()
	rwm.Lock()
	rwm.RLock()
	rwm = sync.RWMutex{}
	rwm.TryLock()
	rwm.TryRLock()
	rwm.RLocker()

	var once sync.Once
	var onceValue sync.OnceValue[error]
	var onceValues1 sync.OnceValues[int, error]
	var onceValues2 sync.OnceValues[int, error]
	onceValues3 := sync.OnceValues(fn2)

	var l sync.Locker
	l = &rwm
	var c1 *sync.Cond
	c1 = sync.NewCond(l)
	c1.Broadcast()
	c1.Wait()
	c1.Signal()
	_ = c1.L

	_ = sync_struct{m: sync.Mutex{}}
}

func fn2(float32, error) {
	return 0, nil
}
