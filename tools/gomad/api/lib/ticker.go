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
	"time"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIM "go.temporal.io/server/tools/gomad/runtime"
)

type Ticker struct {
	C       chan time.Time
	started int64
	stopped bool
}

func NewTicker(d time.Duration) *Ticker {
	t := &Ticker{C: make(chan time.Time)}
	t.start(d)
	return t
}

func (t *Ticker) Stop() {
	t.stopped = true
}

func (t *Ticker) Reset(d time.Duration) {
	t.started = NowMs() // invalidates the last ticker
	t.start(d)
}

func (t *Ticker) start(d time.Duration) {
	SIMLANG.InternalGo(func() {
		started := t.started
		for {
			SIM.Sleep(d)
			if t.stopped {
				return // already stopped, don't fire!
			}
			if t.started != started {
				return // ticker was reset!
			}
			SIMLANG.ChanSend(t.C, Now())
		}
	})
}

func Tick(d time.Duration) chan time.Time {
	return NewTicker(d).C
}
