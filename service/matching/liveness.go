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

package matching

import (
	"sync/atomic"
	"time"
)

type (
	liveness struct {
		ttl    func() time.Duration
		onIdle func()
		timer  atomic.Value
	}
)

func newLiveness(
	ttl func() time.Duration,
	onIdle func(),
) *liveness {
	return &liveness{
		ttl:    ttl,
		onIdle: onIdle,
	}
}

func (l *liveness) Start() {
	l.timer.Store(time.AfterFunc(l.ttl(), l.onIdle))
}

func (l *liveness) Stop() {
	var nilTimer *time.Timer
	if t, ok := l.timer.Swap(nilTimer).(*time.Timer); ok && t != nil {
		t.Stop()
	}
}

func (l *liveness) markAlive() {
	if t, ok := l.timer.Load().(*time.Timer); ok && t != nil {
		t.Reset(l.ttl())
	}
}
