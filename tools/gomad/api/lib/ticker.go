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
	stopped bool         // used only in real (non-sim) mode
	stopCh  chan struct{} // sim mode: cooperative stop signal; closed by Stop()/Reset()
	doneCh  chan struct{} // sim mode: closed by ticker goroutine on stop; Stop()/Reset() waits on it
	real    *time.Ticker // used in real (non-sim) mode
}

func NewTicker(d time.Duration) *Ticker {
	if SIM.TryAnySimulator() == nil {
		t := &Ticker{C: make(chan time.Time, 1)}
		t.real = time.NewTicker(d)
		go func() {
			for tm := range t.real.C {
				if t.stopped {
					return
				}
				select {
				case t.C <- tm:
				default:
				}
			}
		}()
		return t
	}
	// Sim mode: unbuffered C so a tick can only be delivered when a receiver is present.
	t := &Ticker{
		C:      make(chan time.Time),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	t.start(d)
	return t
}

func (t *Ticker) Stop() {
	if t.real != nil {
		t.stopped = true
		t.real.Stop()
		return
	}
	// Close stopCh cooperatively, then wait for the ticker goroutine to
	// acknowledge by closing doneCh. This ensures ticker.Stop() returns only
	// after the last cooperative interaction completes at the current simulated
	// time, so SIMLIB.Now() called by the caller reflects the stop time.
	SIMLANG.ChanClose(t.stopCh)
	SIMLANG.ChanRcv(t.doneCh)
}

func (t *Ticker) Reset(d time.Duration) {
	if t.real != nil {
		t.real.Reset(d)
		return
	}
	// Capture the old channels and replace them so t.start(d) below uses fresh
	// ones. This must happen before the goroutine runs.
	oldStopCh := t.stopCh
	oldDoneCh := t.doneCh
	t.stopCh = make(chan struct{})
	t.doneCh = make(chan struct{})
	// Capture the intended absolute first-tick time now, before the combined
	// goroutine runs. By the time the combined goroutine calls startAt(), the
	// simulated clock may have advanced (e.g. a concurrent timer fires while the
	// old ticker goroutine is being stopped), so we snapshot the time here from
	// the caller's perspective and pass it in explicitly.
	targetFirstMs := NowMs() + d.Milliseconds()
	if SIM.IsCooperativeGoroutine() {
		// Called from a cooperative goroutine: perform the stop+restart sequence
		// directly. Using InternalGo + native channel would deadlock: the
		// scheduler can't advance to let the InternalGo goroutine send to
		// nativeDone while the current cooperative goroutine is blocking on it.
		SIMLANG.ChanClose(oldStopCh)
		SIMLANG.ChanRcv(oldDoneCh)
		t.startAt(targetFirstMs, d)
		return
	}
	// Called from a native goroutine: delegate into the scheduler via InternalGo
	// and block on a native channel until it completes.
	nativeDone := make(chan struct{}, 1)
	SIMLANG.InternalGo(func() {
		SIMLANG.ChanClose(oldStopCh)
		SIMLANG.ChanRcv(oldDoneCh)
		t.startAt(targetFirstMs, d)
		nativeDone <- struct{}{}
	})
	<-nativeDone
}

func (t *Ticker) start(d time.Duration) {
	t.startAt(NowMs()+d.Milliseconds(), d)
}

func (t *Ticker) startAt(targetFirstMs int64, d time.Duration) {
	doneCh := t.doneCh
	stopCh := t.stopCh
	SIMLANG.InternalGo(func() {
		// Compute the remaining duration until the intended first tick, clamped
		// to zero if we're already past it (e.g. scheduler was delayed).
		firstD := time.Duration(targetFirstMs-NowMs()) * time.Millisecond
		if firstD < 0 {
			firstD = 0
		}
		for iterD := firstD; ; iterD = d {
			timer := NewTimer(iterD)
			if SIMLANG.Select(
				0, SIMLANG.RcvChan(stopCh), nil,
				0, SIMLANG.RcvChan(timer.C), nil,
			).Case == 0 {
				SIMLANG.ChanClose(doneCh)
				return
			}

			// Non-suspending check: read the cooperative scheduler's closed flag
			// directly. Because only one cooperative goroutine runs at a time,
			// this read is safe and does not yield to the scheduler. If we instead
			// used a Select with a default case, the goroutine would briefly suspend
			// even for the default branch, allowing the stop DG and a new tick
			// receiver to both become runnable before we reach the delivery select
			// below — which would then non-deterministically deliver a spurious tick.
			if SIMLANG.ChanIsClosed(stopCh) {
				SIMLANG.ChanClose(doneCh)
				return
			}

			// Deliver tick: either receive the stop signal (case 0) or deliver a
			// tick to a waiting receiver (case 1). Because ChanIsClosed above did
			// not suspend, stopCh is guaranteed open at this point, so no receiver
			// can yet exist in the cooperative scheduler (the caller is still
			// blocked on the ChanClose delegation). We will block here until the
			// stop DG closes stopCh (case 0) or a receiver arrives on t.C (case 1).
			//
			// Capture Now() here in the goroutine's registered context; the
			// writeFn closure is called from the scheduler goroutine (not
			// registered), so calling Now() inside the closure would fall back
			// to real wall-clock time.
			now := Now()
			if SIMLANG.Select(
				0, SIMLANG.RcvChan(stopCh), nil,
				1, SIMLANG.SndChan(t.C), func() any { return now },
			).Case == 0 {
				SIMLANG.ChanClose(doneCh)
				return
			}
		}
	})
}

func Tick(d time.Duration) chan time.Time {
	return NewTicker(d).C
}
