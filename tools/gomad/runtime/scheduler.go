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

package sim_runtime

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"go.temporal.io/server/tools/gomad/util/verify"
)

type (
	scheduler struct {
		Drng            *rand.Rand
		clock           clock
		debug           bool
		active          *goroutine // TODO: get rid of
		synchronizer    Synchronizer
		doneCh          chan struct{}
		tickCh          chan struct{}
		goroutines      map[goroutineId]*goroutine
		channels   map[uintptr]Channel // keyed by uintptr so GC can collect hchans
		channelsMu sync.Mutex         // protects channels from concurrent non-coop goroutine access
		stuckTimeout    time.Duration
		timerQueue      timerQueue

		// keepAliveCh is closed by Join() to signal that no new work is expected;
		// the scheduler exits cleanly once all in-flight goroutines finish.
		keepAliveCh chan struct{}

		// wakeUpCh is signaled by addFromNative() to wake the scheduler when it
		// is idle (no goroutines in the event queue) but keepAliveCh is still open.
		wakeUpCh chan struct{}

		// blockedGoroutines holds the description of goroutines that are all blocked
		// when currentEventQueue() yields to wakeUpCh instead of panicking immediately.
		// This allows non-coop goroutines a chance to add new work before we give up.
		blockedGoroutines []string

		// incomingGoroutines receives goroutines added from non-scheduler goroutines
		// (e.g. testing.tRunner).  Drained at the start of each tick to avoid
		// concurrent map writes.
		incomingGoroutines chan *goroutine
	}
)

func initScheduler(drng *rand.Rand, debug bool, keepAliveCh chan struct{}) *scheduler {
	clock := newClock()
	return &scheduler{
		debug:              debug,
		clock:              clock,
		Drng:               drng,
		doneCh:             make(chan struct{}),
		tickCh:             make(chan struct{}, 1),
		synchronizer:       newSynchronizer(drng),
		goroutines:         map[goroutineId]*goroutine{},
		channels:           make(map[uintptr]Channel),
		stuckTimeout:       stuckTimeout * TimeoutMultiplier,
		timerQueue:         newTimerQueue(drng, clock),
		keepAliveCh:        keepAliveCh,
		wakeUpCh:           make(chan struct{}, 1),
		incomingGoroutines: make(chan *goroutine, 256),
	}
}

// suspend pauses the active cooperative goroutine's execution.
// For goroutines not owned by the cooperative scheduler (the calling-goroutine
// from Start(), testing.tRunner goroutines, …), non-blocking suspends are
// silently skipped; blocking operations are not supported and will panic.
func suspend(b *syncBlock, tags ...Tag) {
	g := tryCurrentGoroutine()
	if g == nil {
		if b.requireSyncMatch || b.delay != 0 {
			panic("blocking simulation operation called from unregistered goroutine")
		}
		return
	}
	g.suspended(b, tags...)
}

// add a new goroutine to the scheduler.
// Must be called from a cooperative goroutine (tryCurrentGoroutine() != nil).
func (s *scheduler) add(g *goroutine) {
	// index goroutine by id; this allows quick lookup later
	s.goroutines[g.id] = g

	// enqueue goroutine to run again later
	s.enqueue(g)

	// suspend the current goroutine (for fair scheduling)
	suspend(&syncBlock{})
}

// addFromNative enqueues g for the scheduler to pick up on its next tick.
// Safe to call from any goroutine (the incomingGoroutines channel is the
// only cross-goroutine write path into the scheduler state).
func (s *scheduler) addFromNative(g *goroutine) {
	s.incomingGoroutines <- g
	// Wake the scheduler if it is currently idle (waiting on wakeUpCh).
	select {
	case s.wakeUpCh <- struct{}{}:
	default:
	}
}

// drainIncoming moves all goroutines queued via addFromNative into the
// scheduler's own goroutines map and event queue.  Must be called only
// from the scheduler loop goroutine.
func (s *scheduler) drainIncoming() {
	for {
		select {
		case g := <-s.incomingGoroutines:
			s.goroutines[g.id] = g
			s.enqueue(g)
		default:
			return
		}
	}
}

// enqueue adds the given goroutine to the scheduler's event queue to be resumed later.
func (s *scheduler) enqueue(g *goroutine) {
	// determine resume delay (e.g. for timers and such)
	var delay int64
	if b := g.syncBlock; b != nil {
		delay = b.delay.Milliseconds()
	}

	// enqueue new event to resume goroutine
	evt := event{id: g.id, readyAt: s.clock.now + delay}
	size := s.timerQueue.enqueue(evt)
	Dbg("⏯️", "enqueue", GoTag(g), AnyTag("qlen", size))
}

// sleep the active goroutine for the given duration.
func (s *scheduler) sleep(d time.Duration) {
	suspend(&syncBlock{delay: d})
}

// time returns the scheduler's current "wall clock" time.
func (s *scheduler) time() time.Time {
	return s.clock.Time()
}

// tick resumes a goroutine until it's done or suspended.
func (s *scheduler) tick() chan struct{} {
	// Incorporate goroutines that arrived from non-scheduler goroutines since
	// the last tick.  This must happen before currentEventQueue() so that
	// newly arrived goroutines are visible to the deadlock detector.
	s.drainIncoming()

	// lookup current event queue
	evtQ := s.currentEventQueue()
	if evtQ == nil {
		// No runnable goroutines. Check whether Join() has been called.
		select {
		case <-s.keepAliveCh:
			// Caller signalled that no more work will arrive — truly done.
			close(s.doneCh)
			return s.tickCh
		default:
		}
		// More work may still arrive via addFromNative; yield to wakeUpCh so
		// the loop blocks cheaply until a new goroutine (or Join) wakes it.
		// If blockedGoroutines is set, loop() will apply a deadlock timeout.
		return s.wakeUpCh
	}

	// choose next goroutine to resume
	nextEvt := evtQ.pop()
	nextGoroutineToRun := nextEvt.id
	g := s.goroutines[nextGoroutineToRun]
	verify.T(g != nil, "goroutine %d is already done", nextGoroutineToRun)

	// resume chosen goroutine
	s.active = g
	g.run() // continue goroutine execution

	// Detect cooperative goroutines that block on native operations (e.g. native
	// channels or select) instead of calling suspend().  Use a real time.AfterFunc
	// so the timer fires even when simulated time is frozen.
	stuckTimer := time.AfterFunc(s.stuckTimeout, func() {
		var diag strings.Builder
		fmt.Fprintf(&diag, "💥simulator stuck waiting for goroutine #%d\n", g.id)
		fmt.Fprintf(&diag, "\n--- simulated goroutines ---\n")
		for _, sg := range s.goroutines {
			fmt.Fprintf(&diag, "  goroutine #%d: %s", sg.id, sg.describeState())
			if sg.sourceLocation != "" {
				fmt.Fprintf(&diag, " (spawned at %s)", sg.sourceLocation)
			}
			if sg.syncBlock != nil {
				fmt.Fprintf(&diag, " blocked on %s", sg.syncBlock.op.string())
				if sg.syncBlock.loc != "" {
					fmt.Fprintf(&diag, " at %s", sg.syncBlock.loc)
				}
			}
			fmt.Fprintf(&diag, "\n")
		}
		fmt.Fprintf(&diag, "\n--- native goroutine stacks ---\n")
		buf := make([]byte, 64<<20)
		n := runtime.Stack(buf, true)
		diag.Write(buf[:n])
		panic(diag.String())
	})
	g.waitUntilStopped() // wait until goroutine is suspended or done
	stuckTimer.Stop()
	s.active = nil

	// process new goroutine state
	switch g.state {
	case suspended:
		switch {

		case g.syncBlock.pt == nil:
			// there is no sync point, just resume this goroutine later
			s.enqueue(g)

		default:
			// identify blocks to resume
			toResume := s.synchronizer.blocksToResume(g.syncBlock)

			if s.debug {
				var tag Tag
				if len(toResume) > 0 {
					ids := make([]string, len(toResume))
					for i, b := range toResume {
						ids[i] = fmt.Sprintf("#%v", b.g.id)
					}
					tag = AnyTag("go", strings.Join(ids, ","))
				} else {
					tag = AnyTag("go", "<>")
				}
				Dbg("🔄️", "sync", tag)
			}

			// enqueue blocks for resume
			for _, block := range toResume {
				// sync first, if the block requires syncing
				if block.onSync != nil {
					block.onSync()
				}
				s.enqueue(block.g)
			}

			// TODO: optimization to speed up syncs without a syncMatch ?
		}
	case done:
		// cleanup; once a goroutine is done, we have no use for it anymore
		delete(s.goroutines, g.id)
	default:
		panic(fmt.Sprintf("goroutine %v has invalid state: %v", g.id, g.describeState()))
	}

	s.tickCh <- struct{}{}
	return s.tickCh
}

func (s *scheduler) currentEventQueue() *eventQueue {
	var evtQ *eventQueue
	for {
		evtQ = s.timerQueue.currentEventQueue()

		// exit if there are no more queues
		if evtQ == nil {
			if len(s.goroutines) > 0 {
				var unresolved []string
				for _, g := range s.goroutines {
					// ignore simulation-internal goroutines
					if !g.internal {
						desc := fmt.Sprintf("#%d", g.id)
						if g.syncBlock != nil {
							desc += fmt.Sprintf(" blocked on %v", g.syncBlock.op.string())
							if g.syncBlock.loc != "" {
								desc += fmt.Sprintf(" at %v", g.syncBlock.loc)
							}
						} else {
							desc += fmt.Sprintf(" (%v)", g.describeState())
						}
						unresolved = append(unresolved, desc)
					}
				}
				sort.Strings(unresolved)
				if len(unresolved) > 0 {
					// Check if Join() has been called (no more native work expected).
					select {
					case <-s.keepAliveCh:
						// Truly done: no new work will arrive.
						verify.T(false,
							"deadlock: all goroutines are blocked\n\t%v", strings.Join(unresolved, "\n\t"))
					default:
						// Non-coop goroutines may still be running and about to add
						// new work via addFromNative. Signal tick() to yield to wakeUpCh
						// with a timeout instead of panicking immediately.
						s.blockedGoroutines = unresolved
					}
				}
			} else {
				s.blockedGoroutines = nil
			}
			return nil
		}

		// move clock forward, if needed
		s.clock.advance(evtQ.readyAt)

		// use next event queue if there are no events here
		if evtQ.len() == 0 {
			s.timerQueue.pop()
			continue
		}

		break
	}
	return evtQ
}
