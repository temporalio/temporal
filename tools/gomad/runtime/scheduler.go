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
	"strings"
	"time"
	"unsafe"

	"go.temporal.io/server/tools/gomad/util/verify"
)

type (
	scheduler struct {
		Drng         *rand.Rand
		clock        clock
		debug        bool
		active       *goroutine // TODO: get rid of
		synchronizer Synchronizer
		doneCh       chan struct{}
		tickCh       chan struct{}
		goroutines   map[goroutineId]*goroutine
		channels     map[unsafe.Pointer]Channel
		stuckTimeout time.Duration
		timerQueue   timerQueue
	}
)

func initScheduler(drng *rand.Rand, debug bool) *scheduler {
	clock := newClock()
	return &scheduler{
		debug:        debug,
		clock:        clock,
		Drng:         drng,
		doneCh:       make(chan struct{}),
		tickCh:       make(chan struct{}, 1),
		synchronizer: newSynchronizer(drng),
		goroutines:   map[goroutineId]*goroutine{},
		channels:     make(map[unsafe.Pointer]Channel),
		stuckTimeout: stuckTimeout * TimeoutMultiplier,
		timerQueue:   newTimerQueue(drng, clock),
	}
}

// suspend pauses the active goroutine's execution.
func suspend(b *syncBlock, tags ...Tag) {
	CurrentSimulator().scheduler.active.suspended(b, tags...)
}

// add a new goroutine to the scheduler.
func (s *scheduler) add(g *goroutine) {
	// index goroutine by id; this allows quick lookup later
	s.goroutines[g.id] = g

	// enqueue goroutine to run again later
	s.enqueue(g)

	// suspend the current goroutine (for fair scheduling)
	suspend(&syncBlock{})
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
	// lookup current event queue
	evtQ := s.currentEventQueue()
	if evtQ == nil {
		// exit if there are no more queues
		close(s.doneCh)
		return s.tickCh
	}

	// choose next goroutine to resume
	nextEvt := evtQ.pop()
	nextGoroutineToRun := nextEvt.id
	g := s.goroutines[nextGoroutineToRun]
	verify.T(g != nil, "goroutine %d is already done", nextGoroutineToRun)

	// resume chosen goroutine
	s.active = g
	g.run()              // continue goroutine execution
	g.waitUntilStopped() // wait until goroutine is suspended or done
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
					// ignore simulation-internal goroutine
					if !g.internal {
						unresolved = append(unresolved, fmt.Sprintf("#%d (%v)", g.id, g.describeState()))
					}
				}
				verify.T(len(unresolved) == 0,
					"no more events but goroutines are not done: %v", strings.Join(unresolved, ", "))
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
