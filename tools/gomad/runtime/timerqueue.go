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
	"math/rand"
	"sort"
)

// timer serves as the source of all events in the system.
// Essentially, it is a priority queue sorted by timestamp.
// During execution, the simulator constantly takes out the nearest timer from the queue,
// triggers the event, and wakes up the task.
// The task also creates new timer events while executing, forming an event loop.
type timerQueue struct {
	clock                clock
	drng                 *rand.Rand
	eventQueues          []*eventQueue
	eventQueuesByReadyAt map[int64]*eventQueue
}

func newTimerQueue(drng *rand.Rand, clock clock) timerQueue {
	return timerQueue{
		clock:                clock,
		drng:                 drng,
		eventQueues:          []*eventQueue{},
		eventQueuesByReadyAt: map[int64]*eventQueue{},
	}
}

func (t *timerQueue) enqueue(evt event) int {
	evtQ, ok := t.eventQueuesByReadyAt[evt.readyAt]
	if !ok {
		evtQ = newEventQueue(t.drng, evt.readyAt)
		t.eventQueuesByReadyAt[evtQ.readyAt] = evtQ
		t.eventQueues = append(t.eventQueues, evtQ)
		sort.Slice(t.eventQueues, func(i, j int) bool {
			return t.eventQueues[i].readyAt < t.eventQueues[j].readyAt
		})
	}
	evtQ.push(evt)
	return t.eventQueues[0].len()
}

func (t *timerQueue) pop() {
	evtQ, rest := t.eventQueues[0], t.eventQueues[1:]
	t.eventQueues = rest
	delete(t.eventQueuesByReadyAt, evtQ.readyAt)
}

func (t *timerQueue) empty() bool {
	return len(t.eventQueues) == 0
}

func (t *timerQueue) currentEventQueue() *eventQueue {
	if t.empty() {
		return nil
	}
	return t.eventQueues[0]
}
