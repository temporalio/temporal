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
	"sort"
)

type (
	event struct {
		id       goroutineId
		readyAt  int64 // readyAt is the unix time when the event is ready to run
		priority int   // priority is randomly generated
	}
	eventQueue struct {
		drng    *rand.Rand
		events  []event             // events are sorted by priority
		dedup   map[goroutineId]any // TODO: remove
		readyAt int64               // readyAt is the unix time when the events are ready to run
	}
)

func newEventQueue(drng *rand.Rand, readyAt int64) *eventQueue {
	return &eventQueue{
		drng:    drng,
		events:  []event{},
		dedup:   map[goroutineId]any{},
		readyAt: readyAt,
	}
}

func (q *eventQueue) push(evt event) {
	if _, ok := q.dedup[evt.id]; ok {
		panic(fmt.Sprintf("duplicate event enqueued for %v", evt.id))
	}
	evt.priority = q.drng.Int()
	q.events = append(q.events, evt)
	q.dedup[evt.id] = true
	sort.Slice(q.events, func(i, j int) bool {
		return q.events[i].priority < q.events[j].priority
	})
}

func (q *eventQueue) pop() event {
	evt, rest := q.events[0], q.events[1:]
	delete(q.dedup, evt.id)
	q.events = rest
	return evt
}

func (q *eventQueue) len() int {
	return len(q.events)
}
