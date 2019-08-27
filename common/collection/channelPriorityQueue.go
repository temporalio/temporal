// Copyright (c) 2019 Uber Technologies, Inc.
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

package collection

import "fmt"

const numPriorities = 2

// channelPriorityQueue is a priority queue built using channels
type channelPriorityQueue struct {
	channels   []chan interface{}
	shutdownCh chan struct{}
}

// PriorityQueue is an interface for a priority queue
type PriorityQueue interface {
	Add(priority int, item interface{}) error
	Remove() (interface{}, bool)
	Destroy()
}

// NewChannelPriorityQueue returns a ChannelPriorityQueue
func NewChannelPriorityQueue(queueSize int) PriorityQueue {
	channels := make([]chan interface{}, numPriorities)
	for i := range channels {
		channels[i] = make(chan interface{}, queueSize)
	}
	return &channelPriorityQueue{
		channels:   channels,
		shutdownCh: make(chan struct{}),
	}
}

// Add adds an item to a channel in the queue. This is blocking and waits for
// the queue to get empty if it is full
func (c *channelPriorityQueue) Add(priority int, item interface{}) error {
	if priority >= numPriorities {
		return fmt.Errorf("trying to add item with invalid priority %v, queue only supports %v priorities", priority, numPriorities)
	}
	select {
	case c.channels[priority] <- item:
	case <-c.shutdownCh:
	}
	return nil
}

// Remove removes an item from the priority queue. This is blocking till an
// element becomes available in the priority queue
func (c *channelPriorityQueue) Remove() (interface{}, bool) {
	// pick from highest priority if exists
	select {
	case item, ok := <-c.channels[0]:
		return item, ok
	case <-c.shutdownCh:
		return nil, false
	default:
	}

	// blocking select from all priorities
	var item interface{}
	var ok bool
	select {
	case item, ok = <-c.channels[0]:
	case item, ok = <-c.channels[1]:
	case <-c.shutdownCh:
	}
	return item, ok
}

// Destroy - destroys the channel priority queue
func (c *channelPriorityQueue) Destroy() {
	close(c.shutdownCh)
}
