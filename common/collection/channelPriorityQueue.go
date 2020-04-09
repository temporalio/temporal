package collection

import "fmt"

const numPriorities = 2

// channelPriorityQueue is a priority queue built using channels
type channelPriorityQueue struct {
	channels   []chan interface{}
	shutdownCh chan struct{}
}

// ChannelPriorityQueue is an interface for a priority queue
type ChannelPriorityQueue interface {
	Add(priority int, item interface{}) bool
	Remove() (interface{}, bool)
	Close()
}

// NewChannelPriorityQueue returns a ChannelPriorityQueue
func NewChannelPriorityQueue(queueSize int) ChannelPriorityQueue {
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
// the queue to get empty if it is full. Returns false if the queue is closed.
func (c *channelPriorityQueue) Add(priority int, item interface{}) bool {
	if priority >= numPriorities {
		panic(fmt.Sprintf("trying to add item with invalid priority %v, queue only supports %v priorities", priority, numPriorities))
	}
	select {
	case c.channels[priority] <- item:
	case <-c.shutdownCh:
		return false
	}
	return true
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
func (c *channelPriorityQueue) Close() {
	close(c.shutdownCh)
}
