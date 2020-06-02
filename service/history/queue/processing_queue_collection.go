// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package queue

import (
	"sort"

	"github.com/uber/cadence/service/history/task"
)

type (
	processingQueueCollection struct {
		level       int
		queues      []ProcessingQueue
		activeQueue ProcessingQueue
	}
)

// NewProcessingQueueCollection creates a new collection for non-overlapping queues
func NewProcessingQueueCollection(
	level int,
	queues []ProcessingQueue,
) ProcessingQueueCollection {
	sortProcessingQueue(queues)
	queueCollection := &processingQueueCollection{
		level:  level,
		queues: queues,
	}
	queueCollection.resetActiveQueue()

	return queueCollection
}

func (c *processingQueueCollection) Level() int {
	return c.level
}

func (c *processingQueueCollection) Queues() []ProcessingQueue {
	return c.queues
}

func (c *processingQueueCollection) ActiveQueue() ProcessingQueue {
	return c.activeQueue
}

func (c *processingQueueCollection) AddTasks(
	tasks map[task.Key]task.Task,
	newReadLevel task.Key,
) {
	activeQueue := c.ActiveQueue()
	activeQueue.AddTasks(tasks, newReadLevel)

	if taskKeyEquals(activeQueue.State().ReadLevel(), activeQueue.State().MaxLevel()) {
		c.resetActiveQueue()
	}
}

func (c *processingQueueCollection) UpdateAckLevels() {
	remainingQueues := make([]ProcessingQueue, 0, len(c.queues))

	for _, queue := range c.queues {
		queue.UpdateAckLevel()
		if !taskKeyEquals(queue.State().AckLevel(), queue.State().MaxLevel()) {
			remainingQueues = append(remainingQueues, queue)
			continue
		}
	}

	c.queues = remainingQueues
}

func (c *processingQueueCollection) Split(
	policy ProcessingQueueSplitPolicy,
) []ProcessingQueue {
	newQueues := make([]ProcessingQueue, 0, len(c.queues))
	nextLevelQueues := []ProcessingQueue{}

	for _, queue := range c.queues {
		splitQueues := queue.Split(policy)
		sortProcessingQueue(splitQueues)
		for _, splitQueue := range splitQueues {
			if splitQueue.State().Level() != c.level {
				nextLevelQueues = append(nextLevelQueues, splitQueue)
			} else {
				newQueues = append(newQueues, splitQueue)
			}
		}
	}

	c.queues = newQueues

	c.resetActiveQueue()

	return nextLevelQueues
}

func (c *processingQueueCollection) Merge(
	incomingQueues []ProcessingQueue,
) {
	sortProcessingQueue(incomingQueues)

	newQueues := make([]ProcessingQueue, 0, len(c.queues)+len(incomingQueues))

	currentQueueIdx := 0
	incomingQueueIdx := 0
	for incomingQueueIdx < len(incomingQueues) && currentQueueIdx < len(c.queues) {
		mergedQueues := c.queues[currentQueueIdx].Merge(incomingQueues[incomingQueueIdx])
		sortProcessingQueue(mergedQueues)
		newQueues = append(newQueues, mergedQueues[:len(mergedQueues)-1]...)

		lastMergedQueue := mergedQueues[len(mergedQueues)-1]
		if currentQueueIdx+1 == len(c.queues) ||
			!c.queues[currentQueueIdx+1].State().AckLevel().Less(lastMergedQueue.State().MaxLevel()) {

			newQueues = append(newQueues, lastMergedQueue)
			incomingQueueIdx++
		} else {
			incomingQueues[incomingQueueIdx] = lastMergedQueue
		}

		currentQueueIdx++
	}

	if incomingQueueIdx < len(incomingQueues) {
		newQueues = append(newQueues, incomingQueues[incomingQueueIdx:]...)
	}

	if currentQueueIdx < len(c.queues) {
		newQueues = append(newQueues, c.queues[currentQueueIdx:]...)
	}

	c.queues = newQueues

	c.resetActiveQueue()
}

func (c *processingQueueCollection) resetActiveQueue() {
	for _, queue := range c.queues {
		if !taskKeyEquals(queue.State().ReadLevel(), queue.State().MaxLevel()) {
			c.activeQueue = queue
			return
		}
	}
	c.activeQueue = nil
}

func sortProcessingQueue(
	queues []ProcessingQueue,
) {
	sort.Slice(queues, func(i, j int) bool {
		if queues[i].State().Level() == queues[j].State().Level() {
			return queues[i].State().AckLevel().Less(queues[j].State().AckLevel())
		}
		return queues[i].State().Level() < queues[j].State().Level()
	})
}
