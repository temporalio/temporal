// Copyright (c) 2017 Uber Technologies, Inc.
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

package task

import (
	"github.com/uber/cadence/common"
)

type (
	// SequentialTaskProcessor is the generic coroutine pool interface
	// which process sequential task
	SequentialTaskProcessor interface {
		common.Daemon
		Submit(task SequentialTask) error
	}

	// SequentialTask is the interface for tasks which should be executed sequentially
	SequentialTask interface {
		// Execute process this task
		Execute() error
		// HandleErr handle the error returned by Execute
		HandleErr(err error) error
		// RetryErr check whether to retry after HandleErr(Execute())
		RetryErr(err error) bool
		// Ack marks the task as successful completed
		Ack()
		// Nack marks the task as unsuccessful completed
		Nack()
	}

	// SequentialTaskQueueFactory is the function which generate a new SequentialTaskQueue
	// for a give SequentialTask
	SequentialTaskQueueFactory func(task SequentialTask) SequentialTaskQueue

	// SequentialTaskQueue is the generic task queue interface which group
	// sequential tasks to be executed one by one
	SequentialTaskQueue interface {
		// QueueID return the ID of the queue, as well as the tasks inside (same)
		QueueID() interface{}
		// Offer push an task to the task set
		Add(task SequentialTask)
		// Poll pop an task from the task set
		Remove() SequentialTask
		// IsEmpty indicate if the task set is empty
		IsEmpty() bool
		// Len return the size of the queue
		Len() int
	}
)
