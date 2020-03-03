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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination interface_mock.go -self_package github.com/uber/cadence/common/task

package task

import (
	"github.com/uber/cadence/common"
)

type (
	// Processor is the generic coroutine pool interface
	// which process tasks
	Processor interface {
		common.Daemon
		Submit(task Task) error
	}

	// Scheduler is the generic interface for scheduling tasks with priority
	// and processing them
	Scheduler interface {
		common.Daemon
		Submit(task PriorityTask) error
		TrySubmit(task PriorityTask) (bool, error)
	}

	// SchedulerType respresents the type of the task scheduler implementation
	SchedulerType int

	// State represents the current state of a task
	State int

	// Task is the interface for tasks which should be executed sequentially
	Task interface {
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
		// State returns the current task state
		State() State
	}

	// PriorityTask is the interface for tasks which have and can be assigned a priority
	PriorityTask interface {
		Task
		// Priority returns the priority of the task, or NoPriority if no priority was previously assigned
		Priority() int
		// SetPriority sets the priority of the task
		SetPriority(int)
	}

	// SequentialTaskQueueFactory is the function which generate a new SequentialTaskQueue
	// for a give SequentialTask
	SequentialTaskQueueFactory func(task Task) SequentialTaskQueue

	// SequentialTaskQueue is the generic task queue interface which group
	// sequential tasks to be executed one by one
	SequentialTaskQueue interface {
		// QueueID return the ID of the queue, as well as the tasks inside (same)
		QueueID() interface{}
		// Offer push an task to the task set
		Add(task Task)
		// Poll pop an task from the task set
		Remove() Task
		// IsEmpty indicate if the task set is empty
		IsEmpty() bool
		// Len return the size of the queue
		Len() int
	}
)

const (
	// SchedulerTypeFIFO is the scheduler type for FIFO scheduler implementation
	SchedulerTypeFIFO SchedulerType = iota + 1
	// SchedulerTypeWRR is the scheduler type for weighted round robin scheduler implementation
	SchedulerTypeWRR
)

const (
	// TaskStatePending is the state for a task when it's waiting to be processed or currently being processed
	TaskStatePending State = iota + 1
	// TaskStateAcked is the state for a task if it has been successfully completed
	TaskStateAcked
	// TaskStateNacked is the state for a task if it can not be processed
	TaskStateNacked
)

const (
	// NoPriority is the value returned if no priority is ever assigned to the task
	NoPriority = -1
)
