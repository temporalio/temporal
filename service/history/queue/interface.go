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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination interface_mock.go -self_package github.com/uber/cadence/service/history/queue

package queue

import (
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/task"
)

type (
	// DomainFilter filters domain
	DomainFilter struct {
		DomainIDs map[string]struct{}
		// by default, a DomainFilter matches domains listed in the Domains field
		// if reverseMatch is true then the DomainFilter matches domains that are
		// not in the Domains field.
		ReverseMatch bool
	}

	// ProcessingQueueState indicates the scope of a task processing queue and its current progress
	ProcessingQueueState interface {
		Level() int
		AckLevel() task.Key
		ReadLevel() task.Key
		MaxLevel() task.Key
		DomainFilter() DomainFilter
	}

	// ProcessingQueue is responsible for keeping track of the state of tasks
	// within the scope defined by its state; it can also be split into multiple
	// ProcessingQueues with non-overlapping scope or be merged with another
	// ProcessingQueue
	ProcessingQueue interface {
		State() ProcessingQueueState
		Split(ProcessingQueueSplitPolicy) []ProcessingQueue
		Merge(ProcessingQueue) []ProcessingQueue
		AddTasks(map[task.Key]task.Task, bool)
		UpdateAckLevel()
		// TODO: add Offload() method
	}

	// ProcessingQueueSplitPolicy determines if one ProcessingQueue should be split
	// into multiple ProcessingQueues
	ProcessingQueueSplitPolicy interface {
		Evaluate(ProcessingQueue) []ProcessingQueueState
	}

	// ProcessingQueueCollection manages a list of non-overlapping ProcessingQueues
	// and keep track of the current active ProcessingQueue
	ProcessingQueueCollection interface {
		Level() int
		Queues() []ProcessingQueue
		ActiveQueue() ProcessingQueue
		AddTasks(map[task.Key]task.Task, bool)
		UpdateAckLevels()
		Split(ProcessingQueueSplitPolicy) []ProcessingQueue
		Merge([]ProcessingQueue)
		// TODO: add Offload() method
	}

	// ProcessingQueueManager manages a set of ProcessingQueueCollection and
	// controls the event loop for loading tasks, updating and persisting
	// ProcessingQueueStates, spliting/merging ProcessingQueue, etc.
	ProcessingQueueManager interface {
		common.Daemon

		NotifyNewTasks([]persistence.Task)
	}
)
