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
	// SequentialTaskProcessor is the generic goroutines interface
	// which process sequential task
	// for the definition of sequential task, see SequentialTask
	SequentialTaskProcessor interface {
		common.Daemon
		Submit(task SequentialTask) error
	}

	// Task is the generic task representation
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
	}

	// SequentialTaskPartitionID is the interface representing the ID of SequentialTask
	SequentialTaskPartitionID interface {
		PartitionID() interface{} // MUST be go primitive type or struct with primitive types
		HashCode() uint32
	}

	// SequentialTask is the interface for tasks which should be executed sequentially
	// one common example is the workflow replication task (containing workflow history),
	// which must be executed one by one, in the order of the first event ID)
	SequentialTask interface {
		Task
		SequentialTaskPartitionID
		// TaskID return the ID of the task, this task ID is used for sorting
		TaskID() int64
	}
)
