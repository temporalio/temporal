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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination task_mock.go

package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
)

type (
	// Task is the generic task interface
	Task interface {
		GetKey() Key
		GetNamespaceID() string
		GetWorkflowID() string
		GetRunID() string
		GetTaskID() int64
		GetVisibilityTime() time.Time
		GetCategory() Category
		GetType() enumsspb.TaskType

		SetTaskID(id int64)
		SetVisibilityTime(timestamp time.Time)

		// TODO: All tasks should have a method returning the versioned transition
		// in which the task is generated.
		// This versioned transiton can be used to determine if the task or
		// the mutable state is stale.
	}

	// HasVersion is a legacy method for tasks that uses only version to determine
	// if the task should be executed or not.
	HasVersion interface {
		GetVersion() int64
	}

	// HasStateMachineTaskType must be implemented by all HSM state machine tasks.
	HasStateMachineTaskType interface {
		StateMachineTaskType() string
	}
	// HasDestination must be implemented by all tasks used in the outbound queue.
	HasDestination interface {
		GetDestination() string
	}
)

// GetShardIDForTask computes the shardID for a given task using the task's namespace, workflow ID and the number of
// history shards in the cluster.
func GetShardIDForTask(task Task, numShards int) int {
	return int(common.WorkflowIDToHistoryShard(task.GetNamespaceID(), task.GetWorkflowID(), int32(numShards)))
}
