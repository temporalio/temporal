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

package matching

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqname"
)

type (
	// taskQueueID is the key that uniquely identifies a task queue
	taskQueueID struct {
		tqname.Name
		namespaceID namespace.ID
		taskType    enumspb.TaskQueueType
	}
)

// newTaskQueueID returns taskQueueID which uniquely identifies as task queue
func newTaskQueueID(namespaceID namespace.ID, taskQueueName string, taskType enumspb.TaskQueueType) (*taskQueueID, error) {
	return newTaskQueueIDWithPartition(namespaceID, taskQueueName, taskType, -1)
}

func newTaskQueueIDWithPartition(
	namespaceID namespace.ID, taskQueueName string, taskType enumspb.TaskQueueType, partition int,
) (*taskQueueID, error) {
	name, err := tqname.Parse(taskQueueName)
	if err != nil {
		return nil, err
	}
	if partition >= 0 {
		name = name.WithPartition(partition)
	}
	return &taskQueueID{
		Name:        name,
		namespaceID: namespaceID,
		taskType:    taskType,
	}, nil
}

func newTaskQueueIDWithVersionSet(id *taskQueueID, versionSet string) *taskQueueID {
	return &taskQueueID{
		Name:        id.Name.WithVersionSet(versionSet),
		namespaceID: id.namespaceID,
		taskType:    id.taskType,
	}
}

func (tid *taskQueueID) OwnsUserData() bool {
	return tid.IsRoot() && tid.VersionSet() == "" && tid.taskType == enumspb.TASK_QUEUE_TYPE_WORKFLOW
}

func (tid *taskQueueID) String() string {
	return fmt.Sprintf("TaskQueue(name:%q part:%d vset:%s type:%s nsid:%.5s…)",
		tid.BaseNameString(),
		tid.Partition(),
		tid.VersionSet(),
		tid.taskType,
		tid.namespaceID.String(),
	)
}
