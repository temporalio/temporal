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
	"bytes"
	"fmt"
	"strconv"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/namespace"
)

type (
	// taskQueueID is the key that uniquely identifies a task queue
	taskQueueID struct {
		qualifiedTaskQueueName
		namespaceID namespace.ID
		taskType    enumspb.TaskQueueType
	}
	// qualifiedTaskQueueName refers to the fully qualified task queue name
	qualifiedTaskQueueName struct {
		name      string // internal name of the tasks list
		baseName  string // original name of the task queue as specified by user
		partition int    // partitionID of task queue
	}
)

const (
	// taskQueuePartitionPrefix is the required naming prefix for any task queue partition other than partition 0
	taskQueuePartitionPrefix = "/_sys/"
)

// newTaskQueueName returns a fully qualified task queue name.
// Fully qualified names contain additional metadata about task queue
// derived from their given name. The additional metadata only makes sense
// when a task queue has more than one partition. When there is more than
// one partition for a user specified task queue, each of the
// individual partitions have an internal name of the form
//
//     /_sys/[original-name]/[partitionID]
//
// The name of the root partition is always the same as the user specified name. Rest of
// the partitions follow the naming convention above. In addition, the task queues partitions
// logically form a N-ary tree where N is configurable dynamically. The tree formation is an
// optimization to allow for partitioned task queues to dispatch tasks with low latency when
// throughput is low - See https://github.com/uber/cadence/issues/2098
//
// Returns error if the given name is non-compliant with the required format
// for task queue names
func newTaskQueueName(name string) (qualifiedTaskQueueName, error) {
	tn := qualifiedTaskQueueName{
		name:     name,
		baseName: name,
	}
	if err := tn.init(); err != nil {
		return qualifiedTaskQueueName{}, err
	}
	return tn, nil
}

// IsRoot returns true if this task queue is a root partition
func (tn *qualifiedTaskQueueName) IsRoot() bool {
	return tn.partition == 0
}

// GetRoot returns the root name for a task queue
func (tn *qualifiedTaskQueueName) GetRoot() string {
	return tn.baseName
}

// Parent returns the name of the parent task queue
// input:
//   degree: Number of children at each level of the tree
// Returns empty string if this task queue is the root
func (tn *qualifiedTaskQueueName) Parent(degree int) string {
	if tn.IsRoot() || degree == 0 {
		return ""
	}
	pid := (tn.partition+degree-1)/degree - 1
	return tn.mkName(pid)
}

func (tn *qualifiedTaskQueueName) mkName(partition int) string {
	if partition == 0 {
		return tn.baseName
	}
	return fmt.Sprintf("%v%v/%v", taskQueuePartitionPrefix, tn.baseName, partition)
}

func (tn *qualifiedTaskQueueName) init() error {
	if !strings.HasPrefix(tn.name, taskQueuePartitionPrefix) {
		return nil
	}

	suffixOff := strings.LastIndex(tn.name, "/")
	if suffixOff <= len(taskQueuePartitionPrefix) {
		return fmt.Errorf("invalid partitioned task queue name %v", tn.name)
	}

	p, err := strconv.Atoi(tn.name[suffixOff+1:])
	if err != nil || p <= 0 {
		return fmt.Errorf("invalid partitioned task queue name %v", tn.name)
	}

	tn.partition = p
	tn.baseName = tn.name[len(taskQueuePartitionPrefix):suffixOff]
	return nil
}

// newTaskQueueID returns taskQueueID which uniquely identfies as task queue
func newTaskQueueID(namespaceID namespace.ID, taskQueueName string, taskType enumspb.TaskQueueType) (*taskQueueID, error) {
	name, err := newTaskQueueName(taskQueueName)
	if err != nil {
		return nil, err
	}
	return &taskQueueID{
		qualifiedTaskQueueName: name,
		namespaceID:            namespaceID,
		taskType:               taskType,
	}, nil
}

func (tid *taskQueueID) String() string {
	var b bytes.Buffer
	b.WriteString("[")
	b.WriteString("name=")
	b.WriteString(tid.name)
	b.WriteString("type=")
	if tid.taskType == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
		b.WriteString("activity")
	} else {
		b.WriteString("workflow")
	}
	b.WriteString("]")
	return b.String()
}
