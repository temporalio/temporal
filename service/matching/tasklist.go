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

package matching

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/uber/cadence/common/persistence"
)

type (
	// taskListID is the key that uniquely identifies a task list
	taskListID struct {
		qualifiedTaskListName
		domainID string
		taskType int
	}
	// qualifiedTaskListName refers to the fully qualified task list name
	qualifiedTaskListName struct {
		name      string // internal name of the tasks list
		baseName  string // original name of the task list as specified by user
		partition int    // partitionID of task list
	}
)

const (
	// taskListPartitionPrefix is the required naming prefix for any task list partition other than partition 0
	taskListPartitionPrefix = "/__cadence_sys/"
)

// newTaskListName returns a fully qualified task list name.
// Fully qualified names contain additional metadata about task list
// derived from their given name. The additional metadata only makes sense
// when a task list has more than one partition. When there is more than
// one partition for a user specified task list, each of the
// individual partitions have an internal name of the form
//
//     /__cadence_sys/[original-name]/[partitionID]
//
// The name of the root partition is always the same as the user specified name. Rest of
// the partitions follow the naming convention above. In addition, the task lists partitions
// logically form a N-ary tree where N is configurable dynamically. The tree formation is an
// optimization to allow for partitioned task lists to dispatch tasks with low latency when
// throughput is low - See https://github.com/uber/cadence/issues/2098
//
// Returns error if the given name is non-compliant with the required format
// for task list names
func newTaskListName(name string) (qualifiedTaskListName, error) {
	tn := qualifiedTaskListName{
		name:     name,
		baseName: name,
	}
	if err := tn.init(); err != nil {
		return qualifiedTaskListName{}, err
	}
	return tn, nil
}

// IsRoot returns true if this task list is a root partition
func (tn *qualifiedTaskListName) IsRoot() bool {
	return tn.partition == 0
}

// GetRoot returns the root name for a task list
func (tn *qualifiedTaskListName) GetRoot() string {
	return tn.baseName
}

// Parent returns the name of the parent task list
// input:
//   degree: Number of children at each level of the tree
// Returns empty string if this task list is the root
func (tn *qualifiedTaskListName) Parent(degree int) string {
	if tn.IsRoot() || degree == 0 {
		return ""
	}
	pid := (tn.partition+degree-1)/degree - 1
	return tn.mkName(pid)
}

func (tn *qualifiedTaskListName) mkName(partition int) string {
	if partition == 0 {
		return tn.baseName
	}
	return fmt.Sprintf("%v%v/%v", taskListPartitionPrefix, tn.baseName, partition)
}

func (tn *qualifiedTaskListName) init() error {
	if !strings.HasPrefix(tn.name, taskListPartitionPrefix) {
		return nil
	}

	suffixOff := strings.LastIndex(tn.name, "/")
	if suffixOff <= len(taskListPartitionPrefix) {
		return fmt.Errorf("invalid partitioned task list name %v", tn.name)
	}

	p, err := strconv.Atoi(tn.name[suffixOff+1:])
	if err != nil || p <= 0 {
		return fmt.Errorf("invalid partitioned task list name %v", tn.name)
	}

	tn.partition = p
	tn.baseName = tn.name[len(taskListPartitionPrefix):suffixOff]
	return nil
}

// newTaskListID returns taskListID which uniquely identfies as task list
func newTaskListID(domainID, taskListName string, taskType int) (*taskListID, error) {
	name, err := newTaskListName(taskListName)
	if err != nil {
		return nil, err
	}
	return &taskListID{
		qualifiedTaskListName: name,
		domainID:              domainID,
		taskType:              taskType,
	}, nil
}

func (tid *taskListID) String() string {
	var b bytes.Buffer
	b.WriteString("[")
	b.WriteString("name=")
	b.WriteString(tid.name)
	b.WriteString("type=")
	if tid.taskType == persistence.TaskListTypeActivity {
		b.WriteString("activity")
	} else {
		b.WriteString("decision")
	}
	b.WriteString("]")
	return b.String()
}
