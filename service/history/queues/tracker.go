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

package queues

import (
	"fmt"

	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
)

type (
	// TODO: make task tracking a standalone component
	// currently it's used as a implementation detail in SliceImpl
	executableTracker struct {
		pendingExecutables  map[tasks.Key]Executable
		pendingPerNamesapce map[namespace.ID]int
	}
)

func newExecutableTracker() *executableTracker {
	return &executableTracker{
		pendingExecutables:  make(map[tasks.Key]Executable),
		pendingPerNamesapce: make(map[namespace.ID]int),
	}
}

func (t *executableTracker) split(
	thisScope Scope,
	thatScope Scope,
) (*executableTracker, *executableTracker) {
	thatPendingExecutables := make(map[tasks.Key]Executable, len(t.pendingExecutables)/2)
	thatPendingPerNamespace := make(map[namespace.ID]int, len(t.pendingPerNamesapce))

	for key, executable := range t.pendingExecutables {
		if thisScope.Contains(executable) {
			continue
		}

		if !thatScope.Contains(executable) {
			panic(fmt.Sprintf("Queue slice encountered task doesn't belong to either scopes during split, scope: %v and %v, task: %v, task type: %v",
				thisScope, thatScope, executable.GetTask(), executable.GetType()))
		}

		namespaceID := namespace.ID(executable.GetNamespaceID())

		delete(t.pendingExecutables, key)
		t.pendingPerNamesapce[namespaceID]--

		thatPendingExecutables[key] = executable
		thatPendingPerNamespace[namespaceID]++
	}

	return t, &executableTracker{
		pendingExecutables:  thatPendingExecutables,
		pendingPerNamesapce: thatPendingPerNamespace,
	}
}

func (t *executableTracker) merge(incomingTracker *executableTracker) *executableTracker {
	thisExecutables, thisPendingTasks := t.pendingExecutables, t.pendingPerNamesapce
	thatExecutables, thatPendingTasks := incomingTracker.pendingExecutables, incomingTracker.pendingPerNamesapce
	if len(thisExecutables) < len(thatExecutables) {
		thisExecutables, thatExecutables = thatExecutables, thisExecutables
		thisPendingTasks = thatPendingTasks
	}

	for key, executable := range thatExecutables {
		thisExecutables[key] = executable
		thisPendingTasks[namespace.ID(executable.GetNamespaceID())]++
	}

	t.pendingExecutables = thisExecutables
	t.pendingPerNamesapce = thisPendingTasks
	return t
}

func (t *executableTracker) add(
	executable Executable,
) {
	t.pendingExecutables[executable.GetKey()] = executable
	t.pendingPerNamesapce[namespace.ID(executable.GetNamespaceID())]++
}

func (t *executableTracker) shrink() tasks.Key {
	minPendingTaskKey := tasks.MaximumKey
	for key, executable := range t.pendingExecutables {
		if executable.State() == ctasks.TaskStateAcked {
			t.pendingPerNamesapce[namespace.ID(executable.GetNamespaceID())]--
			delete(t.pendingExecutables, key)
			continue
		}

		minPendingTaskKey = tasks.MinKey(minPendingTaskKey, key)
	}

	for namespaceID, numPending := range t.pendingPerNamesapce {
		if numPending == 0 {
			delete(t.pendingPerNamesapce, namespaceID)
		}
	}

	return minPendingTaskKey
}

func (t *executableTracker) clear() {
	for _, executable := range t.pendingExecutables {
		executable.Cancel()
	}

	t.pendingExecutables = make(map[tasks.Key]Executable)
	t.pendingPerNamesapce = make(map[namespace.ID]int)
}
