// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

// Grouper groups tasks and constructs predicates for those groups.
type Grouper interface {
	// Key returns the group key for a given task.
	Key(task tasks.Task) (key any)
	// Predicate constructs a prdicate from a slice of keys.
	Predicate(keys []any) tasks.Predicate
}

type GrouperNamespaceID struct {
}

func (GrouperNamespaceID) Key(task tasks.Task) (key any) {
	return task.GetNamespaceID()
}

func (GrouperNamespaceID) Predicate(keys []any) tasks.Predicate {
	pendingNamespaceIDs := make([]string, len(keys))
	for i, namespaceID := range keys {
		// Assume predicate is only called with keys returned from GrouperNamespaceID.Key()
		pendingNamespaceIDs[i] = namespaceID.(string)
	}
	return tasks.NewNamespacePredicate(pendingNamespaceIDs)
}

var _ Grouper = GrouperNamespaceID{}

// NamespaceIDAndDestination is the key for grouping tasks by namespace ID and destination.
type NamespaceIDAndDestination struct {
	NamespaceID string
	Destination string
}

// OutboundTaskGroupNamespaceIDAndDestination is the key for grouping tasks by task type namespace ID and destination.
type OutboundTaskGroupNamespaceIDAndDestination struct {
	TaskGroup   string
	NamespaceID string
	Destination string
}

type GrouperStateMachineNamespaceIDAndDestination struct {
}

func (g GrouperStateMachineNamespaceIDAndDestination) Key(task tasks.Task) (key any) {
	return g.KeyTyped(task)
}

func (GrouperStateMachineNamespaceIDAndDestination) KeyTyped(task tasks.Task) (key OutboundTaskGroupNamespaceIDAndDestination) {
	destGetter, ok := task.(tasks.HasDestination)
	var dest string
	if ok {
		dest = destGetter.GetDestination()
	}
	smtGetter, ok := task.(tasks.HasStateMachineTaskType)
	var smt string
	if ok {
		smt = smtGetter.StateMachineTaskType()
	}
	return OutboundTaskGroupNamespaceIDAndDestination{
		TaskGroup:   smt,
		NamespaceID: task.GetNamespaceID(),
		Destination: dest,
	}
}

func (GrouperStateMachineNamespaceIDAndDestination) Predicate(keys []any) tasks.Predicate {
	pred := predicates.Empty[tasks.Task]()
	for _, anyKey := range keys {
		// Assume predicate is only called with keys returned from OutboundTaskGroupNamespaceIDAndDestination.Key()
		key := anyKey.(OutboundTaskGroupNamespaceIDAndDestination)
		pred = predicates.Or(pred, predicates.And(
			tasks.NewOutboundTaskGroupPredicate([]string{key.TaskGroup}),
			tasks.NewNamespacePredicate([]string{key.NamespaceID}),
			tasks.NewDestinationPredicate([]string{key.Destination}),
		))
	}
	return pred
}

var _ Grouper = GrouperStateMachineNamespaceIDAndDestination{}
