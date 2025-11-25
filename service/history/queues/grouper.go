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

type GrouperStateMachineNamespaceIDAndDestination struct {
}

func (g GrouperStateMachineNamespaceIDAndDestination) Key(task tasks.Task) (key any) {
	return g.KeyTyped(task)
}

func (GrouperStateMachineNamespaceIDAndDestination) KeyTyped(task tasks.Task) (key tasks.TaskGroupNamespaceIDAndDestination) {
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
	return tasks.TaskGroupNamespaceIDAndDestination{
		TaskGroup:   smt,
		NamespaceID: task.GetNamespaceID(),
		Destination: dest,
	}
}

func (GrouperStateMachineNamespaceIDAndDestination) Predicate(keys []any) tasks.Predicate {
	if len(keys) == 0 {
		return predicates.Empty[tasks.Task]()
	}
	groups := make([]tasks.TaskGroupNamespaceIDAndDestination, len(keys))
	for i, anyKey := range keys {
		// Assume predicate is only called with keys returned from OutboundTaskGroupNamespaceIDAndDestination.Key()
		key := anyKey.(tasks.TaskGroupNamespaceIDAndDestination)
		groups[i] = key
	}
	return tasks.NewOutboundTaskPredicate(groups)
}

var _ Grouper = GrouperStateMachineNamespaceIDAndDestination{}
