package entity

import "go.temporal.io/server/tests/umpire/fact"

const NamespaceType = fact.NamespaceType

// NamespaceID is the root identity for namespace-scoped entities.
type NamespaceID struct{ id string }

func Namespace(id string) NamespaceID {
	return NamespaceID{id: id}
}

func (n NamespaceID) String() string {
	return "namespace:" + n.id
}

func (n NamespaceID) Workflow(workflowID string) WorkflowID {
	return WorkflowID{parent: n, id: workflowID}
}

func (n NamespaceID) TaskQueue(name string) TaskQueueID {
	return TaskQueueID{namespace: n, name: name}
}
