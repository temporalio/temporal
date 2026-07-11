package umpire

import (
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

// NamespacePath builds entity keys rooted at a namespace.
type NamespacePath struct {
	namespaceID string
}

// Namespace starts building an entity key path rooted at a namespace ID.
func Namespace(namespaceID string) NamespacePath {
	return NamespacePath{namespaceID: namespaceID}
}

func (p NamespacePath) ancestors() []umpirefw.EntityID {
	if p.namespaceID == "" {
		return nil
	}
	return []umpirefw.EntityID{umpirefw.NewEntityID(fact.NamespaceType, p.namespaceID)}
}

// Workflow descends to a workflow rooted at the namespace.
func (p NamespacePath) Workflow(workflowID string) WorkflowPath {
	return WorkflowPath{namespaceID: p.namespaceID, workflowID: workflowID}
}

// TaskQueue returns the registry key for a task queue entity.
func (p NamespacePath) TaskQueue(name string) string {
	return umpirefw.EntityPathKey(&umpirefw.EntityPath{
		EntityID:  umpirefw.NewEntityID(fact.TaskQueueType, name),
		Ancestors: p.ancestors(),
	})
}

// WorkflowPath builds entity keys rooted at a workflow within a namespace.
type WorkflowPath struct {
	namespaceID string
	workflowID  string
}

// Update returns the registry key for a workflow update entity
// (keyed under its parent workflow).
func (p WorkflowPath) Update(updateID string) string {
	ancestors := Namespace(p.namespaceID).ancestors()
	ancestors = append(ancestors, umpirefw.NewEntityID(fact.WorkflowType, p.workflowID))
	return umpirefw.EntityPathKey(&umpirefw.EntityPath{
		EntityID:  umpirefw.NewEntityID(fact.WorkflowUpdateType, updateID),
		Ancestors: ancestors,
	})
}

// Task returns the registry key for a workflow task entity
// (keyed under its task queue).
func (p WorkflowPath) Task(taskQueue, runID string) string {
	ancestors := Namespace(p.namespaceID).ancestors()
	ancestors = append(ancestors, umpirefw.NewEntityID(fact.TaskQueueType, taskQueue))
	return umpirefw.EntityPathKey(&umpirefw.EntityPath{
		EntityID:  umpirefw.NewEntityID(fact.WorkflowTaskType, taskQueue+":"+p.workflowID+":"+runID),
		Ancestors: ancestors,
	})
}
