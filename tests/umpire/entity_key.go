package umpire

import (
	umpirefw "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

// WorkflowPath builds entity keys rooted at a workflow.
type WorkflowPath struct {
	workflowID string
}

// Workflow starts building an entity key path rooted at a workflow ID.
func Workflow(workflowID string) WorkflowPath {
	return WorkflowPath{workflowID: workflowID}
}

// Update returns the registry key for a workflow update entity.
func (p WorkflowPath) Update(updateID string) string {
	parentID := umpirefw.NewEntityID(fact.WorkflowType, p.workflowID)
	return umpirefw.IdentityKey(&umpirefw.Identity{
		EntityID: umpirefw.NewEntityID(fact.WorkflowUpdateType, updateID),
		ParentID: &parentID,
	})
}

// Task returns the registry key for a workflow task entity.
func (p WorkflowPath) Task(taskQueue, runID string) string {
	tqID := umpirefw.NewEntityID(fact.TaskQueueType, taskQueue)
	return umpirefw.IdentityKey(&umpirefw.Identity{
		EntityID: umpirefw.NewEntityID(fact.WorkflowTaskType, taskQueue+":"+p.workflowID+":"+runID),
		ParentID: &tqID,
	})
}
