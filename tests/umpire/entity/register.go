package entity

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

// RegisterDefaultEntities registers the default entity types with a registry.
func RegisterDefaultEntities(r *umpire.Registry) {
	r.RegisterFact(
		&fact.WorkflowStarted{},
		&fact.WorkflowExecutionCompleted{},
		&fact.WorkflowTaskAdded{},
		&fact.WorkflowTaskPolled{},
		&fact.WorkflowTaskStored{},
		&fact.WorkflowTaskDiscarded{},
		&fact.WorkflowTerminated{},
		&fact.SpeculativeWorkflowTaskScheduled{},
		&fact.WorkflowUpdateRequested{},
		&fact.WorkflowUpdateAdmitted{},
		&fact.WorkflowUpdateAccepted{},
		&fact.WorkflowUpdateCompleted{},
		&fact.WorkflowUpdateRejected{},
		&fact.WorkflowUpdateAborted{},
	)

	r.RegisterEntity(
		func() umpire.Entity { return NewWorkflow() },
		&fact.WorkflowStarted{},
		&fact.WorkflowExecutionCompleted{},
	)

	r.RegisterEntity(
		func() umpire.Entity { return NewTaskQueue() },
		&fact.WorkflowTaskAdded{},
		&fact.WorkflowTaskPolled{},
	)

	r.RegisterEntity(
		func() umpire.Entity { return NewWorkflowTask() },
		&fact.WorkflowTaskAdded{},
		&fact.WorkflowTaskPolled{},
		&fact.WorkflowTaskStored{},
		&fact.SpeculativeWorkflowTaskScheduled{},
	)

	r.RegisterEntity(
		func() umpire.Entity { return NewWorkflowUpdate() },
		&fact.WorkflowUpdateRequested{},
		&fact.WorkflowUpdateAdmitted{},
		&fact.WorkflowUpdateAccepted{},
		&fact.WorkflowUpdateCompleted{},
		&fact.WorkflowUpdateRejected{},
	)
}
