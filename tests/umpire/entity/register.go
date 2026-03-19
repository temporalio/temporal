package entity

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

// RegisterDefaultEntities registers the default entity types with a registry.
func RegisterDefaultEntities(r *umpire.Registry) {
	r.RegisterFact(
		&fact.WorkflowStarted{},
		&fact.WorkflowTaskAdded{},
		&fact.WorkflowTaskPolled{},
		&fact.WorkflowTaskCompleted{},
		&fact.WorkflowTaskStored{},
		&fact.WorkflowTaskDiscarded{},
		&fact.WorkflowTerminated{},
		&fact.SpeculativeWorkflowTaskScheduled{},
		&fact.ActivityTaskAdded{},
		&fact.ActivityTaskPolled{},
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
	)

	r.RegisterEntity(
		func() umpire.Entity { return NewTaskQueue() },
		&fact.WorkflowTaskAdded{},
		&fact.WorkflowTaskPolled{},
		&fact.ActivityTaskAdded{},
		&fact.ActivityTaskPolled{},
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
