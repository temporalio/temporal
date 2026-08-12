package model

import (
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
)

// DefaultEntity is one entry in the default entity set: an entity factory plus the
// facts that route to it. DefaultEntities (below) is the single source of truth for
// which entities the umpire models — both the passive side (RegisterDefaultEntities,
// which routes facts to them) and the active side (planner.DefaultModels, which
// plans over them) derive from it, so adding an entity is a one-line change in one
// place, not two that can drift.
type DefaultEntity struct {
	New   umpire.EntityFactory
	Facts []umpire.Fact
}

// DefaultEntities is the canonical default entity set.
func DefaultEntities() []DefaultEntity {
	return []DefaultEntity{
		{
			New:   func() umpire.Entity { return NewWorkflow() },
			Facts: []umpire.Fact{&fact.WorkflowStarted{}, &fact.WorkflowExecutionCompleted{}},
		},
		{
			New:   func() umpire.Entity { return NewWorkflowRun() },
			Facts: []umpire.Fact{&fact.WorkflowRunStarted{}, &fact.WorkflowRunCompleted{}, &fact.WorkflowRunContinuedAsNew{}},
		},
		{
			New:   func() umpire.Entity { return NewTaskQueue() },
			Facts: []umpire.Fact{&fact.WorkflowTaskAdded{}, &fact.WorkflowTaskPolled{}},
		},
		{
			New: func() umpire.Entity { return NewWorkflowTask() },
			Facts: []umpire.Fact{
				&fact.WorkflowTaskAdded{},
				&fact.WorkflowTaskPolled{},
				&fact.WorkflowTaskStored{},
				&fact.SpeculativeWorkflowTaskScheduled{},
			},
		},
		{
			New: func() umpire.Entity { return NewNexusOperation() },
			Facts: []umpire.Fact{
				&fact.NexusOperationScheduled{},
				&fact.NexusOperationAttemptFailed{},
				&fact.NexusOperationStarted{},
				&fact.NexusOperationSucceeded{},
				&fact.NexusOperationFailed{},
				&fact.NexusOperationCanceled{},
				&fact.NexusOperationTimedOut{},
				&fact.NexusOperationRejected{},
			},
		},
	}
}

// defaultFacts is the full set of fact probes the decoder must recognize. It is a
// superset of the per-entity subscriptions in DefaultEntities: it also includes
// broadcast / settle facts (e.g. WorkflowTerminated) that no
// single entity subscribes to but entities still handle in OnFact.
func defaultFacts() []umpire.Fact {
	return []umpire.Fact{
		&fact.WorkflowStarted{},
		&fact.WorkflowExecutionCompleted{},
		&fact.WorkflowRunStarted{},
		&fact.WorkflowRunCompleted{},
		&fact.WorkflowRunContinuedAsNew{},
		&fact.WorkflowTaskAdded{},
		&fact.WorkflowTaskPolled{},
		&fact.WorkflowTaskStored{},
		&fact.WorkflowTaskDiscarded{},
		&fact.WorkflowTerminated{},
		&fact.SpeculativeWorkflowTaskScheduled{},
		&fact.NexusOperationScheduled{},
		&fact.NexusOperationAttemptFailed{},
		&fact.NexusOperationStarted{},
		&fact.NexusOperationSucceeded{},
		&fact.NexusOperationFailed{},
		&fact.NexusOperationCanceled{},
		&fact.NexusOperationTimedOut{},
		&fact.NexusOperationRejected{},
	}
}

// RegisterDefaultEntities registers the default facts and entities with a registry.
func RegisterDefaultEntities(r *umpire.ModelState) {
	r.RegisterFact(defaultFacts()...)
	for _, e := range DefaultEntities() {
		r.RegisterEntity(e.New, e.Facts...)
	}
}
