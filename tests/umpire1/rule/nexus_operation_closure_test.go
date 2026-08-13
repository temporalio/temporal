package rule

import (
	"testing"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire1/fact"
	"go.temporal.io/server/tests/umpire1/model"
)

func nexusPath(workflowID, schedEventID string) *umpire.EntityPath {
	self := umpire.NewEntityID(model.NexusOperationType, workflowID+":"+schedEventID)
	wf := umpire.NewEntityID(model.WorkflowType, workflowID)
	return &umpire.EntityPath{EntityID: self, Ancestors: []umpire.EntityID{wf}}
}

func makeNexusScheduled(workflowID, schedEventID string) *fact.NexusOperationScheduled {
	f := &fact.NexusOperationScheduled{}
	f.ScheduledEventID, f.WorkflowID, f.EntityPath = schedEventID, workflowID, nexusPath(workflowID, schedEventID)
	return f
}

func makeNexusStarted(workflowID, schedEventID string) *fact.NexusOperationStarted {
	f := &fact.NexusOperationStarted{}
	f.ScheduledEventID, f.WorkflowID, f.EntityPath = schedEventID, workflowID, nexusPath(workflowID, schedEventID)
	return f
}

func makeNexusSucceeded(workflowID, schedEventID string) *fact.NexusOperationSucceeded {
	f := &fact.NexusOperationSucceeded{}
	f.ScheduledEventID, f.WorkflowID, f.EntityPath = schedEventID, workflowID, nexusPath(workflowID, schedEventID)
	return f
}

func TestNexusOperationClosure_DetectsStartedAfterClose(t *testing.T) {
	reg := newTestModelState()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))
	routeFact(t, reg, makeNexusScheduled("wf1", "5"))
	routeFact(t, reg, makeNexusStarted("wf1", "5")) // started after the caller workflow closed

	violations := checkSafetyRule(reg, &NexusOperationClosure{})
	if len(violations) == 0 {
		t.Fatal("expected violation for operation started after caller workflow closed")
	}
}

func TestNexusOperationClosure_DetectsSettledAfterClose(t *testing.T) {
	reg := newTestModelState()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))
	routeFact(t, reg, makeNexusScheduled("wf1", "5"))
	routeFact(t, reg, makeNexusSucceeded("wf1", "5")) // settled after the caller workflow closed

	violations := checkSafetyRule(reg, &NexusOperationClosure{})
	if len(violations) == 0 {
		t.Fatal("expected violation for operation settled after caller workflow closed")
	}
}

func TestNexusOperationClosure_NoViolation_SettledBeforeClose(t *testing.T) {
	reg := newTestModelState()
	// Operation settles first, then the caller workflow closes.
	routeFact(t, reg, makeNexusScheduled("wf1", "5"))
	routeFact(t, reg, makeNexusSucceeded("wf1", "5"))
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))

	violations := checkSafetyRule(reg, &NexusOperationClosure{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for operation settled before close, got %d: %+v", len(violations), violations)
	}
}

func TestNexusOperationClosure_NoViolation_WorkflowNotClosed(t *testing.T) {
	reg := newTestModelState()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeNexusScheduled("wf1", "5"))
	routeFact(t, reg, makeNexusStarted("wf1", "5"))

	violations := checkSafetyRule(reg, &NexusOperationClosure{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations when caller workflow is not closed, got %d", len(violations))
	}
}
