package rule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/fact"
	"go.temporal.io/server/tests/umpire2/model"
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

func makeWorkflowClosedAt(workflowID, outcome string, eventTime time.Time) *fact.WorkflowExecutionClosed {
	closed := &fact.WorkflowExecutionClosed{}
	closed.WorkflowID = workflowID
	closed.RunID = "run-id"
	closed.NamespaceID = "namespace-id"
	closed.Outcome = outcome
	closed.SetEventTime(eventTime)
	closed.EntityPath = &umpire.EntityPath{EntityID: umpire.NewEntityID(model.WorkflowType, workflowID)}
	return closed
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

func TestNexusOperationClosureUsesEventTimeForEveryWorkflowCloseOutcome(t *testing.T) {
	closedAt := time.Date(2026, time.August, 12, 15, 0, 0, 0, time.UTC)
	startedAt := closedAt.Add(time.Second)
	reg := newTestModelState()
	closed := makeWorkflowClosedAt("wf1", "failed", closedAt)
	started := makeNexusStarted("wf1", "5")
	started.SetEventTime(startedAt)
	scheduled := makeNexusScheduled("wf1", "5")
	scheduled.SetEventTime(closedAt.Add(-time.Second))

	routeFact(t, reg, closed)
	routeFact(t, reg, scheduled)
	routeFact(t, reg, started)

	violations := checkSafetyRule(reg, &NexusOperationClosure{})
	require.Len(t, violations, 1)
	require.Contains(t, violations[0].Message, "started after caller workflow closed")
}

func TestNexusOperationClosureIgnoresDeliveryOrderWhenEventTimeIsValid(t *testing.T) {
	closedAt := time.Date(2026, time.August, 12, 15, 0, 0, 0, time.UTC)
	reg := newTestModelState()
	closed := makeWorkflowClosedAt("wf1", "completed", closedAt)
	routeFact(t, reg, closed)
	scheduled := makeNexusScheduled("wf1", "5")
	scheduled.SetEventTime(closedAt.Add(-2 * time.Second))
	succeeded := makeNexusSucceeded("wf1", "5")
	succeeded.SetEventTime(closedAt.Add(-time.Second))
	routeFact(t, reg, scheduled)
	routeFact(t, reg, succeeded)

	require.Empty(t, checkSafetyRule(reg, &NexusOperationClosure{}))
}
