package rulebook

import (
	"testing"
)

func TestWorkflowUpdateClosureRule_DetectsAcceptedAfterClose(t *testing.T) {
	reg := newTestRegistry()
	// Start and complete workflow.
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))
	// Accept an update after workflow closed.
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateClosureRule{})
	if len(violations) == 0 {
		t.Fatal("expected violation for update accepted after workflow closed")
	}
}

func TestWorkflowUpdateClosureRule_DetectsCompletedAfterClose(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateClosureRule{})
	if len(violations) == 0 {
		t.Fatal("expected violation for update completed after workflow closed")
	}
}

func TestWorkflowUpdateClosureRule_NoViolation_WorkflowNotClosed(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateClosureRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations when workflow is not closed, got %d", len(violations))
	}
}

func TestWorkflowUpdateClosureRule_NoViolation_UpdateBeforeClose(t *testing.T) {
	reg := newTestRegistry()
	// Update completes before workflow closes.
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateClosureRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for update completed before close, got %d", len(violations))
	}
}
