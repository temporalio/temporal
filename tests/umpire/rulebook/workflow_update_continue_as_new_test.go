package rulebook

import (
	"testing"
)

func TestWorkflowUpdateContinueAsNewRule_DetectsStuckOnCompletedWorkflow(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateContinueAsNewRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for admitted update on completed workflow, got %d", len(violations))
	}
}

func TestWorkflowUpdateContinueAsNewRule_NoViolation_UpdateAccepted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateContinueAsNewRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for accepted update, got %d", len(violations))
	}
}

func TestWorkflowUpdateContinueAsNewRule_NoViolation_WorkflowNotCompleted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateContinueAsNewRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations when workflow is not completed, got %d", len(violations))
	}
}
