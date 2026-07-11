package rulebook

import (
	"testing"
)

func TestWorkflowUpdateContextClearRule_DetectsStrandedUpdate(t *testing.T) {
	reg := newTestRegistry()
	// Update admitted, but no pending task and workflow not completed.
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateContextClearRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for stranded update, got %d", len(violations))
	}
}

func TestWorkflowUpdateContextClearRule_DetectsStrandedAcceptedUpdate(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	// Task was polled (no longer pending).
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
	routeFact(t, reg, makeWorkflowTaskPolled("tq", "wf1", "run1", true))

	violations := checkLivenessRule(reg, &WorkflowUpdateContextClearRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for stranded accepted update, got %d", len(violations))
	}
}

func TestWorkflowUpdateContextClearRule_NoViolation_PendingTaskExists(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	// Pending task exists.
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateContextClearRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations when pending task exists, got %d", len(violations))
	}
}

func TestWorkflowUpdateContextClearRule_NoViolation_WorkflowCompleted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowCompleted("wf1"))

	// Workflow completed — closure rule handles this, not context clear.
	violations := checkLivenessRule(reg, &WorkflowUpdateContextClearRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for completed workflow, got %d", len(violations))
	}
}

func TestWorkflowUpdateContextClearRule_NoViolation_UpdateCompleted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowStarted("wf1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateContextClearRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for completed update, got %d", len(violations))
	}
}
