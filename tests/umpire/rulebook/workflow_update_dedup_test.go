package rulebook

import (
	"testing"
)

func TestWorkflowUpdateDeduplicationRule_DetectsUnresolvedDuplicates(t *testing.T) {
	reg := newTestRegistry()
	// Send same update twice (RequestCount becomes 2), then admit but don't complete.
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateDeduplicationRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for unresolved duplicate, got %d", len(violations))
	}
}

func TestWorkflowUpdateDeduplicationRule_NoViolation_SingleRequest(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateDeduplicationRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for single request, got %d", len(violations))
	}
}

func TestWorkflowUpdateDeduplicationRule_NoViolation_Completed(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &WorkflowUpdateDeduplicationRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for completed duplicate, got %d", len(violations))
	}
}
