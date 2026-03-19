package rulebook

import (
	"testing"
	"time"

	"go.temporal.io/server/tests/umpire/entity"
)

func TestWorkflowUpdateStateConsistencyRule_DetectsAcceptedAtMismatch(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	// Force AcceptedAt while FSM is still "admitted" — inconsistent state.
	for _, entry := range reg.QueryEntities(entity.WorkflowUpdateType, 0) {
		wu := entry.Entity.(*entity.WorkflowUpdate)
		wu.AcceptedAt = time.Now()
	}

	violations := checkSafetyRule(reg, &WorkflowUpdateStateConsistencyRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for AcceptedAt/state mismatch, got %d", len(violations))
	}
}

func TestWorkflowUpdateStateConsistencyRule_NoViolation_NormalTransitions(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateStateConsistencyRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for consistent state, got %d", len(violations))
	}
}

func TestWorkflowUpdateStateConsistencyRule_NoViolation_Rejected(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateRejected("wf1", "upd1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateStateConsistencyRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for rejected update, got %d", len(violations))
	}
}

func TestWorkflowUpdateStateConsistencyRule_NoViolation_AdmittedOnly(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateStateConsistencyRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for admitted-only update, got %d", len(violations))
	}
}
