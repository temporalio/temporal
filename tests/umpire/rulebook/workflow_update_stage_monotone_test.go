package rulebook

import (
	"testing"

	"go.temporal.io/server/tests/umpire/entity"
)

func TestWorkflowUpdateStageMonotoneRule_DetectsRegression(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	// Run rule once to record highWater at "accepted" (stage 2).
	rule := &WorkflowUpdateStageMonotoneRule{}
	violations := checkSafetyRule(reg, rule)
	if len(violations) != 0 {
		t.Fatalf("expected no violations initially, got %d", len(violations))
	}

	// Force the FSM back to "admitted" to simulate a regression.
	for _, entry := range reg.QueryEntities(entity.WorkflowUpdateType, 0) {
		wu := entry.Entity.(*entity.WorkflowUpdate)
		wu.FSM.SetState("admitted")
	}

	violations = checkSafetyRule(reg, rule)
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for stage regression, got %d", len(violations))
	}
}

func TestWorkflowUpdateStageMonotoneRule_NoViolation_NormalProgression(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateStageMonotoneRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for normal forward progression, got %d", len(violations))
	}
}

func TestWorkflowUpdateStageMonotoneRule_NoViolation_Rejection(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateRejected("wf1", "upd1"))

	violations := checkSafetyRule(reg, &WorkflowUpdateStageMonotoneRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for rejection, got %d", len(violations))
	}
}
