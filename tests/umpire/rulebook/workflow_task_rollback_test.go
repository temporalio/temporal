package rulebook

import (
	"testing"
)

func TestSpeculativeTaskRollbackRule_DetectsRollback(t *testing.T) {
	reg := newTestRegistry()

	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))
	routeFact(t, reg, makeSpecWorkflowTaskPolled("tq", "wf1", "run1", true))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &SpeculativeTaskRollbackRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 rollback violation, got %d", len(violations))
	}
	if violations[0].Rule != "SpeculativeTaskRollbackRule" {
		t.Fatalf("wrong model: %s", violations[0].Rule)
	}
	if violations[0].Tags["updateID"] != "upd1" {
		t.Fatalf("wrong updateID tag: %s", violations[0].Tags["updateID"])
	}
}

func TestSpeculativeTaskRollbackRule_NoViolation_UpdateCompleted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))
	routeFact(t, reg, makeSpecWorkflowTaskPolled("tq", "wf1", "run1", true))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &SpeculativeTaskRollbackRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for completed update, got %d", len(violations))
	}
}

func TestSpeculativeTaskRollbackRule_NoViolation_NoSpeculativeTask(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &SpeculativeTaskRollbackRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations without speculative task, got %d", len(violations))
	}
}
