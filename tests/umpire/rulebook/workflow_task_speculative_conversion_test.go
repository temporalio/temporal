package rulebook

import (
	"testing"
)

func TestSpeculativeConversionRule_DetectsStuckAfterConversion(t *testing.T) {
	reg := newTestRegistry()
	// Speculative task created and stored (converted).
	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))
	routeFact(t, reg, makeSpecWorkflowTaskStored("tq", "wf1", "run1"))
	// Update admitted but stuck.
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &SpeculativeConversionRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for stuck update after conversion, got %d", len(violations))
	}
}

func TestSpeculativeConversionRule_NoViolation_UpdateAccepted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))
	routeFact(t, reg, makeSpecWorkflowTaskStored("tq", "wf1", "run1"))
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &SpeculativeConversionRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for accepted update, got %d", len(violations))
	}
}

func TestSpeculativeConversionRule_NoViolation_NoConversion(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &SpeculativeConversionRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations without speculative conversion, got %d", len(violations))
	}
}
