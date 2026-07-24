package rule

import (
	"testing"
)

func TestEntityTransitionLegality_NoViolationOnLegalOrder(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))

	violations := checkSafetyRule(reg, &EntityTransitionLegality{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for a legal transition sequence, got %d: %+v", len(violations), violations)
	}
}

func TestEntityTransitionLegality_DetectsIllegalTransition(t *testing.T) {
	reg := newTestRegistry()
	// "accept" before "admit" is illegal from the initial state; the Lifecycle
	// records it instead of silently dropping it.
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	violations := checkSafetyRule(reg, &EntityTransitionLegality{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for an illegal transition, got %d", len(violations))
	}
}

// A duplicate span (the false positive that previously kept this rule
// unregistered) must not be flagged: re-observing "accepted" while already
// accepted is a benign no-op, not an illegal transition.
func TestEntityTransitionLegality_NoViolationOnDuplicateSpan(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1")) // duplicate accepted span

	violations := checkSafetyRule(reg, &EntityTransitionLegality{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for a duplicate span, got %d: %+v", len(violations), violations)
	}
}

// A stale span arriving after the update reached a terminal state must not be
// flagged: a terminal entity absorbs late/out-of-order facts as no-ops.
func TestEntityTransitionLegality_NoViolationOnStaleAfterTerminal(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1")) // admitted -> completed (terminal)
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))  // stale accepted span

	violations := checkSafetyRule(reg, &EntityTransitionLegality{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for a stale post-terminal span, got %d: %+v", len(violations), violations)
	}
}

// Parity with the former WorkflowUpdateStageMonotone: an out-of-order forward span
// (accept before admit, from the initial state) is a genuine illegal transition.
func TestEntityTransitionLegality_DetectsRegressionFromInitial(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1")) // accept from "unspecified"

	violations := checkSafetyRule(reg, &EntityTransitionLegality{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for accept-before-admit, got %d", len(violations))
	}
}

// Parity with the former WorkflowUpdateLossPrevention: stuck in "admitted".
func TestEntityProgress_DetectsStuckAdmitted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &EntityProgress{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for an update stuck in admitted, got %d", len(violations))
	}
}

// Parity with the former WorkflowUpdateCompletion: stuck in "accepted".
func TestEntityProgress_DetectsStuckAccepted(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))

	violations := checkLivenessRule(reg, &EntityProgress{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for an update stuck in accepted, got %d", len(violations))
	}
}

func TestEntityProgress_NoViolationWhenTerminal(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1")) // reaches terminal "completed"

	violations := checkLivenessRule(reg, &EntityProgress{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations when the entity reached a terminal state, got %d: %+v", len(violations), violations)
	}
}

// A non-must-progress resting state ("unspecified": requested but not admitted)
// must not fire — proving EntityProgress is not a blunt reach-terminal rule.
func TestEntityProgress_NoViolationForUnspecified(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))

	violations := checkLivenessRule(reg, &EntityProgress{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for an update only requested (unspecified), got %d: %+v", len(violations), violations)
	}
}
