package rulebook

import (
	"testing"
)

func TestSpeculativeTaskCreationRule_DetectsDuplicateTasks(t *testing.T) {
	reg := newTestRegistry()
	// Normal task exists, then speculative scheduled for same workflow.
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))

	violations := checkSafetyRule(reg, &SpeculativeTaskCreationRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for spec task with existing normal task, got %d", len(violations))
	}
}

func TestSpeculativeTaskCreationRule_NoViolation_OnlyNormalTask(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))

	violations := checkSafetyRule(reg, &SpeculativeTaskCreationRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for only normal task, got %d", len(violations))
	}
}

func TestSpeculativeTaskCreationRule_NoViolation_OnlySpeculativeTask(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))

	violations := checkSafetyRule(reg, &SpeculativeTaskCreationRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for only speculative task, got %d", len(violations))
	}
}

func TestSpeculativeTaskCreationRule_NoViolation_NormalTaskPolled(t *testing.T) {
	reg := newTestRegistry()
	// Normal task already polled (not pending), then speculative created.
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
	routeFact(t, reg, makeWorkflowTaskPolled("tq", "wf1", "run1", true))
	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))

	violations := checkSafetyRule(reg, &SpeculativeTaskCreationRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations when normal task is already polled, got %d", len(violations))
	}
}
