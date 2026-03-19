package rulebook

import (
	"context"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	matchingservice "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
	"go.temporal.io/server/tests/umpire/fact"
)

// TestWorkflowTaskStarvationRule_NonSpeculative_ViaImporter verifies that
// AddWorkflowTaskRequest is now imported as AddWorkflowTask (not StoreWorkflowTask),
// so the WorkflowTask entity enters the "added" FSM state and the starvation model fires.
func TestWorkflowTaskStarvationRule_NonSpeculative_ViaImporter(t *testing.T) {
	reg := newTestRegistry()
	imp := entity.NewFactDecoder()

	req := &matchingservice.AddWorkflowTaskRequest{
		TaskQueue: &taskqueuepb.TaskQueue{Name: "tq"},
		Execution: &commonpb.WorkflowExecution{WorkflowId: "wf1", RunId: "run1"},
	}
	mv := imp.ImportRequest(req)
	if mv == nil {
		t.Fatal("ImportRequest returned nil for AddWorkflowTaskRequest")
	}
	if _, ok := mv.(*fact.WorkflowTaskAdded); !ok {
		t.Fatalf("expected *fact.WorkflowTaskAdded, got %T", mv)
	}

	routeFact(t, reg, mv)

	violations := checkLivenessRule(reg, &WorkflowTaskStarvationRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation: non-speculative task stuck in 'added', got %d", len(violations))
	}
	if violations[0].Rule != "WorkflowTaskStarvationRule" {
		t.Fatalf("wrong model: %s", violations[0].Rule)
	}
}

func TestWorkflowTaskStarvationRule_DetectsStuckTask(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))

	violations := checkLivenessRule(reg, &WorkflowTaskStarvationRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for stuck task, got %d", len(violations))
	}
	if violations[0].Rule != "WorkflowTaskStarvationRule" {
		t.Fatalf("wrong model: %s", violations[0].Rule)
	}
	if violations[0].Tags["workflowID"] != "wf1" {
		t.Fatalf("wrong workflowID tag: %s", violations[0].Tags["workflowID"])
	}
}

func TestWorkflowTaskStarvationRule_DetectsStoredTask(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
	routeFact(t, reg, makeWorkflowTaskStored("tq", "wf1", "run1"))

	violations := checkLivenessRule(reg, &WorkflowTaskStarvationRule{})
	if len(violations) != 1 {
		t.Fatalf("expected 1 violation for stored task, got %d", len(violations))
	}
	if violations[0].Tags["state"] != "stored" {
		t.Fatalf("expected state=stored tag, got %q", violations[0].Tags["state"])
	}
	if violations[0].Tags["workflowID"] != "wf1" {
		t.Fatalf("wrong workflowID tag: %s", violations[0].Tags["workflowID"])
	}
}

func TestWorkflowTaskStarvationRule_NoViolation_StoredThenPolled(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
	routeFact(t, reg, makeWorkflowTaskStored("tq", "wf1", "run1"))
	routeFact(t, reg, makeWorkflowTaskPolled("tq", "wf1", "run1", true))

	violations := checkLivenessRule(reg, &WorkflowTaskStarvationRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for polled task, got %d", len(violations))
	}
}

func TestWorkflowTaskStarvationRule_NoViolation_TaskPolled(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
	routeFact(t, reg, makeWorkflowTaskPolled("tq", "wf1", "run1", true))

	violations := checkLivenessRule(reg, &WorkflowTaskStarvationRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for polled task, got %d", len(violations))
	}
}

func TestWorkflowTaskStarvationRule_NoViolation_SpeculativeTask(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))

	violations := checkLivenessRule(reg, &WorkflowTaskStarvationRule{})
	if len(violations) != 0 {
		t.Fatalf("expected no violations for speculative task, got %d", len(violations))
	}
}

func TestWorkflowTaskStarvationRule_ResolvedByPoll(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))

	rb := umpire.NewRulebook()
	rb.RegisterLiveness(func() umpire.LivenessRule { return &WorkflowTaskStarvationRule{} })
	if err := rb.InitRules(reg, log.NewNoopLogger(), umpire.RuleConfig{}); err != nil {
		t.Fatalf("InitRules failed: %v", err)
	}

	// First non-final check: rule runs, records pending.
	v1 := rb.Check(context.Background(), false)
	if len(v1) != 0 {
		t.Fatalf("non-final check should not return violations, got %d", len(v1))
	}

	// Resolve: task gets polled.
	routeFact(t, reg, makeWorkflowTaskPolled("tq", "wf1", "run1", true))

	// Second check: rule sees dirty entity, calls Resolve.
	v2 := rb.Check(context.Background(), true)
	if len(v2) != 0 {
		t.Fatalf("expected 0 violations after poll resolved starvation, got %d", len(v2))
	}
}

func TestWorkflowTaskStarvationRule_UnresolvedAtTeardown(t *testing.T) {
	reg := newTestRegistry()
	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))

	rb := umpire.NewRulebook()
	rb.RegisterLiveness(func() umpire.LivenessRule { return &WorkflowTaskStarvationRule{} })
	if err := rb.InitRules(reg, log.NewNoopLogger(), umpire.RuleConfig{}); err != nil {
		t.Fatalf("InitRules failed: %v", err)
	}

	// Non-final check: records pending.
	rb.Check(context.Background(), false)

	// Final check without resolution: pending becomes violation.
	v := rb.Check(context.Background(), true)
	if len(v) != 1 {
		t.Fatalf("expected 1 violation for unresolved starvation at teardown, got %d", len(v))
	}
}
