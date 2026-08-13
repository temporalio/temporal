package rule

// This file preserves the disabled Workflow Update support for future work.
// All archived source is commented out intentionally.

// tests/umpire1/rule/entity_lifecycle.go
// // EntityProgress is a generic liveness rule: an entity must not be left in a
// // state its Lifecycle marks as "must progress" (LifecycleSpec.MustProgress).
// // For WorkflowUpdate, whose must-progress states are {admitted, accepted}, it
// // replaces both WorkflowUpdateLossPrevention (stuck admitted) and
// // WorkflowUpdateCompletion (stuck accepted). Entities that declare no
// // must-progress states are unaffected, so it is safe across all entity types.
//
// tests/umpire1/rule/nexus_operation_closure.go
// // NexusOperationClosure checks that no Nexus operation transitions after its
// // caller workflow has reached a terminal state. It is the direct analog of
// // WorkflowUpdateClosure, and inherits the same fidelity caveats (workflow close is
// // observed for only one path; *At timestamps are observation-time) — triage
// // against the real Nexus suite before relying on it (see UMPIRE.md).
//
// tests/umpire1/rule/workflow_update_closure.go
// package rule
//
// import (
// 	"time"
//
// 	"go.temporal.io/server/common/testing/umpire"
// 	"go.temporal.io/server/tests/umpire1/model"
// )
//
// // WorkflowUpdateClosure checks that no update transitions occur after
// // the parent workflow has reached a terminal state.
// type WorkflowUpdateClosure struct{}
//
// func (m *WorkflowUpdateClosure) Name() string {
// 	return "WorkflowUpdateClosureRule"
// }
//
// func (m *WorkflowUpdateClosure) CheckSafety(c *umpire.SafetyContext) {
// 	// Build set of completed workflow IDs with their completion times.
// 	completedWorkflows := make(map[string]time.Time)
// 	for r := range c.Changed[model.Workflow]() {
// 		wf := r.Entity
// 		if wf.WorkflowID == "" {
// 			continue
// 		}
// 		if wf.FSM.Current() == "completed" && !wf.CompletedAt.IsZero() {
// 			completedWorkflows[wf.WorkflowID] = wf.CompletedAt
// 		}
// 	}
//
// 	for r := range c.Changed[model.WorkflowUpdate]() {
// 		wu := r.Entity
// 		if wu.WorkflowID == "" || wu.UpdateID == "" {
// 			continue
// 		}
// 		closedAt, closed := completedWorkflows[wu.WorkflowID]
// 		if !closed {
// 			continue
// 		}
// 		violated := false
// 		// Check if update accepted after workflow closed.
// 		if !wu.AcceptedAt().IsZero() && wu.AcceptedAt().After(closedAt) {
// 			violated = true
// 			c.Eval(r.Key+":accepted-after-close", false, umpire.Violation{
// 				Message: "workflow update accepted after workflow closed",
// 				Tags: map[string]string{
// 					"workflowID": wu.WorkflowID,
// 					"updateID":   wu.UpdateID,
// 					"closedAt":   closedAt.Format(time.RFC3339),
// 					"acceptedAt": wu.AcceptedAt().Format(time.RFC3339),
// 				},
// 			})
// 		}
// 		// Check if update completed after workflow closed.
// 		if !wu.CompletedAt().IsZero() && wu.CompletedAt().After(closedAt) {
// 			violated = true
// 			c.Eval(r.Key+":completed-after-close", false, umpire.Violation{
// 				Message: "workflow update completed after workflow closed",
// 				Tags: map[string]string{
// 					"workflowID":  wu.WorkflowID,
// 					"updateID":    wu.UpdateID,
// 					"closedAt":    closedAt.Format(time.RFC3339),
// 					"completedAt": wu.CompletedAt().Format(time.RFC3339),
// 				},
// 			})
// 		}
// 		if !violated {
// 			c.Pass(r.Key)
// 		}
// 	}
// }

// tests/umpire1/rule/workflow_update_closure_test.go
// package rule
//
// import (
// 	"testing"
// )
//
// func TestWorkflowUpdateClosureRule_DetectsAcceptedAfterClose(t *testing.T) {
// 	reg := newTestModelState()
// 	// Start and complete workflow.
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowCompleted("wf1"))
// 	// Accept an update after workflow closed.
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
//
// 	violations := checkSafetyRule(reg, &WorkflowUpdateClosure{})
// 	if len(violations) == 0 {
// 		t.Fatal("expected violation for update accepted after workflow closed")
// 	}
// }
//
// func TestWorkflowUpdateClosureRule_DetectsCompletedAfterClose(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowCompleted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))
//
// 	violations := checkSafetyRule(reg, &WorkflowUpdateClosure{})
// 	if len(violations) == 0 {
// 		t.Fatal("expected violation for update completed after workflow closed")
// 	}
// }
//
// func TestWorkflowUpdateClosureRule_NoViolation_WorkflowNotClosed(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
//
// 	violations := checkSafetyRule(reg, &WorkflowUpdateClosure{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations when workflow is not closed, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateClosureRule_NoViolation_UpdateBeforeClose(t *testing.T) {
// 	reg := newTestModelState()
// 	// Update completes before workflow closes.
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowCompleted("wf1"))
//
// 	violations := checkSafetyRule(reg, &WorkflowUpdateClosure{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for update completed before close, got %d", len(violations))
// 	}
// }

// tests/umpire1/rule/workflow_update_context_clear.go
// package rule
//
// import (
// 	"fmt"
//
// 	"go.temporal.io/server/common/testing/umpire"
// 	"go.temporal.io/server/tests/umpire1/model"
// )
//
// // WorkflowUpdateContextClear detects updates stranded in a
// // non-terminal state when no pending workflow task exists to drive them forward.
// // This covers both stale-token scenarios and context-clear scenarios
// // where the update registry is lost and no new task is scheduled.
// type WorkflowUpdateContextClear struct{}
//
// func (m *WorkflowUpdateContextClear) Name() string {
// 	return "WorkflowUpdateContextClearRule"
// }
//
// func (m *WorkflowUpdateContextClear) CheckLiveness(c *umpire.LivenessContext) {
// 	// Build set of workflows that have a pending (non-polled) task.
// 	workflowsWithPendingTask := make(map[string]bool)
// 	for r := range c.Changed[model.WorkflowTask]() {
// 		wt := r.Entity
// 		if wt.WorkflowID == "" {
// 			continue
// 		}
// 		state := wt.FSM.Current()
// 		if state == "added" || state == "stored" {
// 			workflowsWithPendingTask[wt.WorkflowID] = true
// 		}
// 	}
//
// 	// Build set of completed workflows.
// 	completedWorkflows := make(map[string]bool)
// 	for r := range c.Changed[model.Workflow]() {
// 		wf := r.Entity
// 		if wf.WorkflowID == "" {
// 			continue
// 		}
// 		if wf.FSM.Current() == "completed" {
// 			completedWorkflows[wf.WorkflowID] = true
// 		}
// 	}
//
// 	for r := range c.Changed[model.WorkflowUpdate]() {
// 		wu := r.Entity
// 		if wu.WorkflowID == "" || wu.UpdateID == "" {
// 			continue
// 		}
// 		// Only care about non-terminal updates.
// 		state := wu.FSM.Current()
// 		if state != "admitted" && state != "accepted" {
// 			c.Resolve(r.Key)
// 			continue
// 		}
// 		// Skip if workflow is completed (closure rule handles that).
// 		if completedWorkflows[wu.WorkflowID] {
// 			continue
// 		}
// 		// Skip if there's a pending task that could drive this update.
// 		if workflowsWithPendingTask[wu.WorkflowID] {
// 			c.Resolve(r.Key)
// 			continue
// 		}
// 		c.Pending(r.Key, umpire.Violation{
// 			Message: fmt.Sprintf("update stranded in %q with no pending workflow task — possible context clear or stale token", state),
// 			Tags: map[string]string{
// 				"workflowID":  wu.WorkflowID,
// 				"updateID":    wu.UpdateID,
// 				"updateState": state,
// 			},
// 		})
// 	}
// }

// tests/umpire1/rule/workflow_update_context_clear_test.go
// package rule
//
// import (
// 	"testing"
// )
//
// func TestWorkflowUpdateContextClearRule_DetectsStrandedUpdate(t *testing.T) {
// 	reg := newTestModelState()
// 	// Update admitted, but no pending task and workflow not completed.
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateContextClear{})
// 	if len(violations) != 1 {
// 		t.Fatalf("expected 1 violation for stranded update, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateContextClearRule_DetectsStrandedAcceptedUpdate(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	// Task was polled (no longer pending).
// 	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
// 	routeFact(t, reg, makeWorkflowTaskPolled("tq", "wf1", "run1", true))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateContextClear{})
// 	if len(violations) != 1 {
// 		t.Fatalf("expected 1 violation for stranded accepted update, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateContextClearRule_NoViolation_PendingTaskExists(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	// Pending task exists.
// 	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateContextClear{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations when pending task exists, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateContextClearRule_NoViolation_WorkflowCompleted(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowCompleted("wf1"))
//
// 	// Workflow completed — closure rule handles this, not context clear.
// 	violations := checkLivenessRule(reg, &WorkflowUpdateContextClear{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for completed workflow, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateContextClearRule_NoViolation_UpdateCompleted(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateContextClear{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for completed update, got %d", len(violations))
// 	}
// }

// tests/umpire1/rule/workflow_update_continue_as_new.go
// package rule
//
// import (
// 	"time"
//
// 	"go.temporal.io/server/common/testing/umpire"
// 	"go.temporal.io/server/tests/umpire1/model"
// )
//
// // WorkflowUpdateContinueAsNew detects updates that remain admitted
// // on a completed workflow. In a continue-as-new scenario, the SDK should
// // retry the update on the new run. If the update stays admitted on the old
// // (completed) run, the retry mechanism may have failed.
// type WorkflowUpdateContinueAsNew struct{}
//
// func (m *WorkflowUpdateContinueAsNew) Name() string {
// 	return "WorkflowUpdateContinueAsNewRule"
// }
//
// func (m *WorkflowUpdateContinueAsNew) CheckLiveness(c *umpire.LivenessContext) {
// 	// Build set of completed workflows.
// 	completedWorkflows := make(map[string]time.Time)
// 	for r := range c.Changed[model.Workflow]() {
// 		wf := r.Entity
// 		if wf.WorkflowID == "" {
// 			continue
// 		}
// 		if wf.FSM.Current() == "completed" && !wf.CompletedAt.IsZero() {
// 			completedWorkflows[wf.WorkflowID] = wf.CompletedAt
// 		}
// 	}
//
// 	for r := range c.Changed[model.WorkflowUpdate]() {
// 		wu := r.Entity
// 		if wu.WorkflowID == "" || wu.UpdateID == "" {
// 			continue
// 		}
// 		// Only care about updates still admitted on a completed workflow.
// 		if wu.FSM.Current() != "admitted" || wu.AdmittedAt().IsZero() {
// 			c.Resolve(r.Key)
// 			continue
// 		}
// 		if _, closed := completedWorkflows[wu.WorkflowID]; !closed {
// 			continue
// 		}
// 		c.Pending(r.Key, umpire.Violation{
// 			Message: "update admitted on completed workflow was not retried to new run — possible continue-as-new retry failure",
// 			Tags: map[string]string{
// 				"workflowID": wu.WorkflowID,
// 				"updateID":   wu.UpdateID,
// 			},
// 		})
// 	}
// }

// tests/umpire1/rule/workflow_update_continue_as_new_test.go
// package rule
//
// import (
// 	"testing"
// )
//
// func TestWorkflowUpdateContinueAsNewRule_DetectsStuckOnCompletedWorkflow(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowCompleted("wf1"))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateContinueAsNew{})
// 	if len(violations) != 1 {
// 		t.Fatalf("expected 1 violation for admitted update on completed workflow, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateContinueAsNewRule_NoViolation_UpdateAccepted(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowCompleted("wf1"))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateContinueAsNew{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for accepted update, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateContinueAsNewRule_NoViolation_WorkflowNotCompleted(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateContinueAsNew{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations when workflow is not completed, got %d", len(violations))
// 	}
// }

// tests/umpire1/rule/workflow_update_dedup.go
// package rule
//
// import (
// 	"fmt"
//
// 	"go.temporal.io/server/common/testing/umpire"
// 	"go.temporal.io/server/tests/umpire1/model"
// )
//
// // WorkflowUpdateDeduplication detects duplicate update requests not resolved.
// type WorkflowUpdateDeduplication struct{}
//
// func (m *WorkflowUpdateDeduplication) Name() string {
// 	return "WorkflowUpdateDeduplicationRule"
// }
//
// func (m *WorkflowUpdateDeduplication) CheckLiveness(c *umpire.LivenessContext) {
// 	for r := range c.Changed[model.WorkflowUpdate]() {
// 		wu := r.Entity
// 		if wu.UpdateID == "" || wu.RequestCount <= 1 {
// 			continue
// 		}
// 		// Only flag updates that actually entered the lifecycle.
// 		// "unspecified" means only requested but never admitted — normal for retries.
// 		state := wu.FSM.Current()
// 		if state == "unspecified" || state == "completed" || state == "rejected" || state == "aborted" {
// 			c.Resolve(r.Key)
// 			continue
// 		}
// 		c.Pending(r.Key, umpire.Violation{
// 			Message: fmt.Sprintf("deduplicated update (requested %d times) stuck in %q — duplicate request not resolved, dedup may be blocking progress", wu.RequestCount, state),
// 			Tags: map[string]string{
// 				"workflowID":   wu.WorkflowID,
// 				"updateID":     wu.UpdateID,
// 				"requestCount": fmt.Sprintf("%d", wu.RequestCount),
// 				"currentState": state,
// 			},
// 		})
// 	}
// }

// tests/umpire1/rule/workflow_update_dedup_test.go
// package rule
//
// import (
// 	"testing"
// )
//
// func TestWorkflowUpdateDeduplicationRule_DetectsUnresolvedDuplicates(t *testing.T) {
// 	reg := newTestModelState()
// 	// Send same update twice (RequestCount becomes 2), then admit but don't complete.
// 	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateDeduplication{})
// 	if len(violations) != 1 {
// 		t.Fatalf("expected 1 violation for unresolved duplicate, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateDeduplicationRule_NoViolation_SingleRequest(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateDeduplication{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for single request, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateDeduplicationRule_NoViolation_Completed(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateDeduplication{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for completed duplicate, got %d", len(violations))
// 	}
// }

// tests/umpire1/rule/workflow_update_history_ordering.go
// package rule
//
// import (
// 	"time"
//
// 	"go.temporal.io/server/common/testing/umpire"
// 	"go.temporal.io/server/tests/umpire1/model"
// )
//
// // WorkflowUpdateHistoryOrdering verifies that when a workflow
// // completes on the same task that carries an update, the update reaches a
// // terminal state (completed or rejected) before or at the time of workflow
// // completion. An update left in accepted state after workflow closure indicates
// // inconsistent history ordering.
// type WorkflowUpdateHistoryOrdering struct{}
//
// func (m *WorkflowUpdateHistoryOrdering) Name() string {
// 	return "WorkflowUpdateHistoryOrderingRule"
// }
//
// func (m *WorkflowUpdateHistoryOrdering) CheckSafety(c *umpire.SafetyContext) {
// 	// Build set of completed workflows.
// 	completedWorkflows := make(map[string]time.Time)
// 	for r := range c.Changed[model.Workflow]() {
// 		wf := r.Entity
// 		if wf.WorkflowID == "" {
// 			continue
// 		}
// 		if wf.FSM.Current() == "completed" && !wf.CompletedAt.IsZero() {
// 			completedWorkflows[wf.WorkflowID] = wf.CompletedAt
// 		}
// 	}
//
// 	for r := range c.Changed[model.WorkflowUpdate]() {
// 		wu := r.Entity
// 		if wu.WorkflowID == "" || wu.UpdateID == "" {
// 			continue
// 		}
// 		closedAt, closed := completedWorkflows[wu.WorkflowID]
// 		if !closed {
// 			continue
// 		}
// 		// An update in "accepted" state after workflow completion means the
// 		// history did not include the required Completed/Rejected event before
// 		// the terminal workflow event.
// 		c.Eval(r.Key, wu.FSM.Current() != "accepted", umpire.Violation{
// 			Message: "update stuck in accepted state after workflow completed — history should include update completion before workflow terminal event",
// 			Tags: map[string]string{
// 				"workflowID":    wu.WorkflowID,
// 				"updateID":      wu.UpdateID,
// 				"acceptedAt":    wu.AcceptedAt().Format(time.RFC3339),
// 				"wfCompletedAt": closedAt.Format(time.RFC3339),
// 			},
// 		})
// 	}
// }

// tests/umpire1/rule/workflow_update_history_ordering_test.go
// package rule
//
// import (
// 	"testing"
// )
//
// func TestWorkflowUpdateHistoryOrderingRule_DetectsAcceptedAfterClose(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowCompleted("wf1"))
//
// 	violations := checkSafetyRule(reg, &WorkflowUpdateHistoryOrdering{})
// 	if len(violations) != 1 {
// 		t.Fatalf("expected 1 violation for accepted update after close, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateHistoryOrderingRule_NoViolation_UpdateCompleted(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowCompleted("wf1"))
//
// 	violations := checkSafetyRule(reg, &WorkflowUpdateHistoryOrdering{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for completed update before close, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateHistoryOrderingRule_NoViolation_WorkflowNotClosed(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowStarted("wf1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
//
// 	violations := checkSafetyRule(reg, &WorkflowUpdateHistoryOrdering{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations when workflow is not closed, got %d", len(violations))
// 	}
// }

// tests/umpire1/rule/workflow_update_worker_skipped.go
// package rule
//
// import (
// 	"time"
//
// 	"go.temporal.io/server/common/testing/umpire"
// 	"go.temporal.io/server/tests/umpire1/model"
// )
//
// // WorkflowUpdateWorkerSkipped detects updates that remain admitted
// // while workflow tasks for the same workflow have been polled by workers.
// // This indicates the worker is ignoring the update, and the server should
// // eventually reject it.
// type WorkflowUpdateWorkerSkipped struct{}
//
// func (m *WorkflowUpdateWorkerSkipped) Name() string {
// 	return "WorkflowUpdateWorkerSkippedRule"
// }
//
// func (m *WorkflowUpdateWorkerSkipped) CheckLiveness(c *umpire.LivenessContext) {
// 	// Build map of workflow→latest polled time.
// 	latestPoll := make(map[string]time.Time)
// 	for r := range c.Changed[model.WorkflowTask]() {
// 		wt := r.Entity
// 		if wt.WorkflowID == "" {
// 			continue
// 		}
// 		if wt.FSM.Current() == "polled" && !wt.PolledAt.IsZero() {
// 			if existing, seen := latestPoll[wt.WorkflowID]; !seen || wt.PolledAt.After(existing) {
// 				latestPoll[wt.WorkflowID] = wt.PolledAt
// 			}
// 		}
// 	}
//
// 	for r := range c.Changed[model.WorkflowUpdate]() {
// 		wu := r.Entity
// 		if wu.WorkflowID == "" || wu.UpdateID == "" {
// 			continue
// 		}
// 		// Only care about updates stuck in admitted.
// 		if wu.FSM.Current() != "admitted" || wu.AdmittedAt().IsZero() {
// 			c.Resolve(r.Key)
// 			continue
// 		}
// 		polledAt, hasPolls := latestPoll[wu.WorkflowID]
// 		if !hasPolls {
// 			continue
// 		}
// 		// Worker polled a task after the update was admitted but didn't process it.
// 		if polledAt.Before(wu.AdmittedAt()) {
// 			continue
// 		}
//
// 		c.Pending(r.Key, umpire.Violation{
// 			Message: "update remains admitted after worker polled workflow task — worker may be ignoring the update",
// 			Tags: map[string]string{
// 				"workflowID": wu.WorkflowID,
// 				"updateID":   wu.UpdateID,
// 			},
// 		})
// 	}
// }

// tests/umpire1/rule/workflow_update_worker_skipped_test.go
// package rule
//
// import (
// 	"testing"
// )
//
// func TestWorkflowUpdateWorkerSkippedRule_DetectsSkippedUpdate(t *testing.T) {
// 	reg := newTestModelState()
// 	// Admit update, then poll a task (worker ignores the update).
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
// 	routeFact(t, reg, makeWorkflowTaskPolled("tq", "wf1", "run1", true))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateWorkerSkipped{})
// 	if len(violations) != 1 {
// 		t.Fatalf("expected 1 violation for skipped update, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateWorkerSkippedRule_NoViolation_UpdateAccepted(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowTaskAdded("tq", "wf1", "run1"))
// 	routeFact(t, reg, makeWorkflowTaskPolled("tq", "wf1", "run1", true))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateWorkerSkipped{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for accepted update, got %d", len(violations))
// 	}
// }
//
// func TestWorkflowUpdateWorkerSkippedRule_NoViolation_NoPolls(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &WorkflowUpdateWorkerSkipped{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations without polled tasks, got %d", len(violations))
// 	}
// }

// tests/umpire1/rule/speculative_conversion.go
// package rule
//
// import (
// 	"go.temporal.io/server/common/testing/umpire"
// 	"go.temporal.io/server/tests/umpire1/model"
// )
//
// // SpeculativeConversion detects updates that remain stuck in admitted
// // state after a speculative workflow task was converted to normal (stored).
// // The conversion should not lose the update — it should still progress to
// // accepted/completed/rejected.
// type SpeculativeConversion struct{}
//
// func (m *SpeculativeConversion) Name() string {
// 	return "SpeculativeConversionRule"
// }
//
// func (m *SpeculativeConversion) CheckLiveness(c *umpire.LivenessContext) {
// 	// Find workflows where a speculative task was converted (stored).
// 	converted := make(map[string]bool)
// 	for r := range c.Changed[model.WorkflowTask]() {
// 		wt := r.Entity
// 		if wt.WorkflowID == "" {
// 			continue
// 		}
// 		// A speculative task that reached "stored" state was converted to normal.
// 		if wt.IsSpeculative && wt.FSM.Current() == "stored" {
// 			converted[wt.WorkflowID] = true
// 		}
// 	}
// 	if len(converted) == 0 {
// 		return
// 	}
//
// 	for r := range c.Changed[model.WorkflowUpdate]() {
// 		wu := r.Entity
// 		if wu.WorkflowID == "" || wu.UpdateID == "" {
// 			continue
// 		}
// 		// Only care about updates stuck in admitted.
// 		if wu.FSM.Current() != "admitted" || wu.AdmittedAt().IsZero() {
// 			c.Resolve(r.Key)
// 			continue
// 		}
// 		if !converted[wu.WorkflowID] {
// 			continue
// 		}
//
// 		c.Pending(r.Key, umpire.Violation{
// 			Message: "update stuck in admitted after speculative task converted to normal — conversion may have lost the update",
// 			Tags: map[string]string{
// 				"workflowID": wu.WorkflowID,
// 				"updateID":   wu.UpdateID,
// 			},
// 		})
// 	}
// }

// tests/umpire1/rule/speculative_conversion_test.go
// package rule
//
// import (
// 	"testing"
// )
//
// func TestSpeculativeConversionRule_DetectsStuckAfterConversion(t *testing.T) {
// 	reg := newTestModelState()
// 	// Speculative task created and stored (converted).
// 	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))
// 	routeFact(t, reg, makeSpecWorkflowTaskStored("tq", "wf1", "run1"))
// 	// Update admitted but stuck.
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &SpeculativeConversion{})
// 	if len(violations) != 1 {
// 		t.Fatalf("expected 1 violation for stuck update after conversion, got %d", len(violations))
// 	}
// }
//
// func TestSpeculativeConversionRule_NoViolation_UpdateAccepted(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))
// 	routeFact(t, reg, makeSpecWorkflowTaskStored("tq", "wf1", "run1"))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &SpeculativeConversion{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for accepted update, got %d", len(violations))
// 	}
// }
//
// func TestSpeculativeConversionRule_NoViolation_NoConversion(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &SpeculativeConversion{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations without speculative conversion, got %d", len(violations))
// 	}
// }

// tests/umpire1/rule/speculative_task_rollback.go
// package rule
//
// import (
// 	"go.temporal.io/server/common/testing/umpire"
// 	"go.temporal.io/server/tests/umpire1/model"
// )
//
// // SpeculativeTaskRollback detects updates accepted on speculative tasks but never completed.
// type SpeculativeTaskRollback struct{}
//
// func (m *SpeculativeTaskRollback) Name() string {
// 	return "SpeculativeTaskRollbackRule"
// }
//
// func (m *SpeculativeTaskRollback) CheckLiveness(c *umpire.LivenessContext) {
// 	speculativePolled := make(map[string]bool)
// 	for r := range c.Changed[model.WorkflowTask]() {
// 		wt := r.Entity
// 		if !wt.IsSpeculative || wt.WorkflowID == "" {
// 			continue
// 		}
// 		if wt.FSM.Current() == "polled" {
// 			speculativePolled[wt.WorkflowID] = true
// 		}
// 	}
// 	if len(speculativePolled) == 0 {
// 		return
// 	}
//
// 	for r := range c.Changed[model.WorkflowUpdate]() {
// 		wu := r.Entity
// 		if wu.UpdateID == "" || wu.WorkflowID == "" {
// 			continue
// 		}
// 		if wu.FSM.Current() != "accepted" || wu.AcceptedAt().IsZero() {
// 			c.Resolve(r.Key)
// 			continue
// 		}
// 		if !speculativePolled[wu.WorkflowID] {
// 			continue
// 		}
// 		c.Pending(r.Key, umpire.Violation{
// 			Message: "update accepted on speculative workflow task but never completed — possible silent rollback",
// 			Tags: map[string]string{
// 				"workflowID": wu.WorkflowID,
// 				"updateID":   wu.UpdateID,
// 			},
// 		})
// 	}
// }

// tests/umpire1/rule/speculative_task_rollback_test.go
// package rule
//
// import (
// 	"testing"
// )
//
// func TestSpeculativeTaskRollbackRule_DetectsRollback(t *testing.T) {
// 	reg := newTestModelState()
//
// 	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))
// 	routeFact(t, reg, makeSpecWorkflowTaskPolled("tq", "wf1", "run1", true))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &SpeculativeTaskRollback{})
// 	if len(violations) != 1 {
// 		t.Fatalf("expected 1 rollback violation, got %d", len(violations))
// 	}
// 	if violations[0].Rule != "SpeculativeTaskRollbackRule" {
// 		t.Fatalf("wrong model: %s", violations[0].Rule)
// 	}
// 	if violations[0].Tags["updateID"] != "upd1" {
// 		t.Fatalf("wrong updateID tag: %s", violations[0].Tags["updateID"])
// 	}
// }
//
// func TestSpeculativeTaskRollbackRule_NoViolation_UpdateCompleted(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeSpeculativeScheduled("tq", "wf1", "run1"))
// 	routeFact(t, reg, makeSpecWorkflowTaskPolled("tq", "wf1", "run1", true))
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &SpeculativeTaskRollback{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for completed update, got %d", len(violations))
// 	}
// }
//
// func TestSpeculativeTaskRollbackRule_NoViolation_NoSpeculativeTask(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &SpeculativeTaskRollback{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations without speculative task, got %d", len(violations))
// 	}
// }

// tests/umpire1/rule/entity_lifecycle_test.go
// package rule
//
// import (
// 	"testing"
// )
//
// func TestEntityTransitionLegality_NoViolationOnLegalOrder(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1"))
//
// 	violations := checkSafetyRule(reg, &EntityTransitionLegality{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for a legal transition sequence, got %d: %+v", len(violations), violations)
// 	}
// }
//
// // A forward jump — observing "accept" before "admit" from the initial state — is
// // NOT flagged: under observe-only we cannot distinguish a missed "admit"
// // observation from an illegal skip, so a jump to a reachable-ahead state is legal.
// // (The genuinely-illegal path — a transition into an unreachable sibling branch —
// // is covered at the framework level in lifecycle_test.go; none of the real entity
// // lifecycles, being converging DAGs, can produce one.)
// func TestEntityTransitionLegality_NoViolationOnForwardJump(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1")) // accept before admit
//
// 	violations := checkSafetyRule(reg, &EntityTransitionLegality{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for a forward jump, got %d: %+v", len(violations), violations)
// 	}
// }
//
// // A duplicate span (the false positive that previously kept this rule
// // unregistered) must not be flagged: re-observing "accepted" while already
// // accepted is a benign no-op, not an illegal transition.
// func TestEntityTransitionLegality_NoViolationOnDuplicateSpan(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1")) // duplicate accepted span
//
// 	violations := checkSafetyRule(reg, &EntityTransitionLegality{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for a duplicate span, got %d: %+v", len(violations), violations)
// 	}
// }
//
// // A stale span arriving after the update reached a terminal state must not be
// // flagged: a terminal entity absorbs late/out-of-order facts as no-ops.
// func TestEntityTransitionLegality_NoViolationOnStaleAfterTerminal(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1")) // admitted -> completed (terminal)
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))  // stale accepted span
//
// 	violations := checkSafetyRule(reg, &EntityTransitionLegality{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for a stale post-terminal span, got %d: %+v", len(violations), violations)
// 	}
// }
//
// // Parity with the former WorkflowUpdateLossPrevention: stuck in "admitted".
// func TestEntityProgress_DetectsStuckAdmitted(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &EntityProgress{})
// 	if len(violations) != 1 {
// 		t.Fatalf("expected 1 violation for an update stuck in admitted, got %d", len(violations))
// 	}
// }
//
// // Parity with the former WorkflowUpdateCompletion: stuck in "accepted".
// func TestEntityProgress_DetectsStuckAccepted(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &EntityProgress{})
// 	if len(violations) != 1 {
// 		t.Fatalf("expected 1 violation for an update stuck in accepted, got %d", len(violations))
// 	}
// }
//
// func TestEntityProgress_NoViolationWhenTerminal(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	routeFact(t, reg, makeWorkflowUpdateCompleted("wf1", "upd1")) // reaches terminal "completed"
//
// 	violations := checkLivenessRule(reg, &EntityProgress{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations when the entity reached a terminal state, got %d: %+v", len(violations), violations)
// 	}
// }
//
// // A non-must-progress resting state ("unspecified": requested but not admitted)
// // must not fire — proving EntityProgress is not a blunt reach-terminal rule.
// func TestEntityProgress_NoViolationForUnspecified(t *testing.T) {
// 	reg := newTestModelState()
// 	routeFact(t, reg, makeWorkflowUpdateRequested("wf1", "upd1"))
//
// 	violations := checkLivenessRule(reg, &EntityProgress{})
// 	if len(violations) != 0 {
// 		t.Fatalf("expected no violations for an update only requested (unspecified), got %d: %+v", len(violations), violations)
// 	}
// }
//
// tests/umpire1/rule/entity_fsm_test.go
// func TestWorkflowUpdate_FSM_Transitions(t *testing.T) {
// 	wu := model.NewWorkflowUpdate()
// 	if wu.FSM.Current() != "unspecified" {
// 		t.Fatalf("expected initial state 'unspecified', got %s", wu.FSM.Current())
// 	}
// 	if !wu.FSM.Can("admit") {
// 		t.Fatal("expected 'admit' to be possible from 'unspecified'")
// 	}
//
// 	ident := &umpire.EntityPath{EntityID: umpire.NewEntityID(model.WorkflowUpdateType, "upd1")}
// 	wu.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
// 		yield(makeWorkflowUpdateAdmitted("wf1", "upd1"))
// 	})
// 	if wu.FSM.Current() != "admitted" {
// 		t.Fatalf("expected 'admitted', got %s", wu.FSM.Current())
// 	}
// 	if wu.AdmittedAt().IsZero() {
// 		t.Fatal("AdmittedAt should be set")
// 	}
//
// 	wu.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
// 		yield(makeWorkflowUpdateAccepted("wf1", "upd1"))
// 	})
// 	if wu.FSM.Current() != "accepted" {
// 		t.Fatalf("expected 'accepted', got %s", wu.FSM.Current())
// 	}
//
// 	wu.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
// 		yield(makeWorkflowUpdateCompleted("wf1", "upd1"))
// 	})
// 	if wu.FSM.Current() != "completed" {
// 		t.Fatalf("expected 'completed', got %s", wu.FSM.Current())
// 	}
// 	if wu.CompletedAt().IsZero() {
// 		t.Fatal("CompletedAt should be set")
// 	}
// }
//
// tests/umpire1/rule/helpers_test.go
// updatev1 "go.temporal.io/api/update/v1"
// func makeWorkflowUpdateAdmitted(workflowID, updateID string) *fact.WorkflowUpdateAdmitted {
// 	wfID := umpire.NewEntityID(model.WorkflowType, workflowID)
// 	updID := umpire.NewEntityID(model.WorkflowUpdateType, updateID)
// 	return &fact.WorkflowUpdateAdmitted{
// 		UpdateID:   updateID,
// 		WorkflowID: workflowID,
// 		EntityPath: &umpire.EntityPath{EntityID: updID, Ancestors: []umpire.EntityID{wfID}},
// 	}
// }
//
// func makeWorkflowUpdateAccepted(workflowID, updateID string) *fact.WorkflowUpdateAccepted {
// 	wfID := umpire.NewEntityID(model.WorkflowType, workflowID)
// 	updID := umpire.NewEntityID(model.WorkflowUpdateType, updateID)
// 	return &fact.WorkflowUpdateAccepted{
// 		UpdateID:   updateID,
// 		WorkflowID: workflowID,
// 		EntityPath: &umpire.EntityPath{EntityID: updID, Ancestors: []umpire.EntityID{wfID}},
// 	}
// }
//
// func makeWorkflowUpdateCompleted(workflowID, updateID string) *fact.WorkflowUpdateCompleted {
// 	wfID := umpire.NewEntityID(model.WorkflowType, workflowID)
// 	updID := umpire.NewEntityID(model.WorkflowUpdateType, updateID)
// 	return &fact.WorkflowUpdateCompleted{
// 		UpdateID:   updateID,
// 		WorkflowID: workflowID,
// 		Success:    true,
// 		EntityPath: &umpire.EntityPath{EntityID: updID, Ancestors: []umpire.EntityID{wfID}},
// 	}
// }
//
// func makeWorkflowUpdateRejected(workflowID, updateID string) *fact.WorkflowUpdateRejected {
// 	wfID := umpire.NewEntityID(model.WorkflowType, workflowID)
// 	updID := umpire.NewEntityID(model.WorkflowUpdateType, updateID)
// 	return &fact.WorkflowUpdateRejected{
// 		UpdateID:   updateID,
// 		WorkflowID: workflowID,
// 		EntityPath: &umpire.EntityPath{EntityID: updID, Ancestors: []umpire.EntityID{wfID}},
// 	}
// }
//
// func makeWorkflowUpdateRequested(workflowID, updateID string) *fact.WorkflowUpdateRequested {
// 	wfID := umpire.NewEntityID(model.WorkflowType, workflowID)
// 	updID := umpire.NewEntityID(model.WorkflowUpdateType, updateID)
// 	return &fact.WorkflowUpdateRequested{
// 		Request: &historyservice.UpdateWorkflowExecutionRequest{
// 			Request: &workflowservice.UpdateWorkflowExecutionRequest{
// 				WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
// 				Request: &updatev1.Request{
// 					Meta:  &updatev1.Meta{UpdateId: updateID},
// 					Input: &updatev1.Input{Name: "handler"},
// 				},
// 			},
// 		},
// 		EntityPath: &umpire.EntityPath{EntityID: updID, Ancestors: []umpire.EntityID{wfID}},
// 	}
// }
