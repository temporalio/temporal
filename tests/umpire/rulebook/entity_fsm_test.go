package rulebook

import (
	"context"
	"testing"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/entity"
)

func TestWorkflowUpdate_FSM_Transitions(t *testing.T) {
	wu := entity.NewWorkflowUpdate()
	if wu.FSM.Current() != "unspecified" {
		t.Fatalf("expected initial state 'unspecified', got %s", wu.FSM.Current())
	}
	if !wu.FSM.Can("admit") {
		t.Fatal("expected 'admit' to be possible from 'unspecified'")
	}

	ident := &umpire.Identity{EntityID: umpire.NewEntityID(entity.WorkflowUpdateType, "upd1")}
	wu.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowUpdateAdmitted("wf1", "upd1"))
	})
	if wu.FSM.Current() != "admitted" {
		t.Fatalf("expected 'admitted', got %s", wu.FSM.Current())
	}
	if wu.AdmittedAt.IsZero() {
		t.Fatal("AdmittedAt should be set")
	}
	if !wu.Admitted.IsTrue() {
		t.Fatal("Admitted marker should be set")
	}

	wu.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowUpdateAccepted("wf1", "upd1"))
	})
	if wu.FSM.Current() != "accepted" {
		t.Fatalf("expected 'accepted', got %s", wu.FSM.Current())
	}

	wu.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowUpdateCompleted("wf1", "upd1"))
	})
	if wu.FSM.Current() != "completed" {
		t.Fatalf("expected 'completed', got %s", wu.FSM.Current())
	}
	if wu.CompletedAt.IsZero() {
		t.Fatal("CompletedAt should be set")
	}
}

func TestWorkflowTask_FSM_AddThenPoll(t *testing.T) {
	wt := entity.NewWorkflowTask()
	if wt.FSM.Current() != "created" {
		t.Fatalf("expected 'created', got %s", wt.FSM.Current())
	}

	ident := &umpire.Identity{EntityID: umpire.NewEntityID(entity.WorkflowTaskType, "tq:wf1:run1")}
	wt.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowTaskAdded("tq", "wf1", "run1"))
	})
	if wt.FSM.Current() != "added" {
		t.Fatalf("expected 'added', got %s", wt.FSM.Current())
	}
	if wt.AddedAt.IsZero() {
		t.Fatal("AddedAt should be set")
	}
	if !wt.Added.IsTrue() {
		t.Fatal("Added marker should be set")
	}

	wt.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowTaskPolled("tq", "wf1", "run1", true))
	})
	if wt.FSM.Current() != "polled" {
		t.Fatalf("expected 'polled', got %s", wt.FSM.Current())
	}
}

func TestWorkflowTask_FSM_SpeculativeTask(t *testing.T) {
	wt := entity.NewWorkflowTask()
	ident := &umpire.Identity{EntityID: umpire.NewEntityID(entity.WorkflowTaskType, "tq:wf1:run1")}
	wt.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeSpeculativeScheduled("tq", "wf1", "run1"))
	})
	if wt.FSM.Current() != "added" {
		t.Fatalf("expected 'added' for speculative task, got %s", wt.FSM.Current())
	}
	if !wt.IsSpeculative {
		t.Fatal("IsSpeculative should be true")
	}
}

func TestWorkflowTask_FSM_PollWithoutReturn_NoTransition(t *testing.T) {
	wt := entity.NewWorkflowTask()
	ident := &umpire.Identity{EntityID: umpire.NewEntityID(entity.WorkflowTaskType, "tq:wf1:run1")}
	wt.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowTaskAdded("tq", "wf1", "run1"))
	})
	wt.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowTaskPolled("tq", "wf1", "run1", false))
	})
	if wt.FSM.Current() != "added" {
		t.Fatalf("expected 'added' (no transition on empty poll), got %s", wt.FSM.Current())
	}
}
