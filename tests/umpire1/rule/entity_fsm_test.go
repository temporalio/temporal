package rule

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire1/model"
)

func TestWorkflowTask_FSM_AddThenPoll(t *testing.T) {
	wt := model.NewWorkflowTask()
	if wt.FSM.Current() != "created" {
		t.Fatalf("expected 'created', got %s", wt.FSM.Current())
	}

	ident := &umpire.EntityPath{EntityID: umpire.NewEntityID(model.WorkflowTaskType, "tq:wf1:run1")}
	require.NoError(t, wt.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowTaskAdded("tq", "wf1", "run1"))
	}))
	if wt.FSM.Current() != "added" {
		t.Fatalf("expected 'added', got %s", wt.FSM.Current())
	}
	if wt.AddedAt.IsZero() {
		t.Fatal("AddedAt should be set")
	}

	require.NoError(t, wt.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowTaskPolled("tq", "wf1", "run1", true))
	}))
	if wt.FSM.Current() != "polled" {
		t.Fatalf("expected 'polled', got %s", wt.FSM.Current())
	}
}

func TestWorkflowTask_FSM_SpeculativeTask(t *testing.T) {
	wt := model.NewWorkflowTask()
	ident := &umpire.EntityPath{EntityID: umpire.NewEntityID(model.WorkflowTaskType, "tq:wf1:run1")}
	require.NoError(t, wt.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeSpeculativeScheduled("tq", "wf1", "run1"))
	}))
	if wt.FSM.Current() != "added" {
		t.Fatalf("expected 'added' for speculative task, got %s", wt.FSM.Current())
	}
	if !wt.IsSpeculative {
		t.Fatal("IsSpeculative should be true")
	}
}

func TestWorkflowTask_FSM_PollWithoutReturn_NoTransition(t *testing.T) {
	wt := model.NewWorkflowTask()
	ident := &umpire.EntityPath{EntityID: umpire.NewEntityID(model.WorkflowTaskType, "tq:wf1:run1")}
	require.NoError(t, wt.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowTaskAdded("tq", "wf1", "run1"))
	}))
	require.NoError(t, wt.OnFact(context.Background(), ident, func(yield func(umpire.Fact) bool) {
		yield(makeWorkflowTaskPolled("tq", "wf1", "run1", false))
	}))
	if wt.FSM.Current() != "added" {
		t.Fatalf("expected 'added' (no transition on empty poll), got %s", wt.FSM.Current())
	}
}
