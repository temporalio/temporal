package model

import (
	"context"
	"fmt"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

const WorkflowRunType = fact.WorkflowRunType

var _ umpire.Entity = (*WorkflowRun)(nil)
var _ umpire.Lifecycled = (*WorkflowRun)(nil)

// WorkflowRun is one execution (WorkflowID + RunID) of a workflow — the run-precise entity, a child
// of Workflow (by id). Modeling the run distinctly is what lets multiple runs of one WorkflowID
// (continue-as-new / retry / reset) be tracked separately. Its lifecycle is created→completed: the
// completion is the transition the Monitor observes (via the completion span, which carries the
// RunID). A `started` state awaits observing WorkflowStart (see workflow.go / UMPIRE_ACTIONS.md);
// fail/cancel/terminate/timeout are further follow-ups.
type WorkflowRun struct {
	WorkflowID  string
	RunID       string
	FSM         *umpire.Lifecycle
	CompletedAt time.Time
	LastSeenAt  time.Time
}

func NewWorkflowRun() *WorkflowRun {
	r := &WorkflowRun{}
	r.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: WorkflowRunCreated,
		States: umpire.States{
			WorkflowRunCreated:   {},
			WorkflowRunCompleted: {},
		},
		Transitions: []umpire.Transition{
			{
				Event: WorkflowRunComplete,
				From:  []string{WorkflowRunCreated},
				To:    WorkflowRunCompleted,
			},
		},
	})
	return r
}

func (r *WorkflowRun) Type() umpire.EntityType { return WorkflowRunType }

func (r *WorkflowRun) Lifecycle() *umpire.Lifecycle { return r.FSM }

func (r *WorkflowRun) OnFact(ctx context.Context, _ *umpire.EntityPath, facts iter.Seq[umpire.Fact]) error {
	for f := range facts {
		if e, ok := f.(*fact.WorkflowRunCompleted); ok {
			if r.WorkflowID == "" {
				r.WorkflowID = e.WorkflowID
				r.RunID = e.RunID
			}
			if r.FSM.Fire(ctx, WorkflowRunComplete) {
				r.CompletedAt = time.Now()
			}
			r.LastSeenAt = time.Now()
		}
	}
	return nil
}

func (r *WorkflowRun) String() string {
	return fmt.Sprintf("WorkflowRun{workflowID=%s, runID=%s, state=%s}", r.WorkflowID, r.RunID, r.FSM.Current())
}

// Lifecycle states and events for WorkflowRun.
const (
	WorkflowRunCreated   WorkflowState = "created"
	WorkflowRunCompleted WorkflowState = "completed"

	WorkflowRunComplete WorkflowEvent = "complete"
)
