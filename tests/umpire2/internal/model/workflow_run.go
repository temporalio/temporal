package model

import (
	"context"
	"fmt"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/fact"
)

const WorkflowRunType = fact.WorkflowRunType

var _ umpire.Entity = (*WorkflowRun)(nil)
var _ umpire.Lifecycled = (*WorkflowRun)(nil)

// WorkflowRun is one execution (WorkflowID + RunID) of a workflow — the run-precise entity, a child
// of Workflow (by id). Modeling the run distinctly is what lets multiple runs of one WorkflowID
// (continue-as-new / retry / reset) be tracked separately, and it records the run's lineage
// (FirstRunID = chain root, PreviousRunID = predecessor) so the run graph can be reconstructed. Its
// lifecycle is created→started→a typed close outcome. Start and close observations carry event
// time and RunID; successor starts also carry the lineage edge and successor identity.
type WorkflowRun struct {
	WorkflowID     string
	RunID          string
	FirstRunID     string
	PreviousRunID  string
	Initiator      string // how this run was created (the typed edge from PreviousRunID)
	FSM            *umpire.Lifecycle
	StartedAt      time.Time
	CompletedAt    time.Time
	ClosedAt       time.Time
	CloseOutcome   string
	SuccessorRunID string
	LastSeenAt     time.Time
}

func NewWorkflowRun() *WorkflowRun {
	r := &WorkflowRun{}
	r.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: WorkflowRunCreated,
		States: umpire.States{
			WorkflowRunCreated:        {},
			WorkflowRunStarted:        {},
			WorkflowRunCompleted:      {umpire.Success},
			WorkflowRunFailed:         {umpire.Failure},
			WorkflowRunCanceled:       {},
			WorkflowRunTerminated:     {},
			WorkflowRunTimedOut:       {umpire.Failure},
			WorkflowRunContinuedAsNew: {},
		},
		Transitions: []umpire.Transition{
			{
				Event: WorkflowRunStart,
				From:  []string{WorkflowRunCreated},
				To:    WorkflowRunStarted,
			},
			{Event: WorkflowRunFail, From: []string{WorkflowRunStarted}, To: WorkflowRunFailed},
			{Event: WorkflowRunCancel, From: []string{WorkflowRunStarted}, To: WorkflowRunCanceled},
			{Event: WorkflowRunTerminate, From: []string{WorkflowRunStarted}, To: WorkflowRunTerminated},
			{Event: WorkflowRunTimeout, From: []string{WorkflowRunStarted}, To: WorkflowRunTimedOut},
			{
				Event: WorkflowRunComplete,
				From:  []string{WorkflowRunStarted},
				To:    WorkflowRunCompleted,
			},
			{
				Event: WorkflowRunContinueAsNew,
				From:  []string{WorkflowRunStarted},
				To:    WorkflowRunContinuedAsNew,
			},
		},
	})
	return r
}

func (r *WorkflowRun) Type() umpire.EntityType { return WorkflowRunType }

func (r *WorkflowRun) Lifecycle() *umpire.Lifecycle { return r.FSM }

func (r *WorkflowRun) OnFact(ctx context.Context, _ *umpire.EntityPath, facts iter.Seq[umpire.Fact]) error {
	for f := range facts {
		switch e := f.(type) {
		case *fact.WorkflowRunStarted:
			if r.WorkflowID == "" {
				r.WorkflowID = e.WorkflowID
				r.RunID = e.RunID
			}
			r.FirstRunID = e.FirstRunID
			r.PreviousRunID = e.PreviousRunID
			r.Initiator = e.Initiator
			if r.FSM.FireAt(ctx, WorkflowRunStart, e.EventTime()) {
				r.StartedAt = eventTimeOrNow(e.EventTime())
			}
			r.LastSeenAt = time.Now()
		case *fact.WorkflowRunCompleted:
			if r.WorkflowID == "" {
				r.WorkflowID = e.WorkflowID
				r.RunID = e.RunID
			}
			if r.FSM.Fire(ctx, WorkflowRunComplete) {
				r.CompletedAt = time.Now()
				r.ClosedAt = r.CompletedAt
				r.CloseOutcome = factOutcomeCompleted
			}
			r.LastSeenAt = time.Now()
		case *fact.WorkflowRunContinuedAsNew:
			if r.WorkflowID == "" {
				r.WorkflowID = e.WorkflowID
				r.RunID = e.RunID
			}
			if r.FSM.Fire(ctx, WorkflowRunContinueAsNew) {
				r.ClosedAt = time.Now()
				r.CloseOutcome = "continued_as_new"
			}
			r.LastSeenAt = time.Now()
		case *fact.WorkflowRunClosed:
			if r.WorkflowID == "" {
				r.WorkflowID = e.WorkflowID
				r.RunID = e.RunID
			}
			event := workflowCloseTransition(e.Outcome)
			if r.CloseOutcome != "" && r.CloseOutcome != e.Outcome {
				r.FSM.RecordIllegalAt(event, e.EventTime())
				r.LastSeenAt = time.Now()
				continue
			}
			if r.FSM.FireAt(ctx, event, e.EventTime()) {
				r.ClosedAt = eventTimeOrNow(e.EventTime())
				r.CloseOutcome = e.Outcome
				r.SuccessorRunID = e.SuccessorRunID
				if e.Outcome == factOutcomeCompleted {
					r.CompletedAt = r.ClosedAt
				}
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
	WorkflowRunCreated        WorkflowState = "created"
	WorkflowRunStarted        WorkflowState = "started"
	WorkflowRunCompleted      WorkflowState = "completed"
	WorkflowRunFailed         WorkflowState = "failed"
	WorkflowRunCanceled       WorkflowState = "canceled"
	WorkflowRunTerminated     WorkflowState = "terminated"
	WorkflowRunTimedOut       WorkflowState = "timed_out"
	WorkflowRunContinuedAsNew WorkflowState = "continued_as_new"

	WorkflowRunStart         WorkflowEvent = "start"
	WorkflowRunComplete      WorkflowEvent = "complete"
	WorkflowRunFail          WorkflowEvent = "fail"
	WorkflowRunCancel        WorkflowEvent = "cancel"
	WorkflowRunTerminate     WorkflowEvent = "terminate"
	WorkflowRunTimeout       WorkflowEvent = "timeout"
	WorkflowRunContinueAsNew WorkflowEvent = "continue_as_new"
)
