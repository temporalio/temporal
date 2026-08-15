package model

import (
	"context"
	"fmt"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/fact"
)

const WorkflowType = fact.WorkflowType

type WorkflowID struct {
	parent NamespaceID
	id     string
}

func (w WorkflowID) String() string {
	return w.parent.String() + "/workflow:" + w.id
}

func (w WorkflowID) Execution(runID string) ExecutionID {
	return ExecutionID{parent: w, id: runID}
}

func (w WorkflowID) Task(taskID string) WorkflowTaskID {
	return WorkflowTaskID{parent: w, id: taskID}
}

type ExecutionID struct {
	parent WorkflowID
	id     string
}

func (e ExecutionID) String() string {
	return e.parent.String() + "/execution:" + e.id
}

var _ umpire.Entity = (*Workflow)(nil)
var _ umpire.Lifecycled = (*Workflow)(nil)

// Workflow represents a workflow execution entity with live Markers.
type Workflow struct {
	WorkflowID   string
	NamespaceID  string
	FSM          *umpire.Lifecycle
	StartedAt    time.Time
	CompletedAt  time.Time
	ClosedAt     time.Time
	CloseOutcome string
	LastSeenAt   time.Time
}

func NewWorkflow() *Workflow {
	wf := &Workflow{}
	wf.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: WorkflowCreated,
		// A started workflow must eventually close: EntityProgress flags one left
		// in WorkflowStarted at teardown (workflow-completion liveness). Benign in-flight
		// closes are settled by the observed-close signal, not teardown timing.
		States: umpire.States{
			WorkflowCreated:    {},
			WorkflowStarted:    {umpire.MustProgress},
			WorkflowCompleted:  {umpire.Success},
			WorkflowFailed:     {umpire.Failure},
			WorkflowCanceled:   {},
			WorkflowTerminated: {},
			WorkflowTimedOut:   {umpire.Failure},
		},
		Transitions: []umpire.Transition{
			{
				Event: WorkflowStart,
				From:  []string{WorkflowCreated},
				To:    WorkflowStarted,
			},
			{
				Event: WorkflowComplete,
				From:  []string{WorkflowStarted},
				To:    WorkflowCompleted,
			},
			{Event: WorkflowFail, From: []string{WorkflowStarted}, To: WorkflowFailed},
			{Event: WorkflowCancel, From: []string{WorkflowStarted}, To: WorkflowCanceled},
			{Event: WorkflowTerminate, From: []string{WorkflowStarted}, To: WorkflowTerminated},
			{Event: WorkflowTimeout, From: []string{WorkflowStarted}, To: WorkflowTimedOut},
		},
	})
	return wf
}

func (wf *Workflow) Type() umpire.EntityType {
	return WorkflowType
}

// Lifecycle exposes the workflow's state machine to generic lifecycle rules.
func (wf *Workflow) Lifecycle() *umpire.Lifecycle {
	return wf.FSM
}

func (wf *Workflow) OnFact(ctx context.Context, _ *umpire.EntityPath, facts iter.Seq[umpire.Fact]) error {
	for f := range facts {
		switch e := f.(type) {
		case *fact.WorkflowStarted:
			if wf.WorkflowID == "" {
				wf.WorkflowID = e.Request.GetStartRequest().GetWorkflowId()
				wf.NamespaceID = e.Request.GetNamespaceId()
			}
			if wf.FSM.Fire(ctx, WorkflowStart) {
				wf.StartedAt = time.Now()
			}
			wf.LastSeenAt = time.Now()
		case *fact.WorkflowExecutionCompleted:
			if wf.WorkflowID == "" {
				wf.WorkflowID = e.WorkflowID
			}
			if wf.FSM.Fire(ctx, WorkflowComplete) {
				wf.CompletedAt = time.Now()
				wf.ClosedAt = wf.CompletedAt
				wf.CloseOutcome = factOutcomeCompleted
			}
			wf.LastSeenAt = time.Now()
		case *fact.WorkflowExecutionClosed:
			if wf.WorkflowID == "" {
				wf.WorkflowID = e.WorkflowID
				wf.NamespaceID = e.NamespaceID
			}
			if e.SuccessorRunID == "" {
				event := workflowCloseTransition(e.Outcome)
				if wf.FSM.FireAt(ctx, event, e.EventTime()) {
					wf.ClosedAt = eventTimeOrNow(e.EventTime())
					wf.CloseOutcome = e.Outcome
					if e.Outcome == factOutcomeCompleted {
						wf.CompletedAt = wf.ClosedAt
					}
				}
			}
			wf.LastSeenAt = time.Now()
		}
	}
	return nil
}

func (wf *Workflow) String() string {
	return fmt.Sprintf("Workflow{workflowID=%s, state=%s}", wf.WorkflowID, wf.FSM.Current())
}

// Lifecycle states and facts for Workflow. Aliased to string so they drop into the
// generic Lifecycle/planner APIs while giving named, typo-checked labels.
type (
	WorkflowState = string
	WorkflowEvent = string
)

const (
	WorkflowCreated    WorkflowState = "created"
	WorkflowStarted    WorkflowState = "started"
	WorkflowCompleted  WorkflowState = "completed"
	WorkflowFailed     WorkflowState = "failed"
	WorkflowCanceled   WorkflowState = "canceled"
	WorkflowTerminated WorkflowState = "terminated"
	WorkflowTimedOut   WorkflowState = "timed_out"

	WorkflowStart     WorkflowEvent = "start"
	WorkflowComplete  WorkflowEvent = "complete"
	WorkflowFail      WorkflowEvent = "fail"
	WorkflowCancel    WorkflowEvent = "cancel"
	WorkflowTerminate WorkflowEvent = "terminate"
	WorkflowTimeout   WorkflowEvent = "timeout"
)
