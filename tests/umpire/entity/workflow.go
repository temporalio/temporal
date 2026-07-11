package entity

import (
	"context"
	"fmt"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
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

func (w WorkflowID) Update(updateID string) WorkflowUpdateID {
	return WorkflowUpdateID{parent: w, id: updateID}
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
	WorkflowID  string
	NamespaceID string
	FSM         *umpire.Lifecycle
	StartedAt   time.Time
	CompletedAt time.Time
	LastSeenAt  time.Time

	// Live Flags
	Running     *umpire.Flag
	WfCompleted *umpire.Flag
}

func NewWorkflow() *Workflow {
	wf := &Workflow{
		Running:     umpire.NewFlag("Workflow:Running"),
		WfCompleted: umpire.NewFlag("Workflow:Completed"),
	}
	wf.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: "created",
		Transitions: []umpire.Transition{
			{Event: "start", From: []string{"created"}, To: "started"},
			{Event: "complete", From: []string{"started"}, To: "completed"},
		},
		// "started" is not marked must-progress: not all close paths are observed
		// yet (only CompleteWorkflowExecution), so requiring completion would
		// false-positive. See the close-signal gap in UMPIRE_PLAN.md.
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

func (wf *Workflow) OnFact(ctx context.Context, _ *umpire.EntityPath, events iter.Seq[umpire.Fact]) error {
	for ev := range events {
		switch e := ev.(type) {
		case *fact.WorkflowStarted:
			if wf.WorkflowID == "" {
				wf.WorkflowID = e.Request.GetStartRequest().GetWorkflowId()
				wf.NamespaceID = e.Request.GetNamespaceId()
			}
			if wf.FSM.Fire(ctx, "start") {
				wf.StartedAt = time.Now()
				wf.Running.Set()
			}
			wf.LastSeenAt = time.Now()
		case *fact.WorkflowExecutionCompleted:
			if wf.WorkflowID == "" {
				wf.WorkflowID = e.WorkflowID
			}
			if wf.FSM.Fire(ctx, "complete") {
				wf.CompletedAt = time.Now()
				wf.Running.Clear()
				wf.WfCompleted.Set()
			}
			wf.LastSeenAt = time.Now()
		}
	}
	return nil
}

func (wf *Workflow) String() string {
	return fmt.Sprintf("Workflow{workflowID=%s, state=%s}", wf.WorkflowID, wf.FSM.Current())
}
