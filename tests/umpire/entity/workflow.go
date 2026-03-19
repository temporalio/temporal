package entity

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/looplab/fsm"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

const (
	WorkflowType          = fact.WorkflowType
	WorkflowExecutionType = fact.WorkflowExecutionType
)

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

// Workflow represents a workflow execution entity with live Markers.
type Workflow struct {
	WorkflowID  string
	NamespaceID string
	FSM         *fsm.FSM
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
	wf.FSM = fsm.NewFSM(
		"created",
		fsm.Events{
			{Name: "start", Src: []string{"created"}, Dst: "started"},
			{Name: "complete", Src: []string{"started"}, Dst: "completed"},
		},
		fsm.Callbacks{},
	)
	return wf
}

func (wf *Workflow) Type() umpire.EntityType {
	return WorkflowType
}

func (wf *Workflow) OnFact(ctx context.Context, _ *umpire.Identity, events iter.Seq[umpire.Fact]) error {
	for ev := range events {
		switch e := ev.(type) {
		case *fact.WorkflowStarted:
			if wf.WorkflowID == "" {
				wf.WorkflowID = e.Request.GetStartRequest().GetWorkflowId()
				wf.NamespaceID = e.Request.GetNamespaceId()
			}
			if wf.FSM.Can("start") {
				_ = wf.FSM.Event(ctx, "start")
				wf.StartedAt = time.Now()
				wf.Running.Set()
			}
			wf.LastSeenAt = time.Now()
		case *fact.WorkflowTaskCompleted:
			_ = e
			if wf.FSM.Can("complete") {
				_ = wf.FSM.Event(ctx, "complete")
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
