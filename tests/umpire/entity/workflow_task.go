package entity

import (
	"context"
	"fmt"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
)

const WorkflowTaskType = fact.WorkflowTaskType

type WorkflowTaskID struct {
	parent WorkflowID
	id     string
}

func (t WorkflowTaskID) String() string {
	return t.parent.String() + "/task:" + t.id
}

var _ umpire.Entity = (*WorkflowTask)(nil)
var _ umpire.Lifecycled = (*WorkflowTask)(nil)

// WorkflowTask represents a workflow task entity with live Markers.
type WorkflowTask struct {
	TaskQueue     string
	WorkflowID    string
	RunID         string
	NamespaceID   string
	FSM           *umpire.Lifecycle
	AddedAt       time.Time
	PolledAt      time.Time
	StoredAt      time.Time
	IsSpeculative bool
	ScheduledAt   time.Time

	// Live Flags — set by FSM transitions.
	Added  *umpire.Flag
	Polled *umpire.Flag
	Stored *umpire.Flag
}

func NewWorkflowTask() *WorkflowTask {
	wt := &WorkflowTask{
		Added:  umpire.NewFlag("WorkflowTask:Added"),
		Polled: umpire.NewFlag("WorkflowTask:Polled"),
		Stored: umpire.NewFlag("WorkflowTask:Stored"),
	}
	wt.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: "created",
		Transitions: []umpire.Transition{
			{Event: "add", From: []string{"created"}, To: "added"},
			// poll: task delivered to worker — valid from either "added" (sync match)
			// or "stored" (async match after DB persistence).
			{Event: "poll", From: []string{"added", "stored"}, To: "polled"},
			{Event: "store", From: []string{"added"}, To: "stored"},
			// discard: task expired or invalidated in matching before being polled.
			{Event: "discard", From: []string{"added", "stored"}, To: "discarded"},
			// terminate: parent workflow reached a terminal state; task is no longer needed.
			{Event: "terminate", From: []string{"created", "added", "stored"}, To: "terminated"},
		},
		// Task progress (added/stored → polled) is checked by WorkflowTaskStarvation,
		// which excludes speculative tasks; not modelled as a generic must-progress here.
	})
	return wt
}

func (wt *WorkflowTask) Type() umpire.EntityType {
	return WorkflowTaskType
}

// Lifecycle exposes the task's state machine to generic lifecycle rules.
func (wt *WorkflowTask) Lifecycle() *umpire.Lifecycle {
	return wt.FSM
}

func (wt *WorkflowTask) OnFact(ctx context.Context, path *umpire.EntityPath, events iter.Seq[umpire.Fact]) error {
	if wt.NamespaceID == "" && path != nil {
		if root := path.Root(); root.Type == NamespaceType {
			wt.NamespaceID = root.ID
		}
	}
	for ev := range events {
		switch e := ev.(type) {
		case *fact.WorkflowTaskAdded:
			if wt.TaskQueue == "" {
				wt.TaskQueue = e.Request.GetTaskQueue().GetName()
				wt.WorkflowID = e.Request.GetExecution().GetWorkflowId()
				wt.RunID = e.Request.GetExecution().GetRunId()
			}
			if wt.FSM.Fire(ctx, "add") {
				wt.AddedAt = time.Now()
				wt.Added.Set()
			}
		case *fact.WorkflowTaskPolled:
			if e.TaskReturned && wt.FSM.Fire(ctx, "poll") {
				wt.PolledAt = time.Now()
				wt.Polled.Set()
			}
		case *fact.WorkflowTaskStored:
			if wt.FSM.Fire(ctx, "store") {
				wt.StoredAt = time.Now()
				wt.Stored.Set()
			}
		case *fact.WorkflowTaskDiscarded:
			// Best-effort settle: use the guarded form so discarding an already
			// terminal task is a no-op rather than a recorded illegal transition.
			if wt.FSM.Can("discard") {
				_ = wt.FSM.Event(ctx, "discard")
			}
		case *fact.WorkflowTerminated:
			// Best-effort settle (broadcast to every task); guarded on purpose.
			if wt.WorkflowID == e.WorkflowID && wt.NamespaceID == e.NamespaceID && wt.FSM.Can("terminate") {
				_ = wt.FSM.Event(ctx, "terminate")
			}
		case *fact.SpeculativeWorkflowTaskScheduled:
			if wt.TaskQueue == "" {
				wt.TaskQueue = e.TaskQueue
				wt.WorkflowID = e.WorkflowID
				wt.RunID = e.RunID
			}
			wt.IsSpeculative = true
			wt.ScheduledAt = time.Now()
			if wt.FSM.Fire(ctx, "add") {
				wt.AddedAt = wt.ScheduledAt
				wt.Added.Set()
			}
		}
	}
	return nil
}

func (wt *WorkflowTask) String() string {
	return fmt.Sprintf("WorkflowTask{taskQueue=%s, workflow=%s:%s, state=%s}",
		wt.TaskQueue, wt.WorkflowID, wt.RunID, wt.FSM.Current())
}
