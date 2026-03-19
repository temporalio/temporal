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

const WorkflowTaskType = fact.WorkflowTaskType

type WorkflowTaskID struct {
	parent WorkflowID
	id     string
}

func (t WorkflowTaskID) String() string {
	return t.parent.String() + "/task:" + t.id
}

var _ umpire.Entity = (*WorkflowTask)(nil)

// WorkflowTask represents a workflow task entity with live Markers.
type WorkflowTask struct {
	TaskQueue     string
	WorkflowID    string
	RunID         string
	FSM           *fsm.FSM
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
	wt.FSM = fsm.NewFSM(
		"created",
		fsm.Events{
			{Name: "add", Src: []string{"created"}, Dst: "added"},
			// poll: task delivered to worker — valid from either "added" (sync match)
			// or "stored" (async match after DB persistence).
			{Name: "poll", Src: []string{"added", "stored"}, Dst: "polled"},
			{Name: "store", Src: []string{"added"}, Dst: "stored"},
			// discard: task expired or invalidated in matching before being polled.
			{Name: "discard", Src: []string{"added", "stored"}, Dst: "discarded"},
			// terminate: parent workflow reached a terminal state; task is no longer needed.
			{Name: "terminate", Src: []string{"created", "added", "stored"}, Dst: "terminated"},
		},
		fsm.Callbacks{},
	)
	return wt
}

func (wt *WorkflowTask) Type() umpire.EntityType {
	return WorkflowTaskType
}

func (wt *WorkflowTask) OnFact(ctx context.Context, _ *umpire.Identity, events iter.Seq[umpire.Fact]) error {
	for ev := range events {
		switch e := ev.(type) {
		case *fact.WorkflowTaskAdded:
			if wt.TaskQueue == "" {
				wt.TaskQueue = e.Request.GetTaskQueue().GetName()
				wt.WorkflowID = e.Request.GetExecution().GetWorkflowId()
				wt.RunID = e.Request.GetExecution().GetRunId()
			}
			if wt.FSM.Can("add") {
				_ = wt.FSM.Event(ctx, "add")
				wt.AddedAt = time.Now()
				wt.Added.Set()
			}
		case *fact.WorkflowTaskPolled:
			if wt.FSM.Can("poll") && e.TaskReturned {
				_ = wt.FSM.Event(ctx, "poll")
				wt.PolledAt = time.Now()
				wt.Polled.Set()
			}
		case *fact.WorkflowTaskStored:
			if wt.FSM.Can("store") {
				_ = wt.FSM.Event(ctx, "store")
				wt.StoredAt = time.Now()
				wt.Stored.Set()
			}
		case *fact.WorkflowTaskDiscarded:
			if wt.FSM.Can("discard") {
				_ = wt.FSM.Event(ctx, "discard")
			}
		case *fact.WorkflowTerminated:
			if wt.WorkflowID == e.WorkflowID && wt.FSM.Can("terminate") {
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
			if wt.FSM.Can("add") {
				_ = wt.FSM.Event(ctx, "add")
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
