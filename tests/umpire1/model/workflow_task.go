package model

import (
	"context"
	"fmt"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire1/fact"
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
}

func NewWorkflowTask() *WorkflowTask {
	wt := &WorkflowTask{}
	wt.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
		Initial: TaskCreated,
		Transitions: []umpire.Transition{
			{
				Event: TaskAdd,
				From:  []string{TaskCreated},
				To:    TaskAdded,
			},
			// poll: task delivered to worker — valid from either TaskAdded (sync match)
			// or TaskStored (async match after DB persistence).
			{
				Event: TaskPoll,
				From:  []string{TaskAdded, TaskStored},
				To:    TaskPolled,
			},
			{
				Event: TaskStore,
				From:  []string{TaskAdded},
				To:    TaskStored,
			},
			// discard: task expired or invalidated in matching before being polled.
			{
				Event: TaskDiscard,
				From:  []string{TaskAdded, TaskStored},
				To:    TaskDiscarded,
			},
			// terminate: parent workflow reached a terminal state; task is no longer needed.
			{
				Event: TaskTerminate,
				From:  []string{TaskCreated, TaskAdded, TaskStored},
				To:    TaskTerminated,
			},
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

func (wt *WorkflowTask) OnFact(ctx context.Context, path *umpire.EntityPath, facts iter.Seq[umpire.Fact]) error {
	if wt.NamespaceID == "" && path != nil {
		if root := path.Root(); root.Type == NamespaceType {
			wt.NamespaceID = root.ID
		}
	}
	for f := range facts {
		switch e := f.(type) {
		case *fact.WorkflowTaskAdded:
			if wt.TaskQueue == "" {
				wt.TaskQueue = e.Request.GetTaskQueue().GetName()
				wt.WorkflowID = e.Request.GetExecution().GetWorkflowId()
				wt.RunID = e.Request.GetExecution().GetRunId()
			}
			if wt.FSM.Fire(ctx, TaskAdd) {
				wt.AddedAt = time.Now()
			}
		case *fact.WorkflowTaskPolled:
			if e.TaskReturned && wt.FSM.Fire(ctx, TaskPoll) {
				wt.PolledAt = time.Now()
			}
		case *fact.WorkflowTaskStored:
			if wt.FSM.Fire(ctx, TaskStore) {
				wt.StoredAt = time.Now()
			}
		case *fact.WorkflowTaskDiscarded:
			// Best-effort settle: use the guarded form so discarding an already
			// terminal task is a no-op rather than a recorded illegal transition.
			if wt.FSM.Can(TaskDiscard) {
				_ = wt.FSM.Event(ctx, TaskDiscard)
			}
		case *fact.WorkflowTerminated:
			// Best-effort settle (broadcast to every task); guarded on purpose.
			if wt.WorkflowID == e.WorkflowID && wt.NamespaceID == e.NamespaceID && wt.FSM.Can(TaskTerminate) {
				_ = wt.FSM.Event(ctx, TaskTerminate)
			}
		case *fact.SpeculativeWorkflowTaskScheduled:
			if wt.TaskQueue == "" {
				wt.TaskQueue = e.TaskQueue
				wt.WorkflowID = e.WorkflowID
				wt.RunID = e.RunID
			}
			wt.IsSpeculative = true
			wt.ScheduledAt = time.Now()
			if wt.FSM.Fire(ctx, TaskAdd) {
				wt.AddedAt = wt.ScheduledAt
			}
		}
	}
	return nil
}

func (wt *WorkflowTask) String() string {
	return fmt.Sprintf("WorkflowTask{taskQueue=%s, workflow=%s:%s, state=%s}",
		wt.TaskQueue, wt.WorkflowID, wt.RunID, wt.FSM.Current())
}

// Lifecycle states and facts for WorkflowTask (aliased to string; see Workflow).
type (
	TaskState = string
	TaskEvent = string
)

const (
	TaskCreated    TaskState = "created"
	TaskAdded      TaskState = "added"
	TaskPolled     TaskState = "polled"
	TaskStored     TaskState = "stored"
	TaskDiscarded  TaskState = "discarded"
	TaskTerminated TaskState = "terminated"

	TaskAdd       TaskEvent = "add"
	TaskPoll      TaskEvent = "poll"
	TaskStore     TaskEvent = "store"
	TaskDiscard   TaskEvent = "discard"
	TaskTerminate TaskEvent = "terminate"
)
