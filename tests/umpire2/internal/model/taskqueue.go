package model

import (
	"context"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/fact"
)

const TaskQueueType = fact.TaskQueueType

type TaskQueueID struct {
	namespace NamespaceID
	name      string
}

func (t TaskQueueID) String() string {
	return t.namespace.String() + "/taskqueue:" + t.name
}

var _ umpire.Entity = (*TaskQueue)(nil)

// TaskQueue represents a task queue entity.
type TaskQueue struct {
	Name              string
	LastEmptyPollTime time.Time
}

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{}
}

func (tq *TaskQueue) Type() umpire.EntityType {
	return TaskQueueType
}

func (tq *TaskQueue) OnFact(_ context.Context, _ *umpire.EntityPath, facts iter.Seq[umpire.Fact]) error {
	for f := range facts {
		switch e := f.(type) {
		case *fact.WorkflowTaskPolled:
			if tq.Name == "" {
				tq.Name = e.Request.GetPollRequest().GetTaskQueue().GetName()
			}
			if !e.TaskReturned {
				tq.LastEmptyPollTime = time.Now()
			}
		}
	}
	return nil
}
