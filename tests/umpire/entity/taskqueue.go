package entity

import (
	"context"
	"iter"
	"time"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
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

func (tq *TaskQueue) OnFact(_ context.Context, _ *umpire.Identity, events iter.Seq[umpire.Fact]) error {
	for ev := range events {
		switch e := ev.(type) {
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
