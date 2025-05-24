package model

import (
	"go.temporal.io/server/common/testing/stamp"
)

// TODO: need to consider task queue type too
type (
	TaskQueue struct {
		stamp.Model[*TaskQueue]
		stamp.Scope[*Namespace]
	}
	NewTaskQueue struct {
		Namespace *Namespace
		Name      stamp.ID
	}
)

func (t *TaskQueue) GetNamespace() *Namespace {
	return t.GetScope()
}

func (t *TaskQueue) Verify() {}
