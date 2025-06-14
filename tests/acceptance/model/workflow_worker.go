package model

import (
	"go.temporal.io/server/common/testing/stamp"
)

type (
	WorkflowWorker struct {
		stamp.Model[*WorkflowWorker]
		stamp.Scope[*TaskQueue]
	}
	NewWorkflowWorker struct {
		TaskQueue *TaskQueue
		Name      stamp.ID
	}
)

func (w *WorkflowWorker) Verify() {}
