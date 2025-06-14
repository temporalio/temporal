package model

import (
	"go.temporal.io/server/common/testing/stamp"
)

type (
	WorkflowExecutionHistory struct {
		stamp.Model[*WorkflowExecutionHistory]
		stamp.Scope[*WorkflowExecution]
	}
)

func (w *WorkflowExecutionHistory) GetNamespace() *Namespace {
	return w.GetScope().GetNamespace()
}

func (w *WorkflowExecutionHistory) Verify() {}
